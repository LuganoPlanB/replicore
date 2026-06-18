import { createHash, randomBytes } from "node:crypto"
import { mkdir } from "node:fs/promises"
import { join } from "node:path"

import Corestore from "corestore"
import Hyperbee from "hyperbee"
import Hypercore from "hypercore"

import { canonicalize } from "./canonical.js"
import { ConsensusStateStore } from "./consensus-state.js"
import { keyIdFromPublicKey, signPayload, verifyPayload } from "./crypto.js"
import { DurabilityWaiter } from "./durability-waiter.js"
import { JoinControl } from "./join-control.js"
import {
  createSignedOperation,
  decryptOperationValue,
  validateLogLink,
  validateOperation,
  verifySignedOperation
} from "./operation.js"
import { MaterializedView } from "./materialized-view.js"
import { NodeRpcRouter } from "./node-rpc.js"
import { buildLeaderStatus, buildNodeStatus, buildReplicationStatus, buildWritersStatus } from "./node-status.js"
import { validatePromotionCredential } from "./promotion-credential.js"
import { ConsensusEngine, majoritySize } from "./raft-engine.js"
import { SwarmNetwork } from "./swarm-network.js"
import { deriveTopic } from "./config.js"
import { deriveJoinKeyPair, resolveTransportIdentity } from "./transport-identity.js"

const JOIN_REQUEST_MAX_SKEW_MS = 5 * 60 * 1000

/**
 * Minimal multi-node swarm whose committed cluster history is represented by
 * one authoritative leader log.
 *
 * During the storage transition, that authoritative log is modeled as the
 * current leader's Hypercore feed. Other per-node feeds still exist as
 * replication scaffolding, but they are not the authoritative CRUD history.
 */
export class HolepunchSwarmNode {
  /**
   * @param {{
   *   dataDir: string,
   *   clusterId: string,
   *   clusterSecret?: Buffer,
   *   role?: "voter" | "learner",
   *   machineId?: string,
   *   topicSalt?: string,
   *   identity: { publicKeyId: string, publicKey: Buffer, secretKey: Buffer, feedKey: string },
   *   authorizedNodes: Array<{ nodeId: string, publicKey: Buffer, feedKey: string }>,
   *   membership?: { version?: number, voters?: string[], learners?: string[], removed?: string[] },
   *   revokedNodeIds?: string[],
   *   encryption?: { currentKeyId: string, keys: Record<string, Buffer> },
   *   encryptionKey?: Buffer,
   *   bootstrap?: Array<string | { host: string, port: number }>,
   *   heartbeatIntervalMs?: number,
   *   heartbeatTtlMs?: number,
   *   electionTimeoutMinMs?: number,
   *   electionTimeoutMaxMs?: number,
   *   requestTimeoutMs?: number,
   *   maxInflightReplication?: number,
   *   electionTimeoutSeed?: string | null,
   *   forwarding?: boolean,
   *   ackDelayMs?: number,
   *   networkPolicy?: { allowedNodeIds?: string[], allowConnection?: (localNodeId: string, remoteNodeId: string) => boolean },
   *   durability?: { requiredFollowerAcks?: number, timeoutMs?: number }
   * }} options
   */
  constructor(options) {
    this.options = {
      heartbeatIntervalMs: 500,
      heartbeatTtlMs: 3000,
      electionTimeoutMinMs: 900,
      electionTimeoutMaxMs: 1500,
      requestTimeoutMs: options.durability?.timeoutMs ?? 5000,
      maxInflightReplication: 16,
      electionTimeoutSeed: null,
      forwarding: true,
      durability: {
        requiredFollowerAcks: 1,
        timeoutMs: 5000,
        ...options.durability
      },
      ...options,
      authorizedNodes: (options.authorizedNodes ?? []).map((node) => ({
        nodeId: node.nodeId,
        publicKey: Buffer.from(node.publicKey),
        feedKey: node.feedKey
      }))
    }
    this.revokedNodeIds = new Set(this.options.revokedNodeIds ?? [])
    this.encryption = this.options.encryption ?? {
      currentKeyId: "default",
      keys: { default: this.options.encryptionKey }
    }
    this.store = null
    this.consensusBee = null
    this.consensusStateStore = null
    this.consensusState = null
    this.membershipState = null
    this.network = null
    this.rpc = null
    this.joinControl = null
    this.transportIdentity = null
    this.viewBee = null
    this.view = null
    this.feedCores = new Map()
    this.heartbeatTimer = null
    this.heartbeatPromise = null
    this.electionTimer = null
    this.syncPromises = new Map()
    this.pendingSync = new Set()
    this.localAppendLock = Promise.resolve()
    this.durabilityWaiter = new DurabilityWaiter({
      feedKey: this.options.identity.feedKey,
      timeoutMs: this.options.durability.timeoutMs,
      requiredFollowerAcks: this.options.durability.requiredFollowerAcks
    })
    this.joinState = {
      accepted: false,
      leaderNodeId: null
    }
    this.sentJoinRequestKeys = new Set()
    this.seenJoinRequestNonces = new Set()
    this.lastHeartbeatByNode = new Map()
    this.splitState = {
      fenced: false,
      reason: null,
      leaderNodeId: null,
      since: null
    }
    this.consensusEngine = new ConsensusEngine({
      localNodeId: this.options.identity.publicKeyId
    })
    this.electionState = this.consensusEngine.state
    this.closing = false
  }

  async start() {
    await mkdir(this.options.dataDir, { recursive: true })
    await mkdir(join(this.options.dataDir, "corestore"), { recursive: true })
    this.transportIdentity = await resolveTransportIdentity({
      dataDir: this.options.dataDir,
      clusterSecret: this.options.clusterSecret,
      machineId: this.options.machineId
    })

    this.store = new Corestore(join(this.options.dataDir, "corestore"))
    await this.store.ready()

    const viewCore = this.store.get({ name: "derived-view" })
    this.viewBee = new Hyperbee(viewCore, { keyEncoding: "utf-8", valueEncoding: "json" })
    this.view = new MaterializedView(this.viewBee)
    await this.viewBee.ready()
    const consensusCore = this.store.get({ name: "consensus-state" })
    this.consensusBee = new Hyperbee(consensusCore, { keyEncoding: "utf-8", valueEncoding: "json" })
    await this.consensusBee.ready()
    this.consensusStateStore = new ConsensusStateStore(this.consensusBee)
    this.consensusState = await this.consensusStateStore.load()
    this.splitState = {
      fenced: this.consensusState.splitFenced === true,
      reason: this.consensusState.splitReason ?? null,
      leaderNodeId: this.consensusState.splitLeaderNodeId ?? null,
      since: null
    }
    this.membershipState = await this.#loadMembershipState()
    this.rpc = new NodeRpcRouter({
      localNodeId: this.options.identity.publicKeyId,
      timeoutMs: this.options.requestTimeoutMs,
      ackDelayMs: this.options.ackDelayMs,
      onPeerIdentity: (nodeId, peer) => this.network?.observePeerIdentity(nodeId, peer),
      onWriteRequest: async (message) => {
        if (this.currentLeader() !== this.options.identity.publicKeyId) {
          throw new Error("This node is not the current leader")
        }

        return message.request.action === "put"
          ? this.#appendKvOperation("put", message.request.key, message.request.value, message.request.options ?? {})
          : this.#appendKvOperation("delete", message.request.key, undefined, message.request.options ?? {})
      },
      onVoteRequest: async (message) => this.#handleVoteRequest(message),
      onWriteAck: (nodeId, seq) => this.#recordAck(nodeId, seq)
    })
    await this.#ensureFeedCore(this.#localNodeRecord())

    for (const node of this.options.authorizedNodes) {
      if (this.#isRevokedNode(node.nodeId) || node.nodeId === this.options.identity.publicKeyId) continue
      await this.#ensureFeedCore(node)
    }

    await this.#startNetworking()

    for (const node of this.options.authorizedNodes) {
      if (this.#isRevokedNode(node.nodeId)) continue
      if (node.nodeId === this.options.identity.publicKeyId) {
        await this.syncFeed(node.nodeId)
        continue
      }

      void this.syncFeed(node.nodeId).catch((error) => {
        if (!this.closing && error?.code !== "REQUEST_CANCELLED" && error?.code !== "SESSION_CLOSED") {
          throw error
        }
      })
    }

    if (!this.#isLearner()) {
      this.#scheduleElectionTimer()
      await this.#runHeartbeat()
      this.heartbeatTimer = setInterval(() => {
        void this.#runHeartbeat().catch((error) => {
          if (!this.closing && error?.code !== "SESSION_CLOSED") {
            throw error
          }
        })
      }, this.options.heartbeatIntervalMs)
      this.heartbeatTimer.unref?.()
    }
  }

  /**
   * Temporarily disconnect this node from the swarm without closing storage.
   * Intended for diagnostics and tests that need live isolation.
   */
  async suspendNetworking() {
    await this.network?.suspend()
  }

  /**
   * Rejoin the swarm after a temporary networking suspension.
   */
  async resumeNetworking() {
    if (this.closing) return

    await this.network?.resume()
    if (!this.#isLearner()) {
      await this.#runHeartbeat()
    }
  }

  /**
   * Test-only connection filter used by adverse-network fixtures.
   *
   * @param {{ allowedNodeIds?: string[], allowConnection?: (localNodeId: string, remoteNodeId: string) => boolean } | null} networkPolicy
   */
  async setNetworkPolicy(networkPolicy = null) {
    this.options.networkPolicy = networkPolicy
    this.network?.setPolicy(networkPolicy)
  }

  get status() {
    return buildNodeStatus({
      nodeId: this.options.identity.publicKeyId,
      role: this.#role(),
      leader: this.currentLeader(),
      knownHeartbeats: [...this.lastHeartbeatByNode.keys()],
      connections: this.network?.connectionCount ?? 0,
      encryptionKeyId: this.encryption.currentKeyId,
      feeds: Object.fromEntries(
        [...this.feedCores.entries()].map(([nodeId, core]) => [nodeId, core.length])
      )
    })
  }

  currentLeader() {
    return this.consensusEngine.currentLeader({
      isLearner: this.#isLearner(),
      heartbeatTtlMs: this.options.heartbeatTtlMs
    })
  }

  /**
   * @param {string} key
   * @param {unknown} value
   * @param {{ keyspace?: string, ttlMs?: number }} [options]
   */
  async put(key, value, options = {}) {
    if (this.#isLearner()) {
      throw this.#createLearnerWriteError()
    }
    if (this.#isSplitFenced()) {
      throw this.#createSplitFencedError()
    }
    if (this.currentLeader() !== this.options.identity.publicKeyId) {
      return this.#forwardWrite({ action: "put", key, value, options })
    }

    return this.#appendKvOperation("put", key, value, options)
  }

  /**
   * @param {string} key
   * @param {{ keyspace?: string, ttlMs?: number }} [options]
   */
  async delete(key, options = {}) {
    if (this.#isLearner()) {
      throw this.#createLearnerWriteError()
    }
    if (this.#isSplitFenced()) {
      throw this.#createSplitFencedError()
    }
    if (this.currentLeader() !== this.options.identity.publicKeyId) {
      return this.#forwardWrite({ action: "delete", key, options })
    }

    return this.#appendKvOperation("delete", key, undefined, options)
  }

  /**
   * @param {string} key
   * @param {{ keyspace?: string }} [options]
   */
  async get(key, options = {}) {
    const current = await this.view.getCurrent(options.keyspace ?? "default", key)
    if (!current) return null
    if (current.metadata.deleted) return { ...current.metadata, value: null }

    return {
      ...current.metadata,
      value: decryptOperationValue(
        {
          type: "put",
          value: current.encryptedValue
        },
        this.encryption.keys
      )
    }
  }

  /**
   * @param {string} key
   * @param {{ keyspace?: string }} [options]
   */
  async getHistory(key, options = {}) {
    return this.view.getHistory(options.keyspace ?? "default", key)
  }

  async getReplicationStatus() {
    const heartbeats = await this.view.getHeartbeats()
    const feeds = {}
    const now = Date.now()

    for (const node of this.options.authorizedNodes) {
      const status = await this.#feedReplicationStatus(node.nodeId, heartbeats, now)
      if (!status) continue
      feeds[node.nodeId] = status
    }

    const authoritativeLogNodeId = this.#authoritativeLogNodeId()
    const authoritativeLog = await this.getAuthoritativeLogStatus()
    const authoritativeReplication =
      feeds[authoritativeLogNodeId] ??
      (await this.#feedReplicationStatus(authoritativeLogNodeId, heartbeats, now))

    return buildReplicationStatus({
      nodeId: this.options.identity.publicKeyId,
      role: this.#role(),
      leader: this.currentLeader(),
      consensus: {
        currentTerm: this.consensusState.currentTerm,
        commitIndex: this.consensusState.commitIndex,
        lastApplied: this.consensusState.lastApplied,
        knownLeader: this.currentLeader() ?? this.#lastKnownLeaderId()
      },
      authoritativeLog: {
        ...authoritativeLog,
        applied: authoritativeReplication?.applied ?? 0,
        lag: authoritativeReplication?.lag ?? authoritativeLog.length,
        staged: authoritativeReplication?.staged ?? {
          count: 0,
          firstSeq: null,
          lastSeq: null,
          latestOpId: null,
          latestKey: null
        },
        connectedPeers: authoritativeReplication?.connectedPeers ?? 0,
        alive: authoritativeReplication?.alive ?? false,
        heartbeatAgeMs: authoritativeReplication?.heartbeatAgeMs ?? null
      },
      splitStatus: { ...this.splitState },
      connections: this.network?.connectionCount ?? 0,
      lastDurableSequence: this.durabilityWaiter.status().lastDurableSequence,
      encryptionKeyId: this.encryption.currentKeyId,
      knownPeerNodeIds: this.network?.knownPeerPublicKeys ?? [],
      membership: this.#membershipStatus(heartbeats),
      promotion: await this.#promotionStatus(),
      network: this.network?.networkStatus() ?? { policyActive: false, allowedNodeIds: [], peers: {} },
      readStatus: this.#readStatus(),
      feeds,
      heartbeats
    })
  }

  getWritersStatus() {
    return buildWritersStatus({
      role: this.#role(),
      currentLeader: this.currentLeader(),
      revokedNodeIds: [...this.revokedNodeIds],
      encryptionKeyId: this.encryption.currentKeyId,
      membershipFingerprint: this.#membershipFingerprint(),
      membership: this.#membershipEntries(),
      authorizedNodes: this.options.authorizedNodes.map((node) => ({
        nodeId: node.nodeId,
        feedKey: node.feedKey,
        role: this.#membershipRole(node.nodeId),
        revoked: this.#isRevokedNode(node.nodeId)
      }))
    })
  }

  async getLeaderStatus() {
    const leader = this.currentLeader() ?? this.#lastKnownLeaderId()
    const heartbeats = await this.view.getHeartbeats()

    return buildLeaderStatus({
      nodeId: this.options.identity.publicKeyId,
      role: this.#role(),
      currentLeader: leader,
      reachable: leader ? this.#isLeaderReachable(leader) : false,
      heartbeat: leader ? (heartbeats[leader] ?? null) : null
    })
  }

  async getConsensusState() {
    return { ...this.consensusState }
  }

  /**
   * Return the single authoritative Raft log descriptor that this node is
   * currently following.
   *
   * While the physical storage migration is still in progress, the
   * authoritative cluster log is the current leader feed, or the last known
   * leader feed while this node is fenced and reconnecting.
   */
  async getAuthoritativeLogStatus() {
    const node = this.#authoritativeLogRecord()
    const core = this.#authoritativeLogCore()

    return {
      nodeId: node.nodeId,
      feedKey: node.feedKey,
      length: core?.length ?? 0,
      term: this.consensusState.currentTerm,
      commitIndex: this.consensusState.commitIndex,
      lastApplied: this.consensusState.lastApplied
    }
  }

  async submitPromotionCredential(credential) {
    if (!this.#isLearner()) {
      throw new Error("Only learners may accept promotion credentials")
    }

    const summary = validatePromotionCredential(credential, {
      clusterId: this.options.clusterId,
      membershipVersion: this.membershipState.current.version,
      learnerNodeId: this.options.identity.publicKeyId,
      learnerNoisePublicKey: this.transportIdentity?.publicKeyHex ?? "",
      authorizedNodes: this.options.authorizedNodes,
      seenCredentialHashes: await this.#seenPromotionCredentialHashes(),
      seenNonces: await this.#seenPromotionNonces(),
      isCaughtUp: await this.#isPromotionEligible()
    })

    const batch = this.consensusBee.batch()
    await batch.put(`promotion/seen/hash/${summary.credentialHash}`, true)
    await batch.put(`promotion/seen/nonce/${credential.payload.nonce}`, true)
    await batch.put("promotion/current", {
      credentialHash: summary.credentialHash,
      targetRole: summary.targetRole,
      signerNodeId: summary.signerNodeId,
      learnerNodeId: summary.learnerNodeId,
      learnerNoisePublicKey: summary.learnerNoisePublicKey,
      expiresAt: summary.expiresAt,
      eligible: true,
      accepted: false
    })
    await batch.flush()

    return await this.#promotionStatus()
  }

  async commitPromotionCredential(credential) {
    if (this.currentLeader() !== this.options.identity.publicKeyId) {
      throw new Error("Only the current leader may commit membership promotions")
    }
    if (this.#jointMembership()) {
      throw new Error("Another membership change is already in progress")
    }

    const learnerNodeId = credential?.payload?.learnerNodeId
    const learnerRecord = this.options.authorizedNodes.find((node) => node.nodeId === learnerNodeId)
    if (!learnerRecord) {
      throw new Error("Promotion target must be a known learner")
    }
    if (this.#membershipRole(learnerNodeId) !== "learner") {
      throw new Error("Promotion target is not currently a learner")
    }

    const learnerNoisePublicKey = this.network?.peerPublicKeyForNodeId(learnerNodeId)
    if (!learnerNoisePublicKey) {
      throw new Error("Promotion target must be connected before promotion")
    }

    const summary = validatePromotionCredential(credential, {
      clusterId: this.options.clusterId,
      membershipVersion: this.membershipState.current.version,
      learnerNodeId,
      learnerNoisePublicKey,
      authorizedNodes: this.#voterNodes().map((node) => ({
        nodeId: node.nodeId,
        publicKey: node.publicKey
      })),
      seenCredentialHashes: await this.#seenPromotionCredentialHashes(),
      seenNonces: await this.#seenPromotionNonces(),
      isCaughtUp: await this.#isLearnerPromotionReady(learnerRecord.feedKey)
    })

    const oldVoters = this.membershipState.current.voters
    const newVoters = [...new Set([...oldVoters, learnerNodeId])].sort()
    const learners = this.membershipState.current.learners.filter((nodeId) => nodeId !== learnerNodeId).sort()
    const removed = this.membershipState.current.removed.filter((nodeId) => nodeId !== learnerNodeId).sort()

    await this.#commitMembershipChange({
      changeType: "promotion",
      targetNodeId: learnerNodeId,
      newVoters,
      learners,
      removed,
      promotion: {
        credentialHash: summary.credentialHash,
        targetRole: summary.targetRole,
        signerNodeId: summary.signerNodeId,
        learnerNodeId: summary.learnerNodeId,
        learnerNoisePublicKey: summary.learnerNoisePublicKey,
        expiresAt: summary.expiresAt,
        eligible: true,
        accepted: true
      }
    })

    return await this.#membershipStatusSnapshot()
  }

  async removeVoter(nodeId) {
    if (this.currentLeader() !== this.options.identity.publicKeyId) {
      throw new Error("Only the current leader may remove voters")
    }
    if (this.#jointMembership()) {
      throw new Error("Another membership change is already in progress")
    }
    if (nodeId === this.options.identity.publicKeyId) {
      throw new Error("Self-removal is not supported in this implementation")
    }
    if (this.#currentMembershipRole(nodeId) !== "voter") {
      throw new Error("Removal target is not currently a voter")
    }

    const newVoters = this.membershipState.current.voters.filter((entry) => entry !== nodeId).sort()
    const learners = this.membershipState.current.learners.filter((entry) => entry !== nodeId).sort()
    const removed = [...new Set([...this.membershipState.current.removed, nodeId])].sort()

    await this.#commitMembershipChange({
      changeType: "removal",
      targetNodeId: nodeId,
      newVoters,
      learners,
      removed
    })

    return await this.#membershipStatusSnapshot()
  }

  /**
   * Transitional hook for consensus implementation and restart-safety tests.
   *
   * @param {Partial<{ currentTerm: number, votedFor: string | null, commitIndex: number, lastApplied: number, membershipVersion: number }>} patch
   */
  async setConsensusState(patch) {
    this.consensusState = await this.consensusStateStore.save(patch)
    return this.getConsensusState()
  }

  async createSnapshot() {
    return this.view.exportSnapshot()
  }

  /**
   * @param {{ version: number, entries: Array<{ key: string, value: unknown }> }} snapshot
   */
  async restoreSnapshot(snapshot) {
    await this.view.importSnapshot(snapshot)
  }

  /**
   * Rotate future writes to a configured encryption key ID.
   *
   * @param {string} keyId
   */
  rotateEncryptionKey(keyId) {
    if (!this.encryption.keys[keyId]) {
      throw new Error(`Unknown encryption key ID: ${keyId}`)
    }
    this.encryption.currentKeyId = keyId
    return { keyId }
  }

  async close() {
    this.closing = true
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
    if (this.electionTimer) clearTimeout(this.electionTimer)
    await Promise.allSettled(this.heartbeatPromise ? [this.heartbeatPromise] : [])
    this.#rejectPendingWrites(new Error("Node is closing"))
    this.pendingSync.clear()
    await this.suspendNetworking()
    this.joinControl?.close()
    this.rpc?.close(new Error("Node is closing"))
    await Promise.allSettled([...this.feedCores.values()].map((core) => core.close()))
    await Promise.allSettled(this.syncPromises.values())
    this.network?.clear()
    if (this.consensusBee) await this.consensusBee.close()
    if (this.viewBee) await this.viewBee.close()
    if (this.store) await this.store.close()
  }

  /**
   * @param {string} nodeId
   */
  async syncFeed(nodeId) {
    if (this.closing && !this.syncPromises.has(nodeId)) {
      return
    }

    if (this.syncPromises.has(nodeId)) {
      this.pendingSync.add(nodeId)
      return this.syncPromises.get(nodeId)
    }

    const promise = this.#syncFeedLoop(nodeId)
    this.syncPromises.set(nodeId, promise)

    try {
      await promise
    } catch (error) {
      if (!this.closing && error?.code !== "REQUEST_CANCELLED" && error?.code !== "SESSION_CLOSED") {
        throw error
      }
    } finally {
      this.syncPromises.delete(nodeId)
      if (!this.closing && this.pendingSync.has(nodeId)) {
        this.pendingSync.delete(nodeId)
        await this.syncFeed(nodeId)
      }
    }
  }

  /**
   * @param {string} nodeId
   */
  async #syncFeedLoop(nodeId) {
    const node = this.#getAuthorizedNode(nodeId)
    const core = this.feedCores.get(nodeId)
    let rawApplied = await this.view.getRawApplied(node.feedKey)

    while (rawApplied < core.length) {
      if (this.closing) return

      const operation = await core.get(rawApplied)
      if (operation.seq !== rawApplied) {
        throw new Error(`Operation sequence mismatch at feed slot ${rawApplied} for ${nodeId}`)
      }
      validateOperation(operation, node, { revokedNodeIds: this.revokedNodeIds })
      if (!verifySignedOperation(operation, node.publicKey)) {
        throw new Error(`Invalid operation at sequence ${rawApplied} for ${nodeId}`)
      }
      const previousOperation = rawApplied === 0 ? null : await core.get(rawApplied - 1)
      validateLogLink(operation, previousOperation, rawApplied)
      if (operation.kind === "heartbeat") {
        await this.view.applyHeartbeat(operation, node.feedKey)
        await this.view.setRawProgress(node.feedKey, {
          applied: operation.seq + 1,
          lastOpId: operation.opId
        })
        await this.#applyRejectedFeedEntries(nodeId, operation.heartbeat?.rejectedFeeds?.[node.feedKey] ?? [])
        this.lastHeartbeatByNode.set(operation.actor, {
          ts: operation.ts,
          feed: node.feedKey,
          seq: operation.seq,
          appliedFeeds: operation.heartbeat?.appliedFeeds ?? {},
          rejectedFeeds: operation.heartbeat?.rejectedFeeds ?? {},
          membershipFingerprint: operation.heartbeat?.membershipFingerprint ?? null
        })
        const watermark = operation.heartbeat?.appliedFeeds?.[node.feedKey]
        if (Number.isInteger(watermark) && watermark > 0) {
          await this.#advanceCommittedFeed(nodeId, watermark)
        }
        await this.#observeRemoteOperation(nodeId, operation)
      } else if (operation.kind === "kv") {
        await this.view.stageEntry(node.feedKey, {
          nodeId,
          source: nodeId === this.options.identity.publicKeyId ? "local" : "remote",
          validation: "valid",
          operation
        })
        await this.view.setRawProgress(node.feedKey, {
          applied: operation.seq + 1,
          lastOpId: operation.opId
        })
        if (nodeId !== this.options.identity.publicKeyId) {
          void this.#sendAck(nodeId, operation.seq)
        }
        await this.#observeRemoteOperation(nodeId, operation)
      } else if (operation.kind === "membership") {
        await this.view.setRawProgress(node.feedKey, {
          applied: operation.seq + 1,
          lastOpId: operation.opId
        })
        if (nodeId !== this.options.identity.publicKeyId) {
          void this.#sendAck(nodeId, operation.seq)
        }
        await this.#observeRemoteOperation(nodeId, operation)
      }
      rawApplied += 1
    }
  }

  async #appendHeartbeat() {
    if (this.closing) return
    if (this.#isLearner()) return

    const leader = this.currentLeader() ?? this.#lastKnownLeaderId()
    if (this.closing) return

    try {
      await this.#withLocalAppendLock(async () => {
        const previousOperation = await this.#previousLocalOperation()
        const heartbeat = {
          leaderId: leader,
          leaderCommitIndex: this.consensusState.commitIndex,
          membershipVersion: this.membershipState.current.version,
          prevLogIndex: previousOperation?.index ?? -1,
          prevLogTerm: previousOperation?.term ?? -1,
          prevLogHash: previousOperation?.entryHash ?? null,
          observedLeader: leader,
          reachableLeader: leader === null ? false : this.#isLeaderReachable(leader),
          appliedFeeds: await this.#appliedFeeds(),
          rejectedFeeds: await this.#rejectedFeeds(),
          membershipFingerprint: this.#membershipFingerprint()
        }
        const operation = createSignedOperation({
          kind: "heartbeat",
          type: "put",
          key: `heartbeat:${this.options.identity.publicKeyId}`,
          keyspace: "system",
          seq: this.#localCore().length,
          term: this.consensusState.currentTerm,
          index: this.#localCore().length,
          prevIndex: previousOperation?.index ?? -1,
          prevHash: previousOperation?.entryHash ?? null,
          feed: this.options.identity.feedKey,
          actor: this.options.identity.publicKeyId,
          secretKey: this.options.identity.secretKey,
          encryptionKey: this.#currentEncryptionKey(),
          encryptionKeyId: this.encryption.currentKeyId,
          heartbeat
        })
        await this.#localCore().append(operation)
      })
      await this.syncFeed(this.options.identity.publicKeyId)
    } catch (error) {
      if (!this.closing || error?.code !== "SESSION_CLOSED") throw error
    }
  }

  async #runHeartbeat() {
    if (this.heartbeatPromise) return this.heartbeatPromise

    const heartbeatPromise = this.#appendHeartbeat().finally(() => {
      if (this.heartbeatPromise === heartbeatPromise) {
        this.heartbeatPromise = null
      }
    })
    this.heartbeatPromise = heartbeatPromise
    return heartbeatPromise
  }

  #scheduleElectionTimer() {
    if (this.closing || this.#isLearner() || this.consensusEngine.role === "leader") return
    if (this.electionTimer) clearTimeout(this.electionTimer)

    const { timeoutMs } = this.consensusEngine.planElectionTimeout({
      minMs: this.options.electionTimeoutMinMs,
      maxMs: this.options.electionTimeoutMaxMs,
      voterNodeIds: this.membershipState.current.voters
    })
    this.electionTimer = setTimeout(() => {
      void this.#handleElectionTimeout().catch((error) => {
        if (!this.closing && error?.code !== "SESSION_CLOSED") {
          throw error
        }
      })
    }, timeoutMs)
    this.electionTimer.unref?.()
  }

  async #handleElectionTimeout() {
    if (this.closing || this.#isLearner() || this.consensusEngine.role === "leader") return
    const currentLeader = this.currentLeader()
    if (currentLeader && (currentLeader === this.options.identity.publicKeyId || this.#isLeaderReachable(currentLeader))) {
      this.#scheduleElectionTimer()
      return
    }

    const lastKnownLeader = currentLeader ?? this.#lastKnownLeaderId()
    if (lastKnownLeader) {
      if (!this.#isSplitFenced()) {
        this.#enterSplitFence("leader-heartbeat-expired", lastKnownLeader)
        await this.#persistSplitState()
        this.#scheduleElectionTimer()
        return
      }

      if (await this.#canTriggerWitnessReelection(lastKnownLeader)) {
        await this.#startElection()
        return
      }

      this.#scheduleElectionTimer()
      return
    }

    await this.#startElection()
  }

  async #startElection() {
    if (this.closing || this.#isLearner()) return

    const voterNodeIds = this.membershipState.current.voters
    const lastLog = await this.#localLastLogInfo()
    const election = this.consensusEngine.startElection({
      currentTerm: this.consensusState.currentTerm,
      voterNodeIds,
      lastLog,
      membershipVersion: this.membershipState.current.version
    })
    const nextTerm = election.nextTerm
    this.consensusState = await this.consensusStateStore.save(election.persistPatch)
    const votes = new Set([this.options.identity.publicKeyId])
    const requiredVotes = election.requiredVotes
    const peers = voterNodeIds
      .filter((nodeId) => nodeId !== this.options.identity.publicKeyId)
      .map((nodeId) => {
        const core = this.feedCores.get(nodeId)
        const peer = core?.peers?.[0] ?? null
        return peer ? { nodeId, peer } : null
      })
      .filter(Boolean)

    for (const { nodeId, peer } of peers) {
      void this.rpc.requestVote({
        targetNodeId: nodeId,
        peer,
        request: election.voteRequest
      }).then(async (response) => {
        if (!this.consensusEngine.isCandidateForTerm(nextTerm) || this.consensusState.currentTerm !== nextTerm) return
        if (response?.term > this.consensusState.currentTerm) {
          await this.#stepDown(response.term, response.leaderNodeId ?? null)
          return
        }
        if (response?.voteGranted) {
          votes.add(nodeId)
          if (votes.size >= requiredVotes) {
            await this.#becomeLeader(nextTerm)
          }
        }
      }).catch(() => {})
    }

    if (votes.size >= requiredVotes) {
      await this.#becomeLeader(nextTerm)
      return
    }

    this.#scheduleElectionTimer()
  }

  async #handleVoteRequest(message) {
    const decision = this.consensusEngine.evaluateVoteRequest({
      consensusState: this.consensusState,
      voterNodeIds: this.membershipState.current.voters,
      isLearner: this.#isLearner(),
      localLog: await this.#localLastLogInfo(),
      localMembershipVersion: this.membershipState.current.version,
      message
    })
    if (decision.persistPatch) {
      this.consensusState = await this.consensusStateStore.save(decision.persistPatch)
    }
    this.#scheduleElectionTimer()
    return {
      ...decision.response,
      term: this.consensusState.currentTerm,
      leaderNodeId: this.currentLeader()
    }
  }

  async #becomeLeader(term) {
    if (this.closing) return
    const transition = this.consensusEngine.becomeLeader({
      term,
      currentTerm: this.consensusState.currentTerm,
      electionTimeoutMaxMs: this.options.electionTimeoutMaxMs
    })
    if (!transition.becameLeader) return
    if (this.electionTimer) {
      clearTimeout(this.electionTimer)
      this.electionTimer = null
    }
    this.consensusState = await this.consensusStateStore.save(transition.persistPatch)
    this.#clearSplitFence()
    await this.#persistSplitState()
    await this.#runHeartbeat()
  }

  async #stepDown(nextTerm, leaderNodeId = null) {
    const transition = this.consensusEngine.stepDown({
      nextTerm,
      currentTerm: this.consensusState.currentTerm,
      leaderNodeId
    })
    this.consensusState = await this.consensusStateStore.save(transition.persistPatch)
    this.#rejectPendingWrites(new Error("Leadership changed while write was in flight"))
    if (leaderNodeId && leaderNodeId !== this.options.identity.publicKeyId) {
      this.#clearSplitFence()
      await this.#persistSplitState()
    }
    this.#scheduleElectionTimer()
  }

  async #observeRemoteOperation(nodeId, operation) {
    const previousOperation = operation?.seq > 0
      ? await this.feedCores.get(nodeId)?.get(operation.seq - 1)
      : null
    const observation = this.consensusEngine.observeRemoteOperation({
      nodeId,
      voterNodeIds: this.membershipState.current.voters,
      consensusState: this.consensusState,
      localMembershipVersion: this.membershipState.current.version,
      operation,
      previousOperation,
      electionTimeoutMaxMs: this.options.electionTimeoutMaxMs
    })
    if (
      !observation.acceptedLeader &&
      observation.refusalReason === "not-elected-leader" &&
      operation?.kind === "heartbeat" &&
      operation.heartbeat?.leaderId === nodeId &&
      operation.heartbeat?.observedLeader === nodeId &&
      await this.#hasWitnessLeaderMajority(nodeId, operation.term)
    ) {
      this.consensusEngine.noteKnownLeader({
        leaderNodeId: nodeId,
        electionTimeoutMs: this.options.electionTimeoutMaxMs
      })
      observation.acceptedLeader = true
      observation.refusalReason = null
    }
    if (observation.persistPatch) {
      this.consensusState = await this.consensusStateStore.save(observation.persistPatch)
    }
    if (observation.acceptedLeader) {
      this.#clearSplitFence()
      await this.#persistSplitState()
    }
    if (observation.acceptedLeader || observation.persistPatch) {
      this.#scheduleElectionTimer()
    }
  }

  async #localLastLogInfo() {
    const core = this.#localCore()
    for (let index = core.length - 1; index >= 0; index -= 1) {
      const operation = await core.get(index)
      if (this.#countsForElectionLog(operation)) {
        return {
          index: operation.index ?? index,
          term: operation.term ?? -1
        }
      }
    }

    return {
      index: -1,
      term: -1
    }
  }

  #countsForElectionLog(operation) {
    if (!operation || typeof operation !== "object") return false
    if (operation.kind !== "heartbeat") return true
    return operation.heartbeat?.observedLeader === operation.actor
  }

  async #startNetworking() {
    const topic = await deriveTopic(this.options)
    this.joinControl ??= new JoinControl({
      onChannelOpen: (session) => this.#maybeSendJoinRequest(session),
      onJoinRequest: (session, message) => {
        void this.#handleJoinRequest(session, message).catch((error) => {
          if (!this.closing) {
            throw error
          }
        })
      },
      onJoinResponse: (session, message) => {
        void this.#handleJoinResponse(session, message).catch((error) => {
          if (!this.closing) {
            throw error
          }
        })
      }
    })
    this.network ??= new SwarmNetwork({
      bootstrap: this.options.bootstrap,
      topic,
      keyPair: this.transportIdentity?.keyPair,
      localNodeId: this.options.identity.publicKeyId,
      authorizedNodes: this.options.authorizedNodes,
      isRevokedNode: (nodeId) => this.#isRevokedNode(nodeId),
      isConnectionAccepted: (nodeId) => this.#acceptsPeerConnection(nodeId),
      replicateConnection: (conn) => {
        this.joinControl?.attachConnection(conn)
        this.store.replicate(conn)
      },
      networkPolicy: this.options.networkPolicy ?? null
    })
    await this.network.start()
  }

  async #appendKvOperation(type, key, value, options) {
    const quorumGroups = this.#writeQuorumGroups()
    if (!this.#hasReachableQuorum(quorumGroups)) {
      throw new Error("Durability requirement not met: no reachable quorum available")
    }

    let operation = null
    let ackPromise = null
    await this.#withLocalAppendLock(async () => {
      const previousOperation = await this.#previousLocalOperation()
      operation = createSignedOperation({
        kind: "kv",
        type,
        key,
        keyspace: options.keyspace,
        value,
        seq: this.#localCore().length,
        term: this.consensusState.currentTerm,
        index: this.#localCore().length,
        prevIndex: previousOperation?.index ?? -1,
        prevHash: previousOperation?.entryHash ?? null,
        feed: this.options.identity.feedKey,
        actor: this.options.identity.publicKeyId,
        secretKey: this.options.identity.secretKey,
        encryptionKey: this.#currentEncryptionKey(),
        encryptionKeyId: this.encryption.currentKeyId,
        ttlMs: options.ttlMs
      })
      ackPromise = this.durabilityWaiter.waitForGroups(
        operation.seq,
        quorumGroups,
        [this.options.identity.publicKeyId]
      )
      ackPromise.catch(() => {})
      await this.#localCore().append(operation)
    })
    await this.syncFeed(this.options.identity.publicKeyId)
    try {
      await ackPromise
    } catch (error) {
      if (this.closing) {
        throw new Error("Node is closing")
      }
      await this.view.markSkippedEntry(this.options.identity.feedKey, operation.seq)
      await this.view.setStagedEntryResolution(this.options.identity.feedKey, operation.seq, "rejected")
      await this.#runHeartbeat()
      throw error
    }
    await this.#advanceCommittedFeed(this.options.identity.publicKeyId, operation.seq + 1)
    await this.#runHeartbeat()
    return operation
  }

  async #forwardWrite(request) {
    if (this.#isLearner()) {
      throw this.#createLearnerWriteError()
    }
    if (this.#isSplitFenced()) {
      throw this.#createSplitFencedError()
    }
    if (!this.options.forwarding) {
      throw new Error("Write forwarding is disabled on this node")
    }

    const leader = this.currentLeader() ?? this.#lastKnownLeaderId()
    if (!leader) throw new Error("No current leader is available")

    const leaderCore = this.feedCores.get(leader)
    const peer = leaderCore.peers[0]
    if (!peer) {
      throw new Error(`Current leader ${leader} is not reachable`)
    }

    return this.rpc.forwardWrite({ targetNodeId: leader, peer, request })
  }

  /**
   * Reject outstanding write waits so shutdown does not leave live timers behind.
   *
   * @param {Error} error
   */
  #rejectPendingWrites(error) {
    this.durabilityWaiter.rejectAll(error)
  }

  /**
   * @param {string} nodeId
   * @param {number} seq
   */
  async #sendAck(nodeId, seq) {
    const core = this.feedCores.get(nodeId)
    const peer = core.peers[0]
    if (!peer) return

    await this.rpc.sendAck({ targetNodeId: nodeId, peer, seq })
  }

  /**
   * @param {string} nodeId
   * @param {number} seq
   */
  #recordAck(nodeId, seq) {
    if (nodeId === this.options.identity.publicKeyId) return
    if (this.#membershipRole(nodeId) !== "voter") return
    this.durabilityWaiter.record(nodeId, seq)
  }

  #localCore() {
    return this.feedCores.get(this.options.identity.publicKeyId)
  }

  /**
   * Return the tracked Hypercore for one authorized node ID.
   *
   * @param {string} nodeId
   */
  #nodeCore(nodeId) {
    return this.feedCores.get(nodeId) ?? null
  }

  /**
   * Resolve the authoritative cluster log node for the current storage model.
   *
   * The leader feed stays authoritative even while a disconnected node is
   * split-fenced; in that case the last known leader remains the only log this
   * node should treat as cluster authority.
   */
  #authoritativeLogNodeId() {
    return this.currentLeader() ?? this.#lastKnownLeaderId() ?? this.options.identity.publicKeyId
  }

  #authoritativeLogRecord() {
    return this.#getAuthorizedNode(this.#authoritativeLogNodeId())
  }

  #authoritativeLogCore() {
    return this.#nodeCore(this.#authoritativeLogNodeId())
  }

  /**
   * Build one node/feed replication summary for diagnostics.
   *
   * @param {string} nodeId
   * @param {Record<string, any>} heartbeats
   * @param {number} now
   */
  async #feedReplicationStatus(nodeId, heartbeats, now) {
    const node = this.#getAuthorizedNode(nodeId)
    const core = this.#nodeCore(nodeId)
    if (!core) return null

    const applied = await this.view.getApplied(node.feedKey)
    const staged = await this.view.getStagedSummary(node.feedKey)
    const heartbeat = heartbeats[nodeId] ?? null

    return {
      feedKey: node.feedKey,
      length: core.length,
      applied,
      lag: core.length - applied,
      staged,
      connectedPeers: core.peers.length,
      alive: heartbeat ? now - new Date(heartbeat.ts).getTime() <= this.options.heartbeatTtlMs : false,
      heartbeatAgeMs: heartbeat ? now - new Date(heartbeat.ts).getTime() : null
    }
  }

  #localNodeRecord() {
    return {
      nodeId: this.options.identity.publicKeyId,
      publicKey: this.options.identity.publicKey,
      feedKey: this.options.identity.feedKey
    }
  }

  /**
   * Serialize local Hypercore appends so the signed operation sequence always
   * matches the physical feed slot.
   *
   * @template T
   * @param {() => Promise<T>} run
   * @returns {Promise<T>}
   */
  async #withLocalAppendLock(run) {
    const previous = this.localAppendLock
    let release = null
    this.localAppendLock = new Promise((resolve) => {
      release = resolve
    })

    await previous
    try {
      return await run()
    } finally {
      release()
    }
  }

  async #previousLocalOperation() {
    const core = this.#localCore()
    if (core.length === 0) return null
    return core.get(core.length - 1)
  }

  #getAuthorizedNode(nodeId) {
    if (nodeId === this.options.identity.publicKeyId) {
      return this.#localNodeRecord()
    }
    const node = this.options.authorizedNodes.find((entry) => entry.nodeId === nodeId)
    if (!node) throw new Error(`Unknown authorized node ${nodeId}`)
    if (this.#isRevokedNode(nodeId)) {
      throw new Error(`Revoked node ${nodeId} is not allowed to replicate`)
    }
    return node
  }

  #reachableVotingNodeIds() {
    return this.#effectiveVoterNodeIds().filter((nodeId) => this.#isLeaderReachable(nodeId))
  }

  #writeQuorumGroups() {
    const joint = this.#jointMembership()
    if (joint) {
      return this.#jointQuorumGroups(joint)
    }
    return [
      {
        eligibleNodeIds: this.membershipState.current.voters,
        requiredCount: this.#majoritySize(this.membershipState.current.voters.length)
      }
    ]
  }

  #hasReachableQuorum(groups) {
    const reachable = new Set(this.#reachableVotingNodeIds())
    return groups.every((group) => {
      if (group.requiredCount <= 0) return true
      const matchedCount = group.eligibleNodeIds
        ? group.eligibleNodeIds.filter((nodeId) => reachable.has(nodeId)).length
        : reachable.size
      return matchedCount >= group.requiredCount
    })
  }

  #role() {
    const role = this.#membershipRole(this.options.identity.publicKeyId)
    return role ?? "learner"
  }

  #isLearner() {
    return this.#role() !== "voter"
  }

  #isSplitFenced() {
    return this.splitState.fenced
  }

  #lastKnownLeaderId() {
    return this.splitState.leaderNodeId ?? this.consensusEngine.state.leaderNodeId ?? null
  }

  #enterSplitFence(reason, leaderNodeId = this.#lastKnownLeaderId()) {
    if (this.consensusEngine.role === "leader") return
    if (!leaderNodeId || leaderNodeId === this.options.identity.publicKeyId) return
    if (this.splitState.fenced && this.splitState.leaderNodeId === leaderNodeId && this.splitState.reason === reason) {
      return
    }

    this.splitState = {
      fenced: true,
      reason,
      leaderNodeId,
      since: new Date().toISOString()
    }
    this.network?.refreshConnectionPermissions()
    this.#rejectPendingWrites(new Error("Leader is unreachable while node is split-fenced"))
  }

  #clearSplitFence() {
    if (!this.splitState.fenced) return
    this.splitState = {
      fenced: false,
      reason: null,
      leaderNodeId: null,
      since: null
    }
    this.network?.refreshConnectionPermissions()
  }

  #acceptsPeerConnection(nodeId) {
    if (!this.#isSplitFenced()) return true
    if (nodeId === this.#lastKnownLeaderId()) return true
    return this.#acceptsWitnessPeerConnection(nodeId)
  }

  /**
   * Keep witness traffic alive only for the narrow reelection path where the
   * cluster is proving that only the leader disappeared.
   *
   * @param {string} nodeId
   */
  #acceptsWitnessPeerConnection(nodeId) {
    if (this.splitState.reason !== "leader-heartbeat-expired") return false
    if (this.#effectiveVoterNodeIds().length < 3) return false
    if (nodeId === this.options.identity.publicKeyId) return false
    if (nodeId === this.#lastKnownLeaderId()) return false
    return this.#effectiveVoterNodeIds().includes(nodeId)
  }

  /**
   * Allow autonomous reelection only when recent witness evidence shows the
   * established leader disappeared and every other voter is still live.
   *
   * @param {string} leaderNodeId
   */
  async #canTriggerWitnessReelection(leaderNodeId) {
    const voterNodeIds = this.#effectiveVoterNodeIds()
    if (voterNodeIds.length < 3) return false
    if (!voterNodeIds.includes(this.options.identity.publicKeyId)) return false
    if (!leaderNodeId || leaderNodeId === this.options.identity.publicKeyId) return false

    const witnessNodeIds = voterNodeIds.filter((nodeId) => nodeId !== leaderNodeId)
    for (const nodeId of witnessNodeIds) {
      if (nodeId === this.options.identity.publicKeyId) continue
      if (!this.#isLeaderReachable(nodeId)) return false
    }

    const heartbeats = await this.view.getHeartbeats()
    const freshnessCutoff = Date.now() - this.options.heartbeatTtlMs
    for (const nodeId of witnessNodeIds) {
      if (nodeId === this.options.identity.publicKeyId) continue
      const heartbeat = heartbeats[nodeId]
      if (!heartbeat) return false
      if (new Date(heartbeat.ts).getTime() < freshnessCutoff) return false
      if (heartbeat.observedLeader !== leaderNodeId) return false
      if (heartbeat.reachableLeader !== false) return false
      if (
        Number.isInteger(heartbeat.membershipVersion) &&
        heartbeat.membershipVersion !== this.membershipState.current.version
      ) {
        return false
      }
    }

    return true
  }

  /**
   * Late joiners may not have cast a vote in the leader's current term. In
   * that case, allow leader adoption only after a fresh majority of voter
   * heartbeats already agrees on the same reachable leader.
   *
   * @param {string} leaderNodeId
   * @param {number} term
   */
  async #hasWitnessLeaderMajority(leaderNodeId, term) {
    const heartbeats = await this.view.getHeartbeats()
    const freshnessCutoff = Date.now() - this.options.heartbeatTtlMs
    let matchingCount = 0

    for (const nodeId of this.membershipState.current.voters) {
      const heartbeat = heartbeats[nodeId]
      if (!heartbeat) continue
      if (new Date(heartbeat.ts).getTime() < freshnessCutoff) continue
      if (heartbeat.observedLeader !== leaderNodeId) continue
      if (heartbeat.reachableLeader !== true) continue
      if (Number.isInteger(heartbeat.term) && heartbeat.term !== term) continue
      matchingCount += 1
    }

    return matchingCount >= this.#majoritySize(this.membershipState.current.voters.length)
  }

  async #persistSplitState() {
    this.consensusState = await this.consensusStateStore.save({
      splitFenced: this.splitState.fenced,
      splitLeaderNodeId: this.splitState.leaderNodeId,
      splitReason: this.splitState.reason
    })
  }

  #createLearnerWriteError() {
    const error = new Error("This node is a read-only learner and cannot accept or proxy writes")
    error.code = "READ_ONLY_LEARNER"
    error.statusCode = 403
    error.leader = this.currentLeader() ?? this.#lastKnownLeaderId()
    return error
  }

  #createSplitFencedError() {
    const error = new Error("This node is split-fenced and is waiting to reconnect to the current leader")
    error.code = "SPLIT_FENCED"
    error.statusCode = 503
    error.leader = this.#lastKnownLeaderId()
    error.splitStatus = { ...this.splitState }
    return error
  }

  async #promotionStatus() {
    const entry = await this.consensusBee.get("promotion/current")
    return entry?.value ?? null
  }

  async #seenPromotionCredentialHashes() {
    const seen = new Set()
    for await (const entry of this.consensusBee.createReadStream({
      gt: "promotion/seen/hash/",
      lt: "promotion/seen/hash/~"
    })) {
      seen.add(entry.key.slice("promotion/seen/hash/".length))
    }
    return seen
  }

  async #seenPromotionNonces() {
    const seen = new Set()
    for await (const entry of this.consensusBee.createReadStream({
      gt: "promotion/seen/nonce/",
      lt: "promotion/seen/nonce/~"
    })) {
      seen.add(entry.key.slice("promotion/seen/nonce/".length))
    }
    return seen
  }

  async #isPromotionEligible() {
    if (!this.#isLearner()) return false
    return this.currentLeader() !== null
  }

  async #isLearnerPromotionReady(feedKey) {
    const learnerNode = this.options.authorizedNodes.find((node) => node.feedKey === feedKey)
    if (!learnerNode) return false
    return (await this.view.getApplied(feedKey)) === this.feedCores.get(learnerNode.nodeId)?.length
  }

  #isLeaderReachable(nodeId) {
    if (this.#isRevokedNode(nodeId)) return false
    if (nodeId === this.options.identity.publicKeyId) return true
    return this.network?.isNodeConnected(nodeId) ?? false
  }

  async #appliedFeeds() {
    const applied = {}
    for (const node of this.#voterNodes()) {
      if (node.nodeId === this.options.identity.publicKeyId) {
        const durableApplied = this.durabilityWaiter.status().lastDurableSequence + 1
        applied[node.feedKey] = Math.max(await this.view.getApplied(node.feedKey), durableApplied)
        continue
      }

      applied[node.feedKey] = await this.view.getApplied(node.feedKey)
    }
    return applied
  }

  async #rejectedFeeds() {
    const rejected = {}

    for (const node of this.#voterNodes()) {
      if (node.nodeId !== this.options.identity.publicKeyId) continue

      const rejectedSeqs = await this.view.getSkippedEntries(node.feedKey)

      if (rejectedSeqs.length > 0) {
        rejected[node.feedKey] = rejectedSeqs
      }
    }

    return rejected
  }

  /**
   * @param {string} nodeId
   * @param {number[]} rejectedSeqs
   */
  async #applyRejectedFeedEntries(nodeId, rejectedSeqs) {
    const node = this.#getAuthorizedNode(nodeId)
    for (const seq of rejectedSeqs) {
      await this.view.markSkippedEntry(node.feedKey, seq)
      await this.view.setStagedEntryResolution(node.feedKey, seq, "rejected")
    }
  }

  /**
   * Advance the committed prefix for a feed without exposing entries that only
   * exist in the raw replicated suffix.
   *
   * @param {string} nodeId
   * @param {number} targetApplied
   */
  async #advanceCommittedFeed(nodeId, targetApplied) {
    const node = this.#getAuthorizedNode(nodeId)
    const core = this.feedCores.get(nodeId)
    const rawApplied = await this.view.getRawApplied(node.feedKey)
    let committedApplied = await this.view.getApplied(node.feedKey)
    const cappedTarget = Math.min(targetApplied, rawApplied, core.length)

    while (committedApplied < cappedTarget) {
      const operation = await core.get(committedApplied)
      if (operation.seq !== committedApplied) {
        throw new Error(`Committed operation sequence mismatch at feed slot ${committedApplied} for ${nodeId}`)
      }
      validateOperation(operation, node, { revokedNodeIds: this.revokedNodeIds })
      if (!verifySignedOperation(operation, node.publicKey)) {
        throw new Error(`Invalid committed operation at sequence ${committedApplied} for ${nodeId}`)
      }

      const shouldSkip = operation.kind === "kv" && await this.view.isSkippedEntry(node.feedKey, operation.seq)

      if (shouldSkip) {
        await this.view.skipCommitted(operation, node.feedKey)
      } else {
        await this.view.applyCommitted(operation, node.feedKey)
        if (operation.kind === "membership") {
          await this.#applyCommittedMembershipOperation(operation)
        }
      }
      if (nodeId === this.options.identity.publicKeyId) {
        this.consensusState = await this.consensusStateStore.save({
          commitIndex: operation.seq,
          lastApplied: operation.seq
        })
      }
      committedApplied += 1
    }
  }

  #membershipFingerprint() {
    const membership = {
      current: this.membershipState?.current ?? null,
      joint: this.membershipState?.joint ?? null,
      entries: this.options.authorizedNodes.map((node) => ({
        nodeId: node.nodeId,
        feedKey: node.feedKey,
        role: this.#membershipRole(node.nodeId)
      }))
    }
    return createHash("sha256").update(canonicalize(membership)).digest("hex")
  }

  #membershipStatus(heartbeats) {
    const localFingerprint = this.#membershipFingerprint()
    const entries = this.#membershipEntries()
    const peerFingerprints = {}
    const mismatchedNodeIds = []
    const matchingNodeIds = []

    for (const node of this.#voterNodes()) {
      if (node.nodeId === this.options.identity.publicKeyId) continue

      const fingerprint = heartbeats[node.nodeId]?.membershipFingerprint ?? null
      peerFingerprints[node.nodeId] = fingerprint
      if (fingerprint === null) continue
      if (fingerprint === localFingerprint) matchingNodeIds.push(node.nodeId)
      else mismatchedNodeIds.push(node.nodeId)
    }

    return {
      localFingerprint,
      localRole: this.#role(),
      version: this.membershipState.current.version,
      joint: this.membershipState.joint,
      voters: entries.filter((entry) => entry.role === "voter"),
      learners: entries.filter((entry) => entry.role === "learner"),
      removed: entries.filter((entry) => entry.role === "removed"),
      peerFingerprints,
      mismatchedNodeIds: mismatchedNodeIds.sort(),
      matchingNodeIds: matchingNodeIds.sort()
    }
  }

  #readStatus() {
    const leader = this.currentLeader() ?? this.#lastKnownLeaderId()
    if (this.#isSplitFenced()) {
      return {
        staleReadsPossible: true,
        reason: "split-fenced",
        leader
      }
    }

    if (leader && leader !== this.options.identity.publicKeyId && !this.#isLeaderReachable(leader)) {
      return {
        staleReadsPossible: true,
        reason: "leader-unreachable",
        leader
      }
    }

    if ((this.network?.connectionCount ?? 0) === 0 && this.options.authorizedNodes.length > 1) {
      return {
        staleReadsPossible: true,
        reason: "no-live-peer-connections",
        leader
      }
    }

    return {
      staleReadsPossible: false,
      reason: null,
      leader
    }
  }

  #currentEncryptionKey() {
    return this.encryption.keys[this.encryption.currentKeyId]
  }

  /**
   * @param {string} nodeId
   */
  #isRevokedNode(nodeId) {
    return this.revokedNodeIds.has(nodeId)
  }

  #currentMembershipRole(nodeId) {
    if (this.membershipState.current.voters.includes(nodeId)) return "voter"
    if (this.membershipState.current.learners.includes(nodeId)) return "learner"
    if (this.membershipState.current.removed.includes(nodeId)) return "removed"
    return null
  }

  #membershipRole(nodeId) {
    if (this.#effectiveVoterNodeIds().includes(nodeId)) {
      return "voter"
    }
    const currentRole = this.#currentMembershipRole(nodeId)
    if (currentRole) return currentRole
    if (!this.options.authorizedNodes.some((node) => node.nodeId === nodeId)) {
      return nodeId === this.options.identity.publicKeyId ? "learner" : null
    }
    return this.#isRevokedNode(nodeId) ? "removed" : "learner"
  }

  #ensureAuthorizedNodeRecord(node) {
    const normalized = {
      nodeId: node.nodeId,
      publicKey: Buffer.isBuffer(node.publicKey) ? node.publicKey : Buffer.from(node.publicKey, "hex"),
      feedKey: node.feedKey
    }
    const existing = this.options.authorizedNodes.find((entry) => entry.nodeId === normalized.nodeId)
    if (!existing) {
      this.options.authorizedNodes.push(normalized)
      return normalized
    }
    if (
      existing.feedKey !== normalized.feedKey ||
      !existing.publicKey.equals(normalized.publicKey)
    ) {
      throw new Error(`Authorized node ${normalized.nodeId} does not match the existing record`)
    }
    return existing
  }

  async #ensureFeedCore(node) {
    if (this.feedCores.has(node.nodeId)) {
      return this.feedCores.get(node.nodeId)
    }

    const isLocal = node.nodeId === this.options.identity.publicKeyId
    const core = isLocal
      ? this.store.get({
          keyPair: {
            publicKey: this.options.identity.publicKey,
            secretKey: this.options.identity.secretKey
          },
          valueEncoding: "json"
        })
      : this.store.get({ key: Buffer.from(node.feedKey, "hex"), valueEncoding: "json" })

    await core.ready()
    if (!isLocal) {
      core.on("peer-add", (peer) => {
        this.network?.trackPeer(true, node.nodeId, peer)
        this.rpc?.sendHello({ targetNodeId: node.nodeId, peer })
      })
      core.on("peer-remove", (peer) => this.network?.trackPeer(false, node.nodeId, peer))
      core.on("append", () => {
        void this.syncFeed(node.nodeId).catch((error) => {
          if (!this.closing && error?.code !== "REQUEST_CANCELLED" && error?.code !== "SESSION_CLOSED") {
            throw error
          }
        })
      })
    }
    this.feedCores.set(node.nodeId, core)
    this.rpc.register(node.nodeId, core)
    return core
  }

  async #ensureJoinedNode(node) {
    const record = this.#ensureAuthorizedNodeRecord(node)
    await this.#ensureFeedCore(record)
    if (record.nodeId !== this.options.identity.publicKeyId) {
      void this.syncFeed(record.nodeId).catch((error) => {
        if (!this.closing && error?.code !== "REQUEST_CANCELLED" && error?.code !== "SESSION_CLOSED") {
          throw error
        }
      })
    }
    return record
  }

  #maybeSendJoinRequest(session) {
    if (
      this.closing ||
      !this.#isLearner() ||
      !this.transportIdentity ||
      this.joinState.accepted ||
      this.sentJoinRequestKeys.has(session.remotePublicKeyHex)
    ) {
      return
    }

    this.sentJoinRequestKeys.add(session.remotePublicKeyHex)
    session.sendRequest(this.#createJoinRequest())
  }

  #createJoinRequest() {
    const payload = {
      v: 1,
      type: "replicore.join-request",
      clusterId: this.options.clusterId,
      machineId: this.transportIdentity.machineId,
      identityPublicKey: this.options.identity.publicKey.toString("hex"),
      feedKey: this.options.identity.feedKey,
      noisePublicKey: this.transportIdentity.publicKeyHex,
      role: "learner",
      issuedAt: new Date().toISOString(),
      nonce: randomBytes(16).toString("base64url")
    }

    return {
      ...payload,
      signature: signPayload(
        this.transportIdentity.joinKeyPair.secretKey,
        Buffer.from(canonicalize(payload))
      )
    }
  }

  async #handleJoinRequest(session, message) {
    if (!message || message.type !== "replicore.join-request") return

    const currentLeader = this.currentLeader()
    if (currentLeader !== this.options.identity.publicKeyId) {
      const leaderHint = currentLeader ?? this.#lastKnownLeaderId()
      session.sendResponse({
        v: 1,
        type: "replicore.join-response",
        ok: false,
        redirect: true,
        leaderNodeId: leaderHint,
        errorCode: currentLeader ? "NOT_LEADER" : "LEADER_UNAVAILABLE",
        error: leaderHint
          ? "Join requests must be handled by the current leader"
          : "No current leader is available"
      })
      return
    }

    try {
      const summary = await this.#validateJoinRequest(session, message)
      this.network?.observePeerIdentity(summary.nodeId, {
        remotePublicKey: session.conn.remotePublicKey
      })
      await this.#ensureJoinedNode({
        nodeId: summary.nodeId,
        publicKey: summary.identityPublicKey,
        feedKey: summary.feedKey
      })

      session.sendResponse({
        v: 1,
        type: "replicore.join-response",
        ok: true,
        redirect: false,
        leaderNodeId: this.options.identity.publicKeyId,
        acceptedLearner: {
          nodeId: summary.nodeId,
          machineId: summary.machineId,
          noisePublicKey: summary.noisePublicKey
        },
        membership: {
          version: this.membershipState.current.version,
          voters: this.#voterNodes().map((node) => ({
            nodeId: node.nodeId,
            publicKey: node.publicKey.toString("hex"),
            feedKey: node.feedKey
          })),
          learners: this.#membershipEntries()
            .filter((entry) => entry.role === "learner")
            .map((entry) => ({
              nodeId: entry.nodeId,
              feedKey: entry.feedKey
            })),
          removed: this.#membershipEntries()
            .filter((entry) => entry.role === "removed")
            .map((entry) => entry.nodeId)
        },
        consensus: {
          currentTerm: this.consensusState.currentTerm,
          commitIndex: this.consensusState.commitIndex,
          lastApplied: this.consensusState.lastApplied
        },
        readOnly: true
      })
    } catch (error) {
      const errorCode = error?.code === "REMOVED_IDENTITY" ? "REMOVED_IDENTITY" : "JOIN_REJECTED"
      session.sendResponse({
        v: 1,
        type: "replicore.join-response",
        ok: false,
        redirect: false,
        leaderNodeId: this.options.identity.publicKeyId,
        errorCode,
        error: error instanceof Error ? error.message : String(error),
        ...(errorCode === "REMOVED_IDENTITY"
          ? {
              membership: {
                version: this.membershipState.current.version,
                voters: this.#voterNodes().map((node) => ({
                  nodeId: node.nodeId,
                  publicKey: node.publicKey.toString("hex"),
                  feedKey: node.feedKey
                })),
                learners: this.#membershipEntries()
                  .filter((entry) => entry.role === "learner")
                  .map((entry) => ({
                    nodeId: entry.nodeId,
                    feedKey: entry.feedKey
                  })),
                removed: this.#membershipEntries()
                  .filter((entry) => entry.role === "removed")
                  .map((entry) => entry.nodeId)
              },
              consensus: {
                currentTerm: this.consensusState.currentTerm,
                commitIndex: this.consensusState.commitIndex,
                lastApplied: this.consensusState.lastApplied
              }
            }
          : {})
      })
    }
  }

  async #handleJoinResponse(_session, message) {
    if (!message || message.type !== "replicore.join-response") return
    if (!message.ok) {
      if (message.membership) {
        await this.#adoptJoinMembershipSnapshot(message)
      }
      if (message.redirect && message.leaderNodeId) {
        this.joinState.leaderNodeId = message.leaderNodeId
        this.consensusEngine.noteKnownLeader({
          leaderNodeId: message.leaderNodeId,
          electionTimeoutMs: this.options.electionTimeoutMaxMs
        })
      }
      return
    }

    if (!message.acceptedLearner || message.acceptedLearner.nodeId !== this.options.identity.publicKeyId) {
      throw new Error("Join response accepted a different learner identity")
    }
    if (message.acceptedLearner.machineId !== this.transportIdentity?.machineId) {
      throw new Error("Join response machineId does not match the local machine identity")
    }

    for (const voter of message.membership?.voters ?? []) {
      await this.#ensureJoinedNode(voter)
    }

    await this.#adoptJoinMembershipSnapshot(message)

    this.joinState = {
      accepted: true,
      leaderNodeId: message.leaderNodeId ?? null
    }
    if (message.leaderNodeId) {
      this.consensusEngine.noteKnownLeader({
        leaderNodeId: message.leaderNodeId,
        electionTimeoutMs: this.options.electionTimeoutMaxMs
      })
    }
  }

  async #validateJoinRequest(session, message) {
    if (message.v !== 1) {
      throw new Error("Join request version must be 1")
    }
    if (message.type !== "replicore.join-request") {
      throw new Error("Join request type must be replicore.join-request")
    }
    if (message.clusterId !== this.options.clusterId) {
      throw new Error("Join request clusterId does not match this cluster")
    }
    if (message.role !== "learner") {
      throw new Error("Join request role must be learner")
    }
    if (typeof message.machineId !== "string" || !/^[0-9a-f]{64}$/i.test(message.machineId)) {
      throw new Error("Join request machineId must be a 32-byte hex string")
    }
    if (typeof message.identityPublicKey !== "string" || !/^[0-9a-f]{64}$/i.test(message.identityPublicKey)) {
      throw new Error("Join request identityPublicKey must be a 32-byte hex string")
    }
    if (typeof message.feedKey !== "string" || !/^[0-9a-f]+$/i.test(message.feedKey)) {
      throw new Error("Join request feedKey must be a hex string")
    }
    if (typeof message.noisePublicKey !== "string" || !/^[0-9a-f]{64}$/i.test(message.noisePublicKey)) {
      throw new Error("Join request noisePublicKey must be a 32-byte hex string")
    }
    if (typeof message.nonce !== "string" || message.nonce.length === 0) {
      throw new Error("Join request nonce is required")
    }
    if (typeof message.issuedAt !== "string") {
      throw new Error("Join request issuedAt must be an ISO-8601 string")
    }
    if (typeof message.signature !== "string" || message.signature.length === 0) {
      throw new Error("Join request signature is required")
    }

    const issuedAt = new Date(message.issuedAt)
    if (Number.isNaN(issuedAt.getTime())) {
      throw new Error("Join request issuedAt must be a valid ISO-8601 string")
    }
    if (Math.abs(Date.now() - issuedAt.getTime()) > JOIN_REQUEST_MAX_SKEW_MS) {
      throw new Error("Join request issuedAt is outside the allowed freshness window")
    }

    const replayKey = `${message.machineId}:${message.nonce}`
    if (this.seenJoinRequestNonces.has(replayKey)) {
      throw new Error("Join request nonce was already used")
    }

    const identityPublicKey = Buffer.from(message.identityPublicKey, "hex")
    const feedKey = Hypercore.key(identityPublicKey).toString("hex")
    if (feedKey !== message.feedKey) {
      throw new Error("Join request feedKey does not match the supplied identity public key")
    }

    const derivedJoinIdentity = await deriveJoinKeyPair({
      clusterSecret: this.options.clusterSecret,
      machineId: message.machineId
    })
    const payload = {
      v: message.v,
      type: message.type,
      clusterId: message.clusterId,
      machineId: message.machineId,
      identityPublicKey: message.identityPublicKey,
      feedKey: message.feedKey,
      noisePublicKey: message.noisePublicKey,
      role: message.role,
      issuedAt: message.issuedAt,
      nonce: message.nonce
    }
    if (
      !verifyPayload(
        derivedJoinIdentity.publicKey,
        Buffer.from(canonicalize(payload)),
        message.signature
      )
    ) {
      throw new Error("Join request signature is invalid")
    }

    const liveNoisePublicKey = session.conn.remotePublicKey.toString("hex")
    if (liveNoisePublicKey !== message.noisePublicKey) {
      throw new Error("Join request noisePublicKey does not match the live connection")
    }

    const nodeId = keyIdFromPublicKey(identityPublicKey)
    if (this.#currentMembershipRole(nodeId) === "removed" || this.#isRevokedNode(nodeId)) {
      const error = new Error("Removed node identities cannot rejoin through learner admission")
      error.code = "REMOVED_IDENTITY"
      throw error
    }

    this.seenJoinRequestNonces.add(replayKey)
    return {
      machineId: message.machineId,
      noisePublicKey: message.noisePublicKey,
      identityPublicKey,
      feedKey,
      nodeId
    }
  }

  async #adoptJoinMembershipSnapshot(message) {
    const previousRole = this.#role()
    this.membershipState = {
      current: this.#normalizeMembershipConfig({
        version: message.membership?.version ?? this.membershipState.current.version,
        voters: (message.membership?.voters ?? []).map((node) => node.nodeId),
        learners: (message.membership?.learners ?? []).map((node) => node.nodeId),
        removed: message.membership?.removed ?? []
      }),
      joint: null
    }
    await this.#persistMembershipState(this.membershipState)
    this.consensusState = await this.consensusStateStore.save({
      currentTerm: message.consensus?.currentTerm ?? this.consensusState.currentTerm,
      commitIndex: message.consensus?.commitIndex ?? this.consensusState.commitIndex,
      lastApplied: message.consensus?.lastApplied ?? this.consensusState.lastApplied,
      membershipVersion: this.membershipState.current.version
    })
    await this.#refreshHeartbeatRole(previousRole)
  }

  #membershipEntries() {
    const entries = this.options.authorizedNodes.map((node) => ({
      nodeId: node.nodeId,
      feedKey: node.feedKey,
      role: this.#membershipRole(node.nodeId)
    }))
    if (this.#isLearner() && !entries.some((entry) => entry.nodeId === this.options.identity.publicKeyId)) {
      entries.push({
        nodeId: this.options.identity.publicKeyId,
        feedKey: this.options.identity.feedKey,
        role: "learner"
      })
    }
    return entries.sort((left, right) => left.nodeId.localeCompare(right.nodeId))
  }

  #voterNodes() {
    return this.options.authorizedNodes.filter((node) => this.#effectiveVoterNodeIds().includes(node.nodeId))
  }

  #effectiveVoterNodeIds() {
    if (!this.membershipState) {
      return this.options.authorizedNodes
        .map((node) => node.nodeId)
        .filter((nodeId) => !this.#isRevokedNode(nodeId))
    }
    if (!this.membershipState.joint) {
      return [...this.membershipState.current.voters]
    }
    return [...new Set([
      ...this.membershipState.joint.oldVoters,
      ...this.membershipState.joint.newVoters
    ])].sort()
  }

  #jointMembership() {
    return this.membershipState?.joint ?? null
  }

  async #loadMembershipState() {
    const currentEntry = await this.consensusBee.get("membership/current")
    const jointEntry = await this.consensusBee.get("membership/joint")
    if (currentEntry?.value) {
      return {
        current: this.#normalizeMembershipConfig(currentEntry.value),
        joint: jointEntry?.value ? this.#normalizeJointMembership(jointEntry.value) : null
      }
    }

    const initial = this.#initialMembershipConfig()
    await this.#persistMembershipState({
      current: initial,
      joint: null
    })
    this.consensusState = await this.consensusStateStore.save({
      membershipVersion: initial.version
    })
    return {
      current: initial,
      joint: null
    }
  }

  #initialMembershipConfig() {
    const configured = this.options.membership ?? {}
    const voters = (configured.voters ?? this.options.authorizedNodes
      .map((node) => node.nodeId)
      .filter((nodeId) => !this.#isRevokedNode(nodeId))).sort()
    const removed = [...new Set(configured.removed ?? [...this.revokedNodeIds])].sort()
    const learners = (configured.learners ?? this.options.authorizedNodes
      .map((node) => node.nodeId)
      .filter((nodeId) => !voters.includes(nodeId) && !removed.includes(nodeId))).sort()
    return this.#normalizeMembershipConfig({
      version: configured.version ?? 0,
      voters,
      learners,
      removed
    })
  }

  #normalizeMembershipConfig(config) {
    return {
      version: config.version ?? 0,
      voters: [...new Set(config.voters ?? [])].sort(),
      learners: [...new Set(config.learners ?? [])].sort(),
      removed: [...new Set(config.removed ?? [])].sort()
    }
  }

  #normalizeJointMembership(joint) {
    return {
      changeType: joint.changeType,
      targetNodeId: joint.targetNodeId,
      fromVersion: joint.fromVersion,
      toVersion: joint.toVersion,
      oldVoters: [...new Set(joint.oldVoters ?? [])].sort(),
      newVoters: [...new Set(joint.newVoters ?? [])].sort()
    }
  }

  async #persistMembershipState(state) {
    const batch = this.consensusBee.batch()
    await batch.put("membership/current", state.current)
    if (state.joint) {
      await batch.put("membership/joint", state.joint)
    } else {
      await batch.del("membership/joint")
    }
    await batch.flush()
  }

  async #commitMembershipChange({ changeType, targetNodeId, newVoters, learners, removed, promotion = null }) {
    const current = this.membershipState.current
    const nextVersion = current.version + 1
    const joint = {
      changeType,
      targetNodeId,
      fromVersion: current.version,
      toVersion: nextVersion,
      oldVoters: [...current.voters].sort(),
      newVoters: [...newVoters].sort()
    }
    const finalConfig = this.#normalizeMembershipConfig({
      version: nextVersion,
      voters: newVoters,
      learners,
      removed
    })
    const quorumGroups = this.#jointQuorumGroups(joint)

    const jointOperation = await this.#appendMembershipOperation({
      phase: "joint",
      changeType,
      targetNodeId,
      fromVersion: current.version,
      toVersion: nextVersion,
      oldVoters: joint.oldVoters,
      newVoters: joint.newVoters,
      learners: finalConfig.learners,
      removed: finalConfig.removed,
      quorumGroups
    })
    await this.#advanceCommittedFeed(this.options.identity.publicKeyId, jointOperation.seq + 1)
    await this.#runHeartbeat()

    const finalOperation = await this.#appendMembershipOperation({
      phase: "final",
      changeType,
      targetNodeId,
      fromVersion: current.version,
      toVersion: nextVersion,
      oldVoters: joint.oldVoters,
      newVoters: joint.newVoters,
      learners: finalConfig.learners,
      removed: finalConfig.removed,
      quorumGroups
    })
    await this.#advanceCommittedFeed(this.options.identity.publicKeyId, finalOperation.seq + 1)

    if (promotion) {
      await this.consensusBee.put("promotion/current", promotion)
    }

    await this.#runHeartbeat()
  }

  #jointQuorumGroups(joint) {
    return [
      {
        eligibleNodeIds: joint.oldVoters,
        requiredCount: this.#majoritySize(joint.oldVoters.length)
      },
      {
        eligibleNodeIds: joint.newVoters,
        requiredCount: this.#majoritySize(joint.newVoters.length)
      }
    ]
  }

  async #appendMembershipOperation({
    phase,
    changeType,
    targetNodeId,
    fromVersion,
    toVersion,
    oldVoters,
    newVoters,
    learners,
    removed,
    quorumGroups
  }) {
    if (!this.#hasReachableQuorum(quorumGroups)) {
      throw new Error("Durability requirement not met: no reachable quorum available")
    }

    let operation = null
    let ackPromise = null
    await this.#withLocalAppendLock(async () => {
      const previousOperation = await this.#previousLocalOperation()
      operation = createSignedOperation({
        kind: "membership",
        type: "put",
        key: `membership:${toVersion}:${phase}`,
        keyspace: "system",
        membership: {
          phase,
          changeType,
          targetNodeId,
          fromVersion,
          toVersion,
          oldVoters,
          newVoters,
          learners,
          removed
        },
        seq: this.#localCore().length,
        term: this.consensusState.currentTerm,
        index: this.#localCore().length,
        prevIndex: previousOperation?.index ?? -1,
        prevHash: previousOperation?.entryHash ?? null,
        feed: this.options.identity.feedKey,
        actor: this.options.identity.publicKeyId,
        secretKey: this.options.identity.secretKey,
        encryptionKey: this.#currentEncryptionKey(),
        encryptionKeyId: this.encryption.currentKeyId
      })
      ackPromise = this.durabilityWaiter.waitForGroups(
        operation.seq,
        quorumGroups,
        [this.options.identity.publicKeyId]
      )
      ackPromise.catch(() => {})
      await this.#localCore().append(operation)
    })

    await this.syncFeed(this.options.identity.publicKeyId)
    try {
      await ackPromise
    } catch (error) {
      await this.view.markSkippedEntry(this.options.identity.feedKey, operation.seq)
      await this.#runHeartbeat()
      throw error
    }
    return operation
  }

  async #applyCommittedMembershipOperation(operation) {
    const previousRole = this.#role()
    const membership = operation.membership
    if (!membership || typeof membership !== "object") {
      throw new Error("Committed membership operation is missing membership metadata")
    }

    if (membership.phase === "joint") {
      this.membershipState = {
        current: { ...this.membershipState.current },
        joint: this.#normalizeJointMembership(membership)
      }
    } else {
      this.membershipState = {
        current: this.#normalizeMembershipConfig({
          version: membership.toVersion,
          voters: membership.newVoters,
          learners: membership.learners,
          removed: membership.removed
        }),
        joint: null
      }
      this.consensusState = await this.consensusStateStore.save({
        membershipVersion: membership.toVersion
      })
    }

    await this.#persistMembershipState(this.membershipState)
    await this.#refreshHeartbeatRole(previousRole)
  }

  async #membershipStatusSnapshot() {
    const heartbeats = await this.view.getHeartbeats()
    return this.#membershipStatus(heartbeats)
  }

  #majoritySize(size) {
    return majoritySize(size)
  }

  async #refreshHeartbeatRole(previousRole) {
    const currentRole = this.#role()
    if (previousRole !== "voter" && currentRole === "voter") {
      if (!this.heartbeatTimer && !this.closing) {
        this.#scheduleElectionTimer()
        await this.#runHeartbeat()
        this.heartbeatTimer = setInterval(() => {
          void this.#runHeartbeat().catch((error) => {
            if (!this.closing && error?.code !== "SESSION_CLOSED") {
              throw error
            }
          })
        }, this.options.heartbeatIntervalMs)
        this.heartbeatTimer.unref?.()
      }
      return
    }

    if (previousRole === "voter" && currentRole !== "voter" && this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
    if (previousRole === "voter" && currentRole !== "voter" && this.electionTimer) {
      clearTimeout(this.electionTimer)
      this.electionTimer = null
    }
  }
}
