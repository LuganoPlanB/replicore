import { createHash } from "node:crypto"
import { mkdir } from "node:fs/promises"
import { join } from "node:path"

import Corestore from "corestore"
import Hyperbee from "hyperbee"

import { canonicalize } from "./canonical.js"
import { ConsensusStateStore } from "./consensus-state.js"
import { DurabilityWaiter } from "./durability-waiter.js"
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
import { SwarmNetwork } from "./swarm-network.js"
import { deriveTopic } from "./config.js"
import { resolveTransportIdentity } from "./transport-identity.js"

/**
 * Minimal multi-node swarm with one feed per node and leader-only writes.
 */
export class HolepunchSwarmNode {
  /**
   * @param {{
   *   dataDir: string,
   *   clusterId: string,
   *   clusterSecret?: Buffer,
   *   role?: "voter" | "learner",
   *   machineId?: string,
   *   nodeIdentitySeed?: Buffer,
   *   topicSalt?: string,
   *   identity: { publicKeyId: string, publicKey: Buffer, secretKey: Buffer, feedKey: string },
   *   authorizedNodes: Array<{ nodeId: string, publicKey: Buffer, feedKey: string }>,
   *   revokedNodeIds?: string[],
   *   encryption?: { currentKeyId: string, keys: Record<string, Buffer> },
   *   encryptionKey?: Buffer,
   *   bootstrap?: Array<string | { host: string, port: number }>,
   *   heartbeatIntervalMs?: number,
   *   heartbeatTtlMs?: number,
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
      forwarding: true,
      durability: {
        requiredFollowerAcks: 1,
        timeoutMs: 5000,
        ...options.durability
      },
      ...options
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
    this.network = null
    this.rpc = null
    this.transportIdentity = null
    this.viewBee = null
    this.view = null
    this.feedCores = new Map()
    this.heartbeatTimer = null
    this.heartbeatPromise = null
    this.syncPromises = new Map()
    this.pendingSync = new Set()
    this.localAppendLock = Promise.resolve()
    this.durabilityWaiter = new DurabilityWaiter({
      feedKey: this.options.identity.feedKey,
      timeoutMs: this.options.durability.timeoutMs,
      requiredFollowerAcks: this.options.durability.requiredFollowerAcks
    })
    this.lastHeartbeatByNode = new Map()
    this.closing = false
  }

  async start() {
    await mkdir(this.options.dataDir, { recursive: true })
    await mkdir(join(this.options.dataDir, "corestore"), { recursive: true })
    this.transportIdentity = await resolveTransportIdentity({
      dataDir: this.options.dataDir,
      clusterSecret: this.options.clusterSecret,
      machineId: this.options.machineId,
      nodeIdentitySeed: this.options.nodeIdentitySeed
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
    this.rpc = new NodeRpcRouter({
      localNodeId: this.options.identity.publicKeyId,
      timeoutMs: this.options.durability.timeoutMs,
      ackDelayMs: this.options.ackDelayMs,
      onWriteRequest: async (message) => {
        if (this.currentLeader() !== this.options.identity.publicKeyId) {
          throw new Error("This node is not the current leader")
        }

        return message.request.action === "put"
          ? this.#appendKvOperation("put", message.request.key, message.request.value, message.request.options ?? {})
          : this.#appendKvOperation("delete", message.request.key, undefined, message.request.options ?? {})
      },
      onWriteAck: (nodeId, seq) => this.#recordAck(nodeId, seq)
    })

    for (const node of this.options.authorizedNodes) {
      if (this.#isRevokedNode(node.nodeId)) continue
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
        core.on("peer-add", (peer) => this.network?.trackPeer(true, node.nodeId, peer))
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
    const now = Date.now()
    return this.options.authorizedNodes
      .filter((node) => {
        if (this.#isRevokedNode(node.nodeId)) return false
        if (node.nodeId === this.options.identity.publicKeyId) return true
        const heartbeat = this.lastHeartbeatByNode.get(node.nodeId)
        if (!heartbeat) return false
        return now - new Date(heartbeat.ts).getTime() <= this.options.heartbeatTtlMs
      })
      .map((node) => node.nodeId)
      .sort()[0] ?? null
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
      const core = this.feedCores.get(node.nodeId)
      if (!core) continue
      const applied = await this.view.getApplied(node.feedKey)
      const staged = await this.view.getStagedSummary(node.feedKey)
      const heartbeat = heartbeats[node.nodeId] ?? null
      feeds[node.nodeId] = {
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

    return buildReplicationStatus({
      nodeId: this.options.identity.publicKeyId,
      role: this.#role(),
      leader: this.currentLeader(),
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
      authorizedNodes: this.options.authorizedNodes.map((node) => ({
        nodeId: node.nodeId,
        feedKey: node.feedKey,
        revoked: this.#isRevokedNode(node.nodeId)
      }))
    })
  }

  async getLeaderStatus() {
    const leader = this.currentLeader()
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

  async submitPromotionCredential(credential) {
    if (!this.#isLearner()) {
      throw new Error("Only learners may accept promotion credentials")
    }

    const summary = validatePromotionCredential(credential, {
      clusterId: this.options.clusterId,
      membershipVersion: this.consensusState.membershipVersion,
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
    await Promise.allSettled(this.heartbeatPromise ? [this.heartbeatPromise] : [])
    this.#rejectPendingWrites(new Error("Node is closing"))
    this.pendingSync.clear()
    await this.suspendNetworking()
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
      }
      rawApplied += 1
    }
  }

  async #appendHeartbeat() {
    if (this.closing) return
    if (this.#isLearner()) return

    const leader = this.currentLeader()
    const heartbeat = {
      observedLeader: leader,
      reachableLeader: leader === null ? false : this.#isLeaderReachable(leader),
      appliedFeeds: await this.#appliedFeeds(),
      rejectedFeeds: await this.#rejectedFeeds(),
      membershipFingerprint: this.#membershipFingerprint()
    }

    if (this.closing) return

    try {
      await this.#withLocalAppendLock(async () => {
        const previousOperation = await this.#previousLocalOperation()
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

  async #startNetworking() {
    const topic = await deriveTopic(this.options)
    this.network ??= new SwarmNetwork({
      bootstrap: this.options.bootstrap,
      topic,
      keyPair: this.transportIdentity?.keyPair,
      localNodeId: this.options.identity.publicKeyId,
      authorizedNodes: this.options.authorizedNodes,
      isRevokedNode: (nodeId) => this.#isRevokedNode(nodeId),
      replicateConnection: (conn) => this.store.replicate(conn),
      networkPolicy: this.options.networkPolicy ?? null
    })
    await this.network.start()
  }

  async #appendKvOperation(type, key, value, options) {
    const followerRequirement = this.options.durability.requiredFollowerAcks
    if (this.#aliveFollowers().length < followerRequirement) {
      throw new Error("Durability requirement not met: no reachable follower available")
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
      ackPromise = this.durabilityWaiter.waitFor(operation.seq, followerRequirement)
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
    if (!this.options.forwarding) {
      throw new Error("Write forwarding is disabled on this node")
    }

    const leader = this.currentLeader()
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
    this.durabilityWaiter.record(nodeId, seq)
  }

  #localCore() {
    return this.feedCores.get(this.options.identity.publicKeyId)
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
    const node = this.options.authorizedNodes.find((entry) => entry.nodeId === nodeId)
    if (!node) throw new Error(`Unknown authorized node ${nodeId}`)
    if (this.#isRevokedNode(nodeId)) {
      throw new Error(`Revoked node ${nodeId} is not allowed to replicate`)
    }
    return node
  }

  #aliveFollowers() {
    const leader = this.options.identity.publicKeyId
    const now = Date.now()
    return [...this.lastHeartbeatByNode.entries()]
      .filter(([nodeId]) => nodeId !== leader)
      .filter(([nodeId]) => !this.#isRevokedNode(nodeId))
      .filter(([, heartbeat]) => now - new Date(heartbeat.ts).getTime() <= this.options.heartbeatTtlMs)
      .filter(([nodeId]) => this.#isLeaderReachable(nodeId))
      .map(([nodeId]) => nodeId)
  }

  #role() {
    return this.#isLearner() ? "learner" : "voter"
  }

  #isLearner() {
    return !this.options.authorizedNodes.some(
      (node) => node.nodeId === this.options.identity.publicKeyId
    )
  }

  #createLearnerWriteError() {
    const error = new Error("This node is a read-only learner and cannot accept or proxy writes")
    error.code = "READ_ONLY_LEARNER"
    error.statusCode = 403
    error.leader = this.currentLeader()
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

  #isLeaderReachable(nodeId) {
    if (this.#isRevokedNode(nodeId)) return false
    if (nodeId === this.options.identity.publicKeyId) return true
    const core = this.feedCores.get(nodeId)
    return !!core?.peers?.length
  }

  async #appliedFeeds() {
    const applied = {}
    for (const node of this.options.authorizedNodes) {
      if (this.#isRevokedNode(node.nodeId)) continue
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

    for (const node of this.options.authorizedNodes) {
      if (this.#isRevokedNode(node.nodeId)) continue
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
      }
      committedApplied += 1
    }
  }

  #membershipFingerprint() {
    const membership = this.options.authorizedNodes.map((node) => ({
      nodeId: node.nodeId,
      feedKey: node.feedKey,
      revoked: this.#isRevokedNode(node.nodeId)
    }))
    return createHash("sha256").update(canonicalize(membership)).digest("hex")
  }

  #membershipStatus(heartbeats) {
    const localFingerprint = this.#membershipFingerprint()
    const peerFingerprints = {}
    const mismatchedNodeIds = []
    const matchingNodeIds = []

    for (const node of this.options.authorizedNodes) {
      if (node.nodeId === this.options.identity.publicKeyId) continue

      const fingerprint = heartbeats[node.nodeId]?.membershipFingerprint ?? null
      peerFingerprints[node.nodeId] = fingerprint
      if (fingerprint === null) continue
      if (fingerprint === localFingerprint) matchingNodeIds.push(node.nodeId)
      else mismatchedNodeIds.push(node.nodeId)
    }

    return {
      localFingerprint,
      peerFingerprints,
      mismatchedNodeIds: mismatchedNodeIds.sort(),
      matchingNodeIds: matchingNodeIds.sort()
    }
  }

  #readStatus() {
    const leader = this.currentLeader()
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
}
