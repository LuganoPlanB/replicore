import { createHash } from "node:crypto"
import { mkdir } from "node:fs/promises"
import { join } from "node:path"

import Corestore from "corestore"
import Hyperbee from "hyperbee"

import { canonicalize } from "./canonical.js"
import { DurabilityWaiter } from "./durability-waiter.js"
import {
  createSignedOperation,
  decryptOperationValue,
  validateOperation,
  verifySignedOperation
} from "./operation.js"
import { MaterializedView } from "./materialized-view.js"
import { NodeRpcRouter } from "./node-rpc.js"
import { SwarmNetwork } from "./swarm-network.js"
import { deriveTopic } from "./config.js"

/**
 * Minimal multi-node swarm with one feed per node and leader-only writes.
 */
export class HolepunchSwarmNode {
  /**
   * @param {{
   *   dataDir: string,
   *   clusterId: string,
   *   topicSalt: string,
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
    this.network = null
    this.rpc = null
    this.viewBee = null
    this.view = null
    this.feedCores = new Map()
    this.heartbeatTimer = null
    this.heartbeatPromise = null
    this.syncPromises = new Map()
    this.pendingSync = new Set()
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

    this.store = new Corestore(join(this.options.dataDir, "corestore"))
    await this.store.ready()

    const viewCore = this.store.get({ name: "derived-view" })
    this.viewBee = new Hyperbee(viewCore, { keyEncoding: "utf-8", valueEncoding: "json" })
    this.view = new MaterializedView(this.viewBee)
    await this.viewBee.ready()
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
      }
      core.on("append", () => {
        void this.syncFeed(node.nodeId).catch((error) => {
          if (!this.closing && error?.code !== "REQUEST_CANCELLED" && error?.code !== "SESSION_CLOSED") {
            throw error
          }
        })
      })
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
    await this.#runHeartbeat()
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
    return {
      nodeId: this.options.identity.publicKeyId,
      leader: this.currentLeader(),
      knownHeartbeats: [...this.lastHeartbeatByNode.keys()],
      connections: this.network?.connectionCount ?? 0,
      encryptionKeyId: this.encryption.currentKeyId,
      feeds: Object.fromEntries(
        [...this.feedCores.entries()].map(([nodeId, core]) => [nodeId, core.length])
      )
    }
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
      const heartbeat = heartbeats[node.nodeId] ?? null
      feeds[node.nodeId] = {
        feedKey: node.feedKey,
        length: core.length,
        applied,
        lag: core.length - applied,
        connectedPeers: core.peers.length,
        alive: heartbeat ? now - new Date(heartbeat.ts).getTime() <= this.options.heartbeatTtlMs : false,
        heartbeatAgeMs: heartbeat ? now - new Date(heartbeat.ts).getTime() : null
      }
    }

    return {
      nodeId: this.options.identity.publicKeyId,
      leader: this.currentLeader(),
      connections: this.network?.connectionCount ?? 0,
      lastDurableSequence: this.durabilityWaiter.status().lastDurableSequence,
      encryptionKeyId: this.encryption.currentKeyId,
      knownPeerNodeIds: this.network?.knownPeerPublicKeys ?? [],
      membership: this.#membershipStatus(heartbeats),
      network: this.network?.networkStatus() ?? { policyActive: false, allowedNodeIds: [], peers: {} },
      readStatus: this.#readStatus(),
      feeds,
      heartbeats
    }
  }

  getWritersStatus() {
    return {
      currentLeader: this.currentLeader(),
      revokedNodeIds: [...this.revokedNodeIds],
      encryptionKeyId: this.encryption.currentKeyId,
      membershipFingerprint: this.#membershipFingerprint(),
      authorizedNodes: this.options.authorizedNodes.map((node) => ({
        nodeId: node.nodeId,
        feedKey: node.feedKey,
        revoked: this.#isRevokedNode(node.nodeId)
      }))
    }
  }

  async getLeaderStatus() {
    const leader = this.currentLeader()
    const heartbeats = await this.view.getHeartbeats()

    return {
      nodeId: this.options.identity.publicKeyId,
      currentLeader: leader,
      reachable: leader ? this.#isLeaderReachable(leader) : false,
      heartbeat: leader ? (heartbeats[leader] ?? null) : null
    }
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
    let applied = await this.view.getApplied(node.feedKey)

    while (applied < core.length) {
      if (this.closing) return

      const operation = await core.get(applied)
      validateOperation(operation, node, { revokedNodeIds: this.revokedNodeIds })
      if (!verifySignedOperation(operation, node.publicKey)) {
        throw new Error(`Invalid operation at sequence ${applied} for ${nodeId}`)
      }
      await this.view.apply(operation, node.feedKey)
      if (operation.kind === "heartbeat") {
        this.lastHeartbeatByNode.set(operation.actor, {
          ts: operation.ts,
          feed: node.feedKey,
          seq: operation.seq,
          appliedFeeds: operation.heartbeat?.appliedFeeds ?? {},
          membershipFingerprint: operation.heartbeat?.membershipFingerprint ?? null
        })
      } else if (operation.kind === "kv" && nodeId !== this.options.identity.publicKeyId) {
        void this.#sendAck(nodeId, operation.seq)
      }
      applied += 1
    }
  }

  async #appendHeartbeat() {
    if (this.closing) return

    const leader = this.currentLeader()
    const operation = createSignedOperation({
      kind: "heartbeat",
      type: "put",
      key: `heartbeat:${this.options.identity.publicKeyId}`,
      keyspace: "system",
      seq: this.#localCore().length,
      feed: this.options.identity.feedKey,
      actor: this.options.identity.publicKeyId,
      secretKey: this.options.identity.secretKey,
      encryptionKey: this.#currentEncryptionKey(),
      encryptionKeyId: this.encryption.currentKeyId,
      heartbeat: {
        observedLeader: leader,
        reachableLeader: leader === null ? false : this.#isLeaderReachable(leader),
        appliedFeeds: await this.#appliedFeeds(),
        membershipFingerprint: this.#membershipFingerprint()
      }
    })

    if (this.closing) return

    try {
      await this.#localCore().append(operation)
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
    this.network ??= new SwarmNetwork({
      bootstrap: this.options.bootstrap,
      topic: deriveTopic(this.options),
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

    const operation = createSignedOperation({
      kind: "kv",
      type,
      key,
      keyspace: options.keyspace,
      value,
      seq: this.#localCore().length,
      feed: this.options.identity.feedKey,
      actor: this.options.identity.publicKeyId,
      secretKey: this.options.identity.secretKey,
      encryptionKey: this.#currentEncryptionKey(),
      encryptionKeyId: this.encryption.currentKeyId,
      ttlMs: options.ttlMs
    })

    const ackPromise = this.durabilityWaiter.waitFor(operation.seq, followerRequirement)
    await this.#localCore().append(operation)
    await this.syncFeed(this.options.identity.publicKeyId)
    await ackPromise
    return operation
  }

  async #forwardWrite(request) {
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
      applied[node.feedKey] = await this.view.getApplied(node.feedKey)
    }
    return applied
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
