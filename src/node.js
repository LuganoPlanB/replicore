import { mkdir } from "node:fs/promises"
import { join } from "node:path"

import Corestore from "corestore"
import Hyperbee from "hyperbee"
import Hyperswarm from "hyperswarm"

import { deriveTopic } from "./config.js"
import {
  createSignedOperation,
  decryptOperationValue,
  validateOperation,
  verifySignedOperation
} from "./operation.js"
import { MaterializedView } from "./materialized-view.js"

const RPC_EXTENSION = "planb-cleard-rpc-v1"

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
    this.swarm = null
    this.viewBee = null
    this.view = null
    this.discovery = null
    this.feedCores = new Map()
    this.heartbeatTimer = null
    this.syncPromises = new Map()
    this.pendingSync = new Set()
    this.rpcExtensions = new Map()
    this.requestId = 0
    this.inflightRequests = new Map()
    this.ackWaiters = new Map()
    this.lastHeartbeatByNode = new Map()
    this.connections = new Set()
    this.closing = false
    this.lastDurableSequence = -1
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
      core.on("append", () => {
        void this.syncFeed(node.nodeId).catch((error) => {
          if (!this.closing && error?.code !== "REQUEST_CANCELLED" && error?.code !== "SESSION_CLOSED") {
            throw error
          }
        })
      })
      this.feedCores.set(node.nodeId, core)
      this.rpcExtensions.set(node.nodeId, this.#registerRpcExtension(core))
    }

    await this.#startNetworking()

    for (const node of this.options.authorizedNodes) {
      if (this.#isRevokedNode(node.nodeId)) continue
      await this.syncFeed(node.nodeId)
    }

    await this.#appendHeartbeat()
    this.heartbeatTimer = setInterval(() => {
      void this.#appendHeartbeat()
    }, this.options.heartbeatIntervalMs)
    this.heartbeatTimer.unref?.()
  }

  /**
   * Temporarily disconnect this node from the swarm without closing storage.
   * Intended for diagnostics and tests that need live isolation.
   */
  async suspendNetworking() {
    if (!this.swarm) return

    for (const conn of this.connections) {
      conn.destroy()
    }
    this.connections.clear()

    if (this.discovery) {
      await this.discovery.destroy()
      this.discovery = null
    }

    await this.swarm.destroy()
    this.swarm = null
  }

  /**
   * Rejoin the swarm after a temporary networking suspension.
   */
  async resumeNetworking() {
    if (this.closing || this.swarm) return

    await this.#startNetworking()
    await this.#appendHeartbeat()
  }

  get status() {
    return {
      nodeId: this.options.identity.publicKeyId,
      leader: this.currentLeader(),
      knownHeartbeats: [...this.lastHeartbeatByNode.keys()],
      connections: this.connections.size,
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
      connections: this.connections.size,
      lastDurableSequence: this.lastDurableSequence,
      encryptionKeyId: this.encryption.currentKeyId,
      knownPeerNodeIds: [...this.connections].map((conn) => conn.remotePublicKey.toString("hex")),
      feeds,
      heartbeats
    }
  }

  getWritersStatus() {
    return {
      currentLeader: this.currentLeader(),
      revokedNodeIds: [...this.revokedNodeIds],
      encryptionKeyId: this.encryption.currentKeyId,
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
    this.#rejectPendingWrites(new Error("Node is closing"))
    this.pendingSync.clear()
    await this.suspendNetworking()
    for (const extension of this.rpcExtensions.values()) extension.destroy()
    await Promise.allSettled([...this.feedCores.values()].map((core) => core.close()))
    await Promise.allSettled(this.syncPromises.values())
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
          appliedFeeds: operation.heartbeat?.appliedFeeds ?? {}
        })
      } else if (operation.kind === "kv" && nodeId !== this.options.identity.publicKeyId) {
        this.#sendAck(nodeId, operation.seq)
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
        appliedFeeds: await this.#appliedFeeds()
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

  async #startNetworking() {
    this.swarm = new Hyperswarm(this.options.bootstrap ? { bootstrap: this.options.bootstrap } : {})
    this.swarm.on("connection", (conn) => {
      this.connections.add(conn)
      conn.once("close", () => this.connections.delete(conn))
      this.store.replicate(conn)
    })

    this.discovery = this.swarm.join(deriveTopic(this.options), { client: true, server: true })
    await this.discovery.flushed()
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

    const ackPromise = this.#waitForFollowerAck(operation.seq, followerRequirement)
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
    const extension = this.rpcExtensions.get(leader)
    const peer = leaderCore.peers[0]
    if (!peer) {
      throw new Error(`Current leader ${leader} is not reachable`)
    }

    const requestId = `${this.options.identity.publicKeyId}-${++this.requestId}`
    const response = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.inflightRequests.delete(requestId)
        reject(new Error(`Timed out forwarding write request ${requestId}`))
      }, this.options.durability.timeoutMs)
      this.inflightRequests.set(requestId, {
        resolve,
        reject,
        timer
      })
    })
    response.catch(() => {})

    extension.send(
      {
        type: "write-request",
        requestId,
        from: this.options.identity.publicKeyId,
        request
      },
      peer
    )

    return response
  }

  /**
   * @param {number} seq
   * @param {number} required
   */
  async #waitForFollowerAck(seq, required) {
    const key = `${this.options.identity.feedKey}:${seq}`
    const existing = this.ackWaiters.get(key) ?? {
      nodes: new Set(),
      resolve: null,
      reject: null,
      timer: null,
      promise: null
    }

    if (!existing.promise) {
      existing.promise = new Promise((resolve, reject) => {
        existing.resolve = resolve
        existing.reject = reject
        existing.timer = setTimeout(() => {
          this.ackWaiters.delete(key)
          reject(new Error(`Timed out waiting for follower acknowledgement for sequence ${seq}`))
        }, this.options.durability.timeoutMs)
      })
      this.ackWaiters.set(key, existing)
    }

    if (existing.nodes.size >= required) {
      clearTimeout(existing.timer)
      this.ackWaiters.delete(key)
      this.lastDurableSequence = Math.max(this.lastDurableSequence, seq)
      return
    }

    return existing.promise
  }

  /**
   * @param {string} nodeId
   * @param {number} seq
   */
  #recordAck(nodeId, seq) {
    const key = `${this.options.identity.feedKey}:${seq}`
    const waiter = this.ackWaiters.get(key)
    if (!waiter) return

    waiter.nodes.add(nodeId)
    if (waiter.nodes.size >= this.options.durability.requiredFollowerAcks) {
      clearTimeout(waiter.timer)
      this.ackWaiters.delete(key)
      this.lastDurableSequence = Math.max(this.lastDurableSequence, seq)
      waiter.resolve()
    }
  }

  /**
   * Reject outstanding write waits so shutdown does not leave live timers behind.
   *
   * @param {Error} error
   */
  #rejectPendingWrites(error) {
    for (const pending of this.inflightRequests.values()) {
      clearTimeout(pending.timer)
      pending.reject(error)
    }
    this.inflightRequests.clear()

    for (const waiter of this.ackWaiters.values()) {
      clearTimeout(waiter.timer)
      waiter.reject(error)
    }
    this.ackWaiters.clear()
  }

  /**
   * @param {string} nodeId
   * @param {number} seq
   */
  #sendAck(nodeId, seq) {
    const core = this.feedCores.get(nodeId)
    const extension = this.rpcExtensions.get(nodeId)
    const peer = core.peers[0]
    if (!peer) return

    extension.send(
      {
        type: "write-ack",
        from: this.options.identity.publicKeyId,
        feedKey: nodeId,
        seq
      },
      peer
    )
  }

  /**
   * @param {import("hypercore")} core
   */
  #registerRpcExtension(core) {
    return core.registerExtension(RPC_EXTENSION, {
      encoding: "json",
      onmessage: async (message, peer) => {
        try {
          if (message.type === "write-request") {
            if (this.currentLeader() !== this.options.identity.publicKeyId) {
              throw new Error("This node is not the current leader")
            }
            const result =
              message.request.action === "put"
                ? await this.#appendKvOperation(
                    "put",
                    message.request.key,
                    message.request.value,
                    message.request.options ?? {}
                  )
                : await this.#appendKvOperation(
                    "delete",
                    message.request.key,
                    undefined,
                    message.request.options ?? {}
                  )

            const responseExtension = this.rpcExtensions.get(message.from)
            responseExtension?.send(
              { type: "write-response", requestId: message.requestId, ok: true, result },
              peer
            )
            return
          }

          if (message.type === "write-response") {
            const pending = this.inflightRequests.get(message.requestId)
            if (!pending) return
            clearTimeout(pending.timer)
            this.inflightRequests.delete(message.requestId)
            if (message.ok) pending.resolve(message.result)
            else pending.reject(new Error(message.error))
            return
          }

          if (message.type === "write-ack") {
            this.#recordAck(message.from, message.seq)
          }
        } catch (error) {
          if (message.type === "write-request") {
            const responseExtension = this.rpcExtensions.get(message.from)
            responseExtension?.send(
              {
                type: "write-response",
                requestId: message.requestId,
                ok: false,
                error: error instanceof Error ? error.message : String(error)
              },
              peer
            )
          }
        }
      }
    })
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
