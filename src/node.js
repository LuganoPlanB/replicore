import { mkdir } from "node:fs/promises"
import { join } from "node:path"

import Corestore from "corestore"
import Hyperbee from "hyperbee"
import Hyperswarm from "hyperswarm"

import { deriveTopic } from "./config.js"
import { decryptOperationValue, createSignedOperation, verifySignedOperation } from "./operation.js"
import { MaterializedView } from "./materialized-view.js"

/**
 * Minimal single-leader node for milestone 1.
 */
export class HolepunchSwarmNode {
  /**
   * @param {{
   *   dataDir: string,
   *   clusterId: string,
   *   topicSalt: string,
   *   role: "leader" | "follower",
   *   identity: { publicKeyId: string, publicKeyPem: string, privateKeyPem: string },
   *   authorizedWriter: { publicKeyId: string, publicKeyPem: string },
   *   encryptionKey: Buffer,
   *   leaderFeedKey?: string,
   *   bootstrap?: Array<string | { host: string, port: number }>
   * }} options
   */
  constructor(options) {
    this.options = options
    this.store = null
    this.swarm = null
    this.logCore = null
    this.viewBee = null
    this.view = null
    this.discovery = null
    this.syncing = null
    this.pendingSync = false
    this.connections = new Set()
  }

  async start() {
    await mkdir(this.options.dataDir, { recursive: true })
    await mkdir(join(this.options.dataDir, "corestore"), { recursive: true })

    this.store = new Corestore(join(this.options.dataDir, "corestore"))
    await this.store.ready()

    this.logCore =
      this.options.role === "leader"
        ? this.store.get({ name: "leader-log", valueEncoding: "json" })
        : this.store.get({ key: Buffer.from(this.options.leaderFeedKey, "hex"), valueEncoding: "json" })

    const viewCore = this.store.get({ name: "derived-view" })
    this.viewBee = new Hyperbee(viewCore, { keyEncoding: "utf-8", valueEncoding: "json" })
    this.view = new MaterializedView(this.viewBee)

    await Promise.all([this.logCore.ready(), this.viewBee.ready()])

    this.swarm = new Hyperswarm(this.options.bootstrap ? { bootstrap: this.options.bootstrap } : {})
    this.swarm.on("connection", (conn) => {
      this.connections.add(conn)
      conn.once("close", () => this.connections.delete(conn))
      this.store.replicate(conn)
    })

    this.discovery = this.swarm.join(deriveTopic(this.options), {
      client: true,
      server: true
    })

    await this.discovery.flushed()
    this.logCore.on("append", () => {
      void this.sync()
    })
    await this.sync()
  }

  get leaderFeedKey() {
    return Buffer.from(this.logCore.key).toString("hex")
  }

  get status() {
    return {
      role: this.options.role,
      leaderFeedKey: this.leaderFeedKey,
      logLength: this.logCore.length,
      connections: this.connections.size
    }
  }

  /**
   * @param {string} key
   * @param {unknown} value
   * @param {{ keyspace?: string, ttlMs?: number }} [options]
   */
  async put(key, value, options = {}) {
    this.#assertLeader()

    const operation = createSignedOperation({
      type: "put",
      key,
      keyspace: options.keyspace,
      value,
      seq: this.logCore.length,
      feed: this.leaderFeedKey,
      actor: this.options.identity.publicKeyId,
      privateKeyPem: this.options.identity.privateKeyPem,
      encryptionKey: this.options.encryptionKey,
      ttlMs: options.ttlMs
    })

    await this.logCore.append(operation)
    await this.sync()
    return operation
  }

  /**
   * @param {string} key
   * @param {{ keyspace?: string, ttlMs?: number }} [options]
   */
  async delete(key, options = {}) {
    this.#assertLeader()

    const operation = createSignedOperation({
      type: "delete",
      key,
      keyspace: options.keyspace,
      seq: this.logCore.length,
      feed: this.leaderFeedKey,
      actor: this.options.identity.publicKeyId,
      privateKeyPem: this.options.identity.privateKeyPem,
      encryptionKey: this.options.encryptionKey,
      ttlMs: options.ttlMs
    })

    await this.logCore.append(operation)
    await this.sync()
    return operation
  }

  /**
   * @param {string} key
   * @param {{ keyspace?: string }} [options]
   */
  async get(key, options = {}) {
    const current = await this.view.getCurrent(options.keyspace ?? "default", key)
    if (!current) return null
    if (current.metadata.deleted) {
      return { ...current.metadata, value: null }
    }

    return {
      ...current.metadata,
      value: decryptOperationValue(
        {
          type: "put",
          value: current.encryptedValue
        },
        this.options.encryptionKey
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

  async sync() {
    if (this.syncing) {
      this.pendingSync = true
      return this.syncing
    }

    this.syncing = this.#syncLoop()

    try {
      await this.syncing
    } finally {
      this.syncing = null
      if (this.pendingSync) {
        this.pendingSync = false
        await this.sync()
      }
    }
  }

  async close() {
    for (const conn of this.connections) {
      conn.destroy()
    }

    if (this.swarm) await this.swarm.destroy()
    if (this.viewBee) await this.viewBee.close()
    if (this.logCore) await this.logCore.close()
    if (this.store) await this.store.close()
  }

  async #syncLoop() {
    const feedKey = this.leaderFeedKey
    let applied = await this.view.getApplied(feedKey)

    while (applied < this.logCore.length) {
      const operation = await this.logCore.get(applied)
      if (!verifySignedOperation(operation, this.options.authorizedWriter.publicKeyPem)) {
        throw new Error(`Invalid operation at sequence ${applied}`)
      }
      await this.view.apply(operation, feedKey)
      applied += 1
    }
  }

  #assertLeader() {
    if (this.options.role !== "leader") {
      throw new Error("Only the leader may append K/V operations in milestone 1")
    }
  }
}
