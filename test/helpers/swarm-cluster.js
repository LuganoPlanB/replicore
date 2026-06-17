import { mkdtemp, rm } from "node:fs/promises"
import { randomBytes, createHash } from "node:crypto"
import os from "node:os"
import path from "node:path"

import createTestnet from "hyperdht/testnet.js"

import { generateIdentity, HolepunchSwarmNode } from "../../src/index.js"

/**
 * Create a deterministic multi-node swarm fixture for integration tests.
 *
 * @param {{
 *   size?: number,
 *   identities?: Array<{ publicKeyId: string, publicKey: Buffer, secretKey: Buffer, feedKey: string }>,
 *   authorizedNodes?: Array<{ nodeId: string, publicKey: Buffer, feedKey: string }>,
 *   dataDirs?: string[],
 *   clusterId?: string,
 *   topicSalt?: string,
 *   encryptionKey?: Buffer,
 *   heartbeatIntervalMs?: number,
 *   heartbeatTtlMs?: number,
 *   forwarding?: boolean,
 *   durability?: { requiredFollowerAcks?: number, timeoutMs?: number },
 *   revokedNodeIds?: string[],
 *   identityLabels?: string[],
 *   bootstrap?: Array<string | { host: string, port: number }>,
 *   testnet?: Awaited<ReturnType<typeof createTestnet>>
 * }} [options]
 */
export async function createSwarmCluster(options = {}) {
  const identities = options.identities ?? createIdentities(options.size ?? 3, options.identityLabels)
  const size = identities.length
  const dirs = []
  const testnet = options.testnet ?? (options.bootstrap ? null : await createTestnet(size))
  const authorizedNodes =
    options.authorizedNodes ??
    identities.map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))
  const encryptionKey = options.encryptionKey ?? randomBytes(32)
  const records = new Map()

  for (const [index, identity] of identities.entries()) {
    const label = options.identityLabels?.[index] ?? defaultNodeLabel(index)
    records.set(identity.publicKeyId, {
      label,
      identity,
      dataDir: options.dataDirs?.[index] ?? (await tempDir(dirs)),
      node: null
    })
  }

  const cluster = {
    dirs,
    testnet,
    identities,
    authorizedNodes,
    encryptionKey,
    options: {
      clusterId: options.clusterId ?? "test-cluster",
      topicSalt: options.topicSalt ?? "test-salt",
      heartbeatIntervalMs: options.heartbeatIntervalMs ?? 500,
      heartbeatTtlMs: options.heartbeatTtlMs ?? 3000,
      forwarding: options.forwarding ?? true,
      durability: options.durability,
      revokedNodeIds: options.revokedNodeIds ?? []
    },

    /**
     * Return all current node records.
     */
    get records() {
      return [...records.values()]
    },

    /**
     * Return all currently running nodes.
     */
    get nodes() {
      return [...records.values()].flatMap((record) => (record.node ? [record.node] : []))
    },

    /**
     * Resolve a record by stable label or node ID.
     *
     * @param {string} selector
     */
    record(selector) {
      const byId = records.get(selector)
      if (byId) return byId

      const byLabel = [...records.values()].find((record) => record.label === selector)
      if (byLabel) return byLabel

      throw new Error(`Unknown cluster node ${selector}`)
    },

    /**
     * Start every configured node that is not already running.
     */
    async startAll() {
      for (const record of records.values()) {
        await this.startNode(record.identity.publicKeyId)
      }
      return this.nodes
    },

    /**
     * Start a single node by label or node ID.
     *
     * @param {string} selector
     */
    async startNode(selector) {
      const record = this.record(selector)
      if (record.node) return record.node

      const nodeOptions = {
        dataDir: record.dataDir,
        clusterId: this.options.clusterId,
        topicSalt: this.options.topicSalt,
        identity: record.identity,
        authorizedNodes: this.authorizedNodes,
        encryptionKey: this.encryptionKey,
        bootstrap: options.bootstrap ?? this.testnet?.bootstrap ?? [],
        heartbeatIntervalMs: this.options.heartbeatIntervalMs,
        heartbeatTtlMs: this.options.heartbeatTtlMs,
        forwarding: this.options.forwarding,
        revokedNodeIds: this.options.revokedNodeIds
      }
      if (this.options.durability) {
        nodeOptions.durability = this.options.durability
      }

      const node = new HolepunchSwarmNode(nodeOptions)

      await node.start()
      record.node = node
      return node
    },

    /**
     * Stop a single node by label or node ID and keep its data directory.
     *
     * @param {string} selector
     */
    async stopNode(selector) {
      const record = this.record(selector)
      if (!record.node) return
      await record.node.close()
      record.node = null
    },

    /**
     * Restart a single node by label or node ID using the same identity and data directory.
     *
     * @param {string} selector
     */
    async restartNode(selector) {
      await this.stopNode(selector)
      return this.startNode(selector)
    },

    /**
     * Close every running node and keep the underlying resources.
     */
    async closeNodes() {
      await Promise.allSettled(
        [...records.values()].map(async (record) => {
          if (!record.node) return
          await record.node.close()
          record.node = null
        })
      )
    },

    /**
     * Destroy underlying test resources after nodes are closed.
     */
    async destroyResources() {
      if (this.testnet && !options.testnet) {
        await this.testnet.destroy()
      }
      await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
    },

    /**
     * Close all nodes and remove every temporary directory.
     */
    async closeAll() {
      await this.closeNodes()
      await this.destroyResources()
    }
  }

  return cluster
}

/**
 * Build deterministic identities so leader ordering is stable across runs.
 *
 * @param {number} size
 * @param {string[]} [labels]
 */
export function createIdentities(size, labels) {
  return Array.from({ length: size }, (_, index) => {
    const label = labels?.[index] ?? defaultNodeLabel(index)
    return generateIdentity(seed(label))
  })
}

/**
 * Derive a stable 32-byte seed from a short label.
 *
 * @param {string} label
 */
export function seed(label) {
  return createHash("sha256").update(label).digest()
}

function defaultNodeLabel(index) {
  return index === 0 ? "leader" : `follower-${index}`
}

/**
 * @param {string[]} dirs
 */
async function tempDir(dirs) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-swarm-"))
  dirs.push(dir)
  return dir
}
