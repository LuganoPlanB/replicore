import { mkdtemp, rm } from "node:fs/promises"
import { randomBytes, createHash } from "node:crypto"
import os from "node:os"
import path from "node:path"

import createTestnet from "hyperdht/testnet.js"

import { generateIdentity, HolepunchSwarmNode } from "../../src/index.js"
import { withTimeout } from "./eventual.js"
import { createTrace } from "./trace.js"

const NODE_LIFECYCLE_TIMEOUT_MS = Number(process.env.REPLICORE_TEST_NODE_TIMEOUT_MS ?? "10000")
const RESOURCE_TIMEOUT_MS = Number(process.env.REPLICORE_TEST_RESOURCE_TIMEOUT_MS ?? "15000")

/**
 * Create a deterministic multi-node swarm fixture for integration tests.
 *
 * @param {{
 *   size?: number,
 *   identities?: Array<{ publicKeyId: string, publicKey: Buffer, secretKey: Buffer, feedKey: string }>,
 *   authorizedNodes?: Array<{ nodeId: string, publicKey: Buffer, feedKey: string }>,
 *   authorizedNodesByNodeId?: Record<string, Array<{ nodeId: string, publicKey: Buffer, feedKey: string }>>,
 *   dataDirs?: string[],
 *   clusterId?: string,
 *   clusterSecret?: Buffer,
 *   topicSalt?: string,
 *   encryptionKey?: Buffer,
 *   heartbeatIntervalMs?: number,
 *   heartbeatTtlMs?: number,
 *   forwarding?: boolean,
 *   ackDelayMsByNodeId?: Record<string, number>,
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
  const trace = createTrace()
  let activePartitionGroups = null

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
    trace,
    options: {
      clusterId: options.clusterId ?? "test-cluster",
      clusterSecret:
        options.clusterSecret ??
        Buffer.from(
          "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
          "hex"
        ),
      topicSalt: options.topicSalt ?? "test-salt",
      heartbeatIntervalMs: options.heartbeatIntervalMs ?? 500,
      heartbeatTtlMs: options.heartbeatTtlMs ?? 3000,
      forwarding: options.forwarding ?? true,
      ackDelayMsByNodeId: options.ackDelayMsByNodeId ?? {},
      durability: options.durability,
      revokedNodeIds: options.revokedNodeIds ?? []
    },

    async diagnostics(nodes = this.nodes) {
      const statuses = await Promise.all(nodes.map((node) => node.getReplicationStatus()))
      return {
        records: this.records.map((record) => ({
          label: record.label,
          nodeId: record.identity.publicKeyId,
          running: record.node !== null,
          dataDir: record.dataDir
        })),
        partitionGroups: activePartitionGroups,
        status: Object.fromEntries(statuses.map((status) => [status.nodeId, status])),
        trace: trace.snapshot()
      }
    },

    async timed(description, operation, options = {}) {
      return withTimeout(description, operation, {
        timeoutMs: options.timeoutMs,
        onTimeout: async () => {
          trace.record(`${description}.timeout`, options.timeoutDetails ?? {})
          return this.diagnostics(options.nodes)
        }
      })
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
      trace.record("cluster.startAll.begin", { size: records.size })
      for (const record of records.values()) {
        await this.startNode(record.identity.publicKeyId)
      }
      trace.record("cluster.startAll.end", {
        runningNodeIds: this.nodes.map((node) => node.options.identity.publicKeyId)
      })
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
      trace.record("cluster.startNode.begin", {
        selector,
        label: record.label,
        nodeId: record.identity.publicKeyId,
        dataDir: record.dataDir
      })

      const nodeOptions = {
        dataDir: record.dataDir,
        clusterId: this.options.clusterId,
        clusterSecret: this.options.clusterSecret,
        machineId: `test-machine:${record.label}`,
        topicSalt: this.options.topicSalt,
        identity: record.identity,
        authorizedNodes: options.authorizedNodesByNodeId?.[record.identity.publicKeyId] ?? this.authorizedNodes,
        encryptionKey: this.encryptionKey,
        bootstrap: options.bootstrap ?? this.testnet?.bootstrap ?? [],
        heartbeatIntervalMs: this.options.heartbeatIntervalMs,
        heartbeatTtlMs: this.options.heartbeatTtlMs,
        forwarding: this.options.forwarding,
        ackDelayMs: this.options.ackDelayMsByNodeId[record.identity.publicKeyId],
        revokedNodeIds: this.options.revokedNodeIds
      }
      if (this.options.durability) {
        nodeOptions.durability = this.options.durability
      }

      const node = new HolepunchSwarmNode(nodeOptions)
      record.node = node

      try {
        await this.timed(`cluster.startNode:${record.label}`, node.start(), {
          timeoutMs: NODE_LIFECYCLE_TIMEOUT_MS,
          timeoutDetails: {
            selector,
            label: record.label,
            nodeId: record.identity.publicKeyId
          }
        })
      } catch (error) {
        record.node = null
        await Promise.allSettled([node.close()])
        throw error
      }

      if (activePartitionGroups) {
        const allowedNodeIds = activePartitionGroups.find((group) =>
          group.includes(record.identity.publicKeyId)
        )
        await node.setNetworkPolicy({
          allowedNodeIds,
          allowConnection: (_localNodeId, remoteNodeId) => allowedNodeIds.includes(remoteNodeId)
        })
      }

      trace.record("cluster.startNode.end", {
        selector,
        label: record.label,
        nodeId: record.identity.publicKeyId
      })
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
      trace.record("cluster.stopNode.begin", {
        selector,
        label: record.label,
        nodeId: record.identity.publicKeyId
      })
      await this.timed(`cluster.stopNode:${record.label}`, record.node.close(), {
        timeoutMs: NODE_LIFECYCLE_TIMEOUT_MS,
        timeoutDetails: {
          selector,
          label: record.label,
          nodeId: record.identity.publicKeyId
        }
      })
      record.node = null
      trace.record("cluster.stopNode.end", {
        selector,
        label: record.label,
        nodeId: record.identity.publicKeyId
      })
    },

    /**
     * Restart a single node by label or node ID using the same identity and data directory.
     *
     * @param {string} selector
     */
    async restartNode(selector) {
      trace.record("cluster.restartNode.begin", { selector })
      await this.stopNode(selector)
      const node = await this.startNode(selector)
      trace.record("cluster.restartNode.end", { selector })
      return node
    },

    /**
     * Temporarily isolate a running node from swarm networking without closing storage.
     *
     * @param {string} selector
     */
    async isolateNode(selector) {
      const record = this.record(selector)
      if (!record.node) throw new Error(`Cannot isolate stopped node ${selector}`)
      trace.record("cluster.isolateNode.begin", {
        selector,
        label: record.label,
        nodeId: record.identity.publicKeyId
      })
      await this.timed(`cluster.isolateNode:${record.label}`, record.node.suspendNetworking(), {
        timeoutMs: NODE_LIFECYCLE_TIMEOUT_MS,
        timeoutDetails: {
          selector,
          label: record.label,
          nodeId: record.identity.publicKeyId
        }
      })
      trace.record("cluster.isolateNode.end", {
        selector,
        label: record.label,
        nodeId: record.identity.publicKeyId
      })
      return record.node
    },

    /**
     * Rejoin a previously isolated node to the swarm.
     *
     * @param {string} selector
     */
    async healNode(selector) {
      const record = this.record(selector)
      if (!record.node) throw new Error(`Cannot heal stopped node ${selector}`)
      trace.record("cluster.healNode.begin", {
        selector,
        label: record.label,
        nodeId: record.identity.publicKeyId
      })
      await this.timed(`cluster.healNode:${record.label}`, record.node.resumeNetworking(), {
        timeoutMs: NODE_LIFECYCLE_TIMEOUT_MS,
        timeoutDetails: {
          selector,
          label: record.label,
          nodeId: record.identity.publicKeyId
        }
      })
      trace.record("cluster.healNode.end", {
        selector,
        label: record.label,
        nodeId: record.identity.publicKeyId
      })
      return record.node
    },

    /**
     * Split the running cluster into internally connected groups and block cross-group links.
     *
     * @param {string[][]} groups
     */
    async partitionGroups(groups) {
      const runningRecords = this.records.filter((record) => record.node)
      const runningNodeIds = this.nodes.map((node) => node.options.identity.publicKeyId).sort()
      const normalizedGroups = groups.map((group) => group.map((selector) => this.record(selector).identity.publicKeyId))
      const flattenedNodeIds = normalizedGroups.flat()
      const uniqueNodeIds = new Set(flattenedNodeIds)

      if (flattenedNodeIds.length !== uniqueNodeIds.size) {
        throw new Error("Partition groups must not contain duplicate nodes")
      }

      if (runningNodeIds.length !== uniqueNodeIds.size || runningNodeIds.some((nodeId) => !uniqueNodeIds.has(nodeId))) {
        throw new Error("Partition groups must include every running node exactly once")
      }

      activePartitionGroups = normalizedGroups.map((group) => [...group].sort())
      trace.record("cluster.partitionGroups.begin", { groups: activePartitionGroups })

      await this.timed(
        "cluster.partitionGroups",
        Promise.all(
          runningRecords.map(async (record) => {
            const allowedNodeIds = activePartitionGroups.find((group) => group.includes(record.identity.publicKeyId))
            await record.node.setNetworkPolicy({
              allowedNodeIds,
              allowConnection: (_localNodeId, remoteNodeId) => allowedNodeIds.includes(remoteNodeId)
            })
          })
        )
      )

      trace.record("cluster.partitionGroups.end", { groups: activePartitionGroups })
    },

    /**
     * Remove any active subgroup partition policy and restore allow-all connectivity.
     */
    async healPartition() {
      const runningRecords = this.records.filter((record) => record.node)
      trace.record("cluster.healPartition.begin", { groups: activePartitionGroups })
      activePartitionGroups = null

      await this.timed(
        "cluster.healPartition",
        Promise.all(
          runningRecords.map(async (record) => {
            await record.node.setNetworkPolicy(null)
          })
        )
      )

      trace.record("cluster.healPartition.end", { groups: activePartitionGroups })
    },

    /**
     * Close every running node and keep the underlying resources.
     */
    async closeNodes() {
      trace.record("cluster.closeNodes.begin", {
        runningNodeIds: this.nodes.map((node) => node.options.identity.publicKeyId)
      })
      await this.timed(
        "cluster.closeNodes",
        Promise.allSettled(
          [...records.values()].map(async (record) => {
            if (!record.node) return
            await record.node.close()
            record.node = null
          })
        ),
        {
          timeoutMs: RESOURCE_TIMEOUT_MS
        }
      )
      trace.record("cluster.closeNodes.end", { runningNodeIds: [] })
    },

    /**
     * Destroy underlying test resources after nodes are closed.
     */
    async destroyResources() {
      trace.record("cluster.destroyResources.begin", {
        hasTestnet: Boolean(this.testnet && !options.testnet),
        dirCount: dirs.length
      })
      if (this.testnet && !options.testnet) {
        await this.timed("cluster.destroyResources.testnet", this.testnet.destroy(), {
          timeoutMs: RESOURCE_TIMEOUT_MS
        })
      }
      await this.timed(
        "cluster.destroyResources.tempDirs",
        Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true }))),
        {
          timeoutMs: RESOURCE_TIMEOUT_MS
        }
      )
      trace.record("cluster.destroyResources.end", { dirCount: dirs.length })
    },

    /**
     * Close all nodes and remove every temporary directory.
     */
    async closeAll() {
      trace.record("cluster.closeAll.begin")
      await this.timed(
        "cluster.closeAll",
        (async () => {
          await this.closeNodes()
          await this.destroyResources()
        })(),
        {
          timeoutMs: RESOURCE_TIMEOUT_MS
        }
      )
      trace.record("cluster.closeAll.end")
    }
  }

  trace.record("cluster.created", {
    size,
    clusterId: cluster.options.clusterId,
    topicSalt: cluster.options.topicSalt,
    heartbeatIntervalMs: cluster.options.heartbeatIntervalMs,
    heartbeatTtlMs: cluster.options.heartbeatTtlMs
  })

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
