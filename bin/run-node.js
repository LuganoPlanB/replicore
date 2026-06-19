#!/usr/bin/env node
import { HolepunchHttpServer, HolepunchSwarmNode, loadRuntimeConfig } from "../src/index.js"

const configPath = process.argv[2]

if (!configPath) {
  console.error("Usage: node bin/run-node.js <config.json>")
  process.exit(1)
}

const config = await loadRuntimeConfig(configPath)

const node = new HolepunchSwarmNode({
  dataDir: config.dataDir,
  clusterId: config.clusterId,
  clusterSecret: config.clusterSecret,
  role: config.role,
  machineId: config.machineId,
  nodeIdentitySeed: config.nodeIdentitySeed,
  identity: config.identity,
  authorizedNodes: config.authorizedNodes,
  revokedNodeIds: config.revokedNodeIds,
  encryption: config.encryption,
  bootstrap: config.bootstrap,
  heartbeatIntervalMs: config.heartbeatIntervalMs,
  heartbeatTtlMs: config.heartbeatTtlMs,
  forwarding: config.forwarding,
  durability: config.durability
})

await node.start()

const http = new HolepunchHttpServer({
  node,
  host: config.http.host,
  port: config.http.port,
  auth: config.auth
})

await http.start()

console.log(
  JSON.stringify(
    {
      type: "node-ready",
      nodeId: config.identity.publicKeyId,
      feedKey: config.identity.feedKey,
      dataDir: config.dataDir,
      http: http.address,
      leader: node.currentLeader()
    },
    null,
    2
  )
)

const shutdown = async () => {
  await http.close()
  await node.close()
  process.exit(0)
}

process.on("SIGINT", () => void shutdown())
process.on("SIGTERM", () => void shutdown())
