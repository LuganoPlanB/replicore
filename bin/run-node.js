#!/usr/bin/env node
import { access } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"

import {
  HolepunchHttpServer,
  HolepunchSwarmNode,
  loadRuntimeConfig,
  readSetupDraft,
  SetupHttpServer,
  writeSetupDraft
} from "../src/index.js"

const cli = parseCli(process.argv.slice(2))

if (cli.setup) {
  const setupConfigPath = cli.configPath ? path.resolve(cli.configPath) : null
  const setupDraftPath = setupConfigPath ? deriveSetupDraftPath(setupConfigPath) : null
  const configExists = setupConfigPath ? await pathExists(setupConfigPath) : false
  const setupServer = new SetupHttpServer({
    uiRoot: path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../dist/setup-ui"),
    state: () => ({
      mode: "setup",
      view: "wizard",
      nodeRunning: false,
      configPath: setupConfigPath,
      configExists
    }),
    loadDraft: setupDraftPath ? () => readSetupDraft(setupDraftPath) : null,
    saveDraft: setupDraftPath ? (draft) => writeSetupDraft(setupDraftPath, draft).then((result) => result.draft) : null
  })

  await setupServer.start()

  console.log(
    JSON.stringify(
      {
        type: "setup-ready",
        configPath: setupConfigPath,
        draftPath: setupDraftPath,
        configExists,
        url: `http://${setupServer.address.address}:${setupServer.address.port}/`,
        http: setupServer.address
      },
      null,
      2
    )
  )

  const shutdown = async () => {
    await setupServer.close()
    process.exit(0)
  }

  process.on("SIGINT", () => void shutdown())
  process.on("SIGTERM", () => void shutdown())
} else {
  if (!cli.configPath) {
    console.error("Usage: node bin/run-node.js <config.json>")
    console.error("   or: node bin/run-node.js --setup [config.json]")
    process.exit(1)
  }

  const config = await loadRuntimeConfig(cli.configPath)

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
}

function parseCli(argv) {
  if (argv[0] === "--setup") {
    return {
      setup: true,
      configPath: argv[1] ?? null
    }
  }

  return {
    setup: false,
    configPath: argv[0] ?? null
  }
}

function deriveSetupDraftPath(configPath) {
  const parsed = path.parse(configPath)
  return path.join(parsed.dir, `${parsed.name}.setup-draft.json`)
}

async function pathExists(filePath) {
  try {
    await access(filePath)
    return true
  } catch {
    return false
  }
}
