#!/usr/bin/env node
import { access, readFile } from "node:fs/promises"
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
  const machineIdentity = await readMachineIdentity()
  const setupConfigPath = cli.configPath ? path.resolve(cli.configPath) : null
  const setupDraftPath = setupConfigPath
    ? deriveSetupDraftPath(setupConfigPath)
    : machineIdentity
      ? path.resolve(`./replicore-node-${machineIdentity}.json`)
      : null
  const configExists = setupConfigPath ? await pathExists(setupConfigPath) : false
  const setupServer = new SetupHttpServer({
    port: asSetupPort(process.env.REPLICORE_SETUP_PORT),
    uiRoot: path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../dist/setup-ui"),
    state: () => ({
      mode: "setup",
      view: "wizard",
      nodeRunning: false,
      configPath: setupConfigPath,
      configExists
    }),
    loadDraft: setupDraftPath
      ? () => readSetupDraft(setupDraftPath)
      : () => Promise.resolve(null),
    saveDraft: async (draft) => {
      const savePath = setupDraftPath
        ?? path.resolve(`./replicore-node-${draft.machineIdentity}.json`)
      const result = await writeSetupDraft(savePath, draft)
      return { ...result.draft, _savedPath: path.basename(result.path) }
    }
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

async function readMachineIdentity() {
  try {
    return (await readFile("/etc/machine-id", "utf8")).trim()
  } catch {
    return ""
  }
}

function asSetupPort(value) {
  if (value === undefined || value === "") return 0
  const port = Number.parseInt(value, 10)
  return Number.isInteger(port) && port > 0 && port < 65536 ? port : 0
}
