import { mkdir, readFile, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"

import { base58Encode, decodeHexOrBase58 } from "../src/base58.js"
import {
  deriveClusterScopedBytes,
  deriveDiscoveryTopic,
  deriveMachineId
} from "../src/cluster-secret.js"

const ROOT = path.dirname(fileURLToPath(import.meta.url))
const DRY_RUN = process.argv.includes("--dry-run")

const CLUSTER_ID_PURPOSE = "replicore:cluster-id:v1"
const NODE_IDENTITY_PURPOSE = "replicore:node-identity:v1"
const ENCRYPTION_KEY_PURPOSE = "replicore:encryption-key:v1"

const REQUIRED_VARS = ["CLUSTER_SECRET"]

function requireEnv(name) {
  const value = process.env[name]
  if (!value || value.length === 0) {
    console.error(`Missing required environment variable: ${name}`)
    process.exit(1)
  }
  return value
}

function parseBooleanEnv(name, fallback) {
  const value = process.env[name]
  if (value === undefined || value === "") return fallback
  const lower = value.toLowerCase()
  if (lower === "true" || lower === "1") return true
  if (lower === "false" || lower === "0") return false
  console.error(`${name} must be true/false/1/0, got: ${value}`)
  process.exit(1)
}

function parseIntEnv(name, fallback) {
  const value = process.env[name]
  if (value === undefined || value === "") return fallback
  const num = Number.parseInt(value, 10)
  if (!Number.isInteger(num)) {
    console.error(`${name} must be an integer, got: ${value}`)
    process.exit(1)
  }
  return num
}

function parseJsonEnv(name, fallback) {
  const value = process.env[name]
  if (value === undefined || value === "") return fallback
  try {
    return JSON.parse(value)
  } catch {
    console.error(`${name} must be valid JSON, got: ${value}`)
    process.exit(1)
  }
}

async function readMachineIdentity() {
  try {
    const content = await readFile("/etc/machine-id", "utf8")
    const trimmed = content.trim()
    if (trimmed.length === 0) return undefined
    return trimmed
  } catch {
    return undefined
  }
}

async function main() {
  for (const name of REQUIRED_VARS) {
    requireEnv(name)
  }

  const clusterSecretRaw = process.env.CLUSTER_SECRET
  let clusterSecret
  try {
    clusterSecret = decodeHexOrBase58(clusterSecretRaw)
  } catch {
    console.error(`CLUSTER_SECRET must be a hex or base58 string, got: ${clusterSecretRaw}`)
    process.exit(1)
  }
  if (clusterSecret.length !== 32) {
    console.error(`CLUSTER_SECRET must decode to 32 bytes, got ${clusterSecret.length} bytes`)
    process.exit(1)
  }

  const clusterId = base58Encode(
    await deriveClusterScopedBytes({
      clusterSecret,
      purpose: CLUSTER_ID_PURPOSE,
      context: "cluster-id",
      length: 8
    })
  )

  const machineIdentity = await readMachineIdentity()
  if (!machineIdentity) {
    console.error("Cannot read /etc/machine-id; mount it from the host with /etc/machine-id:/etc/machine-id:ro")
    process.exit(1)
  }

  const identitySeed = await deriveClusterScopedBytes({
    clusterSecret,
    purpose: NODE_IDENTITY_PURPOSE,
    context: machineIdentity,
    length: 32
  })

  const encryptionKey = await deriveClusterScopedBytes({
    clusterSecret,
    purpose: ENCRYPTION_KEY_PURPOSE,
    context: clusterId,
    length: 32
  })

  const topic = await deriveDiscoveryTopic({ clusterSecret, clusterId })

  const machineId = await deriveMachineId({ clusterSecret, machineIdentity })

  console.log(
    JSON.stringify({
      type: "derived-keys",
      clusterId,
      topic: base58Encode(topic),
      machineId: base58Encode(machineId)
    })
  )

  const initCluster = parseBooleanEnv("INIT_CLUSTER", false)
  const role = initCluster ? "voter" : "learner"

  const config = {
    dataDir: process.env.DATA_DIR || "/data",
    clusterId,
    clusterSecret: base58Encode(clusterSecret),
    identitySeed: base58Encode(identitySeed),
    encryptionKey: base58Encode(encryptionKey),
    machineIdentity,
    role,
    initCluster,
    http: {
      host: process.env.HTTP_HOST || "0.0.0.0",
      port: parseIntEnv("HTTP_PORT", 3000)
    },
    auth: {
      tokens: parseJsonEnv("AUTH_TOKENS", {})
    },
    bootstrap: parseJsonEnv("BOOTSTRAP", []),
    heartbeatIntervalMs: parseIntEnv("HEARTBEAT_INTERVAL_MS", 500),
    heartbeatTtlMs: parseIntEnv("HEARTBEAT_TTL_MS", 3000),
    electionTimeoutMinMs: parseIntEnv("ELECTION_TIMEOUT_MIN_MS", 900),
    electionTimeoutMaxMs: parseIntEnv("ELECTION_TIMEOUT_MAX_MS", 1500),
    requestTimeoutMs: parseIntEnv("REQUEST_TIMEOUT_MS", 5000),
    maxInflightReplication: parseIntEnv("MAX_INFLIGHT_REPLICATION", 16),
    forwarding: true,
    durability: {
      requiredFollowerAcks: parseIntEnv("DURABILITY_ACKS", 1),
      timeoutMs: parseIntEnv("DURABILITY_TIMEOUT_MS", 5000)
    }
  }

  if (process.env.ELECTION_TIMEOUT_SEED) {
    config.electionTimeoutSeed = process.env.ELECTION_TIMEOUT_SEED
  }

  if (!initCluster && role === "voter") {
    console.error(
      "Voter role requires INIT_CLUSTER=true.\n" +
        "Set INIT_CLUSTER=true, or remove ROLE=voter to join as a learner."
    )
    process.exit(1)
  }

  if (DRY_RUN) {
    console.log(
      JSON.stringify({ type: "config-dry-run", config }, null, 2)
    )
    process.exit(0)
  }

  const dataDir = config.dataDir
  await mkdir(dataDir, { recursive: true })

  const configPath = path.join(dataDir, "runtime-config.json")
  await writeFile(configPath, JSON.stringify(config, null, 2), "utf8")

  console.log(
    JSON.stringify({
      type: "entrypoint-config-written",
      configPath,
      dataDir,
      clusterId,
      httpPort: config.http.port
    })
  )

  const { spawn } = await import("node:child_process")
  const child = spawn(
    process.execPath,
    [path.join(ROOT, "run-node.js"), configPath],
    { stdio: "inherit" }
  )

  child.on("exit", (code) => process.exit(code ?? 1))
}

main()
