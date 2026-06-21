import { mkdir, readFile, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"

import { base58Encode } from "../src/base58.js"
import {
  deriveClusterScopedBytes,
  deriveDiscoveryTopic
} from "../src/cluster-secret.js"

const ROOT = path.dirname(fileURLToPath(import.meta.url))
const DRY_RUN = process.argv.includes("--dry-run")

const NODE_IDENTITY_PURPOSE = "replicore:node-identity:v1"
const ENCRYPTION_KEY_PURPOSE = "replicore:encryption-key:v1"

const REQUIRED_VARS = ["CLUSTER_ID", "CLUSTER_SECRET"]

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

function validateHex(value, name, byteLength) {
  if (typeof value !== "string" || !/^[0-9a-fA-F]+$/.test(value)) {
    console.error(`${name} must be a hex string, got: ${value}`)
    process.exit(1)
  }
  if (byteLength !== undefined && Buffer.from(value, "hex").length !== byteLength) {
    console.error(`${name} must decode to ${byteLength} bytes, got ${Buffer.from(value, "hex").length} bytes`)
    process.exit(1)
  }
}

function bufferToHex(buf) {
  return buf.toString("hex")
}

async function main() {
  for (const name of REQUIRED_VARS) {
    requireEnv(name)
  }

  const clusterId = process.env.CLUSTER_ID
  const clusterSecretHex = process.env.CLUSTER_SECRET

  validateHex(clusterSecretHex, "CLUSTER_SECRET", 32)

  const clusterSecret = Buffer.from(clusterSecretHex, "hex")

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

  console.log(
    JSON.stringify({
      type: "derived-keys",
      topicBase58: base58Encode(topic),
      identitySeed: bufferToHex(identitySeed),
      encryptionKey: bufferToHex(encryptionKey),
      machineIdentity
    })
  )

  const config = {
    dataDir: process.env.DATA_DIR || "/data",
    clusterId,
    clusterSecret: clusterSecretHex,
    identitySeed: bufferToHex(identitySeed),
    encryptionKey: bufferToHex(encryptionKey),
    machineIdentity,
    role: process.env.ROLE || "voter",
    initCluster: parseBooleanEnv("INIT_CLUSTER", false),
    compatibilityMode: process.env.COMPATIBILITY_MODE || undefined,
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
    forwarding: parseBooleanEnv("FORWARDING", true),
    durability: {
      requiredFollowerAcks: parseIntEnv("DURABILITY_ACKS", 1),
      timeoutMs: parseIntEnv("DURABILITY_TIMEOUT_MS", 5000)
    }
  }

  if (process.env.ELECTION_TIMEOUT_SEED) {
    config.electionTimeoutSeed = process.env.ELECTION_TIMEOUT_SEED
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
      role: config.role,
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
