import { readFile } from "node:fs/promises"
import path from "node:path"

import { base58Encode, decodeHexOrBase58 } from "./base58.js"
import { deriveClusterScopedBytes } from "./cluster-secret.js"
import { generateIdentity } from "./crypto.js"
import { verifyOrPersistRuntimeGuardrails } from "./runtime-guardrails.js"

const CLUSTER_ID_PURPOSE = "replicore:cluster-id:v1"
const NODE_IDENTITY_PURPOSE = "replicore:node-identity:v1"
const ENCRYPTION_KEY_PURPOSE = "replicore:encryption-key:v1"

/**
 * Load and normalize a node runtime config from JSON.
 *
 * @param {string} configPath
 */
export async function loadRuntimeConfig(configPath) {
  const absolutePath = path.resolve(configPath)
  const raw = JSON.parse(await readFile(absolutePath, "utf8"))

  const clusterSecret = requireHex(raw.clusterSecret, "clusterSecret", 32)
  const machineId = normalizeMachineIdentity(raw)
  const initCluster = normalizeInitCluster(raw.initCluster)
  const role = raw.role ?? (initCluster ? "voter" : "learner")
  if (role !== "voter" && role !== "learner") {
    throw new Error("role must be either voter or learner")
  }
  if (initCluster && role !== "voter") {
    throw new Error("initCluster may only be used with voter role")
  }

  const clusterId = raw.clusterId ?? base58Encode(
    await deriveClusterScopedBytes({
      clusterSecret,
      purpose: CLUSTER_ID_PURPOSE,
      context: "cluster-id",
      length: 8
    })
  )

  const identitySeed = raw.identitySeed
    ? requireHex(raw.identitySeed, "identitySeed")
    : await deriveClusterScopedBytes({
        clusterSecret,
        purpose: NODE_IDENTITY_PURPOSE,
        context: machineId,
        length: 32
      })

  const identity = generateIdentity(identitySeed)

  const encryption = raw.encryption
    ? normalizeEncryption(raw)
    : raw.encryptionKey
      ? normalizeEncryption(raw)
      : {
          currentKeyId: "default",
          keys: {
            default: await deriveClusterScopedBytes({
              clusterSecret,
              purpose: ENCRYPTION_KEY_PURPOSE,
              context: clusterId,
              length: 32
            })
          }
        }

  let authorizedNodes = []
  const revokedNodeIds = []

  if (initCluster) {
    authorizedNodes = [{
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }]
  } else if (role === "voter") {
    throw new Error(
      'Secret-first voter config requires initCluster: true; joining nodes must use role "learner"'
    )
  }

  const raft = normalizeRaftTimings(raw)
  const dataDir = path.resolve(path.dirname(absolutePath), raw.dataDir)

  await verifyOrPersistRuntimeGuardrails({
    dataDir,
    clusterId,
    clusterSecret,
    identity,
    compatibilityMode: "secret-first",
    initCluster
  })

  return {
    configPath: absolutePath,
    dataDir,
    clusterId,
    clusterSecret,
    compatibilityMode: "secret-first",
    role,
    initCluster,
    machineId,
    bootstrap: normalizeBootstrap(raw.bootstrap ?? []),
    heartbeatIntervalMs: raft.heartbeatIntervalMs,
    heartbeatTtlMs: raft.heartbeatTtlMs,
    electionTimeoutMinMs: raft.electionTimeoutMinMs,
    electionTimeoutMaxMs: raft.electionTimeoutMaxMs,
    requestTimeoutMs: raft.requestTimeoutMs,
    maxInflightReplication: raft.maxInflightReplication,
    electionTimeoutSeed: raft.electionTimeoutSeed,
    forwarding: raw.forwarding ?? true,
    durability: {
      requiredFollowerAcks: raw.durability?.requiredFollowerAcks ?? 1,
      timeoutMs: raw.durability?.timeoutMs ?? 5000
    },
    identity,
    authorizedNodes,
    revokedNodeIds,
    encryption,
    http: {
      host: raw.http?.host ?? "127.0.0.1",
      port: raw.http?.port ?? 0
    },
    auth: {
      tokens: raw.auth?.tokens ?? {}
    }
  }
}

function normalizeInitCluster(value) {
  if (value === undefined) return false
  if (typeof value !== "boolean") {
    throw new Error("initCluster must be a boolean when provided")
  }
  return value
}

function normalizeMachineIdentity(raw) {
  const configMachineIdentity =
    raw.machineIdentity === undefined ? undefined : requireString(raw.machineIdentity, "machineIdentity")
  const legacyMachineId = raw.machineId === undefined ? undefined : requireString(raw.machineId, "machineId")

  if (configMachineIdentity && legacyMachineId && configMachineIdentity !== legacyMachineId) {
    throw new Error("machineIdentity and machineId must match when both are provided")
  }

  return configMachineIdentity ?? legacyMachineId
}

function normalizeEncryption(raw) {
  if (raw.encryption && typeof raw.encryption === "object") {
    const currentKeyId = requireString(raw.encryption.currentKeyId, "encryption.currentKeyId")
    const rawKeys = raw.encryption.keys
    if (!rawKeys || typeof rawKeys !== "object" || Array.isArray(rawKeys)) {
      throw new Error("encryption.keys must be an object")
    }

    const keys = {}
    for (const [keyId, value] of Object.entries(rawKeys)) {
      keys[requireString(keyId, `encryption.keys.${keyId}`)] = requireHex(
        value,
        `encryption.keys.${keyId}`,
        32
      )
    }

    if (!keys[currentKeyId]) {
      throw new Error("encryption.currentKeyId must reference a configured key")
    }

    return { currentKeyId, keys }
  }

  return {
    currentKeyId: "default",
    keys: {
      default: requireHex(raw.encryptionKey, "encryptionKey", 32)
    }
  }
}

function normalizeBootstrap(raw) {
  if (!Array.isArray(raw)) {
    throw new Error("bootstrap must be an array")
  }

  return raw.map((entry, index) => {
    if (typeof entry === "string") return entry
    if (!entry || typeof entry !== "object") {
      throw new Error(`bootstrap[${index}] must be a string or object`)
    }
    return {
      host: requireString(entry.host, `bootstrap[${index}].host`),
      port: requireNumber(entry.port, `bootstrap[${index}].port`)
    }
  })
}

function normalizeRaftTimings(raw) {
  const heartbeatIntervalMs = requireBoundedInteger(
    raw.heartbeatIntervalMs ?? raw.raft?.heartbeatIntervalMs ?? 500,
    "heartbeatIntervalMs",
    { min: 25, max: 60_000 }
  )
  const heartbeatTtlMs = requireBoundedInteger(
    raw.heartbeatTtlMs ?? raw.raft?.leaderLeaseMs ?? 3000,
    "heartbeatTtlMs",
    { min: heartbeatIntervalMs * 2, max: 300_000 }
  )
  const electionTimeoutMinMs = requireBoundedInteger(
    raw.electionTimeoutMinMs ?? raw.raft?.electionTimeoutMinMs ?? 900,
    "electionTimeoutMinMs",
    { min: heartbeatIntervalMs + 100, max: 300_000 }
  )
  const electionTimeoutMaxMs = requireBoundedInteger(
    raw.electionTimeoutMaxMs ?? raw.raft?.electionTimeoutMaxMs ?? 1500,
    "electionTimeoutMaxMs",
    { min: electionTimeoutMinMs, max: 300_000 }
  )
  if (electionTimeoutMaxMs === electionTimeoutMinMs) {
    throw new Error("electionTimeoutMaxMs must be greater than electionTimeoutMinMs")
  }

  const requestTimeoutMs = requireBoundedInteger(
    raw.requestTimeoutMs ?? raw.raft?.requestTimeoutMs ?? 5000,
    "requestTimeoutMs",
    { min: electionTimeoutMaxMs, max: 300_000 }
  )
  const maxInflightReplication = requireBoundedInteger(
    raw.maxInflightReplication ?? raw.raft?.maxInflightReplication ?? 16,
    "maxInflightReplication",
    { min: 1, max: 1024 }
  )
  const electionTimeoutSeed = raw.electionTimeoutSeed ?? raw.raft?.electionTimeoutSeed ?? null
  if (electionTimeoutSeed !== null && typeof electionTimeoutSeed !== "string") {
    throw new Error("electionTimeoutSeed must be a string when provided")
  }

  return {
    heartbeatIntervalMs,
    heartbeatTtlMs,
    electionTimeoutMinMs,
    electionTimeoutMaxMs,
    requestTimeoutMs,
    maxInflightReplication,
    electionTimeoutSeed
  }
}

function requireString(value, field) {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${field} must be a non-empty string`)
  }
  return value
}

function requireNumber(value, field) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${field} must be a finite number`)
  }
  return value
}

function requireBoundedInteger(value, field, { min, max }) {
  if (!Number.isInteger(value)) {
    throw new Error(`${field} must be an integer`)
  }
  if (value < min || value > max) {
    throw new Error(`${field} must be between ${min} and ${max}`)
  }
  return value
}

function requireHex(value, field, exactLength) {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${field} must be a non-empty hex or base58 string`)
  }

  let buffer
  try {
    buffer = decodeHexOrBase58(value)
  } catch {
    throw new Error(`${field} must be a hex or base58 string`)
  }

  if (exactLength && buffer.length !== exactLength) {
    throw new Error(`${field} must decode to ${exactLength} bytes`)
  }
  return buffer
}
