import { readFile } from "node:fs/promises"
import path from "node:path"

import { generateIdentity } from "./crypto.js"

/**
 * Load and normalize a node runtime config from JSON.
 *
 * @param {string} configPath
 */
export async function loadRuntimeConfig(configPath) {
  const absolutePath = path.resolve(configPath)
  const raw = JSON.parse(await readFile(absolutePath, "utf8"))

  const identitySeed = requireHex(raw.identitySeed ?? raw.nodeIdentitySeed, "identitySeed")
  const identity = generateIdentity(identitySeed)
  const role = normalizeRole(raw.role)
  const initCluster = normalizeInitCluster(raw.initCluster)
  const compatibilityMode = normalizeCompatibilityMode(raw.compatibilityMode)
  const machineId = normalizeMachineIdentity(raw)

  let authorizedNodes = normalizeAuthorizedNodes(raw, {
    required: compatibilityMode === "legacy-static-membership"
  })
  const localIncluded = authorizedNodes.some((node) => node.nodeId === identity.publicKeyId)
  let normalizedCompatibilityMode = compatibilityMode
  let revokedNodeIds = []

  if (compatibilityMode === "legacy-static-membership") {
    if (initCluster) {
      throw new Error('initCluster cannot be combined with compatibilityMode "legacy-static-membership"')
    }
    if (role === "voter" && !localIncluded) {
      throw new Error("Authorized nodes do not include the local identity")
    }
    if (role === "learner" && localIncluded) {
      throw new Error("Learner config must not include the local identity in authorizedNodes")
    }
    if (role === "learner") {
      throw new Error('compatibilityMode "legacy-static-membership" is incompatible with role "learner"')
    }
    revokedNodeIds = normalizeRevokedNodeIds(raw, authorizedNodes)
  } else {
    normalizedCompatibilityMode = "secret-first"
    if (authorizedNodes.length > 0) {
      throw new Error(
        'Static membership config requires compatibilityMode "legacy-static-membership"'
      )
    }
    if (raw.revokedNodeIds !== undefined) {
      throw new Error(
        'Secret-first config must omit revokedNodeIds until membership is committed in-cluster'
      )
    }
    if (role === "learner") {
      if (initCluster) {
        throw new Error("initCluster may only be used with voter role")
      }
    } else if (initCluster) {
      authorizedNodes = [{
        nodeId: identity.publicKeyId,
        publicKey: identity.publicKey,
        feedKey: identity.feedKey
      }]
    } else {
      throw new Error(
        'Secret-first voter config requires initCluster: true; joining nodes must use role "learner"'
      )
    }
  }
  if (revokedNodeIds.includes(identity.publicKeyId)) {
    throw new Error("Local identity is revoked in config")
  }
  const encryption = normalizeEncryption(raw)
  const raft = normalizeRaftTimings(raw)

  return {
    configPath: absolutePath,
    dataDir: path.resolve(path.dirname(absolutePath), raw.dataDir),
    clusterId: requireString(raw.clusterId, "clusterId"),
    clusterSecret: requireHex(raw.clusterSecret, "clusterSecret", 32),
    compatibilityMode: normalizedCompatibilityMode,
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

function normalizeRole(value) {
  if (value === undefined) return "voter"
  if (value === "voter" || value === "learner") return value
  throw new Error("role must be either voter or learner")
}

function normalizeInitCluster(value) {
  if (value === undefined) return false
  if (typeof value !== "boolean") {
    throw new Error("initCluster must be a boolean when provided")
  }
  return value
}

function normalizeCompatibilityMode(value) {
  if (value === undefined) return null
  if (value === "legacy-static-membership") return value
  throw new Error(
    'compatibilityMode must be "legacy-static-membership" when provided'
  )
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

function normalizeAuthorizedNodes(raw, options = {}) {
  const required = options.required ?? true

  if (Array.isArray(raw.authorizedNodeSeeds) && raw.authorizedNodeSeeds.length > 0) {
    return raw.authorizedNodeSeeds.map((seed, index) => {
      const identity = generateIdentity(requireHex(seed, `authorizedNodeSeeds[${index}]`))
      return {
        nodeId: identity.publicKeyId,
        publicKey: identity.publicKey,
        feedKey: identity.feedKey
      }
    })
  }

  if (Array.isArray(raw.authorizedNodes) && raw.authorizedNodes.length > 0) {
    return raw.authorizedNodes.map((node, index) => ({
      nodeId: requireString(node.nodeId, `authorizedNodes[${index}].nodeId`),
      publicKey: requireHex(node.publicKey, `authorizedNodes[${index}].publicKey`),
      feedKey: requireString(node.feedKey, `authorizedNodes[${index}].feedKey`)
    }))
  }

  if (!required) return []
  throw new Error("Config must include authorizedNodeSeeds or authorizedNodes")
}

function normalizeRevokedNodeIds(raw, authorizedNodes) {
  if (raw.revokedNodeIds === undefined) return []
  if (!Array.isArray(raw.revokedNodeIds)) {
    throw new Error("revokedNodeIds must be an array")
  }

  const knownNodeIds = new Set(authorizedNodes.map((node) => node.nodeId))
  return raw.revokedNodeIds.map((nodeId, index) => {
    const normalized = requireString(nodeId, `revokedNodeIds[${index}]`)
    if (!knownNodeIds.has(normalized)) {
      throw new Error(`revokedNodeIds[${index}] must reference an authorized node`)
    }
    return normalized
  })
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
  if (typeof value !== "string" || !/^[0-9a-fA-F]+$/.test(value)) {
    throw new Error(`${field} must be a hex string`)
  }

  const buffer = Buffer.from(value, "hex")
  if (exactLength && buffer.length !== exactLength) {
    throw new Error(`${field} must decode to ${exactLength} bytes`)
  }
  return buffer
}
