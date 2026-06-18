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

  const identitySeed = requireHex(raw.identitySeed, "identitySeed")
  const identity = generateIdentity(identitySeed)
  const role = normalizeRole(raw.role)
  const compatibilityMode = normalizeCompatibilityMode(raw.compatibilityMode)

  const authorizedNodes = normalizeAuthorizedNodes(raw)
  if (compatibilityMode !== "legacy-static-membership") {
    throw new Error(
      'Static membership config requires compatibilityMode "legacy-static-membership"'
    )
  }
  const localIncluded = authorizedNodes.some((node) => node.nodeId === identity.publicKeyId)
  if (role === "voter" && !localIncluded) {
    throw new Error("Authorized nodes do not include the local identity")
  }
  if (role === "learner" && localIncluded) {
    throw new Error("Learner config must not include the local identity in authorizedNodes")
  }
  if (role === "learner") {
    throw new Error('compatibilityMode "legacy-static-membership" is incompatible with role "learner"')
  }
  const revokedNodeIds = normalizeRevokedNodeIds(raw, authorizedNodes)
  if (revokedNodeIds.includes(identity.publicKeyId)) {
    throw new Error("Local identity is revoked in config")
  }
  const encryption = normalizeEncryption(raw)

  return {
    configPath: absolutePath,
    dataDir: path.resolve(path.dirname(absolutePath), raw.dataDir),
    clusterId: requireString(raw.clusterId, "clusterId"),
    clusterSecret: requireHex(raw.clusterSecret, "clusterSecret", 32),
    compatibilityMode,
    role,
    machineId: raw.machineId === undefined ? undefined : requireString(raw.machineId, "machineId"),
    bootstrap: normalizeBootstrap(raw.bootstrap ?? []),
    heartbeatIntervalMs: raw.heartbeatIntervalMs ?? 500,
    heartbeatTtlMs: raw.heartbeatTtlMs ?? 3000,
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

function normalizeCompatibilityMode(value) {
  if (value === "legacy-static-membership") return value
  throw new Error(
    'compatibilityMode must be set to "legacy-static-membership" for file-based static membership configs'
  )
}

function normalizeAuthorizedNodes(raw) {
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
