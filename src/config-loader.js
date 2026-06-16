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
  const encryptionKey = requireHex(raw.encryptionKey, "encryptionKey", 32)
  const identity = generateIdentity(identitySeed)

  const authorizedNodes = normalizeAuthorizedNodes(raw)
  if (!authorizedNodes.some((node) => node.nodeId === identity.publicKeyId)) {
    throw new Error("Authorized nodes do not include the local identity")
  }

  return {
    configPath: absolutePath,
    dataDir: path.resolve(path.dirname(absolutePath), raw.dataDir),
    clusterId: requireString(raw.clusterId, "clusterId"),
    topicSalt: requireString(raw.topicSalt, "topicSalt"),
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
    encryptionKey,
    http: {
      host: raw.http?.host ?? "127.0.0.1",
      port: raw.http?.port ?? 0
    },
    auth: {
      tokens: raw.auth?.tokens ?? {}
    }
  }
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
