import { createHash } from "node:crypto"

import { canonicalize } from "./canonical.js"
import { decryptString, encryptString, signPayload, verifyPayload } from "./crypto.js"

/**
 * Build and sign a leader operation before appending it to Hypercore.
 *
 * @param {{
 *   type: "put" | "delete",
 *   key: string,
 *   keyspace?: string,
  *   value?: unknown,
  *   seq: number,
  *   feed: string,
  *   actor: string,
 *   secretKey: Buffer,
  *   encryptionKey: Buffer,
  *   encryptionKeyId?: string,
 *   ttlMs?: number,
 *   kind?: "kv" | "heartbeat",
 *   heartbeat?: null | {
 *     observedLeader: string | null,
 *     reachableLeader: boolean,
 *     appliedFeeds: Record<string, number>
 *   }
 * }} input
 * @returns {Record<string, unknown>}
 */
export function createSignedOperation(input) {
  const ts = new Date().toISOString()
  const expiresAt = new Date(Date.now() + (input.ttlMs ?? 1000 * 60 * 60 * 24 * 30 * 6)).toISOString()
  const kind = input.kind ?? "kv"
  const value =
    kind === "kv" && input.type === "put"
      ? {
          ...encryptString(JSON.stringify(input.value), input.encryptionKey),
          keyId: input.encryptionKeyId ?? "default"
        }
      : null

  const unsigned = {
    v: 1,
    kind,
    feed: input.feed,
    seq: input.seq,
    type: input.type,
    key: input.key,
    keyspace: input.keyspace ?? "default",
    value,
    heartbeat: input.heartbeat ?? null,
    ts,
    expiresAt,
    actor: input.actor
  }

  const unsignedBytes = Buffer.from(canonicalize(unsigned))
  const opId = createHash("sha256").update(unsignedBytes).digest("hex")
  const signature = signPayload(input.secretKey, unsignedBytes)

  return {
    ...unsigned,
    opId,
    signature
  }
}

/**
 * @param {Record<string, unknown>} operation
 * @param {Buffer} publicKey
 * @returns {boolean}
 */
export function verifySignedOperation(operation, publicKey) {
  const { signature, opId, ...unsigned } = operation
  if (typeof signature !== "string" || typeof opId !== "string") return false

  const unsignedBytes = Buffer.from(canonicalize(unsigned))
  const expectedOpId = createHash("sha256").update(unsignedBytes).digest("hex")
  if (expectedOpId !== opId) return false

  return verifyPayload(publicKey, unsignedBytes, signature)
}

/**
 * Validate the operation shape and its feed/actor invariants.
 *
 * @param {Record<string, unknown>} operation
 * @param {{ nodeId: string, feedKey: string }} expected
 * @param {{ revokedNodeIds?: Set<string> }} [options]
 */
export function validateOperation(operation, expected, options = {}) {
  if (operation.v !== 1) {
    throw new Error(`Unsupported operation version: ${operation.v}`)
  }

  if (operation.actor !== expected.nodeId) {
    throw new Error(`Operation actor mismatch: expected ${expected.nodeId}`)
  }

  if (options.revokedNodeIds?.has(expected.nodeId)) {
    throw new Error(`Operation signer is revoked: ${expected.nodeId}`)
  }

  if (operation.feed !== expected.feedKey) {
    throw new Error(`Operation feed mismatch: expected ${expected.feedKey}`)
  }

  if (typeof operation.seq !== "number" || operation.seq < 0 || !Number.isInteger(operation.seq)) {
    throw new Error("Operation sequence must be a non-negative integer")
  }

  if (typeof operation.opId !== "string" || typeof operation.signature !== "string") {
    throw new Error("Operation must include opId and signature")
  }

  if (operation.kind === "heartbeat") {
    if (operation.type !== "put") {
      throw new Error("Heartbeat operations must use type=put")
    }
    if (typeof operation.heartbeat !== "object" || operation.heartbeat === null) {
      throw new Error("Heartbeat operation must include heartbeat metadata")
    }
    return
  }

  if (operation.kind !== "kv") {
    throw new Error(`Unsupported operation kind: ${operation.kind}`)
  }

  if (operation.type !== "put" && operation.type !== "delete") {
    throw new Error(`Unsupported K/V operation type: ${operation.type}`)
  }

  if (typeof operation.key !== "string" || typeof operation.keyspace !== "string") {
    throw new Error("K/V operation must include key and keyspace")
  }

  if (operation.type === "put" && !operation.value) {
    throw new Error("Put operation must include an encrypted value")
  }

  if (operation.type === "delete" && operation.value !== null) {
    throw new Error("Delete operation must store a null value payload")
  }
}

/**
 * @param {Record<string, unknown>} operation
 * @param {Buffer | Record<string, Buffer>} encryptionKeys
 * @returns {unknown}
 */
export function decryptOperationValue(operation, encryptionKeys) {
  if (operation.type !== "put") return null
  const payload = /** @type {{ alg: string, iv: string, ciphertext: string, tag: string, keyId?: string }} */ (
    operation.value
  )
  return JSON.parse(decryptString(payload, resolveEncryptionKey(encryptionKeys, payload.keyId)))
}

/**
 * @param {Buffer | Record<string, Buffer>} encryptionKeys
 * @param {string | undefined} keyId
 */
function resolveEncryptionKey(encryptionKeys, keyId) {
  if (Buffer.isBuffer(encryptionKeys)) {
    return encryptionKeys
  }

  const resolvedKeyId = keyId ?? "default"
  const encryptionKey = encryptionKeys[resolvedKeyId]
  if (!encryptionKey) {
    throw new Error(`Missing encryption key for keyId ${resolvedKeyId}`)
  }
  return encryptionKey
}
