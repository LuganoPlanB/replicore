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
 *   term?: number,
 *   index?: number,
 *   prevIndex?: number,
 *   prevHash?: string | null,
 *   kind?: "kv" | "heartbeat",
 *   heartbeat?: null | {
 *     observedLeader: string | null,
 *     reachableLeader: boolean,
 *     appliedFeeds: Record<string, number>,
 *     membershipFingerprint?: string
 *   }
 * }} input
 * @returns {Record<string, unknown>}
 */
export function createSignedOperation(input) {
  const ts = new Date().toISOString()
  const expiresAt = new Date(Date.now() + (input.ttlMs ?? 1000 * 60 * 60 * 24 * 30 * 6)).toISOString()
  const kind = input.kind ?? "kv"
  const index = input.index ?? input.seq
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
    term: input.term ?? 0,
    index,
    prevIndex: input.prevIndex ?? (index === 0 ? -1 : index - 1),
    prevHash: input.prevHash ?? null,
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
  const entryHash = createHash("sha256").update(unsignedBytes).digest("hex")
  const opId = entryHash
  const signature = signPayload(input.secretKey, unsignedBytes)

  return {
    ...unsigned,
    entryHash,
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
  const { signature, opId, entryHash, ...unsigned } = operation
  if (typeof signature !== "string" || typeof opId !== "string" || typeof entryHash !== "string") return false

  const unsignedBytes = Buffer.from(canonicalize(unsigned))
  const expectedEntryHash = createHash("sha256").update(unsignedBytes).digest("hex")
  if (expectedEntryHash !== entryHash || expectedEntryHash !== opId) return false

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

  if (typeof operation.term !== "number" || operation.term < 0 || !Number.isInteger(operation.term)) {
    throw new Error("Operation term must be a non-negative integer")
  }

  if (typeof operation.index !== "number" || operation.index < 0 || !Number.isInteger(operation.index)) {
    throw new Error("Operation logical index must be a non-negative integer")
  }

  if (operation.index !== operation.seq) {
    throw new Error(`Operation logical index mismatch: expected ${operation.seq}`)
  }

  if (typeof operation.prevIndex !== "number" || !Number.isInteger(operation.prevIndex)) {
    throw new Error("Operation previous index must be an integer")
  }

  if (operation.index === 0) {
    if (operation.prevIndex !== -1) {
      throw new Error("Genesis operation must use prevIndex=-1")
    }
    if (operation.prevHash !== null) {
      throw new Error("Genesis operation must use prevHash=null")
    }
  } else {
    if (operation.prevIndex !== operation.index - 1) {
      throw new Error(`Operation previous index mismatch: expected ${operation.index - 1}`)
    }
    if (typeof operation.prevHash !== "string" || operation.prevHash.length === 0) {
      throw new Error("Non-genesis operation must include prevHash")
    }
  }

  if (typeof operation.entryHash !== "string" || typeof operation.opId !== "string" || typeof operation.signature !== "string") {
    throw new Error("Operation must include entryHash, opId, and signature")
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
 * Validate one operation against the previously accepted entry in the same
 * physical feed.
 *
 * @param {Record<string, unknown>} operation
 * @param {Record<string, unknown> | null} previousOperation
 * @param {number} slot
 */
export function validateLogLink(operation, previousOperation, slot) {
  if (operation.index !== slot) {
    throw new Error(`Operation feed slot mismatch: expected logical index ${slot}`)
  }

  if (slot === 0) {
    if (previousOperation !== null) {
      throw new Error("Genesis operation cannot have a previous entry")
    }
    return
  }

  if (!previousOperation) {
    throw new Error(`Missing previous operation for slot ${slot}`)
  }

  if (previousOperation.index !== slot - 1) {
    throw new Error(`Previous operation index mismatch at slot ${slot}`)
  }

  if (operation.prevIndex !== previousOperation.index) {
    throw new Error(`Operation previous index does not match slot ${slot - 1}`)
  }

  if (operation.prevHash !== previousOperation.entryHash) {
    const error = new Error(`Operation previous hash mismatch at slot ${slot}`)
    error.code = "LOG_MISMATCH"
    error.lastMatchingIndex = previousOperation.index
    throw error
  }

  if (operation.term < previousOperation.term) {
    const error = new Error(`Operation term regression at slot ${slot}`)
    error.code = "LOG_MISMATCH"
    error.lastMatchingIndex = previousOperation.index
    throw error
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
