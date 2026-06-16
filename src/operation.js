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
      ? encryptString(JSON.stringify(input.value), input.encryptionKey)
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
 * @param {Record<string, unknown>} operation
 * @param {Buffer} encryptionKey
 * @returns {unknown}
 */
export function decryptOperationValue(operation, encryptionKey) {
  if (operation.type !== "put") return null
  const payload = /** @type {{ alg: string, iv: string, ciphertext: string, tag: string }} */ (
    operation.value
  )
  return JSON.parse(decryptString(payload, encryptionKey))
}
