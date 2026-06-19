import { createHash } from "node:crypto"

import { canonicalize } from "./canonical.js"
import { signPayload, verifyPayload } from "./crypto.js"

/**
 * Create a signed promotion credential from a canonical payload.
 *
 * @param {{
 *   payload: {
 *     v: 1,
 *     type: string,
 *     clusterId: string,
 *     membershipVersion: number,
 *     learnerNodeId: string,
 *     learnerNoisePublicKey: string,
 *     targetRole: string,
 *     issuedAt: string,
 *     expiresAt: string,
 *     nonce: string,
 *     signerNodeId: string
 *   },
 *   signerSecretKey: Buffer
 * }} input
 */
export function createPromotionCredential(input) {
  const payloadBytes = Buffer.from(canonicalize(input.payload))
  return {
    payload: input.payload,
    signature: signPayload(input.signerSecretKey, payloadBytes)
  }
}

/**
 * @param {{ payload: Record<string, unknown>, signature: string }} credential
 */
export function hashPromotionCredential(credential) {
  return createHash("sha256").update(canonicalize(credential)).digest("hex")
}

/**
 * Validate a promotion credential against the current learner context.
 *
 * @param {{ payload: Record<string, unknown>, signature: string }} credential
 * @param {{
 *   clusterId: string,
 *   membershipVersion: number,
 *   learnerNodeId: string,
 *   learnerNoisePublicKey: string,
 *   authorizedNodes: Array<{ nodeId: string, publicKey: Buffer }>,
 *   now?: Date,
 *   seenCredentialHashes?: Set<string>,
 *   seenNonces?: Set<string>,
 *   isCaughtUp?: boolean
 * }} context
 */
export function validatePromotionCredential(credential, context) {
  if (!credential || typeof credential !== "object") {
    throw new Error("Promotion credential must be an object")
  }
  if (!credential.payload || typeof credential.payload !== "object") {
    throw new Error("Promotion credential payload is required")
  }
  if (typeof credential.signature !== "string" || credential.signature.length === 0) {
    throw new Error("Promotion credential signature is required")
  }

  const payload = credential.payload
  if (payload.v !== 1) throw new Error("Promotion credential version must be 1")
  if (payload.type !== "replicore.promotion") {
    throw new Error("Promotion credential type must be replicore.promotion")
  }
  if (payload.clusterId !== context.clusterId) {
    throw new Error("Promotion credential clusterId does not match this cluster")
  }
  if (payload.membershipVersion !== context.membershipVersion) {
    throw new Error("Promotion credential membershipVersion does not match")
  }
  if (payload.learnerNodeId !== context.learnerNodeId) {
    throw new Error("Promotion credential learnerNodeId does not match this learner")
  }
  if (payload.learnerNoisePublicKey !== context.learnerNoisePublicKey) {
    throw new Error("Promotion credential learnerNoisePublicKey does not match this learner")
  }
  if (payload.targetRole !== "voter") {
    throw new Error("Promotion credential targetRole must be voter")
  }
  if (typeof payload.nonce !== "string" || payload.nonce.length === 0) {
    throw new Error("Promotion credential nonce is required")
  }

  const signer = context.authorizedNodes.find((node) => node.nodeId === payload.signerNodeId)
  if (!signer) {
    throw new Error("Promotion credential signer is not an authorized voter")
  }

  const issuedAt = new Date(payload.issuedAt)
  const expiresAt = new Date(payload.expiresAt)
  if (Number.isNaN(issuedAt.getTime()) || Number.isNaN(expiresAt.getTime())) {
    throw new Error("Promotion credential timestamps must be valid ISO-8601 strings")
  }
  if (issuedAt.getTime() > expiresAt.getTime()) {
    throw new Error("Promotion credential expiresAt must be after issuedAt")
  }

  const now = context.now ?? new Date()
  if (expiresAt.getTime() <= now.getTime()) {
    throw new Error("Promotion credential has expired")
  }
  if (context.isCaughtUp === false) {
    throw new Error("Learner must catch up before accepting a promotion credential")
  }

  const credentialHash = hashPromotionCredential(credential)
  if (context.seenCredentialHashes?.has(credentialHash)) {
    throw new Error("Promotion credential hash was already submitted")
  }
  if (context.seenNonces?.has(payload.nonce)) {
    throw new Error("Promotion credential nonce was already submitted")
  }

  const payloadBytes = Buffer.from(canonicalize(payload))
  if (!verifyPayload(signer.publicKey, payloadBytes, credential.signature)) {
    throw new Error("Promotion credential signature is invalid")
  }

  return {
    credentialHash,
    signerNodeId: payload.signerNodeId,
    learnerNodeId: payload.learnerNodeId,
    learnerNoisePublicKey: payload.learnerNoisePublicKey,
    targetRole: payload.targetRole,
    expiresAt: payload.expiresAt
  }
}
