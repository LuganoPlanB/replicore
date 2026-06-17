import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes
} from "node:crypto"
import crypto from "hypercore-crypto"
import Hypercore from "hypercore"

/**
 * Create a stable node identity for signing operations.
 *
 * @param {Buffer} [seed]
 * @returns {{ publicKeyId: string, publicKey: Buffer, secretKey: Buffer, feedKey: string }}
 */
export function generateIdentity(seed) {
  const keyPair = crypto.keyPair(seed)

  return {
    publicKeyId: keyIdFromPublicKey(keyPair.publicKey),
    publicKey: keyPair.publicKey,
    secretKey: keyPair.secretKey,
    feedKey: Buffer.from(Hypercore.key(keyPair.publicKey)).toString("hex")
  }
}

/**
 * Convert a public key to the stable node id used in operations.
 *
 * @param {Buffer} publicKey
 * @returns {string}
 */
export function keyIdFromPublicKey(publicKey) {
  return createHash("sha256").update(publicKey).digest("hex")
}

/**
 * @param {Buffer} secretKey
 * @param {Buffer} payload
 * @returns {string}
 */
export function signPayload(secretKey, payload) {
  return Buffer.from(crypto.sign(payload, secretKey)).toString("base64url")
}

/**
 * @param {Buffer} publicKey
 * @param {Buffer} payload
 * @param {string} signature
 * @returns {boolean}
 */
export function verifyPayload(publicKey, payload, signature) {
  return crypto.verify(payload, Buffer.from(signature, "base64url"), publicKey)
}

/**
 * @param {string} plaintext
 * @param {Buffer} key
 * @returns {{ alg: string, iv: string, ciphertext: string, tag: string }}
 */
export function encryptString(plaintext, key) {
  if (key.length !== 32) {
    throw new Error("Encryption key must be 32 bytes for aes-256-gcm")
  }

  const iv = randomBytes(12)
  const cipher = createCipheriv("aes-256-gcm", key, iv)
  const ciphertext = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()])
  const tag = cipher.getAuthTag()

  return {
    alg: "aes-256-gcm",
    iv: iv.toString("base64url"),
    ciphertext: ciphertext.toString("base64url"),
    tag: tag.toString("base64url")
  }
}

/**
 * @param {{ alg: string, iv: string, ciphertext: string, tag: string }} payload
 * @param {Buffer} key
 * @returns {string}
 */
export function decryptString(payload, key) {
  if (payload.alg !== "aes-256-gcm") {
    throw new Error(`Unsupported encryption algorithm: ${payload.alg}`)
  }

  const decipher = createDecipheriv(
    "aes-256-gcm",
    key,
    Buffer.from(payload.iv, "base64url")
  )
  decipher.setAuthTag(Buffer.from(payload.tag, "base64url"))
  const plaintext = Buffer.concat([
    decipher.update(Buffer.from(payload.ciphertext, "base64url")),
    decipher.final()
  ])

  return plaintext.toString("utf8")
}
