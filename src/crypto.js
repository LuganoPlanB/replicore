import {
  createCipheriv,
  createDecipheriv,
  createHash,
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  randomBytes,
  sign,
  verify
} from "node:crypto"

/**
 * Create a stable node identity for signing operations.
 *
 * @returns {{ publicKeyId: string, publicKeyPem: string, privateKeyPem: string }}
 */
export function generateIdentity() {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519")
  const publicKeyPem = publicKey.export({ type: "spki", format: "pem" }).toString()
  const privateKeyPem = privateKey.export({ type: "pkcs8", format: "pem" }).toString()

  return {
    publicKeyId: keyIdFromPublicKey(publicKeyPem),
    publicKeyPem,
    privateKeyPem
  }
}

/**
 * Convert a PEM public key to the stable node id used in operations.
 *
 * @param {string} publicKeyPem
 * @returns {string}
 */
export function keyIdFromPublicKey(publicKeyPem) {
  const der = createPublicKey(publicKeyPem).export({ type: "spki", format: "der" })
  return createHash("sha256").update(der).digest("hex")
}

/**
 * @param {string} privateKeyPem
 * @param {Buffer} payload
 * @returns {string}
 */
export function signPayload(privateKeyPem, payload) {
  return sign(null, payload, createPrivateKey(privateKeyPem)).toString("base64url")
}

/**
 * @param {string} publicKeyPem
 * @param {Buffer} payload
 * @param {string} signature
 * @returns {boolean}
 */
export function verifyPayload(publicKeyPem, payload, signature) {
  return verify(
    null,
    payload,
    createPublicKey(publicKeyPem),
    Buffer.from(signature, "base64url")
  )
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
