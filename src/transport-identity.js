import { mkdir, readFile, writeFile } from "node:fs/promises"
import { join } from "node:path"

import crypto from "hypercore-crypto"
import DHT from "hyperdht"

import { deriveJoinSeed, deriveMachineId, deriveNoiseSeed } from "./cluster-secret.js"

const TRANSPORT_IDENTITY_FILE = "transport-identity.json"

/**
 * Resolve the Holepunch transport identity for one node and persist the first
 * derived public key so unexpected machine-id drift fails closed.
 *
 * @param {{
 *   dataDir: string,
 *   clusterSecret?: Buffer,
 *   machineId?: string
 * }} options
 * @returns {Promise<null | {
 *   machineId: string,
 *   keyPair: { publicKey: Buffer, secretKey: Buffer },
 *   joinKeyPair: { publicKey: Buffer, secretKey: Buffer },
 *   publicKey: Buffer,
 *   publicKeyHex: string,
 *   joinPublicKey: Buffer,
 *   joinPublicKeyHex: string,
 *   machineIdentitySource: string
 * }>}
 */
export async function resolveTransportIdentity(options) {
  const derived = await resolveDerivedIdentity(options)
  if (!derived) return null

  const keyPair = DHT.keyPair(derived.noiseSeed)
  const joinKeyPair = crypto.keyPair(derived.joinSeed)
  await persistTransportIdentity({
    dataDir: options.dataDir,
    machineId: derived.machineId,
    publicKeyHex: keyPair.publicKey.toString("hex"),
    joinPublicKeyHex: joinKeyPair.publicKey.toString("hex"),
    machineIdentitySource: derived.machineIdentitySource
  })

  return {
    machineId: derived.machineId,
    keyPair,
    joinKeyPair,
    publicKey: keyPair.publicKey,
    publicKeyHex: keyPair.publicKey.toString("hex"),
    joinPublicKey: joinKeyPair.publicKey,
    joinPublicKeyHex: joinKeyPair.publicKey.toString("hex"),
    machineIdentitySource: derived.machineIdentitySource
  }
}

/**
 * @param {{
 *   clusterSecret?: Buffer,
 *   machineId?: string
 * }} options
 * @returns {Promise<null | {
 *   machineId: string,
 *   noiseSeed: Buffer,
 *   joinSeed: Buffer,
 *   machineIdentitySource: string
 * }>}
 */
async function resolveDerivedIdentity(options) {
  if (!options.clusterSecret) return null

  const machineIdentity = options.machineId ?? (await readMachineIdentity())
  const machineIdentitySource = options.machineId ? "config.machineId" : "/etc/machine-id"
  const machineId = (
    await deriveMachineId({
      clusterSecret: options.clusterSecret,
      machineIdentity
    })
  ).toString("hex")
  const [noiseSeed, joinSeed] = await Promise.all([
    deriveNoiseSeed({
      clusterSecret: options.clusterSecret,
      machineId
    }),
    deriveJoinSeed({
      clusterSecret: options.clusterSecret,
      machineId
    })
  ])

  return {
    machineId,
    noiseSeed,
    joinSeed,
    machineIdentitySource
  }
}

/**
 * Derive the join-signing key pair for one cluster-scoped machine identifier.
 *
 * @param {{ clusterSecret: Buffer, machineId: string }} options
 */
export async function deriveJoinKeyPair(options) {
  const seed = await deriveJoinSeed({
    clusterSecret: options.clusterSecret,
    machineId: options.machineId
  })
  const keyPair = crypto.keyPair(seed)
  return {
    keyPair,
    publicKey: keyPair.publicKey,
    publicKeyHex: keyPair.publicKey.toString("hex")
  }
}

/**
 * @returns {Promise<string>}
 */
async function readMachineIdentity() {
  const machineIdentity = (await readFile("/etc/machine-id", "utf8")).trim()
  if (!machineIdentity) {
    throw new Error("Machine identity source /etc/machine-id is empty")
  }
  return machineIdentity
}

/**
 * @param {{
 *   dataDir: string,
 *   machineId: string,
 *   publicKeyHex: string,
 *   joinPublicKeyHex: string,
 *   machineIdentitySource: string
 * }} options
 */
async function persistTransportIdentity(options) {
  await mkdir(options.dataDir, { recursive: true })
  const filePath = join(options.dataDir, TRANSPORT_IDENTITY_FILE)

  try {
    const existing = JSON.parse(await readFile(filePath, "utf8"))
    if (
      existing.machineId !== options.machineId ||
      existing.publicKey !== options.publicKeyHex ||
      existing.joinPublicKey !== options.joinPublicKeyHex
    ) {
      throw new Error(
        `Persisted transport identity does not match derived identity in ${filePath}`
      )
    }
    return
  } catch (error) {
    if (error?.code !== "ENOENT") throw error
  }

  await writeFile(
    filePath,
    JSON.stringify(
      {
        version: 2,
        machineId: options.machineId,
        publicKey: options.publicKeyHex,
        joinPublicKey: options.joinPublicKeyHex,
        machineIdentitySource: options.machineIdentitySource
      },
      null,
      2
    )
  )
}
