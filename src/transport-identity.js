import { mkdir, readFile, writeFile } from "node:fs/promises"
import { join } from "node:path"

import DHT from "hyperdht"

import { deriveNoiseSeed } from "./cluster-secret.js"

const TRANSPORT_IDENTITY_FILE = "transport-identity.json"

/**
 * Resolve the Holepunch transport identity for one node and persist the first
 * derived public key so unexpected machine-id drift fails closed.
 *
 * @param {{
 *   dataDir: string,
 *   clusterSecret?: Buffer,
 *   machineId?: string,
 *   nodeIdentitySeed?: Buffer
 * }} options
 * @returns {Promise<null | {
 *   keyPair: { publicKey: Buffer, secretKey: Buffer },
 *   publicKey: Buffer,
 *   publicKeyHex: string,
 *   seedSource: string
 * }>}
 */
export async function resolveTransportIdentity(options) {
  const seed = await resolveTransportSeed(options)
  if (!seed) return null

  const keyPair = DHT.keyPair(seed)
  await persistTransportIdentity({
    dataDir: options.dataDir,
    publicKeyHex: keyPair.publicKey.toString("hex"),
    seedSource:
      options.nodeIdentitySeed ? "nodeIdentitySeed" : options.machineId ? "machineId" : "/etc/machine-id"
  })

  return {
    keyPair,
    publicKey: keyPair.publicKey,
    publicKeyHex: keyPair.publicKey.toString("hex"),
    seedSource:
      options.nodeIdentitySeed ? "nodeIdentitySeed" : options.machineId ? "machineId" : "/etc/machine-id"
  }
}

/**
 * @param {{
 *   clusterSecret?: Buffer,
 *   machineId?: string,
 *   nodeIdentitySeed?: Buffer
 * }} options
 * @returns {Promise<Buffer | null>}
 */
async function resolveTransportSeed(options) {
  if (options.nodeIdentitySeed) {
    if (!Buffer.isBuffer(options.nodeIdentitySeed) || options.nodeIdentitySeed.length !== 32) {
      throw new Error("nodeIdentitySeed must decode to 32 bytes")
    }
    return options.nodeIdentitySeed
  }

  if (!options.clusterSecret) return null

  const machineIdentity = options.machineId ?? (await readMachineIdentity())
  return deriveNoiseSeed({
    clusterSecret: options.clusterSecret,
    machineIdentity
  })
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
 * @param {{ dataDir: string, publicKeyHex: string, seedSource: string }} options
 */
async function persistTransportIdentity(options) {
  await mkdir(options.dataDir, { recursive: true })
  const filePath = join(options.dataDir, TRANSPORT_IDENTITY_FILE)

  try {
    const existing = JSON.parse(await readFile(filePath, "utf8"))
    if (existing.publicKey !== options.publicKeyHex) {
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
        version: 1,
        publicKey: options.publicKeyHex,
        seedSource: options.seedSource
      },
      null,
      2
    )
  )
}
