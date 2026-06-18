import { createHash } from "node:crypto"

import { argon2id } from "hash-wasm"

export const CLUSTER_SECRET_KDF_PARAMS = Object.freeze({
  memorySize: 65536,
  iterations: 3,
  parallelism: 1
})

export const DISCOVERY_TOPIC_PURPOSE = "replicore:dht-topic:v1"
export const NOISE_NODE_KEY_PURPOSE = "replicore:noise-node-key:v1"
export const CLUSTER_SECRET_SALT_PREFIX = "replicore:kdf:v1"

/**
 * Derive purpose-scoped bytes from the shared cluster secret.
 *
 * @param {{
 *   clusterSecret: Buffer,
 *   purpose: string,
 *   context: string,
 *   length: number
 * }} input
 * @returns {Promise<Buffer>}
 */
export async function deriveClusterScopedBytes(input) {
  if (!Buffer.isBuffer(input.clusterSecret) || input.clusterSecret.length === 0) {
    throw new Error("clusterSecret must be a non-empty Buffer")
  }
  if (typeof input.purpose !== "string" || input.purpose.length === 0) {
    throw new Error("purpose must be a non-empty string")
  }
  if (typeof input.context !== "string" || input.context.length === 0) {
    throw new Error("context must be a non-empty string")
  }
  if (!Number.isInteger(input.length) || input.length <= 0) {
    throw new Error("length must be a positive integer")
  }

  const output = await argon2id({
    password: input.clusterSecret,
    salt: Buffer.from(
      `${CLUSTER_SECRET_SALT_PREFIX}:${input.purpose}:${input.context}`,
      "utf8"
    ),
    ...CLUSTER_SECRET_KDF_PARAMS,
    hashLength: input.length,
    outputType: "binary"
  })

  return Buffer.from(output)
}

/**
 * Derive the shared swarm topic from a cluster secret and cluster identifier.
 *
 * @param {{ clusterSecret: Buffer, clusterId: string }} input
 * @returns {Promise<Buffer>}
 */
export function deriveDiscoveryTopic(input) {
  return deriveClusterScopedBytes({
    clusterSecret: input.clusterSecret,
    purpose: DISCOVERY_TOPIC_PURPOSE,
    context: input.clusterId,
    length: 32
  })
}

/**
 * Derive a Noise seed for one machine identity input.
 *
 * @param {{ clusterSecret: Buffer, machineIdentity: string }} input
 * @returns {Promise<Buffer>}
 */
export function deriveNoiseSeed(input) {
  return deriveClusterScopedBytes({
    clusterSecret: input.clusterSecret,
    purpose: NOISE_NODE_KEY_PURPOSE,
    context: input.machineIdentity,
    length: 32
  })
}

/**
 * Legacy topic derivation kept only for older direct test fixtures.
 *
 * @param {{ clusterId: string, topicSalt: string }} input
 * @returns {Buffer}
 */
export function deriveLegacyTopic(input) {
  const hash = createHash("blake2b512")
  hash.update(`planb-cleard-kv-swarm:v1:${input.clusterId}:${input.topicSalt}`)
  return hash.digest().subarray(0, 32)
}
