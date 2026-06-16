import { createHash } from "node:crypto"

/**
 * Derive the shared Hyperswarm topic from public cluster inputs.
 *
 * @param {{ clusterId: string, topicSalt: string }} input
 * @returns {Buffer}
 */
export function deriveTopic(input) {
  const hash = createHash("blake2b512")
  hash.update(`planb-cleard-kv-swarm:v1:${input.clusterId}:${input.topicSalt}`)
  return hash.digest().subarray(0, 32)
}
