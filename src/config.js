import { deriveDiscoveryTopic, deriveLegacyTopic } from "./cluster-secret.js"

/**
 * Derive the shared Hyperswarm topic from the configured cluster inputs.
 *
 * `clusterSecret` is the production path. `topicSalt` remains available so the
 * existing direct-constructor tests can migrate in smaller slices.
 *
 * @param {{ clusterId: string, clusterSecret?: Buffer, topicSalt?: string }} input
 * @returns {Promise<Buffer>}
 */
export async function deriveTopic(input) {
  if (input.clusterSecret) {
    return deriveDiscoveryTopic({
      clusterSecret: input.clusterSecret,
      clusterId: input.clusterId
    })
  }

  if (typeof input.topicSalt === "string" && input.topicSalt.length > 0) {
    return deriveLegacyTopic(input)
  }

  throw new Error("deriveTopic requires clusterSecret or topicSalt")
}
