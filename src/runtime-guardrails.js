import { createHash } from "node:crypto"
import { mkdir, readFile, writeFile } from "node:fs/promises"
import { join } from "node:path"

const RUNTIME_GUARDRAILS_FILE = "replicore-runtime.json"

/**
 * Bind one data directory to one local identity and cluster secret hash so
 * operator mistakes fail closed on restart.
 *
 * @param {{
 *   dataDir: string,
 *   clusterId: string,
 *   clusterSecret: Buffer,
 *   identity: { publicKeyId: string, publicKey: Buffer, feedKey: string },
 *   compatibilityMode: string,
 *   initCluster: boolean
 * }} options
 */
export async function verifyOrPersistRuntimeGuardrails(options) {
  await mkdir(options.dataDir, { recursive: true })
  const filePath = join(options.dataDir, RUNTIME_GUARDRAILS_FILE)
  const next = {
    version: 1,
    clusterId: options.clusterId,
    clusterSecretHash: hashClusterSecret(options.clusterSecret),
    identity: {
      nodeId: options.identity.publicKeyId,
      publicKey: options.identity.publicKey.toString("hex"),
      feedKey: options.identity.feedKey
    },
    startupMode: {
      compatibilityMode: options.compatibilityMode,
      initCluster: options.initCluster
    }
  }

  try {
    const persisted = JSON.parse(await readFile(filePath, "utf8"))
    assertPersistedIdentity(persisted, filePath)

    if (persisted.clusterId !== next.clusterId) {
      throw new Error(`Persisted clusterId does not match ${filePath}`)
    }
    if (persisted.clusterSecretHash !== next.clusterSecretHash) {
      throw new Error(`Persisted clusterSecret does not match ${filePath}`)
    }
    if (
      persisted.identity.nodeId !== next.identity.nodeId ||
      persisted.identity.publicKey !== next.identity.publicKey ||
      persisted.identity.feedKey !== next.identity.feedKey
    ) {
      throw new Error(`Persisted node identity does not match ${filePath}`)
    }
    if (options.initCluster && persisted.startupMode?.initCluster !== true) {
      throw new Error(
        `initCluster cannot be used on a data directory that already joined another cluster: ${filePath}`
      )
    }
    return
  } catch (error) {
    if (error?.code !== "ENOENT") throw error
  }

  await writeFile(filePath, JSON.stringify(next, null, 2))
}

function hashClusterSecret(clusterSecret) {
  return createHash("sha256").update(clusterSecret).digest("hex")
}

function assertPersistedIdentity(persisted, filePath) {
  if (!persisted?.identity?.nodeId || !persisted.identity.publicKey || !persisted.identity.feedKey) {
    throw new Error(`Persisted identity metadata is missing in ${filePath}`)
  }
}
