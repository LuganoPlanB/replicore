import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import test from "node:test"
import assert from "node:assert/strict"

import {
  CLUSTER_SECRET_KDF_PARAMS,
  deriveDiscoveryTopic,
  deriveNoiseSeed,
  generateIdentity,
  loadRuntimeConfig,
  readSnapshotFile,
  writeSnapshotFile
} from "../src/index.js"

test("loadRuntimeConfig derives identities and resolves paths", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/node-1.json"), "utf8")
    )
    raw.dataDir = "./data"

    await writeFile(configPath, JSON.stringify(raw, null, 2))

    const config = await loadRuntimeConfig(configPath)
    assert.equal(config.clusterId, "local-demo")
    assert.equal(config.clusterSecret.length, 32)
    assert.equal(config.http.port, 3001)
    assert.equal(config.authorizedNodes.length, 3)
    assert.equal(config.dataDir, path.join(dir, "data"))
    assert.equal(
      config.authorizedNodes.some((node) => node.nodeId === config.identity.publicKeyId),
      true
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig requires a 32-byte clusterSecret", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-secret-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/node-1.json"), "utf8")
    )
    raw.dataDir = "./data"

    delete raw.clusterSecret
    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await assert.rejects(loadRuntimeConfig(configPath), /clusterSecret must be a hex string/)

    raw.clusterSecret = "xyz"
    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await assert.rejects(loadRuntimeConfig(configPath), /clusterSecret must be a hex string/)

    raw.clusterSecret = "aa"
    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await assert.rejects(
      loadRuntimeConfig(configPath),
      /clusterSecret must decode to 32 bytes/
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig supports encryption keyrings and revoked writers", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-rotation-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/node-1.json"), "utf8")
    )
    raw.dataDir = "./data"
    raw.encryption = {
      currentKeyId: "primary",
      keys: {
        primary: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        next: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      }
    }
    delete raw.encryptionKey
    const revokedIdentity = generateIdentity(Buffer.from(raw.authorizedNodeSeeds[2], "hex"))
    raw.revokedNodeIds = [revokedIdentity.publicKeyId]

    await writeFile(configPath, JSON.stringify(raw, null, 2))

    const config = await loadRuntimeConfig(configPath)
    assert.equal(config.encryption.currentKeyId, "primary")
    assert.equal(Buffer.isBuffer(config.encryption.keys.primary), true)
    assert.deepEqual(config.revokedNodeIds, [revokedIdentity.publicKeyId])
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("cluster secret derivation is deterministic and purpose-separated", async () => {
  const clusterSecret = Buffer.from(
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "hex"
  )

  assert.deepEqual(CLUSTER_SECRET_KDF_PARAMS, {
    memorySize: 65536,
    iterations: 3,
    parallelism: 1
  })

  const topicA = await deriveDiscoveryTopic({
    clusterSecret,
    clusterId: "cluster-a"
  })
  const topicARepeat = await deriveDiscoveryTopic({
    clusterSecret,
    clusterId: "cluster-a"
  })
  const topicB = await deriveDiscoveryTopic({
    clusterSecret,
    clusterId: "cluster-b"
  })
  const noiseA = await deriveNoiseSeed({
    clusterSecret,
    machineIdentity: "machine-a"
  })
  const noiseARepeat = await deriveNoiseSeed({
    clusterSecret,
    machineIdentity: "machine-a"
  })
  const noiseB = await deriveNoiseSeed({
    clusterSecret,
    machineIdentity: "machine-b"
  })

  assert.equal(topicA.length, 32)
  assert.equal(noiseA.length, 32)
  assert.deepEqual(topicA, topicARepeat)
  assert.deepEqual(noiseA, noiseARepeat)
  assert.notDeepEqual(topicA, topicB)
  assert.notDeepEqual(noiseA, noiseB)
  assert.notDeepEqual(topicA, noiseA)
})

test("snapshot file helpers round-trip JSON snapshots", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-snapshot-"))

  try {
    const filePath = path.join(dir, "snapshot.json")
    const snapshot = {
      version: 1,
      createdAt: new Date().toISOString(),
      entries: [{ key: "kv/current/default/hash:test", value: { deleted: false } }]
    }

    await writeSnapshotFile(filePath, snapshot)
    const loaded = await readSnapshotFile(filePath)
    assert.deepEqual(loaded, snapshot)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})
