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
  resolveTransportIdentity,
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
    assert.equal(config.compatibilityMode, "legacy-static-membership")
    assert.equal(config.machineId, "local-demo-node-1")
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

test("loadRuntimeConfig requires an explicit legacy compatibility mode for static membership files", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-compat-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/node-1.json"), "utf8")
    )
    raw.dataDir = "./data"

    delete raw.compatibilityMode
    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await assert.rejects(
      loadRuntimeConfig(configPath),
      /compatibilityMode must be set to "legacy-static-membership"/
    )

    raw.compatibilityMode = "wrong-mode"
    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await assert.rejects(
      loadRuntimeConfig(configPath),
      /compatibilityMode must be set to "legacy-static-membership"/
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

test("loadRuntimeConfig accepts explicit node transport identity overrides", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-transport-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/node-1.json"), "utf8")
    )
    raw.dataDir = "./data"
    raw.machineId = "override-machine"
    raw.nodeIdentitySeed =
      "abababababababababababababababababababababababababababababababab"

    await writeFile(configPath, JSON.stringify(raw, null, 2))

    const config = await loadRuntimeConfig(configPath)
    assert.equal(config.machineId, "override-machine")
    assert.equal(config.nodeIdentitySeed.length, 32)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig rejects learner role in legacy static membership mode", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-learner-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/node-1.json"), "utf8")
    )
    raw.dataDir = "./data"
    raw.role = "learner"
    raw.identitySeed = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
    raw.authorizedNodeSeeds = raw.authorizedNodeSeeds.slice(1)

    await writeFile(configPath, JSON.stringify(raw, null, 2))

    await assert.rejects(
      loadRuntimeConfig(configPath),
      /legacy-static-membership" is incompatible with role "learner"/
    )
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

test("resolveTransportIdentity derives stable machine-scoped transport keys", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-transport-"))

  try {
    const clusterSecretA = Buffer.from(
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "hex"
    )
    const clusterSecretB = Buffer.from(
      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "hex"
    )

    const machineA = await resolveTransportIdentity({
      dataDir: path.join(dir, "machine-a"),
      clusterSecret: clusterSecretA,
      machineId: "machine-a"
    })
    const machineARepeat = await resolveTransportIdentity({
      dataDir: path.join(dir, "machine-a"),
      clusterSecret: clusterSecretA,
      machineId: "machine-a"
    })
    const machineB = await resolveTransportIdentity({
      dataDir: path.join(dir, "machine-b"),
      clusterSecret: clusterSecretA,
      machineId: "machine-b"
    })
    const clusterB = await resolveTransportIdentity({
      dataDir: path.join(dir, "cluster-b"),
      clusterSecret: clusterSecretB,
      machineId: "machine-a"
    })

    assert.equal(machineA.publicKeyHex, machineARepeat.publicKeyHex)
    assert.notEqual(machineA.publicKeyHex, machineB.publicKeyHex)
    assert.notEqual(machineA.publicKeyHex, clusterB.publicKeyHex)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("resolveTransportIdentity fails closed if the persisted identity no longer matches", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-transport-mismatch-"))

  try {
    await resolveTransportIdentity({
      dataDir: dir,
      clusterSecret: Buffer.from(
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "hex"
      ),
      machineId: "machine-a"
    })

    await assert.rejects(
      resolveTransportIdentity({
        dataDir: dir,
        clusterSecret: Buffer.from(
          "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
          "hex"
        ),
        machineId: "machine-a"
      }),
      /Persisted transport identity does not match derived identity/
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
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
