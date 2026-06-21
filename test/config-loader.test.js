import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import test from "node:test"
import assert from "node:assert/strict"

import {
  CLUSTER_SECRET_KDF_PARAMS,
  deriveDiscoveryTopic,
  deriveJoinSeed,
  deriveMachineId,
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
    assert.equal(config.electionTimeoutMinMs, 900)
    assert.equal(config.electionTimeoutMaxMs, 1500)
    assert.equal(config.requestTimeoutMs, 5000)
    assert.equal(config.maxInflightReplication, 16)
    assert.equal(
      config.authorizedNodes.some((node) => node.nodeId === config.identity.publicKeyId),
      true
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig supports a secret-first learner config", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-secret-first-"))

  try {
    const configPath = path.join(dir, "joiner.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/joiner.json"), "utf8")
    )
    raw.dataDir = "./data"

    await writeFile(configPath, JSON.stringify(raw, null, 2))

    const config = await loadRuntimeConfig(configPath)
    assert.equal(config.compatibilityMode, "secret-first")
    assert.equal(config.role, "learner")
    assert.equal(config.initCluster, false)
    assert.equal(config.machineId, "local-demo-node-4")
    assert.deepEqual(config.authorizedNodes, [])
    assert.deepEqual(config.revokedNodeIds, [])
    assert.equal(config.dataDir, path.join(dir, "data"))
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig supports an explicit initCluster bootstrap voter", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-init-cluster-"))

  try {
    const configPath = path.join(dir, "init-node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/init-node.json"), "utf8")
    )
    raw.dataDir = "./data"

    await writeFile(configPath, JSON.stringify(raw, null, 2))

    const config = await loadRuntimeConfig(configPath)
    assert.equal(config.compatibilityMode, "secret-first")
    assert.equal(config.role, "voter")
    assert.equal(config.initCluster, true)
    assert.equal(config.authorizedNodes.length, 1)
    assert.equal(config.authorizedNodes[0].nodeId, config.identity.publicKeyId)
    assert.equal(config.machineId, "local-demo-init-node")

    const persisted = JSON.parse(
      await readFile(path.join(dir, "data", "replicore-runtime.json"), "utf8")
    )
    assert.equal(persisted.identity.nodeId, config.identity.publicKeyId)
    assert.equal(persisted.startupMode.initCluster, true)
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
    await assert.rejects(loadRuntimeConfig(configPath), /clusterSecret must be a non-empty hex or base58 string/)

    raw.clusterSecret = "xyz"
    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await assert.rejects(
      loadRuntimeConfig(configPath),
      /clusterSecret must decode to 32 bytes/
    )

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
      /Static membership config requires compatibilityMode "legacy-static-membership"/
    )

    raw.compatibilityMode = "wrong-mode"
    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await assert.rejects(
      loadRuntimeConfig(configPath),
      /compatibilityMode must be "legacy-static-membership" when provided/
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig rejects a secret-first voter without initCluster", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-missing-init-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/init-node.json"), "utf8")
    )
    raw.dataDir = "./data"
    delete raw.initCluster

    await writeFile(configPath, JSON.stringify(raw, null, 2))

    await assert.rejects(
      loadRuntimeConfig(configPath),
      /Secret-first voter config requires initCluster: true/
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig rejects a changed identity for an existing data dir", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-identity-drift-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/init-node.json"), "utf8")
    )
    raw.dataDir = "./data"

    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await loadRuntimeConfig(configPath)

    raw.identitySeed = "7777777777777777777777777777777777777777777777777777777777777777"
    await writeFile(configPath, JSON.stringify(raw, null, 2))

    await assert.rejects(loadRuntimeConfig(configPath), /Persisted node identity does not match/)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig rejects a changed clusterSecret for an existing data dir", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-secret-drift-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/init-node.json"), "utf8")
    )
    raw.dataDir = "./data"

    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await loadRuntimeConfig(configPath)

    raw.clusterSecret = "9999999999999999999999999999999999999999999999999999999999999999"
    await writeFile(configPath, JSON.stringify(raw, null, 2))

    await assert.rejects(loadRuntimeConfig(configPath), /Persisted clusterSecret does not match/)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig rejects initCluster on a data dir that already joined as a learner", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-init-reuse-"))

  try {
    const configPath = path.join(dir, "node.json")
    const learnerRaw = JSON.parse(
      await readFile(path.resolve("examples/local/init-joiner.json"), "utf8")
    )
    learnerRaw.dataDir = "./data"

    await writeFile(configPath, JSON.stringify(learnerRaw, null, 2))
    await loadRuntimeConfig(configPath)

    const initRaw = {
      ...learnerRaw,
      role: "voter",
      initCluster: true
    }
    delete initRaw.role
    await writeFile(configPath, JSON.stringify(initRaw, null, 2))

    await assert.rejects(
      loadRuntimeConfig(configPath),
      /initCluster cannot be used on a data directory that already joined another cluster/
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig rejects missing persisted identity metadata", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-missing-persisted-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/init-node.json"), "utf8")
    )
    raw.dataDir = "./data"

    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await loadRuntimeConfig(configPath)

    const persistedPath = path.join(dir, "data", "replicore-runtime.json")
    const persisted = JSON.parse(await readFile(persistedPath, "utf8"))
    delete persisted.identity.nodeId
    await writeFile(persistedPath, JSON.stringify(persisted, null, 2))

    await assert.rejects(loadRuntimeConfig(configPath), /Persisted identity metadata is missing/)
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

test("loadRuntimeConfig accepts an explicit machine identity source override", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-transport-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/node-1.json"), "utf8")
    )
    raw.dataDir = "./data"
    raw.machineIdentity = "override-machine"

    await writeFile(configPath, JSON.stringify(raw, null, 2))

    const config = await loadRuntimeConfig(configPath)
    assert.equal(config.machineId, "override-machine")
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig accepts the legacy machineId alias", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-machine-id-alias-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/node-1.json"), "utf8")
    )
    raw.dataDir = "./data"
    delete raw.machineIdentity
    raw.machineId = "legacy-machine-id"

    await writeFile(configPath, JSON.stringify(raw, null, 2))

    const config = await loadRuntimeConfig(configPath)
    assert.equal(config.machineId, "legacy-machine-id")
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("loadRuntimeConfig validates raft timing bounds", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-config-raft-"))

  try {
    const configPath = path.join(dir, "node.json")
    const raw = JSON.parse(
      await readFile(path.resolve("examples/local/node-1.json"), "utf8")
    )
    raw.dataDir = "./data"

    raw.electionTimeoutMinMs = 50
    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await assert.rejects(loadRuntimeConfig(configPath), /electionTimeoutMinMs must be between/)

    raw.electionTimeoutMinMs = 900
    raw.electionTimeoutMaxMs = 900
    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await assert.rejects(
      loadRuntimeConfig(configPath),
      /electionTimeoutMaxMs must be greater than electionTimeoutMinMs/
    )

    raw.electionTimeoutMaxMs = 1500
    raw.requestTimeoutMs = 1000
    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await assert.rejects(loadRuntimeConfig(configPath), /requestTimeoutMs must be between/)

    raw.requestTimeoutMs = 5000
    raw.maxInflightReplication = 0
    await writeFile(configPath, JSON.stringify(raw, null, 2))
    await assert.rejects(loadRuntimeConfig(configPath), /maxInflightReplication must be between/)
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
    machineId: "machine-a"
  })
  const noiseARepeat = await deriveNoiseSeed({
    clusterSecret,
    machineId: "machine-a"
  })
  const noiseB = await deriveNoiseSeed({
    clusterSecret,
    machineId: "machine-b"
  })
  const machineA = await deriveMachineId({
    clusterSecret,
    machineIdentity: "machine-a"
  })
  const machineARepeat = await deriveMachineId({
    clusterSecret,
    machineIdentity: "machine-a"
  })
  const machineB = await deriveMachineId({
    clusterSecret,
    machineIdentity: "machine-b"
  })
  const joinA = await deriveJoinSeed({
    clusterSecret,
    machineId: machineA.toString("hex")
  })

  assert.equal(topicA.length, 32)
  assert.equal(machineA.length, 32)
  assert.equal(noiseA.length, 32)
  assert.deepEqual(topicA, topicARepeat)
  assert.deepEqual(machineA, machineARepeat)
  assert.deepEqual(noiseA, noiseARepeat)
  assert.notDeepEqual(topicA, topicB)
  assert.notDeepEqual(machineA, machineB)
  assert.notDeepEqual(noiseA, noiseB)
  assert.notDeepEqual(topicA, noiseA)
  assert.notDeepEqual(machineA, noiseA)
  assert.notDeepEqual(joinA, noiseA)
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

    assert.equal(machineA.machineId.length, 64)
    assert.equal(machineA.machineId, machineARepeat.machineId)
    assert.equal(machineA.publicKeyHex, machineARepeat.publicKeyHex)
    assert.equal(machineA.joinPublicKeyHex, machineARepeat.joinPublicKeyHex)
    assert.notEqual(machineA.machineId, "machine-a")
    assert.notEqual(machineA.machineId, machineB.machineId)
    assert.notEqual(machineA.machineId, clusterB.machineId)
    assert.notEqual(machineA.publicKeyHex, machineB.publicKeyHex)
    assert.notEqual(machineA.publicKeyHex, clusterB.publicKeyHex)
    assert.notEqual(machineA.joinPublicKeyHex, machineB.joinPublicKeyHex)
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
