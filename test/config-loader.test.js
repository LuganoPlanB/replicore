import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import test from "node:test"
import assert from "node:assert/strict"

import {
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
