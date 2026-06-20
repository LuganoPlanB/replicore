import { mkdtemp, rm, stat } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import test from "node:test"
import assert from "node:assert/strict"

import {
  readSetupDraft,
  SETUP_DRAFT_SCHEMA_VERSION,
  writeSetupDraft
} from "../src/setup-draft-store.js"

test("setup draft store writes and reloads normalized draft content", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "replicore-setup-draft-"))

  try {
    const filePath = path.join(dir, "setup", "draft.json")
    const stored = await writeSetupDraft(filePath, {
      selectedInterface: "  eth0  ",
      bindHost: " 127.0.0.1 ",
      clusterSecret: "AA".repeat(32),
      machineIdentity: " machine-id ",
      machineId: "BB".repeat(32)
    })

    assert.equal(stored.path, filePath)
    assert.equal(stored.draft.schemaVersion, SETUP_DRAFT_SCHEMA_VERSION)
    assert.equal(stored.draft.selectedInterface, "eth0")
    assert.equal(stored.draft.bindHost, "127.0.0.1")
    assert.equal(stored.draft.clusterSecret, "aa".repeat(32))
    assert.equal(stored.draft.machineIdentity, "machine-id")
    assert.equal(stored.draft.machineId, "bb".repeat(32))

    const reloaded = await readSetupDraft(filePath)
    assert.deepEqual(reloaded, stored.draft)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("setup draft store overwrites an existing draft", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "replicore-setup-draft-"))

  try {
    const filePath = path.join(dir, "draft.json")
    await writeSetupDraft(filePath, {
      selectedInterface: "eth0",
      bindHost: "127.0.0.1",
      clusterSecret: "aa".repeat(32),
      machineIdentity: "machine-a",
      machineId: "bb".repeat(32)
    })
    await writeSetupDraft(filePath, {
      selectedInterface: "wlan0",
      bindHost: "192.168.1.10",
      clusterSecret: "cc".repeat(32),
      machineIdentity: "machine-b",
      machineId: "dd".repeat(32)
    })

    const reloaded = await readSetupDraft(filePath)
    assert.equal(reloaded.selectedInterface, "wlan0")
    assert.equal(reloaded.bindHost, "192.168.1.10")
    assert.equal(reloaded.clusterSecret, "cc".repeat(32))
    assert.equal(reloaded.machineIdentity, "machine-b")
    assert.equal(reloaded.machineId, "dd".repeat(32))
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test("setup draft store uses restrictive file permissions where supported", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "replicore-setup-draft-"))

  try {
    const filePath = path.join(dir, "draft.json")
    await writeSetupDraft(filePath, {
      selectedInterface: "eth0",
      bindHost: "127.0.0.1",
      clusterSecret: "aa".repeat(32),
      machineIdentity: "machine-a",
      machineId: "bb".repeat(32)
    })

    if (process.platform !== "win32") {
      const mode = (await stat(filePath)).mode & 0o777

      if (mode !== 0o777) {
        assert.equal(mode, 0o600)
      }
    }
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})
