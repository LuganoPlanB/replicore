import test from "node:test"
import assert from "node:assert/strict"

import { normalizeSetupDraft, normalizeSetupMachineIdInput } from "../src/setup-validation.js"

test("normalizeSetupMachineIdInput trims machine identity and decodes cluster secret", () => {
  const normalized = normalizeSetupMachineIdInput({
    clusterSecret: "aa".repeat(32),
    machineIdentity: "  machine-id-value  "
  })

  assert.equal(Buffer.isBuffer(normalized.clusterSecret), true)
  assert.equal(normalized.clusterSecret.length, 32)
  assert.equal(normalized.machineIdentity, "machine-id-value")
})

test("normalizeSetupMachineIdInput rejects invalid machine-id inputs", () => {
  assert.throws(
    () => normalizeSetupMachineIdInput({ clusterSecret: "zz", machineIdentity: "machine" }),
    /clusterSecret must be a hex string/
  )
  assert.throws(
    () => normalizeSetupMachineIdInput({ clusterSecret: "aa", machineIdentity: "machine" }),
    /clusterSecret must decode to 32 bytes/
  )
  assert.throws(
    () => normalizeSetupMachineIdInput({ clusterSecret: "aa".repeat(32), machineIdentity: "   " }),
    /machineIdentity must be a non-empty string/
  )
})

test("normalizeSetupDraft returns the persisted setup schema", () => {
  const normalized = normalizeSetupDraft({
    selectedInterface: "  eth0  ",
    bindHost: " 192.168.1.10 ",
    clusterSecret: "AB".repeat(32),
    machineIdentity: " machine-id-value ",
    machineId: "CD".repeat(32)
  })

  assert.equal(normalized.schemaVersion, 1)
  assert.equal(typeof normalized.updatedAt, "string")
  assert.equal(normalized.selectedInterface, "eth0")
  assert.equal(normalized.bindHost, "192.168.1.10")
  assert.equal(normalized.clusterSecret, "ab".repeat(32))
  assert.equal(normalized.machineIdentity, "machine-id-value")
  assert.equal(normalized.machineId, "cd".repeat(32))
})

test("normalizeSetupDraft rejects incomplete or invalid draft fields", () => {
  assert.throws(
    () => normalizeSetupDraft({}),
    /selectedInterface must be a non-empty string/
  )
  assert.throws(
    () => normalizeSetupDraft({
      selectedInterface: "eth0",
      bindHost: "127.0.0.1",
      clusterSecret: "aa".repeat(32),
      machineIdentity: "machine",
      machineId: "gg"
    }),
    /machineId must be a hex string/
  )
  assert.throws(
    () => normalizeSetupDraft({
      selectedInterface: "eth0",
      bindHost: "127.0.0.1",
      clusterSecret: "aa".repeat(32),
      machineIdentity: "machine",
      machineId: "bb".repeat(32),
      schemaVersion: 0
    }),
    /schemaVersion must be a positive integer/
  )
})
