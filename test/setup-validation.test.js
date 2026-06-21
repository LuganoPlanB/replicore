import test from "node:test"
import assert from "node:assert/strict"

import { normalizeSetupDraft, normalizeSetupMachineIdInput } from "../src/setup-validation.js"
import { base58Encode } from "../src/base58.js"

function b58(hexPair, count) {
  return base58Encode(Buffer.from(hexPair.repeat(count ?? 32), "hex"))
}

const VALID_SECRET = b58("aa")
const VALID_MACHINE_ID = b58("bb")
const SHORT_SECRET = b58("aa", 1)

test("normalizeSetupMachineIdInput trims machine identity and decodes cluster secret", () => {
  const normalized = normalizeSetupMachineIdInput({
    clusterSecret: VALID_SECRET,
    machineIdentity: "  machine-id-value  "
  })

  assert.equal(Buffer.isBuffer(normalized.clusterSecret), true)
  assert.equal(normalized.clusterSecret.length, 32)
  assert.equal(normalized.machineIdentity, "machine-id-value")
})

test("normalizeSetupMachineIdInput rejects invalid machine-id inputs", () => {
  assert.throws(
    () => normalizeSetupMachineIdInput({ clusterSecret: SHORT_SECRET, machineIdentity: "machine" }),
    /clusterSecret must decode to 32 bytes/
  )
  assert.throws(
    () => normalizeSetupMachineIdInput({ clusterSecret: VALID_SECRET, machineIdentity: "   " }),
    /machineIdentity must be a non-empty string/
  )
})

test("normalizeSetupDraft returns the persisted setup schema", () => {
  const normalized = normalizeSetupDraft({
    selectedInterface: "  eth0  ",
    bindHost: " 192.168.1.10 ",
    clusterSecret: VALID_SECRET,
    machineIdentity: " machine-id-value ",
    machineId: VALID_MACHINE_ID
  })

  assert.equal(normalized.schemaVersion, 1)
  assert.equal(typeof normalized.updatedAt, "string")
  assert.equal(normalized.initCluster, false)
  assert.equal(normalized.role, "learner")
  assert.equal(normalized.selectedInterface, "eth0")
  assert.equal(normalized.bindHost, "192.168.1.10")
  assert.equal(normalized.clusterSecret, VALID_SECRET)
  assert.equal(normalized.machineIdentity, "machine-id-value")
  assert.equal(normalized.machineId, VALID_MACHINE_ID)
})

test("normalizeSetupDraft normalizes initCluster and role", () => {
  const init = normalizeSetupDraft({
    selectedInterface: "eth0",
    bindHost: "127.0.0.1",
    clusterSecret: VALID_SECRET,
    machineIdentity: "machine",
    machineId: VALID_MACHINE_ID,
    initCluster: true,
    role: "voter"
  })
  assert.equal(init.initCluster, true)
  assert.equal(init.role, "voter")

  const join = normalizeSetupDraft({
    selectedInterface: "eth0",
    bindHost: "127.0.0.1",
    clusterSecret: VALID_SECRET,
    machineIdentity: "machine",
    machineId: VALID_MACHINE_ID,
    initCluster: false,
    role: "learner"
  })
  assert.equal(join.initCluster, false)
  assert.equal(join.role, "learner")

  const defaults = normalizeSetupDraft({
    selectedInterface: "eth0",
    bindHost: "127.0.0.1",
    clusterSecret: VALID_SECRET,
    machineIdentity: "machine",
    machineId: VALID_MACHINE_ID
  })
  assert.equal(defaults.initCluster, false)
  assert.equal(defaults.role, "learner")
})

test("normalizeSetupDraft rejects initCluster with non-voter role", () => {
  assert.throws(
    () => normalizeSetupDraft({
      selectedInterface: "eth0",
      bindHost: "127.0.0.1",
      clusterSecret: VALID_SECRET,
      machineIdentity: "machine",
      machineId: VALID_MACHINE_ID,
      initCluster: true,
      role: "learner"
    }),
    /initCluster may only be used with voter role/
  )
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
      clusterSecret: VALID_SECRET,
      machineIdentity: "machine",
      machineId: b58("aa", 1)
    }),
    /machineId must decode to 32 bytes/
  )
  assert.throws(
    () => normalizeSetupDraft({
      selectedInterface: "eth0",
      bindHost: "127.0.0.1",
      clusterSecret: VALID_SECRET,
      machineIdentity: "machine",
      machineId: VALID_MACHINE_ID,
      schemaVersion: 0
    }),
    /schemaVersion must be a positive integer/
  )
})
