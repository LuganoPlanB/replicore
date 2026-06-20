import test from "node:test"
import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"

import { SetupHttpServer } from "../src/index.js"
import { deriveMachineId, MACHINE_ID_PURPOSE } from "../src/cluster-secret.js"
import { readSetupDraft, writeSetupDraft } from "../src/setup-draft-store.js"
import { requestJson } from "./helpers/http-crud.js"

test("setup http server exposes setup state and no CRUD routes", { concurrency: false }, async () => {
  const server = new SetupHttpServer({
    state: () => ({ mode: "setup", configPath: "/tmp/node-config.json" })
  })

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`

    const state = await requestJson(`${baseUrl}/setup/state`)
    assert.equal(state.status, 200)
    assert.deepEqual(state.payload, {
      mode: "setup",
      configPath: "/tmp/node-config.json"
    })

    const missingCrud = await requestJson(`${baseUrl}/kv/${encodeURIComponent("hash:test")}`)
    assert.equal(missingCrud.status, 404)
    assert.deepEqual(missingCrud.payload, { error: "Not found" })
  } finally {
    await server.close()
  }
})

test("run-node setup mode starts without loading a node config", { concurrency: false }, async () => {
  const child = spawn(process.execPath, ["bin/run-node.js", "--setup"], {
    cwd: process.cwd(),
    stdio: ["ignore", "pipe", "pipe"]
  })

  let stdout = ""
  let stderr = ""
  child.stdout.on("data", (chunk) => {
    stdout += String(chunk)
  })
  child.stderr.on("data", (chunk) => {
    stderr += String(chunk)
  })

  const ready = await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      child.kill("SIGTERM")
      reject(new Error(`Timed out waiting for setup-ready output\nstdout:\n${stdout}\nstderr:\n${stderr}`))
    }, 5000)

    child.once("exit", (code, signal) => {
      clearTimeout(timeout)
      reject(new Error(`Setup process exited early with code=${code} signal=${signal}\nstdout:\n${stdout}\nstderr:\n${stderr}`))
    })

    child.stdout.on("data", () => {
      const trimmed = stdout.trim()
      if (!trimmed) return
      try {
        const parsed = JSON.parse(trimmed)
        if (parsed.type === "setup-ready") {
          clearTimeout(timeout)
          resolve(parsed)
        }
      } catch {
        // Wait for the full JSON payload.
      }
    })
  })

  try {
    assert.equal(ready.type, "setup-ready")
    assert.equal(ready.configPath, null)
    assert.equal(typeof ready.http?.port, "number")

    const state = await requestJson(`http://${ready.http.address}:${ready.http.port}/setup/state`)
    assert.equal(state.status, 200)
    assert.deepEqual(state.payload, {
      mode: "setup",
      configPath: null
    })
  } finally {
    child.kill("SIGTERM")
    await new Promise((resolve) => child.once("exit", resolve))
  }
})

test("setup http server derives machine identifiers through the backend", { concurrency: false }, async () => {
  const server = new SetupHttpServer()

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`
    const clusterSecret = "ab".repeat(32)
    const machineIdentity = "  machine-id-value  "

    const response = await requestJson(`${baseUrl}/setup/derive-machine-id`, {
      method: "POST",
      body: {
        clusterSecret,
        machineIdentity
      }
    })

    assert.equal(response.status, 200)
    assert.deepEqual(response.payload, {
      machineId: (await deriveMachineId({
        clusterSecret: Buffer.from(clusterSecret, "hex"),
        machineIdentity
      })).toString("hex"),
      kdf: "argon2d",
      purpose: MACHINE_ID_PURPOSE
    })
  } finally {
    await server.close()
  }
})

test("setup http server rejects invalid machine-id derivation input", { concurrency: false }, async () => {
  const server = new SetupHttpServer()

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`

    const response = await requestJson(`${baseUrl}/setup/derive-machine-id`, {
      method: "POST",
      body: {
        clusterSecret: "aa".repeat(31),
        machineIdentity: " "
      }
    })

    assert.equal(response.status, 400)
    assert.equal(response.payload.error, "clusterSecret must decode to 32 bytes")
  } finally {
    await server.close()
  }
})

test("setup http server persists and reloads setup drafts without exposing file paths", { concurrency: false }, async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "replicore-setup-http-"))
  const draftPath = path.join(dir, "node.setup-draft.json")
  const server = new SetupHttpServer({
    loadDraft: () => readSetupDraft(draftPath),
    saveDraft: (draft) => writeSetupDraft(draftPath, draft).then((result) => result.draft)
  })

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`

    const missing = await requestJson(`${baseUrl}/setup/draft`)
    assert.equal(missing.status, 404)
    assert.deepEqual(missing.payload, { error: "Setup draft not found" })

    const create = await requestJson(`${baseUrl}/setup/draft`, {
      method: "POST",
      body: {
        selectedInterface: "eth0",
        bindHost: "127.0.0.1",
        clusterSecret: "aa".repeat(32),
        machineIdentity: "machine-a",
        machineId: "bb".repeat(32)
      }
    })

    assert.equal(create.status, 200)
    assert.deepEqual(create.payload, {
      draft: {
        schemaVersion: 1,
        updatedAt: create.payload.draft.updatedAt,
        selectedInterface: "eth0",
        bindHost: "127.0.0.1",
        clusterSecret: "aa".repeat(32),
        machineIdentity: "machine-a",
        machineId: "bb".repeat(32)
      }
    })
    assert.equal("path" in create.payload.draft, false)

    const reload = await requestJson(`${baseUrl}/setup/draft`)
    assert.equal(reload.status, 200)
    assert.deepEqual(reload.payload, create.payload)

    const overwrite = await requestJson(`${baseUrl}/setup/draft`, {
      method: "POST",
      body: {
        selectedInterface: "wlan0",
        bindHost: "192.168.1.5",
        clusterSecret: "cc".repeat(32),
        machineIdentity: "machine-b",
        machineId: "dd".repeat(32)
      }
    })

    assert.equal(overwrite.status, 200)
    assert.equal(overwrite.payload.draft.selectedInterface, "wlan0")
    assert.equal(overwrite.payload.draft.bindHost, "192.168.1.5")
    assert.equal(overwrite.payload.draft.clusterSecret, "cc".repeat(32))
    assert.equal(overwrite.payload.draft.machineIdentity, "machine-b")
    assert.equal(overwrite.payload.draft.machineId, "dd".repeat(32))
  } finally {
    await server.close()
    await rm(dir, { recursive: true, force: true })
  }
})

test("setup http server rejects invalid setup drafts", { concurrency: false }, async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "replicore-setup-http-"))
  const draftPath = path.join(dir, "node.setup-draft.json")
  const server = new SetupHttpServer({
    loadDraft: () => readSetupDraft(draftPath),
    saveDraft: (draft) => writeSetupDraft(draftPath, draft).then((result) => result.draft)
  })

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`
    const response = await requestJson(`${baseUrl}/setup/draft`, {
      method: "POST",
      body: {
        selectedInterface: "",
        bindHost: "127.0.0.1",
        clusterSecret: "aa".repeat(32),
        machineIdentity: "machine-a",
        machineId: "bb".repeat(32)
      }
    })

    assert.equal(response.status, 400)
    assert.deepEqual(response.payload, {
      error: "selectedInterface must be a non-empty string"
    })
  } finally {
    await server.close()
    await rm(dir, { recursive: true, force: true })
  }
})

test("setup http server reports when setup draft storage is unavailable", { concurrency: false }, async () => {
  const server = new SetupHttpServer()

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`
    const response = await requestJson(`${baseUrl}/setup/draft`)

    assert.equal(response.status, 409)
    assert.deepEqual(response.payload, {
      error: "Setup draft storage is unavailable"
    })
  } finally {
    await server.close()
  }
})
