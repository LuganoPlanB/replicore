import test from "node:test"
import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"

import { HolepunchHttpServer, SetupHttpServer } from "../src/index.js"
import { deriveMachineId, MACHINE_ID_PURPOSE } from "../src/cluster-secret.js"
import { readSetupDraft, writeSetupDraft } from "../src/setup-draft-store.js"
import { requestJson } from "./helpers/http-crud.js"

test("setup http server exposes setup state and no CRUD routes", { concurrency: false }, async () => {
  const server = new SetupHttpServer({
    state: () => ({
      mode: "setup",
      view: "wizard",
      nodeRunning: false,
      configPath: "/tmp/node-config.json"
    })
  })

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`

    const state = await requestJson(`${baseUrl}/setup/state`)
    assert.equal(state.status, 200)
    assert.deepEqual(state.payload, {
      mode: "setup",
      view: "wizard",
      nodeRunning: false,
      configPath: "/tmp/node-config.json"
    })

    const missingCrud = await requestJson(`${baseUrl}/kv/${encodeURIComponent("hash:test")}`)
    assert.equal(missingCrud.status, 404)
    assert.deepEqual(missingCrud.payload, { error: "Not found" })
  } finally {
    await server.close()
  }
})

test("setup http server serves built setup assets alongside setup routes", { concurrency: false }, async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "replicore-setup-ui-"))
  const assetsDir = path.join(dir, "assets")
  const server = new SetupHttpServer({
    uiRoot: dir,
    state: () => ({ mode: "setup" })
  })

  try {
    await mkdir(assetsDir, { recursive: true })
    await writeFile(path.join(dir, "index.html"), "<!doctype html><html><body>setup-ui</body></html>\n")
    await writeFile(path.join(assetsDir, "app.js"), "console.log('setup-ui')\n")
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`

    const rootResponse = await fetch(baseUrl)
    assert.equal(rootResponse.status, 200)
    assert.match(rootResponse.headers.get("content-type") ?? "", /^text\/html/)
    assert.match(await rootResponse.text(), /setup-ui/)

    const assetResponse = await fetch(`${baseUrl}/assets/app.js`)
    assert.equal(assetResponse.status, 200)
    assert.match(assetResponse.headers.get("content-type") ?? "", /^text\/javascript/)
    assert.match(await assetResponse.text(), /setup-ui/)

    const state = await requestJson(`${baseUrl}/setup/state`)
    assert.equal(state.status, 200)
    assert.deepEqual(state.payload, { mode: "setup" })
  } finally {
    await server.close()
    await rm(dir, { recursive: true, force: true })
  }
})

test("run-node setup mode starts without loading a node config", { concurrency: false }, async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "replicore-setup-cli-"))
  const configPath = path.join(dir, "node.json")
  const child = spawn(process.execPath, ["bin/run-node.js", "--setup", configPath], {
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
    assert.equal(ready.configPath, configPath)
    assert.equal(ready.draftPath, path.join(dir, "node.setup-draft.json"))
    assert.equal(ready.configExists, false)
    assert.equal(typeof ready.url, "string")
    assert.equal(typeof ready.http?.port, "number")

    const state = await requestJson(`http://${ready.http.address}:${ready.http.port}/setup/state`)
    assert.equal(state.status, 200)
    assert.deepEqual(state.payload, {
      mode: "setup",
      view: "wizard",
      nodeRunning: false,
      configPath,
      configExists: false
    })

    const interfaces = await requestJson(`http://${ready.http.address}:${ready.http.port}/setup/interfaces`)
    assert.equal(interfaces.status, 200)
    assert.equal(Array.isArray(interfaces.payload.interfaces), true)

    const rootResponse = await fetch(ready.url)
    assert.equal(rootResponse.status, 200)
    assert.match(rootResponse.headers.get("content-type") ?? "", /^text\/html/)
  } finally {
    child.kill("SIGTERM")
    await new Promise((resolve) => child.once("exit", resolve))
    await rm(dir, { recursive: true, force: true })
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

test("setup http server exposes normalized network interfaces", { concurrency: false }, async () => {
  const server = new SetupHttpServer({
    listNetworkInterfaces: () => [
      {
        name: "eth0",
        family: "IPv4",
        address: "192.168.1.10",
        netmask: "255.255.255.0",
        mac: "aa:bb:cc:dd:ee:ff",
        internal: false,
        cidr: "192.168.1.10/24",
        eligibleForBind: true
      }
    ]
  })

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`
    const response = await requestJson(`${baseUrl}/setup/interfaces`)

    assert.equal(response.status, 200)
    assert.deepEqual(response.payload, {
      interfaces: [
        {
          name: "eth0",
          family: "IPv4",
          address: "192.168.1.10",
          netmask: "255.255.255.0",
          mac: "aa:bb:cc:dd:ee:ff",
          internal: false,
          cidr: "192.168.1.10/24",
          eligibleForBind: true
        }
      ]
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

test("setup http server rejects malformed JSON without calling deriveMachineId", { concurrency: false }, async () => {
  let deriveCalls = 0
  const server = new SetupHttpServer({
    deriveMachineId: async () => {
      deriveCalls += 1
      return Buffer.alloc(32)
    }
  })

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`
    const clusterSecret = "ab".repeat(32)
    const machineIdentity = "machine-a"
    const response = await fetch(`${baseUrl}/setup/derive-machine-id`, {
      method: "POST",
      headers: {
        "content-type": "application/json"
      },
      body: `{"clusterSecret":"${clusterSecret}","machineIdentity":"${machineIdentity}"`
    })

    assert.equal(response.status, 400)
    const payload = await response.json()
    assert.deepEqual(payload, { error: "Invalid JSON body" })
    assert.equal(deriveCalls, 0)
    assert.ok(!JSON.stringify(payload).includes(clusterSecret))
    assert.ok(!JSON.stringify(payload).includes(machineIdentity))
  } finally {
    await server.close()
  }
})

test("setup http server rejects oversized draft bodies before saveDraft", { concurrency: false }, async () => {
  let saveCalls = 0
  const server = new SetupHttpServer({
    loadDraft: async () => null,
    saveDraft: async () => {
      saveCalls += 1
      return {}
    }
  })

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`
    const hugeBody = JSON.stringify({
      clusterSecret: "aa".repeat(32),
      machineIdentity: "machine-a",
      draft: "x".repeat(70 * 1024)
    })
    const response = await fetch(`${baseUrl}/setup/draft`, {
      method: "POST",
      headers: {
        "content-type": "application/json"
      },
      body: hugeBody
    })

    assert.equal(response.status, 413)
    const payload = await response.json()
    assert.equal(payload.error, "Request body too large")
    assert.equal(saveCalls, 0)
    assert.ok(!JSON.stringify(payload).includes("aa".repeat(32)))
    assert.ok(!JSON.stringify(payload).includes("machine-a"))
  } finally {
    await server.close()
  }
})

test("setup JSON responses carry the shared security headers", { concurrency: false }, async () => {
  const server = new SetupHttpServer({
    loadDraft: async () => null,
    saveDraft: async (draft) => draft
  })

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`

    const success = await fetch(`${baseUrl}/setup/state`)
    assertJsonSecurityHeaders(success)
    assert.equal(success.status, 200)

    const malformed = await fetch(`${baseUrl}/setup/derive-machine-id`, {
      method: "POST",
      headers: {
        "content-type": "application/json"
      },
      body: "{\"clusterSecret\":"
    })
    assertJsonSecurityHeaders(malformed)
    assert.equal(malformed.status, 400)

    const tooLarge = await fetch(`${baseUrl}/setup/draft`, {
      method: "POST",
      headers: {
        "content-type": "application/json"
      },
      body: JSON.stringify({ draft: "x".repeat(70 * 1024) })
    })
    assertJsonSecurityHeaders(tooLarge)
    assert.equal(tooLarge.status, 413)
  } finally {
    await server.close()
  }
})

test("setup HTTP internal errors are sanitized for clients and logged with the injected logger", { concurrency: false }, async () => {
  const logs = []
  const server = new SetupHttpServer({
    logger: {
      error(...args) {
        logs.push(args)
      }
    },
    state: async () => {
      throw new Error("sensitive-setup-marker")
    }
  })

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`
    const response = await fetch(`${baseUrl}/setup/state`)

    assert.equal(response.status, 500)
    assertJsonSecurityHeaders(response)
    const payload = await response.json()
    assert.deepEqual(payload, {
      error: "Internal server error",
      code: "INTERNAL_ERROR"
    })
    assert.ok(!JSON.stringify(payload).includes("sensitive-setup-marker"))
    assert.equal(logs.length, 1)
    assert.equal(logs[0][0], "setup http internal error")
    assert.equal(logs[0][1].error?.message, "sensitive-setup-marker")
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
    assert.equal(missing.status, 200)
    assert.deepEqual(missing.payload, { draft: null })

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

test("node http server does not expose setup routes", { concurrency: false }, async () => {
  const node = {
    async setHttpAddress() {},
    async getReplicationStatus() {
      return { ok: true }
    },
    getWritersStatus() {
      return { ok: true }
    },
    async getLeaderStatus() {
      return { ok: true }
    }
  }
  const server = new HolepunchHttpServer({ node })

  try {
    await server.start()
    const baseUrl = `http://${server.address.address}:${server.address.port}`

    const setupState = await requestJson(`${baseUrl}/setup/state`)
    assert.equal(setupState.status, 404)
    assert.deepEqual(setupState.payload, { error: "Not found" })

    const setupDraft = await requestJson(`${baseUrl}/setup/draft`)
    assert.equal(setupDraft.status, 404)
    assert.deepEqual(setupDraft.payload, { error: "Not found" })

    const replication = await requestJson(`${baseUrl}/status/replication`)
    assert.equal(replication.status, 200)
    assert.deepEqual(replication.payload, { ok: true })
  } finally {
    await server.close()
  }
})

function assertJsonSecurityHeaders(response) {
  assert.match(response.headers.get("content-type") ?? "", /^application\/json; charset=utf-8$/)
  assert.match(response.headers.get("content-length") ?? "", /^\d+$/)
  assert.equal(response.headers.get("connection"), "close")
  assert.equal(response.headers.get("x-content-type-options"), "nosniff")
  assert.equal(response.headers.get("referrer-policy"), "no-referrer")
  assert.equal(response.headers.get("x-frame-options"), "DENY")
  assert.equal(
    response.headers.get("content-security-policy"),
    "default-src 'none'; frame-ancestors 'none'; base-uri 'none'"
  )
  assert.equal(response.headers.get("strict-transport-security"), null)
}
