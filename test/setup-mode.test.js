import test from "node:test"
import assert from "node:assert/strict"
import { spawn } from "node:child_process"

import { SetupHttpServer } from "../src/index.js"
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
