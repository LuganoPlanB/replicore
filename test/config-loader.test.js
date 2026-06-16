import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import test from "node:test"
import assert from "node:assert/strict"

import { loadRuntimeConfig } from "../src/index.js"

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
