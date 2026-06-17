import assert from "node:assert/strict"
import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import test from "node:test"

import Corestore from "corestore"
import Hyperbee from "hyperbee"

import { MaterializedView } from "../src/index.js"

test("materialized view keeps raw and committed feed progress separate", { concurrency: false }, async () => {
  await withView(async ({ view }) => {
    await view.setRawProgress("feed-a", { applied: 7, lastOpId: "raw-7" })
    await view.setCommittedProgress("feed-a", { applied: 3, lastOpId: "committed-3" })

    assert.equal(await view.getRawApplied("feed-a"), 7)
    assert.equal(await view.getApplied("feed-a"), 3)
    assert.deepEqual(await view.getFeedProgress("feed-a"), {
      rawApplied: 7,
      rawLastOpId: "raw-7",
      committedApplied: 3,
      committedLastOpId: "committed-3"
    })
  })
})

test("materialized view feed progress survives restart", { concurrency: false }, async () => {
  const dirs = []
  const dataDir = await tempDir(dirs)
  let first = null
  let second = null

  try {
    first = await openView(dataDir)
    await first.view.setRawProgress("feed-a", { applied: 11, lastOpId: "raw-11" })
    await first.view.setCommittedProgress("feed-a", { applied: 4, lastOpId: "committed-4" })
    await first.close()
    first = null

    second = await openView(dataDir)
    assert.deepEqual(await second.view.getFeedProgress("feed-a"), {
      rawApplied: 11,
      rawLastOpId: "raw-11",
      committedApplied: 4,
      committedLastOpId: "committed-4"
    })
  } finally {
    await Promise.allSettled([first?.close(), second?.close()].filter(Boolean))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("materialized view persists staged entries without exposing committed data", { concurrency: false }, async () => {
  const stagedOperation = {
    kind: "kv",
    type: "put",
    keyspace: "default",
    key: "hash:staged",
    actor: "node-a",
    seq: 8,
    opId: "staged-8",
    ts: "2026-06-17T00:00:00.000Z"
  }

  await withView(async ({ view }) => {
    await view.stageEntry("feed-a", {
      nodeId: "node-a",
      source: "local",
      validation: "valid",
      operation: stagedOperation
    })

    assert.deepEqual(await view.getStagedEntries("feed-a"), [
      {
        feedKey: "feed-a",
        nodeId: "node-a",
        source: "local",
        validation: "valid",
        resolution: "pending",
        seq: 8,
        opId: "staged-8",
        kind: "kv",
        type: "put",
        keyspace: "default",
        key: "hash:staged",
        actor: "node-a",
        ts: "2026-06-17T00:00:00.000Z",
        expiresAt: null
      }
    ])
    assert.equal(await view.getCurrent("default", "hash:staged"), null)
    assert.deepEqual(await view.getHistory("default", "hash:staged"), [])

    const snapshot = await view.exportSnapshot()
    assert.ok(snapshot.entries.every((entry) => !String(entry.key).includes("/staged/")))
  })
})

test("materialized view staged entries survive restart", { concurrency: false }, async () => {
  const dirs = []
  const dataDir = await tempDir(dirs)
  let first = null
  let second = null

  try {
    first = await openView(dataDir)
    await first.view.stageEntry("feed-a", {
      nodeId: "node-a",
      source: "remote",
      validation: "valid",
      operation: {
        kind: "kv",
        type: "delete",
        keyspace: "default",
        key: "hash:staged-restart",
        actor: "node-a",
        seq: 4,
        opId: "staged-restart-4",
        ts: "2026-06-17T00:00:01.000Z"
      }
    })
    await first.close()
    first = null

    second = await openView(dataDir)
    assert.deepEqual(await second.view.getStagedEntries("feed-a"), [
      {
        feedKey: "feed-a",
        nodeId: "node-a",
        source: "remote",
        validation: "valid",
        resolution: "pending",
        seq: 4,
        opId: "staged-restart-4",
        kind: "kv",
        type: "delete",
        keyspace: "default",
        key: "hash:staged-restart",
        actor: "node-a",
        ts: "2026-06-17T00:00:01.000Z",
        expiresAt: null
      }
    ])
    assert.equal(await second.view.getCurrent("default", "hash:staged-restart"), null)
    assert.deepEqual(await second.view.getHistory("default", "hash:staged-restart"), [])
  } finally {
    await Promise.allSettled([first?.close(), second?.close()].filter(Boolean))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

async function withView(run) {
  const dirs = []
  let opened = null

  try {
    opened = await openView(await tempDir(dirs))
    await run(opened)
  } finally {
    await Promise.allSettled([opened?.close()].filter(Boolean))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
}

async function openView(dataDir) {
  const store = new Corestore(path.join(dataDir, "corestore"))
  await store.ready()
  const core = store.get({ name: "derived-view" })
  const bee = new Hyperbee(core, { keyEncoding: "utf-8", valueEncoding: "json" })
  await bee.ready()
  const view = new MaterializedView(bee)

  return {
    bee,
    view,
    async close() {
      await bee.close()
      await store.close()
    }
  }
}

async function tempDir(dirs) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "replicore-materialized-view-"))
  dirs.push(dir)
  return dir
}
