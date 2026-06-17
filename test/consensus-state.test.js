import assert from "node:assert/strict"
import { createHash, randomBytes } from "node:crypto"
import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import test from "node:test"

import { HolepunchSwarmNode, generateIdentity } from "../src/index.js"

test("consensus state persists a recorded vote across restart", { concurrency: false }, async () => {
  await withNodeRestart(async ({ node }) => {
    await node.setConsensusState({
      currentTerm: 3,
      votedFor: "node-b"
    })
  }, async ({ restarted }) => {
    assert.deepEqual(await restarted.getConsensusState(), {
      currentTerm: 3,
      votedFor: "node-b",
      commitIndex: -1,
      lastApplied: -1,
      membershipVersion: 0
    })
  })
})

test("consensus state persists commit progress across restart", { concurrency: false }, async () => {
  await withNodeRestart(async ({ node }) => {
    await node.setConsensusState({
      currentTerm: 4,
      commitIndex: 12
    })
  }, async ({ restarted }) => {
    assert.deepEqual(await restarted.getConsensusState(), {
      currentTerm: 4,
      votedFor: null,
      commitIndex: 12,
      lastApplied: -1,
      membershipVersion: 0
    })
  })
})

test("consensus state persists apply progress and membership version across restart", { concurrency: false }, async () => {
  await withNodeRestart(async ({ node }) => {
    await node.setConsensusState({
      currentTerm: 7,
      commitIndex: 19,
      lastApplied: 19,
      membershipVersion: 2
    })
  }, async ({ restarted }) => {
    assert.deepEqual(await restarted.getConsensusState(), {
      currentTerm: 7,
      votedFor: null,
      commitIndex: 19,
      lastApplied: 19,
      membershipVersion: 2
    })
  })
})

async function withNodeRestart(beforeRestart, afterRestart) {
  const dirs = []
  const identity = generateIdentity(seed("consensus-state"))
  const authorizedNodes = [
    {
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }
  ]
  const dataDir = await tempDir(dirs)
  const encryptionKey = randomBytes(32)
  let node = null
  let restarted = null

  try {
    node = new HolepunchSwarmNode({
      dataDir,
      clusterId: "consensus-state",
      topicSalt: "consensus-state",
      identity,
      authorizedNodes,
      encryptionKey,
      bootstrap: []
    })
    await node.start()
    await beforeRestart({ node })
    await node.close()
    node = null

    restarted = new HolepunchSwarmNode({
      dataDir,
      clusterId: "consensus-state",
      topicSalt: "consensus-state",
      identity,
      authorizedNodes,
      encryptionKey,
      bootstrap: []
    })
    await restarted.start()
    await afterRestart({ restarted })
  } finally {
    await Promise.allSettled([node?.close(), restarted?.close()].filter(Boolean))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
}

function seed(label) {
  return createHash("sha256").update(label).digest()
}

async function tempDir(dirs) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "replicore-consensus-state-"))
  dirs.push(dir)
  return dir
}
