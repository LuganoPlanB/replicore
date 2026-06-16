import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import test from "node:test"
import assert from "node:assert/strict"
import { randomBytes } from "node:crypto"

import createTestnet from "hyperdht/testnet.js"

import { generateIdentity, HolepunchSwarmNode } from "../src/index.js"

test("leader operations replicate to followers and rebuild after restart", async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity()
    const authorizedWriter = {
      publicKeyId: leaderIdentity.publicKeyId,
      publicKeyPem: leaderIdentity.publicKeyPem
    }

    const leaderDir = await tempDir(dirs)
    const leader = new HolepunchSwarmNode({
      dataDir: leaderDir,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      role: "leader",
      identity: leaderIdentity,
      authorizedWriter,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await leader.start()
    nodes.push(leader)

    const follower1 = await createFollower({
      dirs,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      leaderFeedKey: leader.leaderFeedKey,
      authorizedWriter,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    nodes.push(follower1)

    const follower2Dir = await tempDir(dirs)
    let follower2 = await createFollower({
      dirs: [],
      dataDir: follower2Dir,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      leaderFeedKey: leader.leaderFeedKey,
      authorizedWriter,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    nodes.push(follower2)

    const putOp = await leader.put("hash:alpha", { message: "hello" })

    await waitFor(async () => (await follower1.get("hash:alpha"))?.value?.message === "hello")
    await waitFor(async () => (await follower2.get("hash:alpha"))?.value?.message === "hello")

    const history = await follower1.getHistory("hash:alpha")
    assert.equal(history.length, 1)
    assert.equal(history[0].opId, putOp.opId)

    await leader.delete("hash:alpha")
    await waitFor(async () => (await follower1.get("hash:alpha"))?.deleted === true)

    await follower2.close()
    nodes.splice(nodes.indexOf(follower2), 1)

    follower2 = await createFollower({
      dirs: [],
      dataDir: follower2Dir,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      leaderFeedKey: leader.leaderFeedKey,
      authorizedWriter,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    nodes.push(follower2)

    await waitFor(async () => (await follower2.get("hash:alpha"))?.deleted === true)
    const restartedHistory = await follower2.getHistory("hash:alpha")
    assert.equal(restartedHistory.length, 2)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

/**
 * @param {{
 *   dirs: string[],
 *   dataDir?: string,
 *   clusterId: string,
 *   topicSalt: string,
 *   leaderFeedKey: string,
 *   authorizedWriter: { publicKeyId: string, publicKeyPem: string },
 *   encryptionKey: Buffer,
 *   bootstrap: Array<string | { host: string, port: number }>
 * }} options
 */
async function createFollower(options) {
  const identity = generateIdentity()
  const dataDir = options.dataDir ?? (await tempDir(options.dirs))
  const follower = new HolepunchSwarmNode({
    dataDir,
    clusterId: options.clusterId,
    topicSalt: options.topicSalt,
    role: "follower",
    identity,
    authorizedWriter: options.authorizedWriter,
    encryptionKey: options.encryptionKey,
    leaderFeedKey: options.leaderFeedKey,
    bootstrap: options.bootstrap
  })
  await follower.start()
  return follower
}

/**
 * @param {string[]} dirs
 */
async function tempDir(dirs) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-swarm-"))
  dirs.push(dir)
  return dir
}

/**
 * @param {() => Promise<boolean>} condition
 * @param {number} [timeoutMs]
 */
async function waitFor(condition, timeoutMs = 10000) {
  const started = Date.now()

  while (Date.now() - started < timeoutMs) {
    if (await condition()) return
    await new Promise((resolve) => setTimeout(resolve, 50))
  }

  throw new Error("Timed out waiting for condition")
}
