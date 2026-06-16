import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import test from "node:test"
import assert from "node:assert/strict"
import { createHash, randomBytes } from "node:crypto"

import createTestnet from "hyperdht/testnet.js"

import { generateIdentity, HolepunchSwarmNode } from "../src/index.js"

test("leader operations replicate to followers and rebuild after restart", async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("leader"))
    const follower1Identity = generateIdentity(seed("follower-1"))
    const follower2Identity = generateIdentity(seed("follower-2"))
    const authorizedNodes = [leaderIdentity, follower1Identity, follower2Identity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const leaderDir = await tempDir(dirs)
    const leader = new HolepunchSwarmNode({
      dataDir: leaderDir,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: leaderIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await leader.start()
    nodes.push(leader)

    const follower1 = await createFollower({
      dirs,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: follower1Identity,
      authorizedNodes,
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
      identity: follower2Identity,
      authorizedNodes,
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
      identity: follower2Identity,
      authorizedNodes,
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

test("followers forward writes to the computed leader and the next alive node becomes leader", async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("leader"))
    const follower1Identity = generateIdentity(seed("follower-1"))
    const follower2Identity = generateIdentity(seed("follower-2"))
    const authorizedNodes = [leaderIdentity, follower1Identity, follower2Identity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const leader = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: leaderIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    const follower1 = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: follower1Identity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    const follower2 = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: follower2Identity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })

    nodes.push(leader, follower1, follower2)
    await Promise.all(nodes.map((node) => node.start()))

    const leaderId = [leaderIdentity, follower1Identity, follower2Identity]
      .map((identity) => identity.publicKeyId)
      .sort()[0]
    await waitFor(async () => follower1.currentLeader() === leaderId)
    await waitFor(async () => nodes.every((node) => node.status.knownHeartbeats.length >= 3))

    const op = await follower1.put("hash:beta", { forwarded: true })
    assert.equal(op.actor, leaderId)
    await waitFor(async () => (await follower2.get("hash:beta"))?.value?.forwarded === true)

    const currentLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === leaderId)
    await currentLeaderNode.close()
    nodes.splice(nodes.indexOf(currentLeaderNode), 1)
    const survivingObserver = nodes[0]

    const expectedNextLeader = [leaderIdentity, follower1Identity, follower2Identity]
      .map((identity) => identity.publicKeyId)
      .filter((nodeId) => nodeId !== leaderId)
      .sort()[0]

    await waitFor(async () => survivingObserver.currentLeader() === expectedNextLeader)
    await waitFor(async () => nodes.every((node) => node.status.knownHeartbeats.length >= 2))

    const nextLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === expectedNextLeader)
    const result = await nextLeaderNode.put("hash:gamma", { failover: true })
    assert.equal(result.actor, expectedNextLeader)
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
 *   identity: { publicKeyId: string, publicKey: Buffer, secretKey: Buffer, feedKey: string },
 *   authorizedNodes: Array<{ nodeId: string, publicKey: Buffer, feedKey: string }>,
 *   encryptionKey: Buffer,
  *   bootstrap: Array<string | { host: string, port: number }>
 * }} options
 */
async function createFollower(options) {
  const dataDir = options.dataDir ?? (await tempDir(options.dirs))
  const follower = new HolepunchSwarmNode({
    dataDir,
    clusterId: options.clusterId,
    topicSalt: options.topicSalt,
    identity: options.identity,
    authorizedNodes: options.authorizedNodes,
    encryptionKey: options.encryptionKey,
    bootstrap: options.bootstrap
  })
  await follower.start()
  return follower
}

function seed(label) {
  return createHash("sha256").update(label).digest()
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
