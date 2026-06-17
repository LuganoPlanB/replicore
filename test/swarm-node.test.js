import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import test from "node:test"
import assert from "node:assert/strict"
import { createHash, randomBytes } from "node:crypto"

import createTestnet from "hyperdht/testnet.js"

import {
  createSignedOperation,
  generateIdentity,
  HolepunchHttpServer,
  HolepunchSwarmNode,
  validateOperation
} from "../src/index.js"
import { waitFor } from "./helpers/eventual.js"

test("leader operations replicate to followers and rebuild after restart", { concurrency: false }, async () => {
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

test(
  "followers forward writes to the computed leader and the next alive node becomes leader",
  { concurrency: false },
  async () => {
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
    await waitFor(async () => Object.keys((await leader.getReplicationStatus()).heartbeats).length >= 3)

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

    const nextLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === expectedNextLeader)
    let result = null
    await waitFor(
      async () => {
        try {
          result = await nextLeaderNode.put("hash:gamma", { failover: true })
          return result.actor === expectedNextLeader
        } catch {
          return false
        }
      },
      {
        description: "failover write through the next leader",
        onTimeout: () => survivingObserver.getReplicationStatus()
      }
    )

    assert.equal(result.actor, expectedNextLeader)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
  }
)

test("authorized HTTP API forwards writes and exposes status routes", { concurrency: false }, async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []
  const servers = []

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

    for (const identity of [leaderIdentity, follower1Identity, follower2Identity]) {
      const node = new HolepunchSwarmNode({
        dataDir: await tempDir(dirs),
        clusterId: "test-cluster",
        topicSalt: "test-salt",
        identity,
        authorizedNodes,
        encryptionKey,
        bootstrap: testnet.bootstrap
      })
      nodes.push(node)
    }

    await Promise.all(nodes.map((node) => node.start()))
    const expectedLeaderId = [leaderIdentity, follower1Identity, follower2Identity]
      .map((identity) => identity.publicKeyId)
      .sort()[0]
    await waitFor(async () => nodes.every((node) => node.currentLeader() === expectedLeaderId))

    for (const node of nodes) {
      const server = new HolepunchHttpServer({
        node,
        auth: {
          tokens: {
            admin: { admin: true, readKeyspaces: ["*"], writeKeyspaces: ["*"] },
            writer: { readKeyspaces: ["default"], writeKeyspaces: ["default"] },
            reader: { readKeyspaces: ["default"], writeKeyspaces: [] }
          }
        }
      })
      await server.start()
      servers.push(server)
    }

    const followerServer = servers[1]
    const baseUrl = `http://${followerServer.address.address}:${followerServer.address.port}`

    const putResponse = await fetch(`${baseUrl}/kv/hash:http?keyspace=default`, {
      method: "PUT",
      headers: {
        authorization: "Bearer writer",
        "content-type": "application/json"
      },
      body: JSON.stringify({ value: { through: "http" } })
    })
    assert.equal(putResponse.status, 200)
    const operation = await putResponse.json()
    assert.equal(typeof operation.opId, "string")

    const unauthorized = await fetch(`${baseUrl}/kv/hash:http?keyspace=default`, {
      headers: { authorization: "Bearer missing" }
    })
    assert.equal(unauthorized.status, 401)

    const readResponse = await fetch(`${baseUrl}/kv/hash:http?keyspace=default`, {
      headers: { authorization: "Bearer reader" }
    })
    assert.equal(readResponse.status, 200)
    const current = await readResponse.json()
    assert.equal(current.value.through, "http")

    const historyResponse = await fetch(`${baseUrl}/kv/hash:http/history?keyspace=default`, {
      headers: { authorization: "Bearer reader" }
    })
    assert.equal(historyResponse.status, 200)
    const history = await historyResponse.json()
    assert.equal(history.history.length, 1)

    const replicationResponse = await fetch(`${baseUrl}/status/replication`)
    assert.equal(replicationResponse.status, 200)
    const replication = await replicationResponse.json()
    assert.equal(replication.nodeId, nodes[1].options.identity.publicKeyId)
    assert.equal(typeof replication.lastDurableSequence, "number")
    assert.equal(typeof replication.knownPeerNodeIds.length, "number")
    assert.equal(typeof replication.feeds[nodes[1].options.identity.publicKeyId].lag, "number")
    assert.equal(typeof replication.feeds[nodes[1].options.identity.publicKeyId].alive, "boolean")

    const writersResponse = await fetch(`${baseUrl}/status/writers`)
    assert.equal(writersResponse.status, 200)
    const writers = await writersResponse.json()
    assert.equal(writers.authorizedNodes.length, 3)

    const leaderResponse = await fetch(`${baseUrl}/status/leader`)
    assert.equal(leaderResponse.status, 200)
    const leader = await leaderResponse.json()
    assert.equal(typeof leader.currentLeader, "string")

    const snapshotForbidden = await fetch(`${baseUrl}/admin/snapshot`, {
      headers: { authorization: "Bearer writer" }
    })
    assert.equal(snapshotForbidden.status, 403)

    const snapshotResponse = await fetch(`${baseUrl}/admin/snapshot`, {
      headers: { authorization: "Bearer admin" }
    })
    assert.equal(snapshotResponse.status, 200)
    const snapshot = await snapshotResponse.json()
    assert.equal(snapshot.version, 1)
  } finally {
    await Promise.allSettled(servers.map((server) => server.close()))
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("operation validation rejects mismatched feed metadata", () => {
  const identity = generateIdentity(seed("validation"))
  const operation = {
    v: 1,
    kind: "kv",
    opId: "x",
    signature: "y",
    feed: "wrong-feed",
    seq: 0,
    type: "delete",
    key: "hash:key",
    keyspace: "default",
    value: null,
    heartbeat: null,
    ts: new Date().toISOString(),
    expiresAt: new Date().toISOString(),
    actor: identity.publicKeyId
  }

  assert.throws(() => {
    validateOperation(operation, {
      nodeId: identity.publicKeyId,
      feedKey: identity.feedKey
    })
  }, /feed mismatch/)
})

test("operation validation rejects revoked writers", () => {
  const identity = generateIdentity(seed("revoked-writer"))
  const operation = createSignedOperation({
    kind: "kv",
    type: "delete",
    key: "hash:key",
    keyspace: "default",
    seq: 0,
    feed: identity.feedKey,
    actor: identity.publicKeyId,
    secretKey: identity.secretKey,
    encryptionKey: randomBytes(32)
  })

  assert.throws(() => {
    validateOperation(
      operation,
      {
        nodeId: identity.publicKeyId,
        feedKey: identity.feedKey
      },
      { revokedNodeIds: new Set([identity.publicKeyId]) }
    )
  }, /revoked/)
})

test(
  "encryption rotation preserves existing reads and exposes revoked writer state",
  { concurrency: false },
  async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []
  const servers = []

  try {
    const leaderIdentity = generateIdentity(seed("leader"))
    const followerIdentity = generateIdentity(seed("follower-1"))
    const revokedIdentity = generateIdentity(seed("follower-2"))
    const authorizedNodes = [leaderIdentity, followerIdentity, revokedIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))
    const encryption = {
      currentKeyId: "primary",
      keys: {
        primary: randomBytes(32),
        next: randomBytes(32)
      }
    }

    const leader = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: leaderIdentity,
      authorizedNodes,
      revokedNodeIds: [revokedIdentity.publicKeyId],
      encryption,
      bootstrap: testnet.bootstrap
    })
    const follower = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: followerIdentity,
      authorizedNodes,
      revokedNodeIds: [revokedIdentity.publicKeyId],
      encryption,
      bootstrap: testnet.bootstrap
    })

    nodes.push(leader, follower)
    await Promise.all(nodes.map((node) => node.start()))
    await waitFor(async () => Object.keys((await leader.getReplicationStatus()).heartbeats).length >= 2)
    const currentLeaderId = [leaderIdentity, followerIdentity].map((identity) => identity.publicKeyId).sort()[0]
    const currentLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === currentLeaderId)
    await waitFor(async () => currentLeaderNode.currentLeader() === currentLeaderId)

    const server = new HolepunchHttpServer({
      node: currentLeaderNode,
      auth: {
        tokens: {
          admin: { admin: true, readKeyspaces: ["*"], writeKeyspaces: ["*"] },
          writer: { readKeyspaces: ["default"], writeKeyspaces: ["default"] }
        }
      }
    })
    await server.start()
    servers.push(server)

    const baseUrl = `http://${server.address.address}:${server.address.port}`
    const firstOperation = await currentLeaderNode.put("hash:before-rotation", { value: "first" })
    assert.equal(firstOperation.value.keyId, "primary")

    const rotateResponse = await fetch(`${baseUrl}/admin/encryption/rotate`, {
      method: "POST",
      headers: {
        authorization: "Bearer admin",
        "content-type": "application/json"
      },
      body: JSON.stringify({ keyId: "next" })
    })
    assert.equal(rotateResponse.status, 200)
    assert.deepEqual(await rotateResponse.json(), { ok: true, keyId: "next" })

    const secondOperation = await currentLeaderNode.put("hash:after-rotation", { value: "second" })
    assert.equal(secondOperation.value.keyId, "next")

    await waitFor(async () => (await follower.get("hash:before-rotation"))?.value?.value === "first")
    await waitFor(async () => (await follower.get("hash:after-rotation"))?.value?.value === "second")

    const writers = currentLeaderNode.getWritersStatus()
    assert.deepEqual(writers.revokedNodeIds, [revokedIdentity.publicKeyId])
    assert.equal(writers.encryptionKeyId, "next")
    assert.equal(
      writers.authorizedNodes.find((node) => node.nodeId === revokedIdentity.publicKeyId)?.revoked,
      true
    )
  } finally {
    await Promise.allSettled(servers.map((server) => server.close()))
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
  }
)

test("a fresh node can restore current state from a snapshot", { concurrency: false }, async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("leader"))
    const followerIdentity = generateIdentity(seed("follower-1"))
    const observerIdentity = generateIdentity(seed("follower-2"))
    const restoreIdentity = generateIdentity(seed("restore"))

    const authorizedNodes = [leaderIdentity, followerIdentity, observerIdentity, restoreIdentity].map(
      (identity) => ({
        nodeId: identity.publicKeyId,
        publicKey: identity.publicKey,
        feedKey: identity.feedKey
      })
    )

    const leader = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: leaderIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    const follower = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    const observer = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: observerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })

    nodes.push(leader, follower, observer)
    await Promise.all(nodes.map((node) => node.start()))
    await waitFor(async () => Object.keys((await leader.getReplicationStatus()).heartbeats).length >= 3)
    const currentLeaderId = [leaderIdentity, followerIdentity, observerIdentity]
      .map((identity) => identity.publicKeyId)
      .sort()[0]
    const currentLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === currentLeaderId)
    await waitFor(async () => currentLeaderNode.currentLeader() === currentLeaderId)

    await currentLeaderNode.put("hash:snapshot", { state: "present" })
    await waitFor(async () => (await follower.get("hash:snapshot"))?.value?.state === "present")

    const snapshot = await follower.createSnapshot()

    const restored = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: restoreIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: []
    })
    nodes.push(restored)
    await restored.start()
    await restored.restoreSnapshot(snapshot)

    const restoredValue = await restored.get("hash:snapshot")
    assert.equal(restoredValue.value.state, "present")
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
