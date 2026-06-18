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
  validateLogLink,
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
    for (const node of nodes) {
      await node.start()
    }

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

test("history keeps actor audit data and logical index order across leader failover", { concurrency: false }, async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("history-order-leader"))
    const follower1Identity = generateIdentity(seed("history-order-follower-1"))
    const follower2Identity = generateIdentity(seed("history-order-follower-2"))
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
    for (const node of nodes) {
      await node.start()
    }

    const firstLeaderId = [leaderIdentity, follower1Identity, follower2Identity]
      .map((identity) => identity.publicKeyId)
      .sort()[0]
    await waitFor(async () => nodes.every((node) => node.currentLeader() === firstLeaderId))

    const firstLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === firstLeaderId)
    const firstWrite = await firstLeaderNode.put("hash:history-order", { phase: "before" })
    await waitFor(async () => (await follower1.get("hash:history-order"))?.value?.phase === "before")

    await firstLeaderNode.close()
    nodes.splice(nodes.indexOf(firstLeaderNode), 1)

    const secondLeaderId = nodes
      .map((node) => node.options.identity.publicKeyId)
      .sort()[0]
    const secondLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === secondLeaderId)
    await waitFor(async () => nodes.every((node) => node.currentLeader() === secondLeaderId))

    const secondWrite = await secondLeaderNode.put("hash:history-order", { phase: "after" })
    await waitFor(async () => {
      const values = await Promise.all(nodes.map((node) => node.get("hash:history-order")))
      return values.every((value) => value?.value?.phase === "after")
    })

    const history = await nodes[0].getHistory("hash:history-order")
    assert.equal(history.length, 2)
    assert.equal(history[0].actor, firstWrite.actor)
    assert.equal(history[0].opId, firstWrite.opId)
    assert.equal(history[1].actor, secondWrite.actor)
    assert.equal(history[1].opId, secondWrite.opId)
    assert.ok(history[0].index < history[1].index)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

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

    for (const node of nodes) {
      await node.start()
    }
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

test("same-secret unknown peers are surfaced as learner candidates without joining membership", { concurrency: false }, async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const clusterSecret = Buffer.from(
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      "hex"
    )
    const wrongSecret = Buffer.from(
      "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const firstIdentity = generateIdentity(seed("learner-candidate-first"))
    const secondIdentity = generateIdentity(seed("learner-candidate-second"))
    const thirdIdentity = generateIdentity(seed("learner-candidate-third"))
    const first = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "learner-candidate-cluster",
      clusterSecret,
      machineId: "learner-machine-first",
      identity: firstIdentity,
      authorizedNodes: [
        {
          nodeId: firstIdentity.publicKeyId,
          publicKey: firstIdentity.publicKey,
          feedKey: firstIdentity.feedKey
        }
      ],
      durability: { requiredFollowerAcks: 0 },
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await first.start()
    nodes.push(first)

    const second = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "learner-candidate-cluster",
      clusterSecret,
      machineId: "learner-machine-second",
      identity: secondIdentity,
      authorizedNodes: [
        {
          nodeId: secondIdentity.publicKeyId,
          publicKey: secondIdentity.publicKey,
          feedKey: secondIdentity.feedKey
        }
      ],
      durability: { requiredFollowerAcks: 0 },
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await second.start()
    nodes.push(second)

    const wrongSecretNode = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "learner-candidate-cluster",
      clusterSecret: wrongSecret,
      machineId: "learner-machine-third",
      identity: thirdIdentity,
      authorizedNodes: [
        {
          nodeId: thirdIdentity.publicKeyId,
          publicKey: thirdIdentity.publicKey,
          feedKey: thirdIdentity.feedKey
        }
      ],
      durability: { requiredFollowerAcks: 0 },
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await wrongSecretNode.start()
    nodes.push(wrongSecretNode)

    await waitFor(async () => {
      const firstStatus = await first.getReplicationStatus()
      const secondStatus = await second.getReplicationStatus()
      return (
        firstStatus.connections > 0 &&
        secondStatus.connections > 0 &&
        firstStatus.network.learnerCandidates.includes(second.transportIdentity.publicKeyHex) &&
        secondStatus.network.learnerCandidates.includes(first.transportIdentity.publicKeyHex)
      )
    })

    const firstStatus = await first.getReplicationStatus()
    const secondStatus = await second.getReplicationStatus()
    const wrongSecretStatus = await wrongSecretNode.getReplicationStatus()

    assert.deepEqual(firstStatus.membership.mismatchedNodeIds, [])
    assert.deepEqual(secondStatus.membership.mismatchedNodeIds, [])
    assert.deepEqual(firstStatus.network.learnerCandidates, [second.transportIdentity.publicKeyHex])
    assert.deepEqual(secondStatus.network.learnerCandidates, [first.transportIdentity.publicKeyHex])
    assert.equal(firstStatus.knownPeerNodeIds.length, 1)
    assert.equal(secondStatus.knownPeerNodeIds.length, 1)
    assert.equal(wrongSecretStatus.connections, 0)
    assert.deepEqual(wrongSecretStatus.network.learnerCandidates, [])

    await first.put("hash:learner-candidate", { state: "leader-only" })
    await new Promise((resolve) => setTimeout(resolve, 250))
    assert.equal(await second.get("hash:learner-candidate"), null)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
    await testnet.destroy()
  }
})

test("a learner catches up for reads, stays out of quorum, and rejects writes", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const clusterSecret = Buffer.from(
      "1111111111111111111111111111111111111111111111111111111111111111",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("learner-leader"))
    const followerIdentity = generateIdentity(seed("learner-follower"))
    const learnerIdentity = generateIdentity(seed("learner-observer"))
    const authorizedNodes = [leaderIdentity, followerIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const leader = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "learner-cluster",
      clusterSecret,
      machineId: "learner-voter-leader",
      identity: leaderIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await leader.start()
    nodes.push(leader)

    const follower = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "learner-cluster",
      clusterSecret,
      machineId: "learner-voter-follower",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await follower.start()
    nodes.push(follower)

    const leaderId = [leaderIdentity.publicKeyId, followerIdentity.publicKeyId].sort()[0]
    await waitFor(async () => leader.currentLeader() === leaderId && follower.currentLeader() === leaderId)

    const leaderNode = leaderId === leaderIdentity.publicKeyId ? leader : follower
    await leaderNode.put("hash:learner-visible", { value: "before-learner" })

    const learner = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "learner-cluster",
      clusterSecret,
      machineId: "learner-read-only",
      identity: learnerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await learner.start()
    nodes.push(learner)

    await waitFor(async () => {
      const visible = await learner.get("hash:learner-visible")
      return visible?.value?.value === "before-learner" && learner.currentLeader() === leaderId
    })

    const learnerStatus = await learner.getReplicationStatus()
    assert.equal(learnerStatus.role, "learner")
    assert.equal(learner.getWritersStatus().role, "learner")
    assert.equal((await learner.getLeaderStatus()).role, "learner")
    assert.equal(leader.getWritersStatus().authorizedNodes.length, 2)

    await assert.rejects(
      learner.put("hash:learner-write", { value: "forbidden" }),
      (error) =>
        error?.code === "READ_ONLY_LEARNER" &&
        error?.leader === leaderId &&
        /read-only learner/.test(error.message)
    )
    await assert.rejects(
      learner.delete("hash:learner-visible"),
      (error) => error?.code === "READ_ONLY_LEARNER"
    )

    await leaderNode.put("hash:learner-after", { value: "after-learner" })
    await waitFor(async () => {
      const replicated = await learner.get("hash:learner-after")
      return replicated?.value?.value === "after-learner"
    })
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
    await testnet.destroy()
  }
})

test("operation validation rejects mismatched feed metadata", () => {
  const identity = generateIdentity(seed("validation"))
  const operation = {
    v: 1,
    kind: "kv",
    entryHash: "x",
    opId: "x",
    signature: "y",
    term: 0,
    index: 0,
    prevIndex: -1,
    prevHash: null,
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

test("operation validation rejects inconsistent logical log metadata", () => {
  const identity = generateIdentity(seed("validation-log"))
  const operation = {
    v: 1,
    kind: "kv",
    entryHash: "hash",
    opId: "hash",
    signature: "sig",
    term: 0,
    index: 1,
    prevIndex: -1,
    prevHash: null,
    feed: identity.feedKey,
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
  }, /logical index mismatch/)
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

test("logical log link validation rejects previous hash mismatch", () => {
  const previous = {
    index: 0,
    entryHash: "expected-hash",
    term: 2
  }
  const operation = {
    index: 1,
    prevIndex: 0,
    prevHash: "wrong-hash",
    term: 2
  }

  assert.throws(() => {
    validateLogLink(operation, previous, 1)
  }, /previous hash mismatch/)
})

test("sync rejects a feed entry with a bad previous hash", { concurrency: false }, async () => {
  await withStandaloneNode(seed("bad-prev-hash"), async ({ node, identity }) => {
    const localCore = node.feedCores.get(identity.publicKeyId)
    const slot = localCore.length
    const previous = slot === 0 ? null : await localCore.get(slot - 1)
    const operation = createSignedOperation({
      kind: "heartbeat",
      type: "put",
      key: `heartbeat:${identity.publicKeyId}`,
      keyspace: "system",
      seq: slot,
      term: 0,
      index: slot,
      prevIndex: previous?.index ?? -1,
      prevHash: "bad-prev-hash",
      feed: identity.feedKey,
      actor: identity.publicKeyId,
      secretKey: identity.secretKey,
      encryptionKey: randomBytes(32),
      heartbeat: {
        observedLeader: identity.publicKeyId,
        reachableLeader: true,
        appliedFeeds: {},
        rejectedFeeds: {},
        membershipFingerprint: "fingerprint"
      }
    })

    await localCore.append(operation)

    await assert.rejects(node.syncFeed(identity.publicKeyId), /previous hash mismatch/)
  })
})

test("sync rejects a feed entry with a corrupted signature", { concurrency: false }, async () => {
  await withStandaloneNode(seed("bad-signature"), async ({ node, identity }) => {
    const localCore = node.feedCores.get(identity.publicKeyId)
    const slot = localCore.length
    const previous = slot === 0 ? null : await localCore.get(slot - 1)
    const operation = createSignedOperation({
      kind: "heartbeat",
      type: "put",
      key: `heartbeat:${identity.publicKeyId}`,
      keyspace: "system",
      seq: slot,
      term: 0,
      index: slot,
      prevIndex: previous?.index ?? -1,
      prevHash: previous?.entryHash ?? null,
      feed: identity.feedKey,
      actor: identity.publicKeyId,
      secretKey: identity.secretKey,
      encryptionKey: randomBytes(32),
      heartbeat: {
        observedLeader: identity.publicKeyId,
        reachableLeader: true,
        appliedFeeds: {},
        rejectedFeeds: {},
        membershipFingerprint: "fingerprint"
      }
    })

    await localCore.append({
      ...operation,
      signature: (() => {
        const signature = Buffer.from(operation.signature, "base64url")
        signature[0] ^= 0x01
        return signature.toString("base64url")
      })()
    })

    await assert.rejects(node.syncFeed(identity.publicKeyId), /Invalid operation/)
  })
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
    for (const node of nodes) {
      await node.start()
    }
    await waitFor(async () => nodes.every((node) => node.status.knownHeartbeats.length >= 2))
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
    for (const node of nodes) {
      await node.start()
    }
    await waitFor(async () => Object.keys((await leader.getReplicationStatus()).heartbeats).length >= 3)
    const currentLeaderId = [leaderIdentity, followerIdentity, observerIdentity]
      .map((identity) => identity.publicKeyId)
      .sort()[0]
    const currentLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === currentLeaderId)
    await waitFor(async () => currentLeaderNode.currentLeader() === currentLeaderId)

    await currentLeaderNode.put("hash:snapshot", { state: "present" })

    const snapshot = await currentLeaderNode.createSnapshot()

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

test("a restored node can serve snapshot reads before rejoin and later catch up under degraded topology", { concurrency: false }, async () => {
  const testnet = await createTestnet(4)
  const dirs = []
  const nodes = []

  let restoredOffline = null
  let restoredOnline = null
  let restoredDir = null

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("leader"))
    const followerIdentity = generateIdentity(seed("follower-1"))
    const observerIdentity = generateIdentity(seed("follower-2"))
    const restoreIdentity = generateIdentity(seed("restore-degraded"))

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
    for (const node of nodes) {
      await node.start()
    }

    await waitFor(async () => Object.keys((await leader.getReplicationStatus()).heartbeats).length >= 3)
    const currentLeaderId = [leaderIdentity, followerIdentity, observerIdentity]
      .map((identity) => identity.publicKeyId)
      .sort()[0]
    const currentLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === currentLeaderId)
    await waitFor(async () => currentLeaderNode.currentLeader() === currentLeaderId)

    await currentLeaderNode.put("hash:degraded-snapshot", { phase: "before-snapshot" })
    await currentLeaderNode.put("hash:degraded-delete", { phase: "before-delete" })
    await waitFor(async () => (await follower.get("hash:degraded-snapshot"))?.value?.phase === "before-snapshot")
    await waitFor(async () => (await observer.get("hash:degraded-delete"))?.value?.phase === "before-delete")

    const snapshot = await currentLeaderNode.createSnapshot()

    await observer.close()

    restoredDir = await tempDir(dirs)
    restoredOffline = new HolepunchSwarmNode({
      dataDir: restoredDir,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: restoreIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: []
    })
    await restoredOffline.start()
    await restoredOffline.restoreSnapshot(snapshot)

    const offlineSnapshotValue = await restoredOffline.get("hash:degraded-snapshot")
    const offlineSnapshotDelete = await restoredOffline.get("hash:degraded-delete")
    assert.equal(offlineSnapshotValue?.value?.phase, "before-snapshot")
    assert.equal(offlineSnapshotDelete?.value?.phase, "before-delete")

    const offlineStatus = await restoredOffline.getReplicationStatus()
    assert.equal(offlineStatus.connections, 0)
    assert.equal(offlineStatus.readStatus.staleReadsPossible, true)

    const afterSnapshot = await currentLeaderNode.put("hash:degraded-after", { phase: "after-snapshot" })
    await currentLeaderNode.delete("hash:degraded-delete")
    await waitFor(async () => (await follower.get("hash:degraded-after"))?.value?.phase === "after-snapshot")
    await waitFor(async () => (await follower.get("hash:degraded-delete"))?.deleted === true)

    await restoredOffline.close()
    restoredOffline = null

    restoredOnline = new HolepunchSwarmNode({
      dataDir: restoredDir,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: restoreIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await restoredOnline.start()

    await waitFor(async () => (await restoredOnline.get("hash:degraded-after"))?.value?.phase === "after-snapshot")
    await waitFor(async () => (await restoredOnline.get("hash:degraded-delete"))?.deleted === true)

    const restoredHistory = await restoredOnline.getHistory("hash:degraded-after")
    assert.equal(restoredHistory.length, 1)
    assert.equal(restoredHistory[0].opId, afterSnapshot.opId)
  } finally {
    await Promise.allSettled([restoredOnline?.close(), restoredOffline?.close()].filter(Boolean))
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("replication status exposes staged entries without exposing committed CRUD state", { concurrency: false }, async () => {
  const testnet = await createTestnet(1)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const identity = generateIdentity(seed("staged-status"))
    const authorizedNodes = [
      {
        nodeId: identity.publicKeyId,
        publicKey: identity.publicKey,
        feedKey: identity.feedKey
      }
    ]

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
    await node.start()

    await node.view.stageEntry(identity.feedKey, {
      nodeId: identity.publicKeyId,
      source: "local",
      validation: "valid",
      operation: {
        kind: "kv",
        type: "put",
        keyspace: "default",
        key: "hash:staged-status",
        actor: identity.publicKeyId,
        seq: 3,
        opId: "staged-status-3",
        ts: "2026-06-17T00:00:02.000Z"
      }
    })

    const status = await node.getReplicationStatus()
    assert.deepEqual(status.feeds[identity.publicKeyId].staged, {
      count: 1,
      firstSeq: 3,
      lastSeq: 3,
      latestOpId: "staged-status-3",
      latestKey: "hash:staged-status"
    })

    assert.equal(await node.get("hash:staged-status"), null)
    assert.deepEqual(await node.getHistory("hash:staged-status"), [])

    const snapshot = await node.createSnapshot()
    assert.ok(snapshot.entries.every((entry) => !String(entry.key).includes("/staged/")))
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("concurrent leader appends keep signed sequence equal to feed slot", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const firstIdentity = generateIdentity(seed("append-lock-first"))
    const secondIdentity = generateIdentity(seed("append-lock-second"))
    const authorizedNodes = [firstIdentity, secondIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const first = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: firstIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap,
      heartbeatIntervalMs: 10
    })
    const second = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: secondIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap,
      heartbeatIntervalMs: 10
    })
    nodes.push(first, second)
    await first.start()
    await second.start()

    const leaderId = [firstIdentity, secondIdentity].map((identity) => identity.publicKeyId).sort()[0]
    const leader = first.options.identity.publicKeyId === leaderId ? first : second
    const follower = leader === first ? second : first
    await waitFor(async () => first.currentLeader() === leaderId)
    await waitFor(async () => second.currentLeader() === leaderId)

    const writes = await Promise.all(
      Array.from({ length: 5 }, (_, index) => leader.put(`hash:append-lock-${index}`, { index }))
    )
    await waitFor(async () => (await follower.get("hash:append-lock-4"))?.value?.index === 4)

    const localCore = leader.feedCores.get(leaderId)
    for (let seq = 0; seq < localCore.length; seq += 1) {
      const operation = await localCore.get(seq)
      assert.equal(operation.seq, seq)
    }
    assert.equal(new Set(writes.map((operation) => operation.seq)).size, writes.length)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("a follower keeps a replicated write staged until the leader advertises the commit watermark", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("watermark-leader"))
    const followerIdentity = generateIdentity(seed("watermark-follower"))
    const authorizedNodes = [leaderIdentity, followerIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const first = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: leaderIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap,
      ackDelayMs: 500,
      durability: {
        requiredFollowerAcks: 1,
        timeoutMs: 4000
      }
    })
    const second = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap,
      ackDelayMs: 500
    })

    nodes.push(first, second)
    await first.start()
    await second.start()

    const currentLeaderId = [leaderIdentity, followerIdentity].map((identity) => identity.publicKeyId).sort()[0]
    const leaderNode = currentLeaderId === leaderIdentity.publicKeyId ? first : second
    const followerNode = leaderNode === first ? second : first

    await waitFor(async () => first.currentLeader() === currentLeaderId)
    await waitFor(async () => second.currentLeader() === currentLeaderId)

    const pendingWrite = leaderNode.put("hash:watermark-staged", { phase: "pending" })
    pendingWrite.catch(() => {})

    await waitFor(async () => (await followerNode.getReplicationStatus()).feeds[currentLeaderId].staged.count === 1)
    assert.equal(await followerNode.get("hash:watermark-staged"), null)
    assert.deepEqual(await followerNode.getHistory("hash:watermark-staged"), [])

    await pendingWrite
    await waitFor(async () => (await followerNode.get("hash:watermark-staged"))?.value?.phase === "pending")

    const status = await followerNode.getReplicationStatus()
    assert.equal(status.feeds[currentLeaderId].staged.count, 0)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("committed feed progress survives follower restart after watermark-driven apply", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  let restarted = null

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("watermark-restart-leader"))
    const followerIdentity = generateIdentity(seed("watermark-restart-follower"))
    const authorizedNodes = [leaderIdentity, followerIdentity].map((identity) => ({
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
    const followerDir = await tempDir(dirs)
    let follower = new HolepunchSwarmNode({
      dataDir: followerDir,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })

    nodes.push(leader, follower)
    await leader.start()
    await follower.start()

    const currentLeaderId = [leaderIdentity, followerIdentity].map((identity) => identity.publicKeyId).sort()[0]
    const leaderNode = currentLeaderId === leaderIdentity.publicKeyId ? leader : follower
    const followerNode = leaderNode === leader ? follower : leader
    const followerIdentityForRestart = followerNode.options.identity
    const followerDirForRestart = followerNode.options.dataDir

    await waitFor(async () => leader.currentLeader() === currentLeaderId)
    await waitFor(async () => follower.currentLeader() === currentLeaderId)

    await leaderNode.put("hash:watermark-restart", { phase: "committed" })
    await waitFor(async () => (await followerNode.get("hash:watermark-restart"))?.value?.phase === "committed")

    const leaderFeedKey = authorizedNodes.find((node) => node.nodeId === currentLeaderId).feedKey
    const beforeRestartProgress = await followerNode.view.getFeedProgress(leaderFeedKey)
    assert.ok(beforeRestartProgress.committedApplied > 0)
    assert.equal((await followerNode.getReplicationStatus()).feeds[currentLeaderId].staged.count, 0)

    await followerNode.close()
    nodes.splice(nodes.indexOf(followerNode), 1)

    restarted = new HolepunchSwarmNode({
      dataDir: followerDirForRestart,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: followerIdentityForRestart,
      authorizedNodes,
      encryptionKey,
      bootstrap: []
    })
    await restarted.start()

    const restartedValue = await restarted.get("hash:watermark-restart")
    assert.equal(restartedValue?.value?.phase, "committed")

    const afterRestartProgress = await restarted.view.getFeedProgress(leaderFeedKey)
    assert.deepEqual(afterRestartProgress, beforeRestartProgress)
    assert.equal((await restarted.getReplicationStatus()).feeds[currentLeaderId].staged.count, 0)
  } finally {
    await Promise.allSettled([restarted?.close(), ...nodes.map((node) => node.close())])
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("a staged delete stays out of reads, history, and snapshots until committed", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("watermark-delete-leader"))
    const followerIdentity = generateIdentity(seed("watermark-delete-follower"))
    const authorizedNodes = [leaderIdentity, followerIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const first = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: leaderIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap,
      ackDelayMs: 500,
      durability: {
        requiredFollowerAcks: 1,
        timeoutMs: 4000
      }
    })
    const second = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap,
      ackDelayMs: 500
    })

    nodes.push(first, second)
    await first.start()
    await second.start()

    const currentLeaderId = [leaderIdentity, followerIdentity].map((identity) => identity.publicKeyId).sort()[0]
    const leaderNode = currentLeaderId === leaderIdentity.publicKeyId ? first : second
    const followerNode = leaderNode === first ? second : first

    await waitFor(async () => first.currentLeader() === currentLeaderId)
    await waitFor(async () => second.currentLeader() === currentLeaderId)

    const putOperation = await leaderNode.put("hash:watermark-delete", { phase: "before-delete" })
    await waitFor(async () => (await followerNode.get("hash:watermark-delete"))?.value?.phase === "before-delete")

    const pendingDelete = leaderNode.delete("hash:watermark-delete")
    pendingDelete.catch(() => {})

    await waitFor(async () => (await followerNode.getReplicationStatus()).feeds[currentLeaderId].staged.count === 1)

    const stagedValue = await followerNode.get("hash:watermark-delete")
    assert.equal(stagedValue?.value?.phase, "before-delete")

    const stagedHistory = await followerNode.getHistory("hash:watermark-delete")
    assert.equal(stagedHistory.length, 1)
    assert.equal(stagedHistory[0].opId, putOperation.opId)

    const stagedSnapshot = await followerNode.createSnapshot()
    const stagedCurrent = stagedSnapshot.entries.find((entry) => entry.key === "kv/current/default/hash:watermark-delete")
    assert.equal(stagedCurrent?.value?.deleted, false)
    assert.ok(stagedSnapshot.entries.some((entry) => entry.key === "kv/value/default/hash:watermark-delete"))
    assert.ok(stagedSnapshot.entries.every((entry) => !String(entry.key).includes("/staged/")))

    await pendingDelete
    await waitFor(async () => (await followerNode.get("hash:watermark-delete"))?.deleted === true)

    const committedHistory = await followerNode.getHistory("hash:watermark-delete")
    assert.equal(committedHistory.length, 2)
    assert.equal(committedHistory[0].opId, putOperation.opId)
    assert.equal(committedHistory[1].type, "delete")

    const committedSnapshot = await followerNode.createSnapshot()
    const committedCurrent = committedSnapshot.entries.find((entry) => entry.key === "kv/current/default/hash:watermark-delete")
    assert.equal(committedCurrent?.value?.deleted, true)
    assert.ok(committedSnapshot.entries.every((entry) => entry.key !== "kv/value/default/hash:watermark-delete"))
    assert.ok(committedSnapshot.entries.every((entry) => !String(entry.key).includes("/staged/")))
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("closing a leader rejects a delayed durability wait without leaving a live timer behind", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("leader"))
    const followerIdentity = generateIdentity(seed("follower-1"))
    const authorizedNodes = [leaderIdentity, followerIdentity].map((identity) => ({
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
      bootstrap: testnet.bootstrap,
      ackDelayMs: 2000,
      durability: {
        requiredFollowerAcks: 1,
        timeoutMs: 4000
      }
    })
    const follower = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap,
      ackDelayMs: 2000
    })

    nodes.push(leader, follower)
    await leader.start()
    await follower.start()

    const currentLeaderId = [leaderIdentity, followerIdentity].map((identity) => identity.publicKeyId).sort()[0]
    const leaderNode =
      currentLeaderId === leaderIdentity.publicKeyId ? leader : follower

    await waitFor(async () => leader.currentLeader() === currentLeaderId)
    await waitFor(async () => follower.currentLeader() === currentLeaderId)

    const pendingWrite = leaderNode.put("hash:closing-leader", { phase: "pending-close" })
    pendingWrite.catch(() => {})
    await waitFor(async () => (await leaderNode.getReplicationStatus()).feeds[leaderNode.options.identity.publicKeyId].length > 1)
    assert.equal(await leaderNode.get("hash:closing-leader"), null)
    assert.deepEqual(await leaderNode.getHistory("hash:closing-leader"), [])

    const pendingWriteRejection = assert.rejects(pendingWrite, /Node is closing/)
    const closePromise = leaderNode.close()
    await pendingWriteRejection
    await closePromise
    nodes.splice(nodes.indexOf(leaderNode), 1)
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

async function withStandaloneNode(identitySeed, run) {
  const dirs = []
  const identity = generateIdentity(identitySeed)
  const authorizedNodes = [
    {
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }
  ]
  const encryptionKey = randomBytes(32)
  let node = null

  try {
    node = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "standalone-node",
      topicSalt: "standalone-node",
      identity,
      authorizedNodes,
      encryptionKey,
      bootstrap: []
    })
    await node.start()
    await run({ node, identity })
  } finally {
    await Promise.allSettled([node?.close()].filter(Boolean))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
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
