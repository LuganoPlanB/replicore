import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import test from "node:test"
import assert from "node:assert/strict"
import { createHash, randomBytes } from "node:crypto"

import createTestnet from "hyperdht/testnet.js"

import {
  createPromotionCredential,
  createSignedOperation,
  generateIdentity,
  HolepunchHttpServer,
  HolepunchSwarmNode,
  validateLogLink,
  validateOperation
} from "../src/index.js"
import { waitFor, waitForNoChange } from "./helpers/eventual.js"
import {
  expectAbsent,
  deleteValue,
  expectDeleted,
  expectCommittedOperation,
  expectCommittedValue,
  expectHistoryOps,
  expectRedirectHints,
  expectWriteRefusal,
  getHistory,
  getValue,
  putValue,
  requestJson
} from "./helpers/http-crud.js"
import { createSwarmCluster } from "./helpers/swarm-cluster.js"

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

    let leaderId = null
    await waitFor(async () => {
      const leaderIds = [leader, follower1, follower2].map((node) => node.currentLeader())
      if (leaderIds[0] === null || !leaderIds.every((current) => current === leaderIds[0])) return false
      leaderId = leaderIds[0]
      return true
    })

    const electedLeader =
      [leader, follower1, follower2].find((node) => node.options.identity.publicKeyId === leaderId) ?? leader
    const putOp = await electedLeader.put("hash:alpha", { message: "hello" })

    await waitFor(async () => (await follower1.get("hash:alpha"))?.value?.message === "hello")
    await waitFor(async () => (await follower2.get("hash:alpha"))?.value?.message === "hello")

    const history = await follower1.getHistory("hash:alpha")
    assert.equal(history.length, 1)
    assert.equal(history[0].opId, putOp.opId)

    await electedLeader.delete("hash:alpha")
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

test("new and restarted followers read the same authoritative leader-log prefix", { concurrency: false }, async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("authoritative-leader"))
    const follower1Identity = generateIdentity(seed("authoritative-follower-1"))
    const follower2Identity = generateIdentity(seed("authoritative-follower-2"))
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

    let leaderId = null
    await waitFor(async () => {
      const current = leader.currentLeader()
      if (!current || follower1.currentLeader() !== current) return false
      leaderId = current
      return true
    })

    const electedLeader = [leader, follower1].find((node) => node.options.identity.publicKeyId === leaderId) ?? leader
    const forwarded = await follower1.put("hash:authoritative-prefix", { source: "forwarded" })
    await waitFor(async () => (await electedLeader.get("hash:authoritative-prefix"))?.value?.source === "forwarded")

    const lateFollower = await createFollower({
      dirs,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: follower2Identity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    nodes.push(lateFollower)

    await waitFor(async () => (await lateFollower.get("hash:authoritative-prefix"))?.value?.source === "forwarded")
    await waitFor(async () =>
      [leader, follower1, lateFollower].every((node) => node.currentLeader() === leaderId)
    )

    const leaderLog = await electedLeader.getAuthoritativeLogStatus()
    const follower1Log = await follower1.getAuthoritativeLogStatus()
    const lateFollowerLog = await lateFollower.getAuthoritativeLogStatus()
    for (const status of [follower1Log, lateFollowerLog]) {
      assert.equal(status.nodeId, leaderLog.nodeId)
      assert.equal(status.feedKey, leaderLog.feedKey)
      assert.equal(status.length, leaderLog.length)
      assert.equal(status.term, leaderLog.term)
    }

    const follower1Replication = await follower1.getReplicationStatus()
    assert.equal(follower1Replication.authoritativeLog.nodeId, leaderLog.nodeId)
    assert.equal(follower1Replication.authoritativeLog.feedKey, leaderLog.feedKey)
    assert.equal(follower1Replication.authoritativeLog.length, leaderLog.length)
    assert.equal(follower1Replication.authoritativeLog.term, leaderLog.term)

    const leaderCoreEntry = await (electedLeader.options.clusterSecret
      ? electedLeader.authoritativeLogCore
      : electedLeader.feedCores.get(leaderId)).get(forwarded.seq)
    const lateFollowerHistory = await lateFollower.getHistory("hash:authoritative-prefix")
    assert.equal(lateFollowerHistory.length, 1)
    assert.equal(lateFollowerHistory[0].opId, leaderCoreEntry.opId)
    assert.equal(lateFollowerHistory[0].feed, leaderLog.feedKey)

    await lateFollower.close()
    nodes.splice(nodes.indexOf(lateFollower), 1)

    const restartedFollower = await createFollower({
      dirs: [],
      dataDir: lateFollower.options.dataDir,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: follower2Identity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    nodes.push(restartedFollower)

    await waitFor(async () => (await restartedFollower.get("hash:authoritative-prefix"))?.value?.source === "forwarded")
    await waitFor(
      async () => {
        const leaderLogStatus = await leader.getAuthoritativeLogStatus()
        const restartedReplication = await restartedFollower.getReplicationStatus()
        const restartedLog = await restartedFollower.getAuthoritativeLogStatus()
        return (
          restartedLog.nodeId === leaderLogStatus.nodeId &&
          restartedLog.feedKey === leaderLogStatus.feedKey &&
          restartedLog.term === leaderLogStatus.term &&
          restartedLog.length === leaderLogStatus.length &&
          restartedReplication.authoritativeLog.nodeId === leaderLogStatus.nodeId &&
          restartedReplication.authoritativeLog.feedKey === leaderLogStatus.feedKey &&
          restartedReplication.authoritativeLog.length === leaderLogStatus.length
        )
      },
      {
        description: "restarted follower authoritative log prefix catch-up"
      }
    )
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("a reconnected follower truncates a divergent authoritative tail and replays the exact leader suffix", { concurrency: false }, async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const firstIdentity = generateIdentity(seed("authoritative-divergent-first"))
    const secondIdentity = generateIdentity(seed("authoritative-divergent-second"))
    const thirdIdentity = generateIdentity(seed("authoritative-divergent-third"))
    const authorizedNodes = [firstIdentity, secondIdentity, thirdIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    for (const identity of [firstIdentity, secondIdentity, thirdIdentity]) {
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
    }

    let leaderId = null
    await waitFor(async () => {
      const current = nodes[0].currentLeader()
      if (!current) return false
      return nodes.every((node) => node.currentLeader() === current) && (leaderId = current)
    })

    const leader = nodes.find((node) => node.options.identity.publicKeyId === leaderId)
    const [staleFollower] = nodes.filter((node) => node !== leader)
    assert.ok(leader)
    assert.ok(staleFollower)

    await leader.put("hash:authoritative-base", { phase: "base" })
    await waitFor(async () => (await staleFollower.get("hash:authoritative-base"))?.value?.phase === "base")

    await staleFollower.suspendNetworking()

    const staleCore = staleFollower.authoritativeLogCore
    const staleSeq = staleCore.length
    const previousOperation = staleSeq === 0 ? null : await staleCore.get(staleSeq - 1)
    const divergentOperation = createSignedOperation({
      kind: "kv",
      type: "put",
      key: "hash:divergent-local-tail",
      value: { phase: "divergent" },
      seq: staleSeq,
      term: previousOperation?.term ?? 0,
      index: staleSeq,
      prevIndex: previousOperation?.index ?? -1,
      prevHash: previousOperation?.entryHash ?? null,
      feed: staleFollower.authoritativeLogIdentity.feedKey,
      actor: staleFollower.options.identity.publicKeyId,
      secretKey: staleFollower.authoritativeLogIdentity.secretKey,
      encryptionKey: staleFollower.encryption.keys[staleFollower.encryption.currentKeyId],
      encryptionKeyId: staleFollower.encryption.currentKeyId
    })
    await staleCore.append(divergentOperation)
    await staleFollower.syncAuthoritativeLog()
    assert.equal(await staleFollower.get("hash:divergent-local-tail"), null)

    await leader.put("hash:authoritative-replayed", { phase: "leader" })

    await staleFollower.resumeNetworking()
    await waitFor(async () => (await staleFollower.get("hash:authoritative-replayed"))?.value?.phase === "leader")

    const leaderHistory = await leader.getHistory("hash:authoritative-replayed")
    const followerHistory = await staleFollower.getHistory("hash:authoritative-replayed")
    assert.equal(followerHistory.length, 1)
    assert.equal(followerHistory[0].opId, leaderHistory[0].opId)

    const staleStatus = await staleFollower.getAuthoritativeLogStatus()
    const leaderStatus = await leader.getAuthoritativeLogStatus()
    assert.equal(staleStatus.length, leaderStatus.length)
    assert.equal(staleStatus.tail.at(-1)?.seq, leaderStatus.tail.at(-1)?.seq)
    assert.equal(await staleFollower.get("hash:divergent-local-tail"), null)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test(
  "followers forward writes to the computed leader and become split-fenced when that leader disappears",
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

    await waitFor(async () => {
      const statuses = await Promise.all(nodes.map((node) => node.getReplicationStatus()))
      return statuses.every((status) =>
        status.splitStatus?.fenced === true &&
        status.splitStatus?.leaderNodeId === leaderId &&
        status.readStatus.reason === "split-fenced"
      )
    }, {
      description: "surviving followers become split-fenced",
      onTimeout: () => survivingObserver.getReplicationStatus()
    })

    const forwardingNode = nodes[0]
    await assert.rejects(
      forwardingNode.put("hash:gamma", { failover: true }),
      /split-fenced|Current leader .* is not reachable|No current leader is available/
    )
    assert.equal(await forwardingNode.get("hash:gamma"), null)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
  }
)

test("history keeps actor audit data and blocks new committed entries while a survivor is split-fenced", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("history-order-leader"))
    const follower1Identity = generateIdentity(seed("history-order-follower-1"))
    const authorizedNodes = [leaderIdentity, follower1Identity].map((identity) => ({
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

    nodes.push(leader, follower1)
    for (const node of nodes) {
      await node.start()
    }

    const firstLeaderId = [leaderIdentity, follower1Identity]
      .map((identity) => identity.publicKeyId)
      .sort()[0]
    await waitFor(async () => nodes.every((node) => node.currentLeader() === firstLeaderId))

    const firstLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === firstLeaderId)
    const firstWrite = await firstLeaderNode.put("hash:history-order", { phase: "before" })
    await waitFor(async () => (await follower1.get("hash:history-order"))?.value?.phase === "before")

    await firstLeaderNode.close()
    nodes.splice(nodes.indexOf(firstLeaderNode), 1)

    await waitFor(async () => {
      const [status] = await Promise.all(nodes.map((node) => node.getReplicationStatus()))
      return status.splitStatus?.fenced === true &&
        status.splitStatus?.leaderNodeId === firstLeaderId
    })

    await assert.rejects(
      follower1.put("hash:history-order", { phase: "after" }),
      /split-fenced|Current leader .* is not reachable|No current leader is available/
    )

    const history = await nodes[0].getHistory("hash:history-order")
    assert.equal(history.length, 1)
    assert.equal(history[0].actor, firstWrite.actor)
    assert.equal(history[0].opId, firstWrite.opId)
    assert.equal((await nodes[0].get("hash:history-order"))?.value?.phase, "before")
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("startup election converges on a single leader with a persisted term", { concurrency: false }, async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const identities = [
      generateIdentity(seed("election-startup-leader")),
      generateIdentity(seed("election-startup-follower-1")),
      generateIdentity(seed("election-startup-follower-2"))
    ]
    const authorizedNodes = identities.map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    for (const identity of identities) {
      const node = new HolepunchSwarmNode({
        dataDir: await tempDir(dirs),
        clusterId: "election-startup-cluster",
        topicSalt: "test-salt",
        identity,
        authorizedNodes,
        encryptionKey,
        bootstrap: testnet.bootstrap
      })
      nodes.push(node)
      await node.start()
    }

    await waitFor(async () => {
      const leaderIds = nodes.map((node) => node.currentLeader())
      return leaderIds[0] !== null && leaderIds.every((leaderId) => leaderId === leaderIds[0])
    }, {
      timeoutMs: 30000,
      description: "startup leader convergence"
    })
    const expectedLeaderId = nodes[0].currentLeader()

    const states = await Promise.all(nodes.map((node) => node.getConsensusState()))
    assert.ok(states.every((state) => state.currentTerm >= 1))
    assert.ok(states.every((state) => state.currentTerm === states[0].currentTerm))
    assert.equal(states.some((state, index) => nodes[index].options.identity.publicKeyId === expectedLeaderId), true)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("leader-only loss in a three-voter cluster elects a replacement after witness verification", { concurrency: false }, async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const identities = [
      generateIdentity(seed("election-failover-leader")),
      generateIdentity(seed("election-failover-follower-1")),
      generateIdentity(seed("election-failover-follower-2"))
    ]
    const authorizedNodes = identities.map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    for (const identity of identities) {
      const node = new HolepunchSwarmNode({
        dataDir: await tempDir(dirs),
        clusterId: "election-failover-cluster",
        topicSalt: "test-salt",
        identity,
        authorizedNodes,
        encryptionKey,
        bootstrap: testnet.bootstrap
      })
      nodes.push(node)
      await node.start()
    }

    await waitFor(async () => {
      const leaderIds = nodes.map((node) => node.currentLeader())
      return leaderIds[0] !== null && leaderIds.every((leaderId) => leaderId === leaderIds[0])
    }, {
      timeoutMs: 30000,
      description: "three-node leader convergence before failover"
    })
    const firstLeaderId = nodes[0].currentLeader()
    const initialTerm = (await nodes[0].getConsensusState()).currentTerm

    const firstLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === firstLeaderId)
    await firstLeaderNode.close()
    nodes.splice(nodes.indexOf(firstLeaderNode), 1)

    await waitFor(async () => {
      const leaders = nodes.map((node) => node.currentLeader())
      return leaders.every((leaderId) => leaderId && leaderId !== firstLeaderId) &&
        new Set(leaders).size === 1
    })

    const replacementStates = await Promise.all(nodes.map((node) => node.getConsensusState()))
    assert.ok(replacementStates.every((state) => state.currentTerm > initialTerm))
    assert.ok(replacementStates.every((state) => state.currentTerm === replacementStates[0].currentTerm))

    const follower = nodes.find((node) => node.currentLeader() !== node.options.identity.publicKeyId)
    const write = await follower.put("hash:replacement-after-leader-loss", { ok: true })
    assert.equal(write.type, "put")
    await waitFor(async () => {
      const values = await Promise.all(nodes.map((node) => node.get("hash:replacement-after-leader-loss")))
      return values.every((entry) => entry?.value?.ok === true)
    })
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("two-node leader loss stays split-fenced and does not autonomously reelect", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const identities = [
      generateIdentity(seed("two-node-leader-loss-leader")),
      generateIdentity(seed("two-node-leader-loss-follower"))
    ]
    const authorizedNodes = identities.map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    for (const identity of identities) {
      const node = new HolepunchSwarmNode({
        dataDir: await tempDir(dirs),
        clusterId: "two-node-leader-loss-cluster",
        topicSalt: "test-salt",
        identity,
        authorizedNodes,
        encryptionKey,
        bootstrap: testnet.bootstrap
      })
      nodes.push(node)
      await node.start()
    }

    await waitFor(async () => {
      const leaderIds = nodes.map((node) => node.currentLeader())
      return leaderIds[0] !== null && leaderIds.every((leaderId) => leaderId === leaderIds[0])
    }, {
      timeoutMs: 30000,
      description: "two-node leader convergence before loss"
    })
    const firstLeaderId = nodes[0].currentLeader()
    const initialTerm = (await nodes[0].getConsensusState()).currentTerm

    const firstLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === firstLeaderId)
    await firstLeaderNode.close()
    nodes.splice(nodes.indexOf(firstLeaderNode), 1)

    const survivor = nodes[0]
    await waitFor(async () => {
      const status = await survivor.getReplicationStatus()
      return status.splitStatus?.fenced === true &&
        status.splitStatus?.leaderNodeId === firstLeaderId &&
        status.readStatus.reason === "split-fenced"
    })

    await survivor.viewBee.put(`system/heartbeats/${firstLeaderId}`, {
      actor: firstLeaderId,
      feed: authorizedNodes.find((node) => node.nodeId === firstLeaderId)?.feedKey ?? null,
      term: initialTerm,
      ts: new Date().toISOString(),
      seq: 999,
      leaderId: firstLeaderId,
      leaderCommitIndex: 0,
      membershipVersion: 0,
      prevLogIndex: -1,
      prevLogTerm: -1,
      prevLogHash: null,
      observedLeader: firstLeaderId,
      reachableLeader: true,
      appliedFeeds: {},
      rejectedFeeds: {},
      membershipFingerprint: "forged-diagnostic-only"
    })

    const forgedStatus = await survivor.getReplicationStatus()
    assert.equal(forgedStatus.heartbeatByNode[firstLeaderId]?.reachableLeader, true)
    assert.equal(forgedStatus.splitStatus?.fenced, true)
    assert.equal(forgedStatus.readStatus.reason, "split-fenced")
    assert.equal(forgedStatus.leaderHealth.splitFenced, true)
    assert.equal(forgedStatus.reelection.allowed, false)
    assert.equal(forgedStatus.reelection.reason, "needs-three-voters")

    await assert.rejects(
      survivor.put("hash:two-node-leader-loss", { blocked: true }),
      /split-fenced|Current leader .* is not reachable|No current leader is available/
    )

    await new Promise((resolve) => setTimeout(resolve, 2_500))

    assert.notEqual(survivor.currentLeader(), survivor.options.identity.publicKeyId)
    const finalState = await survivor.getConsensusState()
    assert.equal(finalState.currentTerm, initialTerm)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("leader loss plus a second missing voter keeps the remaining voters split-fenced", { concurrency: false }, async () => {
  const testnet = await createTestnet(4)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const identities = [
      generateIdentity(seed("blocked-reelection-leader")),
      generateIdentity(seed("blocked-reelection-follower-1")),
      generateIdentity(seed("blocked-reelection-follower-2")),
      generateIdentity(seed("blocked-reelection-follower-3"))
    ]
    const authorizedNodes = identities.map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    for (const identity of identities) {
      const node = new HolepunchSwarmNode({
        dataDir: await tempDir(dirs),
        clusterId: "blocked-reelection-cluster",
        topicSalt: "test-salt",
        identity,
        authorizedNodes,
        encryptionKey,
        bootstrap: testnet.bootstrap
      })
      nodes.push(node)
      await node.start()
    }

    await waitFor(async () => {
      const leaderIds = nodes.map((node) => node.currentLeader())
      return leaderIds[0] !== null && leaderIds.every((leaderId) => leaderId === leaderIds[0])
    }, {
      timeoutMs: 30000,
      description: "four-node leader convergence before blocked reelection"
    })
    const firstLeaderId = nodes[0].currentLeader()
    const initialTerm = (await nodes[0].getConsensusState()).currentTerm

    const firstLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === firstLeaderId)
    const secondMissingNode = nodes.find((node) => node.options.identity.publicKeyId !== firstLeaderId)
    await firstLeaderNode.close()
    await secondMissingNode.close()
    nodes.splice(nodes.indexOf(firstLeaderNode), 1)
    nodes.splice(nodes.indexOf(secondMissingNode), 1)

    await waitFor(async () => {
      const statuses = await Promise.all(nodes.map((node) => node.getReplicationStatus()))
      return statuses.every((status) =>
        status.splitStatus?.fenced === true &&
        status.splitStatus?.leaderNodeId === firstLeaderId &&
        status.readStatus.reason === "split-fenced"
      )
    })

    await new Promise((resolve) => setTimeout(resolve, 2_500))

    assert.ok(nodes.every((node) => node.currentLeader() !== node.options.identity.publicKeyId))
    const states = await Promise.all(nodes.map((node) => node.getConsensusState()))
    assert.ok(states.every((state) => state.currentTerm === initialTerm))
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("leader writes require a voter majority, not just one follower acknowledgement", { concurrency: false }, async () => {
  const testnet = await createTestnet(5)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const identities = [
      generateIdentity(seed("quorum-leader")),
      generateIdentity(seed("quorum-follower-1")),
      generateIdentity(seed("quorum-follower-2")),
      generateIdentity(seed("quorum-follower-3")),
      generateIdentity(seed("quorum-follower-4"))
    ]
    const authorizedNodes = identities.map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    for (const identity of identities) {
      const node = new HolepunchSwarmNode({
        dataDir: await tempDir(dirs),
        clusterId: "test-cluster",
        topicSalt: "test-salt",
        identity,
        authorizedNodes,
        encryptionKey,
        bootstrap: testnet.bootstrap,
        durability: {
          requiredFollowerAcks: 1,
          timeoutMs: 1500
        }
      })
      nodes.push(node)
      await node.start()
    }

    await waitFor(async () => {
      const leaderIds = nodes.map((node) => node.currentLeader())
      return leaderIds[0] !== null && leaderIds.every((leaderId) => leaderId === leaderIds[0])
    })

    const leaderId = nodes[0].currentLeader()
    const leaderNode = nodes.find((node) => node.options.identity.publicKeyId === leaderId)
    const followers = nodes.filter((node) => node !== leaderNode)

    await waitFor(async () => {
      const status = await leaderNode.getReplicationStatus()
      return followers.every((node) => {
        const peer = status.peerReplication[node.options.identity.publicKeyId]
        return peer?.alive === true && peer?.connectedPeers > 0
      })
    })

    await followers[0].close()
    await followers[1].close()

    const majorityWrite = await leaderNode.put("hash:majority-write", { phase: "majority" })
    assert.equal((await leaderNode.getConsensusState()).commitIndex, majorityWrite.seq)

    await followers[2].close()
    await assert.rejects(
      leaderNode.put("hash:minority-write", { phase: "minority" }),
      /Durability requirement not met: no reachable quorum available/
    )
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("follower heartbeat diagnostics do not grant leader authority", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const encryptionKey = randomBytes(32)
    const firstIdentity = generateIdentity(seed("diagnostic-heartbeat-first"))
    const secondIdentity = generateIdentity(seed("diagnostic-heartbeat-second"))
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
      bootstrap: testnet.bootstrap
    })
    const second = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: secondIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })

    nodes.push(first, second)
    await first.start()
    await second.start()

    let leaderNode = null
    await waitFor(async () => {
      const convergedLeaderId = first.currentLeader()
      if (convergedLeaderId === null || convergedLeaderId !== second.currentLeader()) return false
      leaderNode = [first, second].find((node) => node.currentLeader() === node.options.identity.publicKeyId) ?? null
      return leaderNode !== null
    })
    const leaderId = leaderNode.currentLeader()
    const followerNode = leaderNode === first ? second : first
    const followerIdentity = followerNode === first ? firstIdentity : secondIdentity

    const followerCore = followerNode.feedCores.get(followerIdentity.publicKeyId)
    const slot = followerCore.length
    const previous = slot === 0 ? null : await followerCore.get(slot - 1)
    const operation = createSignedOperation({
      kind: "heartbeat",
      type: "put",
      key: `heartbeat:${followerIdentity.publicKeyId}`,
      keyspace: "system",
      seq: slot,
      term: (await followerNode.getConsensusState()).currentTerm,
      index: slot,
      prevIndex: previous?.index ?? -1,
      prevHash: previous?.entryHash ?? null,
      feed: followerIdentity.feedKey,
      actor: followerIdentity.publicKeyId,
      secretKey: followerIdentity.secretKey,
      encryptionKey,
      heartbeat: {
        leaderId: followerIdentity.publicKeyId,
        leaderCommitIndex: (await followerNode.getConsensusState()).commitIndex,
        membershipVersion: 0,
        prevLogIndex: previous?.index ?? -1,
        prevLogTerm: previous?.term ?? -1,
        prevLogHash: previous?.entryHash ?? null,
        observedLeader: followerIdentity.publicKeyId,
        reachableLeader: true,
        appliedFeeds: {},
        rejectedFeeds: {},
        membershipFingerprint: "diagnostic-only"
      }
    })
    await followerCore.append(operation)
    await leaderNode.syncFeed(followerIdentity.publicKeyId)

    const status = await leaderNode.getReplicationStatus()
    assert.equal(leaderNode.currentLeader(), leaderId)
    assert.equal(status.consensus.currentTerm, (await leaderNode.getConsensusState()).currentTerm)
    assert.equal(status.consensus.commitIndex, (await leaderNode.getConsensusState()).commitIndex)
    assert.equal(status.consensus.lastApplied, (await leaderNode.getConsensusState()).lastApplied)
    assert.equal(status.consensus.knownLeader, leaderId)
    assert.ok(status.heartbeatByNode[followerIdentity.publicKeyId])
    assert.equal(status.heartbeatByNode[followerIdentity.publicKeyId].actor, followerIdentity.publicKeyId)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("consensus state persists votedFor across restart", { concurrency: false }, async () => {
  const testnet = await createTestnet(3)
  const dirs = []

  try {
    const encryptionKey = randomBytes(32)
    const identity = generateIdentity(seed("election-persisted-vote"))
    const peerIdentity = generateIdentity(seed("election-persisted-peer"))
    const authorizedNodes = [identity, peerIdentity].map((entry) => ({
      nodeId: entry.publicKeyId,
      publicKey: entry.publicKey,
      feedKey: entry.feedKey
    }))

    const dataDir = await tempDir(dirs)
    let node = new HolepunchSwarmNode({
      dataDir,
      clusterId: "election-persisted-cluster",
      topicSalt: "test-salt",
      identity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await node.start()

    await node.setConsensusState({
      currentTerm: 7,
      votedFor: peerIdentity.publicKeyId
    })
    await node.close()

    node = new HolepunchSwarmNode({
      dataDir,
      clusterId: "election-persisted-cluster",
      topicSalt: "test-salt",
      identity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await node.start()

    const state = await node.getConsensusState()
    assert.equal(state.currentTerm, 7)
    assert.equal(state.votedFor, peerIdentity.publicKeyId)

    await node.close()
  } finally {
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
    let expectedLeaderId = null
    await waitFor(async () => {
      const leaderIds = nodes.map((node) => node.currentLeader())
      if (leaderIds[0] === null || !leaderIds.every((leaderId) => leaderId === leaderIds[0])) return false
      expectedLeaderId = leaderIds[0]
      return true
    })

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

    const leaderNode = nodes.find((node) => node.options.identity.publicKeyId === expectedLeaderId)
    const leaderServer = servers[nodes.indexOf(leaderNode)]
    const followerServer = servers.find((server) => server !== leaderServer)
    const followerNode = nodes[servers.indexOf(followerServer)]
    await waitForWritableWitness(followerNode)
    const baseUrl = `http://${followerServer.address.address}:${followerServer.address.port}`
    const leaderBaseUrl = `http://${leaderServer.address.address}:${leaderServer.address.port}`

    const operation = expectCommittedOperation(
      await putValue(baseUrl, "hash:http", { through: "http" }),
      { type: "put", key: "hash:http", keyspace: "default" }
    )

    await waitFor(async () => {
      const payload = expectWriteRefusal(
        await putValue(leaderBaseUrl, "hash:http-direct", { through: "leader" }),
        { status: 503, code: "not-witness-entrypoint" }
      )
      expectRedirectHints(payload)
      return true
    })

    assert.equal((await getValue(baseUrl, "hash:http", { token: "missing" })).status, 401)

    await waitFor(async () => {
      try {
        expectCommittedValue(await getValue(baseUrl, "hash:http"), { through: "http" })
        return true
      } catch {
        return false
      }
    })

    await waitFor(async () => {
      try {
        expectHistoryOps(await getHistory(baseUrl, "hash:http"), [{ type: "put", opId: operation.opId }])
        return true
      } catch {
        return false
      }
    })

    await waitFor(async () => {
      const values = await Promise.all(nodes.map((node) => node.get("hash:http")))
      return values.every((value) => value?.value?.through === "http")
    })

    expectCommittedValue(await getValue(leaderBaseUrl, "hash:http"), { through: "http" })

    const deleteOperation = expectCommittedOperation(
      await deleteValue(baseUrl, "hash:http"),
      { type: "delete", key: "hash:http", keyspace: "default" }
    )

    await waitFor(async () => {
      const values = await Promise.all(nodes.map((node) => node.get("hash:http")))
      return values.every((value) => value?.deleted === true)
    })

    expectDeleted(await getValue(baseUrl, "hash:http"))
    expectDeleted(await getValue(leaderBaseUrl, "hash:http"))

    expectHistoryOps(await getHistory(baseUrl, "hash:http"), [
      { type: "put", opId: operation.opId },
      { type: "delete", opId: deleteOperation.opId }
    ])

    const replicationResponse = await requestJson(`${baseUrl}/status/replication`)
    assert.equal(replicationResponse.status, 200)
    const replication = replicationResponse.payload
    assert.equal(replication.nodeId, followerNode.options.identity.publicKeyId)
    assert.equal(typeof replication.lastDurableSequence, "number")
    assert.equal(typeof replication.knownPeerNodeIds.length, "number")
    assert.equal(typeof replication.peerReplication[followerNode.options.identity.publicKeyId].lag, "number")
    assert.equal(typeof replication.peerReplication[followerNode.options.identity.publicKeyId].alive, "boolean")
    assert.equal(typeof replication.consensus.currentTerm, "number")
    assert.equal(typeof replication.consensus.membershipVersion, "number")
    assert.equal(typeof replication.leaderHealth.reachable, "boolean")
    assert.equal(typeof replication.witnessHealth.freshWitnessCount, "number")
    assert.equal(typeof replication.quorum.majority, "number")
    assert.equal(typeof replication.peerCache.total, "number")
    assert.equal(typeof replication.antiEntropy.state, "string")
    assert.equal(replication.recentRefusal.reason, null)
    assert.equal(typeof replication.reelection.allowed, "boolean")

    const writersResponse = await requestJson(`${baseUrl}/status/writers`)
    assert.equal(writersResponse.status, 200)
    const writers = writersResponse.payload
    assert.equal(writers.authorizedNodes.length, 3)
    assert.equal(typeof writers.currentTerm, "number")
    assert.equal(typeof writers.membershipVersion, "number")
    assert.equal(typeof writers.quorum.majority, "number")
    assert.equal(typeof writers.peerCache.total, "number")

    const leaderResponse = await requestJson(`${baseUrl}/status/leader`)
    assert.equal(leaderResponse.status, 200)
    const leader = leaderResponse.payload
    assert.equal(typeof leader.currentLeader, "string")
    assert.equal(typeof leader.currentTerm, "number")
    assert.equal(typeof leader.membershipVersion, "number")
    assert.equal(typeof leader.witnessHealth.quorum, "number")
    assert.equal(typeof leader.reelection.allowed, "boolean")

    assert.equal((await requestJson(`${baseUrl}/admin/snapshot`, { token: "writer" })).status, 403)

    const snapshotResponse = await requestJson(`${baseUrl}/admin/snapshot`, { token: "admin" })
    assert.equal(snapshotResponse.status, 200)
    const snapshot = snapshotResponse.payload
    assert.equal(snapshot.version, 1)
  } finally {
    await Promise.allSettled(servers.map((server) => server.close()))
    await Promise.allSettled(nodes.map((node) => node.close()))
    await testnet.destroy()
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
  }
})

test("HTTP CRUD failure before commit stays absent after leader restart", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 800,
    ackDelayMsByNodeId: {},
    durability: {
      requiredFollowerAcks: 1,
      timeoutMs: 300
    }
  })
  const servers = []

  try {
    await cluster.startAll()

    const leaderId = await waitForClusterLeaderId(cluster)
    const followerIds = cluster.records
      .map((record) => record.identity.publicKeyId)
      .filter((nodeId) => nodeId !== leaderId)
      .sort()
    cluster.options.ackDelayMsByNodeId = Object.fromEntries(followerIds.map((nodeId) => [nodeId, 1000]))
    await Promise.all(followerIds.map((nodeId) => cluster.restartNode(nodeId)))
    await waitForClusterLeaderId(cluster)

    const witness = cluster.record(followerIds[0]).node
    const leader = cluster.record(leaderId).node
    const server = new HolepunchHttpServer({
      node: witness,
      auth: {
        tokens: {
          writer: { readKeyspaces: ["default"], writeKeyspaces: ["default"] },
          reader: { readKeyspaces: ["default"], writeKeyspaces: [] }
        }
      }
    })
    await server.start()
    servers.push(server)

    const baseUrl = `http://${server.address.address}:${server.address.port}`
    const refused = expectWriteRefusal(
      await putValue(baseUrl, "hash:http-timeout", { phase: "timed-out" }),
      { status: [500, 503] }
    )
    if (refused.retryable !== undefined) {
      assert.equal(refused.retryable, true)
    }

    await waitFor(
      async () => (await leader.getReplicationStatus()).authoritativeLog.stagedTail.count === 0,
      {
        description: "timed-out HTTP write clears the staged authoritative tail",
        onTimeout: () => cluster.diagnostics()
      }
    )

    assert.equal(await leader.get("hash:http-timeout"), null)
    assert.deepEqual(await leader.getHistory("hash:http-timeout"), [])

    const restartedLeader = await cluster.restartNode(leaderId)
    await waitForClusterLeaderId(cluster)
    assert.equal(await restartedLeader.get("hash:http-timeout"), null)
    assert.deepEqual(await restartedLeader.getHistory("hash:http-timeout"), [])
    expectAbsent(await getValue(baseUrl, "hash:http-timeout"))
  } finally {
    await Promise.allSettled(servers.map((server) => server.close()))
    await cluster.closeAll()
  }
})

test("acknowledged HTTP CRUD survives leader restart with one committed history entry", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 800
  })
  const servers = []

  try {
    await cluster.startAll()
    const leaderId = await waitForClusterLeaderId(cluster)
    const witnessId = cluster.records
      .map((record) => record.identity.publicKeyId)
      .filter((nodeId) => nodeId !== leaderId)
      .sort()[0]
    const witness = cluster.record(witnessId).node
    await waitForWritableWitness(witness)

    const server = new HolepunchHttpServer({
      node: witness,
      auth: {
        tokens: {
          writer: { readKeyspaces: ["default"], writeKeyspaces: ["default"] },
          reader: { readKeyspaces: ["default"], writeKeyspaces: [] }
        }
      }
    })
    await server.start()
    servers.push(server)

    const baseUrl = `http://${server.address.address}:${server.address.port}`
    let committedResponse = null
    await waitFor(
      async () => {
        const response = await putValue(baseUrl, "hash:http-restart", { phase: "committed" })
        if (response.status !== 200) {
          committedResponse = response
          return false
        }
        committedResponse = response
        return true
      },
      {
        description: "witness HTTP write to commit after startup",
        onTimeout: async () => ({
          response: committedResponse,
          witness: await witness.getReplicationStatus(),
          cluster: await cluster.diagnostics()
        })
      }
    )
    const operation = expectCommittedOperation(committedResponse, {
      type: "put",
      key: "hash:http-restart",
      keyspace: "default"
    })

    await waitFor(async () => {
      const values = await Promise.all(cluster.nodes.map((node) => node.get("hash:http-restart")))
      return values.every((value) => value?.value?.phase === "committed")
    })

    await cluster.restartNode(leaderId)
    await waitForClusterLeaderId(cluster)

    await waitFor(async () => {
      const values = await Promise.all(cluster.nodes.map((node) => node.get("hash:http-restart")))
      return values.every((value) => value?.value?.phase === "committed")
    })

    expectCommittedValue(await getValue(baseUrl, "hash:http-restart"), { phase: "committed" })
    expectHistoryOps(await getHistory(baseUrl, "hash:http-restart"), [{ type: "put", opId: operation.opId }])

    for (const node of cluster.nodes) {
      const history = await node.getHistory("hash:http-restart")
      assert.equal(history.length, 1)
      assert.equal(history[0].opId, operation.opId)
    }
  } finally {
    await Promise.allSettled(servers.map((server) => server.close()))
    await cluster.closeAll()
  }
})

test("learner HTTP CRUD stays read-only while serving caught-up reads", { concurrency: false }, async () => {
  const leaderIdentity = generateIdentity(seed("http-learner-leader"))
  const followerIdentity = generateIdentity(seed("http-learner-follower"))
  const learnerIdentity = generateIdentity(seed("http-learner-node"))
  const identities = [leaderIdentity, followerIdentity, learnerIdentity]
  const authorizedNodes = identities.map((identity) => ({
    nodeId: identity.publicKeyId,
    publicKey: identity.publicKey,
    feedKey: identity.feedKey
  }))
  const cluster = await createSwarmCluster({
    identities,
    authorizedNodes,
    membership: {
      version: 1,
      voters: [leaderIdentity.publicKeyId, followerIdentity.publicKeyId].sort(),
      learners: [learnerIdentity.publicKeyId],
      removed: []
    },
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 800
  })
  const servers = []

  try {
    await cluster.startAll()
    const leaderId = await waitForClusterLeaderId(cluster)
    const leaderNode = cluster.record(leaderId).node
    const learnerNode = cluster.record(learnerIdentity.publicKeyId).node

    await leaderNode.put("hash:http-learner-visible", { phase: "before-http" })
    await waitFor(async () => (await learnerNode.get("hash:http-learner-visible"))?.value?.phase === "before-http")

    const server = new HolepunchHttpServer({
      node: learnerNode,
      auth: {
        tokens: {
          writer: { readKeyspaces: ["default"], writeKeyspaces: ["default"] },
          reader: { readKeyspaces: ["default"], writeKeyspaces: [] }
        }
      }
    })
    await server.start()
    servers.push(server)

    const baseUrl = `http://${server.address.address}:${server.address.port}`
    expectCommittedValue(await getValue(baseUrl, "hash:http-learner-visible"), { phase: "before-http" })

    const learnerPut = expectWriteRefusal(
      await putValue(baseUrl, "hash:http-learner-blocked", { phase: "blocked" }),
      { status: 403, code: "read-only-learner" }
    )
    assert.equal(learnerPut.retryable, false)

    const learnerDelete = expectWriteRefusal(
      await deleteValue(baseUrl, "hash:http-learner-visible"),
      { status: 403, code: "read-only-learner" }
    )
    assert.equal(learnerDelete.retryable, false)

    await leaderNode.put("hash:http-learner-after", { phase: "after-http" })
    await waitFor(async () => (await learnerNode.get("hash:http-learner-after"))?.value?.phase === "after-http")
    expectCommittedValue(await getValue(baseUrl, "hash:http-learner-after"), { phase: "after-http" })

    const learnerStatus = await requestJson(`${baseUrl}/status/replication`)
    assert.equal(learnerStatus.status, 200)
    assert.equal(learnerStatus.payload.role, "learner")
    assert.equal(learnerStatus.payload.membership.localRole, "learner")
    assert.equal(await learnerNode.get("hash:http-learner-blocked"), null)
  } finally {
    await Promise.allSettled(servers.map((server) => server.close()))
    await cluster.closeAll()
  }
})

test("wrong-secret nodes do not discover or mirror cluster CRUD state", { concurrency: false }, async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []
  const servers = []

  try {
    const clusterSecret = Buffer.from(
      "abababababababababababababababababababababababababababababababab",
      "hex"
    )
    const wrongSecret = Buffer.from(
      "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("http-wrong-secret-leader"))
    const followerIdentity = generateIdentity(seed("http-wrong-secret-follower"))
    const wrongIdentity = generateIdentity(seed("http-wrong-secret-node"))

    const voters = [leaderIdentity, followerIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const leader = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "http-wrong-secret-cluster",
      clusterSecret,
      machineId: "http-wrong-secret-leader-machine",
      identity: leaderIdentity,
      authorizedNodes: voters,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    const follower = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "http-wrong-secret-cluster",
      clusterSecret,
      machineId: "http-wrong-secret-follower-machine",
      identity: followerIdentity,
      authorizedNodes: voters,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    const wrongSecretNode = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "http-wrong-secret-cluster",
      clusterSecret: wrongSecret,
      machineId: "http-wrong-secret-other-machine",
      identity: wrongIdentity,
      authorizedNodes: [{
        nodeId: wrongIdentity.publicKeyId,
        publicKey: wrongIdentity.publicKey,
        feedKey: wrongIdentity.feedKey
      }],
      durability: { requiredFollowerAcks: 0 },
      encryptionKey,
      bootstrap: testnet.bootstrap
    })

    nodes.push(leader, follower, wrongSecretNode)
    await leader.start()
    await follower.start()
    await wrongSecretNode.start()

    await waitFor(async () => {
      const firstLeader = leader.currentLeader()
      return firstLeader !== null && firstLeader === follower.currentLeader()
    })
    const leaderNode = leader.currentLeader() === leaderIdentity.publicKeyId ? leader : follower
    const witnessNode = leaderNode === leader ? follower : leader

    for (const node of [witnessNode, wrongSecretNode]) {
      const server = new HolepunchHttpServer({
        node,
        auth: {
          tokens: {
            writer: { readKeyspaces: ["default"], writeKeyspaces: ["default"] },
            reader: { readKeyspaces: ["default"], writeKeyspaces: [] }
          }
        }
      })
      await server.start()
      servers.push(server)
    }

    const clusterServer = servers.find(
      (server) => server.options.node.options.identity.publicKeyId === witnessNode.options.identity.publicKeyId
    )
    const wrongServer = servers.find(
      (server) => server.options.node.options.identity.publicKeyId === wrongSecretNode.options.identity.publicKeyId
    )
    const clusterBaseUrl = `http://${clusterServer.address.address}:${clusterServer.address.port}`
    const wrongBaseUrl = `http://${wrongServer.address.address}:${wrongServer.address.port}`

    const clusterOperation = expectCommittedOperation(
      await putValue(clusterBaseUrl, "hash:http-wrong-secret", { side: "cluster" }),
      { type: "put", key: "hash:http-wrong-secret", keyspace: "default" }
    )
    assert.ok(clusterOperation.opId)

    await waitFor(async () => (await witnessNode.get("hash:http-wrong-secret"))?.value?.side === "cluster")
    expectCommittedValue(await getValue(clusterBaseUrl, "hash:http-wrong-secret"), { side: "cluster" })
    expectAbsent(await getValue(wrongBaseUrl, "hash:http-wrong-secret"))

    const wrongWrite = expectWriteRefusal(
      await putValue(wrongBaseUrl, "hash:http-wrong-secret", { side: "wrong-secret" }),
      { status: 503, code: /not-witness-entrypoint|leader-unreachable/ }
    )
    assert.equal(typeof wrongWrite.error, "string")

    expectCommittedValue(await getValue(clusterBaseUrl, "hash:http-wrong-secret"), { side: "cluster" })
    expectAbsent(await getValue(wrongBaseUrl, "hash:http-wrong-secret"))

    const wrongStatus = await wrongSecretNode.getReplicationStatus()
    assert.equal(wrongStatus.connections, 0)
    assert.deepEqual(wrongStatus.network.learnerCandidates, [])
    assert.deepEqual(wrongStatus.membership.voters.map((entry) => entry.nodeId), [wrongIdentity.publicKeyId])
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

test("an init-cluster voter can admit and replicate to a secret-first learner", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const clusterSecret = Buffer.from(
      "5656565656565656565656565656565656565656565656565656565656565656",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const initIdentity = generateIdentity(seed("init-cluster-voter"))
    const learnerIdentity = generateIdentity(seed("init-cluster-learner"))

    const initialVoter = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "init-cluster-join-flow",
      clusterSecret,
      machineId: "init-cluster-voter-machine",
      identity: initIdentity,
      authorizedNodes: [
        {
          nodeId: initIdentity.publicKeyId,
          publicKey: initIdentity.publicKey,
          feedKey: initIdentity.feedKey
        }
      ],
      durability: { requiredFollowerAcks: 0 },
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await initialVoter.start()
    nodes.push(initialVoter)

    await waitFor(async () => initialVoter.currentLeader() === initIdentity.publicKeyId)
    await initialVoter.put("hash:init-cluster-before", { value: "before-join" })

    const learner = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "init-cluster-join-flow",
      clusterSecret,
      role: "learner",
      machineId: "init-cluster-learner-machine",
      identity: learnerIdentity,
      authorizedNodes: [],
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await learner.start()
    nodes.push(learner)

    await waitFor(async () => {
      const value = await learner.get("hash:init-cluster-before")
      return value?.value?.value === "before-join"
    })

    const learnerStatus = await learner.getReplicationStatus()
    assert.equal(learnerStatus.membership.localRole, "learner")
    assert.deepEqual(learnerStatus.membership.voters.map((entry) => entry.nodeId), [initIdentity.publicKeyId])
    assert.equal(initialVoter.getWritersStatus().authorizedNodes.some((entry) => entry.nodeId === learnerIdentity.publicKeyId), true)
    await initialVoter.put("hash:init-cluster-after", { value: "after-join" })
    await waitFor(async () => {
      const value = await learner.get("hash:init-cluster-after")
      return value?.value?.value === "after-join"
    })
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
    await testnet.destroy()
  }
})

test("a duplicate machine identity is rejected during learner admission", { concurrency: false }, async () => {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const clusterSecret = Buffer.from(
      "5757575757575757575757575757575757575757575757575757575757575757",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const voterIdentity = generateIdentity(seed("duplicate-machine-voter"))
    const learnerAIdentity = generateIdentity(seed("duplicate-machine-learner-a"))
    const learnerBIdentity = generateIdentity(seed("duplicate-machine-learner-b"))

    const voter = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "duplicate-machine-id-cluster",
      clusterSecret,
      machineId: "duplicate-machine-voter-id",
      identity: voterIdentity,
      authorizedNodes: [
        {
          nodeId: voterIdentity.publicKeyId,
          publicKey: voterIdentity.publicKey,
          feedKey: voterIdentity.feedKey
        }
      ],
      durability: { requiredFollowerAcks: 0 },
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await voter.start()
    nodes.push(voter)

    await waitFor(async () => voter.currentLeader() === voterIdentity.publicKeyId)
    await voter.put("hash:duplicate-machine-before", { value: "before-join" })

    const learnerA = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "duplicate-machine-id-cluster",
      clusterSecret,
      role: "learner",
      machineId: "duplicate-machine-shared-id",
      identity: learnerAIdentity,
      authorizedNodes: [],
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await learnerA.start()
    nodes.push(learnerA)

    await waitFor(async () => {
      const value = await learnerA.get("hash:duplicate-machine-before")
      return value?.value?.value === "before-join"
    })
    assert.equal(learnerA.joinState.accepted, true)

    const learnerB = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "duplicate-machine-id-cluster",
      clusterSecret,
      role: "learner",
      machineId: "duplicate-machine-shared-id",
      identity: learnerBIdentity,
      authorizedNodes: [],
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await learnerB.start()
    nodes.push(learnerB)

    await waitForNoChange(
      async () => voter.getWritersStatus().authorizedNodes.map((entry) => entry.nodeId).sort(),
      { stableMs: 1000, timeoutMs: 4000 }
    )
    await waitForNoChange(async () => learnerB.joinState.accepted, { stableMs: 1000, timeoutMs: 4000 })

    const voterStatus = await voter.getReplicationStatus()
    assert.equal(
      voter.getWritersStatus().authorizedNodes.some((entry) => entry.nodeId === learnerBIdentity.publicKeyId),
      false
    )
    assert.equal(
      voterStatus.membership.learners.some((entry) => entry.nodeId === learnerBIdentity.publicKeyId),
      false
    )
    assert.equal(learnerB.joinState.accepted, false)
    assert.equal((await learnerB.get("hash:duplicate-machine-before"))?.value?.value ?? null, null)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
    await testnet.destroy()
  }
})

test("two independently initialized clusters with the same secret do not auto-merge", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const clusterSecret = Buffer.from(
      "6767676767676767676767676767676767676767676767676767676767676767",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const firstIdentity = generateIdentity(seed("same-secret-init-first"))
    const secondIdentity = generateIdentity(seed("same-secret-init-second"))

    const first = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "same-secret-double-init",
      clusterSecret,
      machineId: "same-secret-init-first-machine",
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
      clusterId: "same-secret-double-init",
      clusterSecret,
      machineId: "same-secret-init-second-machine",
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

    await waitFor(async () => {
      const firstStatus = await first.getReplicationStatus()
      const secondStatus = await second.getReplicationStatus()
      return (
        first.currentLeader() === firstIdentity.publicKeyId &&
        second.currentLeader() === secondIdentity.publicKeyId &&
        firstStatus.network.learnerCandidates.includes(second.transportIdentity.publicKeyHex) &&
        secondStatus.network.learnerCandidates.includes(first.transportIdentity.publicKeyHex)
      )
    })

    await first.put("hash:double-init-first", { value: "first-only" })
    await second.put("hash:double-init-second", { value: "second-only" })
    await new Promise((resolve) => setTimeout(resolve, 250))

    assert.equal((await first.get("hash:double-init-second"))?.value?.value ?? null, null)
    assert.equal((await second.get("hash:double-init-first"))?.value?.value ?? null, null)

    const firstStatus = await first.getReplicationStatus()
    const secondStatus = await second.getReplicationStatus()
    assert.deepEqual(firstStatus.membership.voters.map((entry) => entry.nodeId), [firstIdentity.publicKeyId])
    assert.deepEqual(secondStatus.membership.voters.map((entry) => entry.nodeId), [secondIdentity.publicKeyId])
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
    await testnet.destroy()
  }
})

test("a learner can join through the leader control channel, catch up, and later become a live voter", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const clusterSecret = Buffer.from(
      "4444444444444444444444444444444444444444444444444444444444444444",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("join-flow-leader"))
    const followerIdentity = generateIdentity(seed("join-flow-follower"))
    const learnerIdentity = generateIdentity(seed("join-flow-learner"))
    const voters = [leaderIdentity, followerIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const firstVoter = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "join-flow-cluster",
      clusterSecret,
      machineId: "join-flow-voter-1",
      identity: leaderIdentity,
      authorizedNodes: voters,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await firstVoter.start()
    nodes.push(firstVoter)

    const secondVoter = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "join-flow-cluster",
      clusterSecret,
      machineId: "join-flow-voter-2",
      identity: followerIdentity,
      authorizedNodes: voters,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await secondVoter.start()
    nodes.push(secondVoter)

    await waitFor(async () => {
      const firstLeader = firstVoter.currentLeader()
      const secondLeader = secondVoter.currentLeader()
      return firstLeader !== null && firstLeader === secondLeader
    })
    const leaderId = firstVoter.currentLeader()
    const leader = leaderId === leaderIdentity.publicKeyId ? firstVoter : secondVoter
    const follower = leader === firstVoter ? secondVoter : firstVoter
    await leader.put("hash:join-before", { value: "before-join" })

    const learner = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "join-flow-cluster",
      clusterSecret,
      role: "learner",
      machineId: "join-flow-learner",
      identity: learnerIdentity,
      authorizedNodes: [],
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await learner.start()
    nodes.push(learner)

    await waitFor(async () => {
      const value = await learner.get("hash:join-before")
      return value?.value?.value === "before-join"
    })

    const learnerStatus = await learner.getReplicationStatus()
    assert.equal(learnerStatus.membership.localRole, "learner")
    assert.deepEqual(
      learnerStatus.membership.voters.map((entry) => entry.nodeId).sort(),
      voters.map((entry) => entry.nodeId).sort()
    )
    assert.equal(leader.getWritersStatus().authorizedNodes.some((entry) => entry.nodeId === learnerIdentity.publicKeyId), true)
    assert.equal(learner.joinState.accepted, true)
    assert.equal(learner.joinState.recovery?.leaderNodeId, leaderId)
    assert.equal(Number.isInteger(learner.joinState.recovery?.consensus?.commitIndex), true)
    assert.equal(Number.isInteger(learner.joinState.recovery?.authoritativeLog?.lastLogIndex), true)
    const promotionWindow = nextCredentialWindow()

    const credential = createPromotionCredential({
      payload: {
        v: 1,
        type: "replicore.promotion",
        clusterId: "join-flow-cluster",
        membershipVersion: 0,
        learnerNodeId: learnerIdentity.publicKeyId,
        learnerNoisePublicKey: learner.transportIdentity.publicKeyHex,
        targetRole: "voter",
        issuedAt: promotionWindow.issuedAt,
        expiresAt: promotionWindow.expiresAt,
        nonce: "join-flow-promotion",
        signerNodeId: leaderIdentity.publicKeyId
      },
      signerSecretKey: leaderIdentity.secretKey
    })

    const accepted = await learner.submitPromotionCredential(credential)
    assert.equal(accepted.eligible, true)

    await leader.commitPromotionCredential(credential)
    await waitFor(async () => {
      const status = await learner.getReplicationStatus()
      return status.membership.localRole === "voter"
    })
    await waitFor(async () => {
      const status = await leader.getReplicationStatus()
      return status.membership.matchingNodeIds.includes(learnerIdentity.publicKeyId)
    })

    await follower.close()
    nodes.splice(nodes.indexOf(follower), 1)

    await waitFor(async () => {
      const status = await leader.getReplicationStatus()
      return status.connections === 1
    })

    await leader.put("hash:join-after", { value: "after-promotion" })
    await waitFor(async () => {
      const value = await learner.get("hash:join-after")
      return value?.value?.value === "after-promotion"
    })
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
    assert.equal(learnerStatus.membership.localRole, "learner")
    assert.deepEqual(
      learnerStatus.membership.voters.map((entry) => entry.nodeId).sort(),
      authorizedNodes.map((entry) => entry.nodeId).sort()
    )
    assert.deepEqual(learnerStatus.membership.learners.map((entry) => entry.nodeId), [
      learnerIdentity.publicKeyId
    ])
    assert.equal(learner.getWritersStatus().role, "learner")
    assert.equal((await learner.getLeaderStatus()).role, "learner")
    assert.equal(
      leader.getWritersStatus().authorizedNodes.some((entry) => entry.nodeId === learnerIdentity.publicKeyId),
      true
    )

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

test("a healed follower converges by recovery pull without requiring a fresh leader write", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const clusterSecret = Buffer.from(
      "7777777777777777777777777777777777777777777777777777777777777777",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("idle-recovery-leader"))
    const followerIdentity = generateIdentity(seed("idle-recovery-follower"))
    const authorizedNodes = [leaderIdentity, followerIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const first = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "idle-recovery-cluster",
      clusterSecret,
      machineId: "idle-recovery-1",
      identity: leaderIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await first.start()
    nodes.push(first)

    const second = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "idle-recovery-cluster",
      clusterSecret,
      machineId: "idle-recovery-2",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await second.start()
    nodes.push(second)

    await waitFor(async () => {
      const currentLeader = first.currentLeader()
      return currentLeader !== null && currentLeader === second.currentLeader()
    })
    const leaderId = first.currentLeader()
    const leader = leaderId === leaderIdentity.publicKeyId ? first : second
    const follower = leader === first ? second : first

    await leader.put("hash:idle-recovery-baseline", { phase: "baseline" })
    await waitFor(async () => (await follower.get("hash:idle-recovery-baseline"))?.value?.phase === "baseline")
    await follower.suspendNetworking()

    const followerCore = follower.authoritativeLogCore
    const divergentSeq = followerCore.length
    const previous = divergentSeq === 0 ? null : await followerCore.get(divergentSeq - 1)
    const divergent = createSignedOperation({
      kind: "kv",
      type: "put",
      key: "hash:idle-recovery-divergent",
      value: { phase: "divergent" },
      seq: divergentSeq,
      term: previous?.term ?? 0,
      index: divergentSeq,
      prevIndex: previous?.index ?? -1,
      prevHash: previous?.entryHash ?? null,
      feed: follower.authoritativeLogIdentity.feedKey,
      actor: follower.options.identity.publicKeyId,
      secretKey: follower.authoritativeLogIdentity.secretKey,
      encryptionKey: follower.encryption.keys[follower.encryption.currentKeyId],
      encryptionKeyId: follower.encryption.currentKeyId
    })
    await followerCore.append(divergent)
    await follower.syncAuthoritativeLog()
    assert.equal(await follower.get("hash:idle-recovery-divergent"), null)

    await follower.resumeNetworking()
    await waitFor(async () => {
      const leaderLog = await leader.getAuthoritativeLogStatus()
      const followerLog = await follower.getAuthoritativeLogStatus()
      return followerLog.length === leaderLog.length
    })
    assert.equal(await follower.get("hash:idle-recovery-divergent"), null)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
    await testnet.destroy()
  }
})

test("snapshot restore rejects tampered integrity metadata", { concurrency: false }, async () => {
  const testnet = await createTestnet(1)
  const dirs = []
  const nodes = []

  try {
    const clusterSecret = Buffer.from(
      "8888888888888888888888888888888888888888888888888888888888888888",
      "hex"
    )
    const identity = generateIdentity(seed("snapshot-integrity"))
    const node = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "snapshot-integrity-cluster",
      clusterSecret,
      machineId: "snapshot-integrity-machine",
      identity,
      authorizedNodes: [{
        nodeId: identity.publicKeyId,
        publicKey: identity.publicKey,
        feedKey: identity.feedKey
      }],
      encryptionKey: randomBytes(32),
      bootstrap: testnet.bootstrap
    })
    await node.start()
    nodes.push(node)

    await waitFor(() => node.currentLeader() === identity.publicKeyId)
    await node.put("hash:snapshot-integrity-value", { value: "ok" })
    const snapshot = await node.createSnapshot()
    const tampered = {
      ...snapshot,
      contentHash: "00".repeat(32)
    }
    await assert.rejects(node.restoreSnapshot(tampered), /content hash mismatch/)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
    await testnet.destroy()
  }
})

test("a live learner connection does not satisfy voter durability after a follower stops", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const clusterSecret = Buffer.from(
      "3333333333333333333333333333333333333333333333333333333333333333",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("learner-durability-leader"))
    const followerIdentity = generateIdentity(seed("learner-durability-follower"))
    const learnerIdentity = generateIdentity(seed("learner-durability-learner"))
    const authorizedNodes = [leaderIdentity, followerIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const firstVoter = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "learner-durability-cluster",
      clusterSecret,
      machineId: "learner-durability-voter-1",
      identity: leaderIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await firstVoter.start()
    nodes.push(firstVoter)

    const secondVoter = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "learner-durability-cluster",
      clusterSecret,
      machineId: "learner-durability-voter-2",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await secondVoter.start()
    nodes.push(secondVoter)

    await waitFor(async () => {
      const firstLeader = firstVoter.currentLeader()
      const secondLeader = secondVoter.currentLeader()
      return firstLeader !== null && firstLeader === secondLeader
    })
    const leaderId = firstVoter.currentLeader()
    const leaderNode = leaderId === leaderIdentity.publicKeyId ? firstVoter : secondVoter
    const followerNode = leaderNode === firstVoter ? secondVoter : firstVoter
    await leaderNode.put("hash:learner-durability-baseline", { value: "ready" })

    const learner = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "learner-durability-cluster",
      clusterSecret,
      machineId: "learner-durability-learner",
      identity: learnerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await learner.start()
    nodes.push(learner)

    await waitFor(async () => {
      const visible = await learner.get("hash:learner-durability-baseline")
      return visible?.value?.value === "ready"
    })

    await followerNode.close()
    nodes.splice(nodes.indexOf(followerNode), 1)

    await waitFor(async () => {
      const status = await leaderNode.getReplicationStatus()
      return status.connections === 1
    })

    await assert.rejects(
      leaderNode.put("hash:learner-durability-rejected", { value: "forbidden" }),
      /Durability requirement not met/
    )
    assert.equal(await leaderNode.get("hash:learner-durability-rejected"), null)
    assert.equal(await learner.get("hash:learner-durability-rejected"), null)
  } finally {
    await Promise.allSettled(nodes.map((node) => node.close()))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
    await testnet.destroy()
  }
})

test("a learner can store a valid promotion credential without becoming a voter yet", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []

  try {
    const clusterSecret = Buffer.from(
      "2222222222222222222222222222222222222222222222222222222222222222",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("promotion-leader"))
    const followerIdentity = generateIdentity(seed("promotion-follower"))
    const learnerIdentity = generateIdentity(seed("promotion-learner"))
    const authorizedNodes = [leaderIdentity, followerIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const leader = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "promotion-cluster",
      clusterSecret,
      machineId: "promotion-voter-leader",
      identity: leaderIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await leader.start()
    nodes.push(leader)

    const follower = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "promotion-cluster",
      clusterSecret,
      machineId: "promotion-voter-follower",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await follower.start()
    nodes.push(follower)

    await waitFor(async () => {
      const leaderNodeId = leader.currentLeader()
      const followerLeaderId = follower.currentLeader()
      return leaderNodeId !== null && leaderNodeId === followerLeaderId
    })
    const leaderId = leader.currentLeader()
    const leaderNode = leaderId === leaderIdentity.publicKeyId ? leader : follower
    await leaderNode.put("hash:promotion-baseline", { value: "ready" })

    const learner = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "promotion-cluster",
      clusterSecret,
      machineId: "promotion-learner-node",
      identity: learnerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await learner.start()
    nodes.push(learner)
    const earlyCredentialWindow = nextCredentialWindow()

    const earlyCredential = createPromotionCredential({
      payload: {
        v: 1,
        type: "replicore.promotion",
        clusterId: "promotion-cluster",
        membershipVersion: 0,
        learnerNodeId: learnerIdentity.publicKeyId,
        learnerNoisePublicKey: learner.transportIdentity.publicKeyHex,
        targetRole: "voter",
        issuedAt: earlyCredentialWindow.issuedAt,
        expiresAt: earlyCredentialWindow.expiresAt,
        nonce: "promotion-early",
        signerNodeId: leaderIdentity.publicKeyId
      },
      signerSecretKey: leaderIdentity.secretKey
    })

    await assert.rejects(learner.submitPromotionCredential(earlyCredential), /catch up/)

    await waitFor(async () => {
      const visible = await learner.get("hash:promotion-baseline")
      return visible?.value?.value === "ready"
    })
    const credentialWindow = nextCredentialWindow()

    const credential = createPromotionCredential({
      payload: {
        v: 1,
        type: "replicore.promotion",
        clusterId: "promotion-cluster",
        membershipVersion: 0,
        learnerNodeId: learnerIdentity.publicKeyId,
        learnerNoisePublicKey: learner.transportIdentity.publicKeyHex,
        targetRole: "voter",
        issuedAt: credentialWindow.issuedAt,
        expiresAt: credentialWindow.expiresAt,
        nonce: "promotion-valid",
        signerNodeId: leaderIdentity.publicKeyId
      },
      signerSecretKey: leaderIdentity.secretKey
    })

    let promotion = null
    await waitFor(async () => {
      try {
        promotion = await learner.submitPromotionCredential(credential)
        return true
      } catch (error) {
        if (/catch up/.test(error.message)) return false
        throw error
      }
    })

    assert.equal(promotion.eligible, true)
    assert.equal(promotion.accepted, false)
    assert.equal(promotion.targetRole, "voter")

    const status = await learner.getReplicationStatus()
    assert.equal(status.role, "learner")
    assert.equal(status.promotion.eligible, true)
    assert.equal(status.promotion.accepted, false)
    assert.equal(status.promotion.signerNodeId, leaderIdentity.publicKeyId)

    await assert.rejects(
      learner.submitPromotionCredential(credential),
      /hash was already submitted/
    )
    await assert.rejects(
      learner.put("hash:promotion-write", { value: "still-forbidden" }),
      (error) => error?.code === "READ_ONLY_LEARNER"
    )
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
        leaderId: identity.publicKeyId,
        leaderCommitIndex: slot,
        membershipVersion: 0,
        prevLogIndex: previous?.index ?? -1,
        prevLogTerm: previous?.term ?? -1,
        prevLogHash: previous?.entryHash ?? null,
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
        leaderId: identity.publicKeyId,
        leaderCommitIndex: slot,
        membershipVersion: 0,
        prevLogIndex: previous?.index ?? -1,
        prevLogTerm: previous?.term ?? -1,
        prevLogHash: previous?.entryHash ?? null,
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
    const durability = {
      requiredFollowerAcks: 1,
      timeoutMs: 20_000
    }
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
      durability,
      bootstrap: testnet.bootstrap
    })
    const follower = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      durability,
      bootstrap: testnet.bootstrap
    })
    const observer = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: observerIdentity,
      authorizedNodes,
      encryptionKey,
      durability,
      bootstrap: testnet.bootstrap
    })

    nodes.push(leader, follower, observer)
    for (const node of nodes) {
      await node.start()
    }
    await waitFor(async () => Object.keys((await leader.getReplicationStatus()).heartbeatByNode).length >= 3)
    const currentLeaderId = [leaderIdentity, followerIdentity, observerIdentity]
      .map((identity) => identity.publicKeyId)
      .sort()[0]
    const currentLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === currentLeaderId)
    await waitFor(async () => currentLeaderNode.currentLeader() === currentLeaderId)
    await waitFor(async () => {
      const status = await currentLeaderNode.getReplicationStatus()
      return nodes
        .filter((node) => node.options.identity.publicKeyId !== currentLeaderId)
        .every((node) => {
          const peer = status.peerReplication[node.options.identity.publicKeyId]
          return peer?.alive === true && peer?.connectedPeers > 0
        })
    })

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
    const durability = {
      requiredFollowerAcks: 1,
      timeoutMs: 20_000
    }
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
      durability,
      bootstrap: testnet.bootstrap
    })
    const follower = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      durability,
      bootstrap: testnet.bootstrap
    })
    const observer = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: observerIdentity,
      authorizedNodes,
      encryptionKey,
      durability,
      bootstrap: testnet.bootstrap
    })

    nodes.push(leader, follower, observer)
    for (const node of nodes) {
      await node.start()
    }

    await waitFor(async () => Object.keys((await leader.getReplicationStatus()).heartbeatByNode).length >= 3)
    const currentLeaderId = [leaderIdentity, followerIdentity, observerIdentity]
      .map((identity) => identity.publicKeyId)
      .sort()[0]
    const currentLeaderNode = nodes.find((node) => node.options.identity.publicKeyId === currentLeaderId)
    await waitFor(async () => currentLeaderNode.currentLeader() === currentLeaderId)
    await waitFor(async () => {
      const status = await currentLeaderNode.getReplicationStatus()
      return nodes
        .filter((node) => node.options.identity.publicKeyId !== currentLeaderId)
        .every((node) => status.peerReplication[node.options.identity.publicKeyId]?.alive === true)
    })

    await currentLeaderNode.put("hash:degraded-snapshot", { phase: "before-snapshot" })
    await currentLeaderNode.put("hash:degraded-delete", { phase: "before-delete" })
    await waitFor(async () => (await follower.get("hash:degraded-snapshot"))?.value?.phase === "before-snapshot")
    await waitFor(async () => (await observer.get("hash:degraded-delete"))?.value?.phase === "before-delete")

    const snapshot = await currentLeaderNode.createSnapshot()

    restoredDir = await tempDir(dirs)
    restoredOffline = new HolepunchSwarmNode({
      dataDir: restoredDir,
      clusterId: "test-cluster",
      topicSalt: "test-salt",
      identity: restoreIdentity,
      authorizedNodes,
      encryptionKey,
      durability,
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
    await waitFor(async () => (await observer.get("hash:degraded-after"))?.value?.phase === "after-snapshot")
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
      durability,
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
    assert.deepEqual(status.peerReplication[identity.publicKeyId].staged, {
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

    await waitFor(async () => {
      const firstLeader = first.currentLeader()
      const secondLeader = second.currentLeader()
      return firstLeader !== null && firstLeader === secondLeader
    })
    const leaderId = first.currentLeader()
    const leader = first.options.identity.publicKeyId === leaderId ? first : second
    const follower = leader === first ? second : first

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

    await waitFor(async () => (await followerNode.getReplicationStatus()).peerReplication[currentLeaderId].staged.count === 1)
    assert.equal(await followerNode.get("hash:watermark-staged"), null)
    assert.deepEqual(await followerNode.getHistory("hash:watermark-staged"), [])

    await pendingWrite
    await waitFor(async () => (await followerNode.get("hash:watermark-staged"))?.value?.phase === "pending")

    const status = await followerNode.getReplicationStatus()
    assert.equal(status.peerReplication[currentLeaderId].staged.count, 0)
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
    assert.equal((await followerNode.getReplicationStatus()).peerReplication[currentLeaderId].staged.count, 0)

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
    assert.equal(afterRestartProgress.committedApplied, beforeRestartProgress.committedApplied)
    assert.equal(afterRestartProgress.committedLastOpId, beforeRestartProgress.committedLastOpId)
    assert.ok(afterRestartProgress.rawApplied >= beforeRestartProgress.rawApplied)
    assert.ok(afterRestartProgress.rawApplied >= afterRestartProgress.committedApplied)
    if (afterRestartProgress.rawApplied === beforeRestartProgress.rawApplied) {
      assert.equal(afterRestartProgress.rawLastOpId, beforeRestartProgress.rawLastOpId)
    }
    assert.equal((await restarted.getReplicationStatus()).peerReplication[currentLeaderId].staged.count, 0)
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

    await waitFor(async () => (await followerNode.getReplicationStatus()).peerReplication[currentLeaderId].staged.count === 1)

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
    await waitFor(async () => (await leaderNode.getReplicationStatus()).peerReplication[leaderNode.options.identity.publicKeyId].length > 1)
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

test("HTTP body size limit enforces maximum request body size through Content-Length header", { concurrency: false }, async () => {
  const testnet = await createTestnet(2)
  const dirs = []
  const nodes = []
  let server = null

  try {
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("http-body-limit-leader"))
    const followerIdentity = generateIdentity(seed("http-body-limit-follower"))
    const authorizedNodes = [leaderIdentity, followerIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const leader = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-body-limit",
      topicSalt: "test-body-limit",
      identity: leaderIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    const follower = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "test-body-limit",
      topicSalt: "test-body-limit",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })

    nodes.push(leader, follower)
    await leader.start()
    await follower.start()

    const leaderId = [leaderIdentity, followerIdentity].sort((a, b) =>
      a.publicKeyId.localeCompare(b.publicKeyId)
    )[0].publicKeyId
    await waitFor(
      async () => nodes.every((node) => node.currentLeader() === leaderId),
      { description: "leader convergence for body limit test" }
    )

    const witness = nodes.find((node) => node.options.identity.publicKeyId !== leaderId)
    server = new HolepunchHttpServer({
      node: witness,
      auth: {
        tokens: { writer: { readKeyspaces: ["default"], writeKeyspaces: ["default"] } }
      }
    })
    await server.start()
    await waitForWritableWitness(witness)
    const baseUrl = `http://${server.address.address}:${server.address.port}`

    const smallResult = await putValue(baseUrl, "key-small", { size: "small" })
    assert.equal(smallResult.status, 200)

    const largeBody = "x".repeat(128 * 1024)
    const largeResponse = await fetch(`${baseUrl}/kv/key-large?keyspace=default`, {
      method: "PUT",
      headers: {
        authorization: "Bearer writer",
        "content-type": "application/json"
      },
      body: largeBody
    })
    assert.equal(largeResponse.status, 413)
    const largePayload = await largeResponse.json()
    assert.equal(largePayload.code, "PAYLOAD_TOO_LARGE")
  } finally {
    await server?.close()
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

/**
 * Wait until every running cluster node agrees on one live leader.
 *
 * @param {Awaited<ReturnType<typeof createSwarmCluster>>} cluster
 */
async function waitForClusterLeaderId(cluster) {
  let leaderId = null
  await waitFor(
    async () => {
      leaderId = cluster.nodes[0]?.currentLeader() ?? null
      return leaderId !== null && cluster.nodes.every((node) => node.currentLeader() === leaderId)
    },
    {
      description: "cluster leader convergence",
      onTimeout: () => cluster.diagnostics()
    }
  )
  return leaderId
}

/**
 * @param {import("../src/index.js").HolepunchSwarmNode} node
 */
async function waitForWritableWitness(node) {
  await waitFor(
    async () => {
      const status = await node.getReplicationStatus()
      return (
        status.leader !== null &&
        status.leader !== status.nodeId &&
        status.leaderHealth?.reachable === true &&
        status.splitStatus?.fenced !== true &&
        status.readStatus?.reason === null
      )
    },
    {
      description: "witness leader-connected readiness",
      onTimeout: async () => node.getReplicationStatus()
    }
  )
}

function seed(label) {
  return createHash("sha256").update(label).digest()
}

function nextCredentialWindow() {
  const issuedAt = new Date(Date.now() - 60_000)
  const expiresAt = new Date(Date.now() + 60 * 60 * 1000)
  return {
    issuedAt: issuedAt.toISOString(),
    expiresAt: expiresAt.toISOString()
  }
}

/**
 * @param {string[]} dirs
 */
async function tempDir(dirs) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-swarm-"))
  dirs.push(dir)
  return dir
}
