import assert from "node:assert/strict"
import test from "node:test"

import { HolepunchHttpServer } from "../src/index.js"
import { assertClusterValue, collectReplicationStatus, waitFor } from "./helpers/eventual.js"
import { createIdentities, createSwarmCluster } from "./helpers/swarm-cluster.js"

test("five-node static membership supports forwarding, replication, and deletes", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 5,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000
  })

  try {
    await cluster.startAll()

    await waitFor(
      async () => cluster.nodes.every((node) => node.status.knownHeartbeats.length >= cluster.identities.length),
      {
        description: "five-node heartbeat convergence",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const leaderId = currentLeaderId(cluster)
    await waitFor(
      async () => cluster.nodes.every((node) => node.currentLeader() === leaderId),
      {
        description: "five-node leader convergence",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const leader = cluster.record(leaderId).node
    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return liveFollowerIds(cluster, leaderId).some(
          (nodeId) => status.feeds[nodeId]?.alive === true && status.feeds[nodeId]?.connectedPeers > 0
        )
      },
      {
        description: "five-node durability precondition",
        onTimeout: () => leader.getReplicationStatus()
      }
    )

    const operation = await leader.put("hash:five-node", { members: 5 })
    assert.equal(operation.actor, leaderId)

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:five-node", { members: 5 }),
      {
        description: "five-node value convergence",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    await leader.delete("hash:five-node")

    await waitFor(
      async () => {
        const values = await Promise.all(cluster.nodes.map((node) => node.get("hash:five-node")))
        return values.every((value) => value?.deleted === true)
      },
      {
        description: "five-node delete convergence",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    for (const status of Object.values(await collectReplicationStatus(cluster.nodes))) {
      assert.equal(Object.keys(status.feeds).length, 5)
      assert.equal(Object.keys(status.heartbeats).length, 5)
      assert.equal(status.leader, leaderId)
    }

    const history = await leader.getHistory("hash:five-node")
    assert.equal(history.length, 2)
  } finally {
    await cluster.closeAll()
  }
})

test("five-node cluster stays durable when two non-leader followers go offline", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 5,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000
  })

  try {
    await cluster.startAll()

    await waitFor(
      async () => cluster.nodes.every((node) => node.status.knownHeartbeats.length >= cluster.identities.length),
      {
        description: "initial five-node convergence",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const leaderId = currentLeaderId(cluster)
    await waitFor(
      async () => cluster.nodes.every((node) => node.currentLeader() === leaderId),
      {
        description: "initial five-node leader convergence",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const offlineFollowers = liveFollowerIds(cluster, leaderId).slice(0, 2)
    const leader = cluster.record(leaderId).node

    await Promise.all(offlineFollowers.map((nodeId) => cluster.stopNode(nodeId)))
    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return liveFollowerIds(cluster, leaderId)
          .filter((nodeId) => !offlineFollowers.includes(nodeId))
          .some((nodeId) => status.feeds[nodeId]?.alive === true && status.feeds[nodeId]?.connectedPeers > 0)
      },
      {
        description: "surviving follower remains durable after two followers stop",
        onTimeout: () => leader.getReplicationStatus()
      }
    )

    const operation = await leader.put("hash:degraded-five", { degraded: true })
    assert.equal(operation.actor, leaderId)

    const liveNodes = cluster.nodes
    await waitFor(
      async () => hasClusterValue(liveNodes, "hash:degraded-five", { degraded: true }),
      {
        description: "live-node convergence while two followers are offline",
        onTimeout: () => collectReplicationStatus(liveNodes)
      }
    )

    const leaderStatus = await leader.getReplicationStatus()
    assert.ok(leaderStatus.lastDurableSequence >= operation.seq)

    await Promise.all(offlineFollowers.map((nodeId) => cluster.restartNode(nodeId)))

    await waitFor(
      async () => cluster.nodes.every((node) => node.status.knownHeartbeats.length >= cluster.identities.length),
      {
        description: "five-node reconvergence after follower restart",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:degraded-five", { degraded: true }),
      {
        description: "restarted follower catch-up",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    await cluster.record(leaderId).node.delete("hash:degraded-five")

    await waitFor(
      async () => {
        const values = await Promise.all(cluster.nodes.map((node) => node.get("hash:degraded-five")))
        return values.every((value) => value?.deleted === true)
      },
      {
        description: "delete convergence after offline followers return",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )
  } finally {
    await cluster.closeAll()
  }
})

test("single surviving node serves reads but blocks writes until a follower returns", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 800
  })

  try {
    await cluster.startAll()

    await waitFor(
      async () => cluster.nodes.every((node) => node.status.knownHeartbeats.length >= cluster.identities.length),
      {
        description: "initial three-node convergence",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const leaderId = currentLeaderId(cluster)
    const leader = cluster.record(leaderId).node
    const offlineFollowers = liveFollowerIds(cluster, leaderId)

    await leader.put("hash:survive-read", { baseline: true })
    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:survive-read", { baseline: true }),
      {
        description: "baseline replication before follower outage",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const baselineStatus = await leader.getReplicationStatus()
    const baselineDurableSequence = baselineStatus.lastDurableSequence

    await Promise.all(offlineFollowers.map((nodeId) => cluster.stopNode(nodeId)))

    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return (
          leader.currentLeader() === leaderId &&
          offlineFollowers.every((nodeId) => status.feeds[nodeId]?.alive === false)
        )
      },
      {
        description: "sole surviving node loses live followers",
        onTimeout: () => leader.getReplicationStatus()
      }
    )

    const current = await leader.get("hash:survive-read")
    assert.deepEqual(current?.value, { baseline: true })

    await assert.rejects(leader.put("hash:blocked-write", { shouldFail: true }), /Durability requirement not met/)
    await assert.rejects(leader.delete("hash:survive-read"), /Durability requirement not met/)

    const blockedStatus = await leader.getReplicationStatus()
    assert.equal(blockedStatus.lastDurableSequence, baselineDurableSequence)

    await cluster.restartNode(offlineFollowers[0])

    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return status.feeds[offlineFollowers[0]]?.alive === true && status.feeds[offlineFollowers[0]]?.connectedPeers > 0
      },
      {
        description: "durability recovers when a follower returns",
        onTimeout: () => leader.getReplicationStatus()
      }
    )

    const recoveryOperation = await leader.put("hash:recovered-write", { recovered: true })
    assert.equal(recoveryOperation.actor, leaderId)

    await waitFor(
      async () => {
        const follower = cluster.record(offlineFollowers[0]).node
        return (await follower.get("hash:recovered-write"))?.value?.recovered === true
      },
      {
        description: "restarted follower receives post-recovery write",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )
  } finally {
    await cluster.closeAll()
  }
})

test("pre-authorized standby node can join later and catch up without config changes", { concurrency: false }, async () => {
  const labels = ["leader", "follower-1", "follower-2", "standby"]
  const cluster = await createSwarmCluster({
    size: 4,
    identityLabels: labels,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000
  })

  try {
    const activeIds = cluster.identities.slice(0, 3).map((identity) => identity.publicKeyId)
    for (const nodeId of activeIds) {
      await cluster.startNode(nodeId)
    }

    await waitFor(
      async () => cluster.nodes.every((node) => node.status.knownHeartbeats.length >= 3),
      {
        description: "three active nodes converge before standby joins",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const leaderId = currentLeaderId({
      identities: cluster.identities.slice(0, 3)
    })
    const leader = cluster.record(leaderId).node
    await leader.put("hash:standby-before", { standby: "baseline" })

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:standby-before", { standby: "baseline" }),
      {
        description: "baseline replication before standby startup",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const standbyId = cluster.identities[3].publicKeyId
    const preJoinStatus = await leader.getReplicationStatus()
    assert.ok(preJoinStatus.feeds[standbyId])
    assert.equal(preJoinStatus.feeds[standbyId].alive, false)

    await cluster.startNode(standbyId)

    await waitFor(
      async () => {
        const joinedLeaderId = currentLeaderId(cluster)
        return cluster.nodes.every((node) => node.currentLeader() === joinedLeaderId)
      },
      {
        description: "cluster converges after standby startup",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    await waitFor(
      async () => {
        const standby = cluster.record(standbyId).node
        return (await standby.get("hash:standby-before"))?.value?.standby === "baseline"
      },
      {
        description: "standby catches up to pre-start writes",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    await leader.put("hash:standby-after", { standby: "joined" })

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:standby-after", { standby: "joined" }),
      {
        description: "all nodes converge after standby joins",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )
  } finally {
    await cluster.closeAll()
  }
})

test("planned node addition works after full-cluster restart with expanded membership", { concurrency: false }, async () => {
  const initialCluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000
  })

  let expandedCluster = null

  try {
    await initialCluster.startAll()

    await waitFor(
      async () => initialCluster.nodes.every((node) => node.status.knownHeartbeats.length >= 3),
      {
        description: "initial three-node convergence",
        onTimeout: () => collectReplicationStatus(initialCluster.nodes)
      }
    )

    const initialLeaderId = currentLeaderId(initialCluster)
    const initialLeader = initialCluster.record(initialLeaderId).node
    await initialLeader.put("hash:before-add", { phase: "before" })

    await waitFor(
      async () => hasClusterValue(initialCluster.nodes, "hash:before-add", { phase: "before" }),
      {
        description: "baseline replication before node addition",
        onTimeout: () => collectReplicationStatus(initialCluster.nodes)
      }
    )

    const expandedIdentities = createIdentities(4, ["leader", "follower-1", "follower-2", "added"])
    const expandedAuthorizedNodes = expandedIdentities.map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    await initialCluster.closeNodes()

    expandedCluster = await createSwarmCluster({
      identities: expandedIdentities,
      authorizedNodes: expandedAuthorizedNodes,
      dataDirs: initialCluster.records.map((record) => record.dataDir),
      clusterId: initialCluster.options.clusterId,
      topicSalt: initialCluster.options.topicSalt,
      encryptionKey: initialCluster.encryptionKey,
      heartbeatIntervalMs: initialCluster.options.heartbeatIntervalMs,
      heartbeatTtlMs: initialCluster.options.heartbeatTtlMs,
      bootstrap: initialCluster.testnet.bootstrap,
      identityLabels: ["leader", "follower-1", "follower-2", "added"]
    })

    await expandedCluster.startAll()

    await waitFor(
      async () => expandedCluster.nodes.every((node) => node.status.knownHeartbeats.length >= 4),
      {
        description: "expanded four-node convergence after restart",
        onTimeout: () => collectReplicationStatus(expandedCluster.nodes)
      }
    )

    await waitFor(
      async () => hasClusterValue(expandedCluster.nodes, "hash:before-add", { phase: "before" }),
      {
        description: "new node catches up to existing state",
        onTimeout: () => collectReplicationStatus(expandedCluster.nodes)
      }
    )

    const expandedLeaderId = currentLeaderId(expandedCluster)
    const expandedLeader = expandedCluster.record(expandedLeaderId).node
    await expandedLeader.put("hash:after-add", { phase: "after" })

    await waitFor(
      async () => hasClusterValue(expandedCluster.nodes, "hash:after-add", { phase: "after" }),
      {
        description: "post-add write converges to all four nodes",
        onTimeout: () => collectReplicationStatus(expandedCluster.nodes)
      }
    )

    for (const status of Object.values(await collectReplicationStatus(expandedCluster.nodes))) {
      assert.equal(Object.keys(status.feeds).length, 4)
    }
  } finally {
    if (expandedCluster) {
      await expandedCluster.closeNodes()
      await expandedCluster.destroyResources()
    }
    await initialCluster.destroyResources()
  }
})

test("node replacement via revocation and new identity restores service without hot membership changes", { concurrency: false }, async () => {
  const initialCluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000
  })

  let replacementCluster = null

  try {
    await initialCluster.startAll()

    await waitFor(
      async () => initialCluster.nodes.every((node) => node.status.knownHeartbeats.length >= 3),
      {
        description: "initial three-node convergence before replacement",
        onTimeout: () => collectReplicationStatus(initialCluster.nodes)
      }
    )

    const leaderId = currentLeaderId(initialCluster)
    const leader = initialCluster.record(leaderId).node
    await leader.put("hash:replacement-before", { replacement: "before" })

    await waitFor(
      async () => hasClusterValue(initialCluster.nodes, "hash:replacement-before", { replacement: "before" }),
      {
        description: "baseline replication before replacement",
        onTimeout: () => collectReplicationStatus(initialCluster.nodes)
      }
    )

    const retainedIdentities = initialCluster.identities.slice(0, 2)
    const retiredIdentity = initialCluster.identities[2]
    const replacementIdentity = createIdentities(1, ["replacement"])[0]
    const activeIdentities = [...retainedIdentities, replacementIdentity]
    const authorizedNodes = [...initialCluster.identities, replacementIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    await initialCluster.closeNodes()

    replacementCluster = await createSwarmCluster({
      identities: activeIdentities,
      authorizedNodes,
      dataDirs: initialCluster.records.slice(0, 2).map((record) => record.dataDir),
      clusterId: initialCluster.options.clusterId,
      topicSalt: initialCluster.options.topicSalt,
      encryptionKey: initialCluster.encryptionKey,
      heartbeatIntervalMs: initialCluster.options.heartbeatIntervalMs,
      heartbeatTtlMs: initialCluster.options.heartbeatTtlMs,
      testnet: initialCluster.testnet,
      revokedNodeIds: [retiredIdentity.publicKeyId],
      identityLabels: ["leader", "follower-1", "replacement"]
    })

    await replacementCluster.startAll()

    await waitFor(
      async () => replacementCluster.nodes.every((node) => node.status.knownHeartbeats.length >= 3),
      {
        description: "replacement cluster convergence",
        onTimeout: () => collectReplicationStatus(replacementCluster.nodes)
      }
    )

    await waitFor(
      async () => hasClusterValue(replacementCluster.nodes, "hash:replacement-before", { replacement: "before" }),
      {
        description: "replacement node catches up to prior state",
        onTimeout: () => collectReplicationStatus(replacementCluster.nodes)
      }
    )

    const replacementLeaderId = currentLeaderId(replacementCluster)
    const replacementLeader = replacementCluster.record(replacementLeaderId).node
    await replacementLeader.put("hash:replacement-after", { replacement: "after" })

    await waitFor(
      async () => hasClusterValue(replacementCluster.nodes, "hash:replacement-after", { replacement: "after" }),
      {
        description: "post-replacement write converges",
        onTimeout: () => collectReplicationStatus(replacementCluster.nodes)
      }
    )

    const writers = replacementLeader.getWritersStatus()
    assert.deepEqual(writers.revokedNodeIds, [retiredIdentity.publicKeyId])
    assert.equal(
      writers.authorizedNodes.find((node) => node.nodeId === retiredIdentity.publicKeyId)?.revoked,
      true
    )
  } finally {
    if (replacementCluster) {
      await replacementCluster.closeNodes()
      await replacementCluster.destroyResources()
    }
    await initialCluster.destroyResources()
  }
})

test("offline follower misses writes, then catches up with full history after restart", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000
  })

  try {
    await cluster.startAll()
    await waitForClusterConvergence(cluster)

    const leaderId = currentLeaderId(cluster)
    const leader = cluster.record(leaderId).node
    const offlineFollowerId = liveFollowerIds(cluster, leaderId)[1]

    await cluster.stopNode(offlineFollowerId)

    const firstWrite = await leader.put("hash:offline-a", { step: "put" })
    const secondWrite = await leader.put("hash:offline-b", { step: "stable" })
    const deleteWrite = await leader.delete("hash:offline-a")

    await waitFor(
      async () => {
        const liveNodes = cluster.nodes
        const deletedValues = await Promise.all(liveNodes.map((node) => node.get("hash:offline-a")))
        const stableValues = await Promise.all(liveNodes.map((node) => node.get("hash:offline-b")))
        return (
          deletedValues.every((value) => value?.deleted === true) &&
          stableValues.every((value) => value?.value?.step === "stable")
        )
      },
      {
        description: "live nodes converge while one follower is offline",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const statusWhileOffline = await leader.getReplicationStatus()
    assert.ok(statusWhileOffline.lastDurableSequence >= deleteWrite.seq)

    const restartedFollower = await cluster.restartNode(offlineFollowerId)
    await waitForClusterConvergence(cluster)

    await waitFor(
      async () => {
        const deletedValue = await restartedFollower.get("hash:offline-a")
        const stableValue = await restartedFollower.get("hash:offline-b")
        return deletedValue?.deleted === true && stableValue?.value?.step === "stable"
      },
      {
        description: "restarted follower catches up after offline writes",
        onTimeout: () => restartedFollower.getReplicationStatus()
      }
    )

    const deletedHistory = await restartedFollower.getHistory("hash:offline-a")
    assert.deepEqual(
      deletedHistory.map((entry) => entry.type),
      ["put", "delete"]
    )
    assert.equal(deletedHistory[0].opId, firstWrite.opId)
    assert.equal(deletedHistory[1].opId, deleteWrite.opId)

    const stableHistory = await restartedFollower.getHistory("hash:offline-b")
    assert.equal(stableHistory.length, 1)
    assert.equal(stableHistory[0].opId, secondWrite.opId)

    await assertClusterInvariants(cluster)
  } finally {
    await cluster.closeAll()
  }
})

test("offline leader yields failover writes and catches up cleanly after restart", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 900,
    durability: {
      requiredFollowerAcks: 1,
      timeoutMs: 750
    }
  })

  try {
    await cluster.startAll()
    await waitForClusterConvergence(cluster)

    const originalLeaderId = currentLeaderId(cluster)
    const originalLeader = cluster.record(originalLeaderId).node

    const beforeFailover = await originalLeader.put("hash:leader-before", { phase: "before" })
    assert.equal(beforeFailover.actor, originalLeaderId)

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:leader-before", { phase: "before" }),
      {
        description: "baseline write before leader outage",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    await cluster.stopNode(originalLeaderId)

    await waitFor(
      async () => {
        const current = currentLeaderNode(cluster)
        return current && current.options.identity.publicKeyId !== originalLeaderId
      },
      {
        description: "leader failover after original leader stops",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const failoverLeader = currentLeaderNode(cluster)
    const failoverLeaderId = failoverLeader.options.identity.publicKeyId
    assert.notEqual(failoverLeaderId, originalLeaderId)

    let duringFailover = null
    await waitFor(
      async () => {
        const currentFailoverLeader = cluster.record(failoverLeaderId).node
        if (currentFailoverLeader.currentLeader() !== failoverLeaderId) return false

        try {
          duringFailover = await currentFailoverLeader.put("hash:leader-during", { phase: "during" })
          return true
        } catch {
          return false
        }
      },
      {
        description: "failover leader accepts writes after original leader outage",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )
    assert.equal(duringFailover.actor, failoverLeaderId)

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:leader-during", { phase: "during" }),
      {
        description: "surviving nodes replicate failover write",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const restartedLeader = await cluster.restartNode(originalLeaderId)

    await waitForClusterConvergence(cluster)

    await waitFor(
      async () => {
        const before = await restartedLeader.get("hash:leader-before")
        const during = await restartedLeader.get("hash:leader-during")
        return before?.value?.phase === "before" && during?.value?.phase === "during"
      },
      {
        description: "restarted leader catches up to failover writes",
        onTimeout: () => restartedLeader.getReplicationStatus()
      }
    )

    const settledLeaderId = currentLeaderId(cluster)
    let afterRecovery = null
    await waitFor(
      async () => {
        const settledLeader = cluster.record(settledLeaderId).node
        if (settledLeader.currentLeader() !== settledLeaderId) return false

        try {
          afterRecovery = {
            ok: true,
            key: "hash:leader-after",
            operation: await settledLeader.put("hash:leader-after", { phase: "after" })
          }
          return true
        } catch {
          return false
        }
      },
      {
        description: "agreed leader accepts post-restart write",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:leader-after", { phase: "after" }),
      {
        description: "post-restart write converges to all nodes",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const beforeHistory = await restartedLeader.getHistory("hash:leader-before")
    const duringHistory = await restartedLeader.getHistory("hash:leader-during")
    const afterHistory = await restartedLeader.getHistory("hash:leader-after")

    assert.equal(beforeHistory.length, 1)
    assert.equal(beforeHistory[0].opId, beforeFailover.opId)
    assert.equal(duringHistory.length, 1)
    assert.equal(duringHistory[0].opId, duringFailover.opId)
    assert.equal(afterHistory.length, 1)
    assert.equal(afterHistory[0].opId, afterRecovery.operation.opId)

    await assertClusterInvariants(cluster)
  } finally {
    await cluster.closeAll()
  }
})

test("rolling restarts across a four-node cluster preserve availability and convergence", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 4,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000
  })

  try {
    await cluster.startAll()

    await waitFor(
      async () => cluster.nodes.every((node) => node.status.knownHeartbeats.length >= 4),
      {
        description: "initial four-node convergence",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const baselineLeaderId = currentLeaderId(cluster)
    const baselineLeader = cluster.record(baselineLeaderId).node
    await baselineLeader.put("hash:rolling-0", { cycle: 0 })

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:rolling-0", { cycle: 0 }),
      {
        description: "baseline write before rolling restarts",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const restartOrder = cluster.records.map((record) => record.identity.publicKeyId)

    for (const [index, nodeId] of restartOrder.entries()) {
      await cluster.restartNode(nodeId)

      await waitFor(
        async () => {
          const liveNodes = cluster.nodes
          if (liveNodes.some((node) => node.currentLeader() === null)) return false
          const liveLeader = liveNodes.find((node) => node.currentLeader() === node.options.identity.publicKeyId)
          if (!liveLeader) return false
          const liveLeaderId = liveLeader.options.identity.publicKeyId
          const status = await liveLeader.getReplicationStatus()
          return liveFollowerIds(cluster, liveLeaderId).some(
            (followerId) => cluster.record(followerId).node && status.feeds[followerId]?.alive === true
          )
        },
        {
          description: `cluster availability after restart ${index + 1}`,
          onTimeout: () => collectReplicationStatus(cluster.nodes)
        }
      )

      let writeResult = null
      await waitFor(
        async () => {
          const liveLeader = cluster.nodes.find((node) => node.currentLeader() === node.options.identity.publicKeyId)
          if (!liveLeader) return false

          try {
            writeResult = await liveLeader.put(`hash:rolling-${index + 1}`, { cycle: index + 1 })
            return true
          } catch {
            return false
          }
        },
        {
          description: `durable write after restart ${index + 1}`,
          onTimeout: () => collectReplicationStatus(cluster.nodes)
        }
      )
      assert.ok(writeResult)

      await waitFor(
        async () => hasClusterValue(cluster.nodes, `hash:rolling-${index + 1}`, { cycle: index + 1 }),
        {
          description: `write convergence after restart ${index + 1}`,
          onTimeout: () => collectReplicationStatus(cluster.nodes)
        }
      )
    }
  } finally {
    await cluster.closeAll()
  }
})

test("follower write forwarding fails while the old leader is unreachable and recovers after failover", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000,
    durability: {
      requiredFollowerAcks: 1,
      timeoutMs: 750
    }
  })

  try {
    await cluster.startAll()

    const leaderId = currentLeaderId(cluster)
    await waitFor(
      async () => cluster.nodes.every((node) => node.currentLeader() === leaderId),
      {
        description: "initial leader convergence before forwarding test",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const followerId = liveFollowerIds(cluster, leaderId)[0]
    const follower = cluster.record(followerId).node
    const targetKey = "hash:forwarding-unreachable"

    await cluster.stopNode(leaderId)

    await assert.rejects(
      follower.put(targetKey, { shouldFail: true }),
      /(Current leader .* is not reachable|Timed out forwarding write request|This node is not the current leader)/
    )

    await waitFor(
      async () => follower.currentLeader() !== leaderId && follower.currentLeader() !== null,
      {
        description: "failover after leader loss",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    assert.equal(await follower.get(targetKey), null)

    const recovered = await follower.put(targetKey, { recovered: true })
    assert.equal(recovered.actor, follower.currentLeader())

    await waitFor(
      async () => hasClusterValue(cluster.nodes, targetKey, { recovered: true }),
      {
        description: "post-failover forwarded write converges",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )
  } finally {
    await cluster.closeAll()
  }
})

test("concurrent writes across failover do not create duplicate accepted operations", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 900,
    durability: {
      requiredFollowerAcks: 1,
      timeoutMs: 750
    }
  })

  try {
    await cluster.startAll()

    const leaderId = currentLeaderId(cluster)
    await waitFor(
      async () => cluster.nodes.every((node) => node.currentLeader() === leaderId),
      {
        description: "initial leader convergence before concurrent failover writes",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const followerId = liveFollowerIds(cluster, leaderId)[0]
    const follower = cluster.record(followerId).node
    await cluster.stopNode(leaderId)

    const attempts = [
      { key: "hash:concurrent-1", delayMs: 0 },
      { key: "hash:concurrent-2", delayMs: 100 },
      { key: "hash:concurrent-3", delayMs: 1000 },
      { key: "hash:concurrent-4", delayMs: 1100 },
      { key: "hash:concurrent-5", delayMs: 1200 }
    ]

    const settled = await Promise.allSettled(
      attempts.map(async ({ key, delayMs }) => {
        await delay(delayMs)
        return {
          key,
          operation: await follower.put(key, { key })
        }
      })
    )

    await waitFor(
      async () => follower.currentLeader() !== leaderId && follower.currentLeader() !== null,
      {
        description: "failover completes during concurrent write test",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const succeeded = settled.flatMap((result, index) =>
      result.status === "fulfilled" ? [{ key: attempts[index].key, operation: result.value.operation }] : []
    )
    const failedKeys = settled.flatMap((result, index) =>
      result.status === "rejected" ? [attempts[index].key] : []
    )

    assert.ok(failedKeys.length > 0)

    const recovered = await follower.put("hash:concurrent-recovered", { recovered: true })
    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:concurrent-recovered", { recovered: true }),
      {
        description: "confirmed post-failover write after concurrent batch",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )
    assert.ok(recovered.opId)

    for (const { key, operation } of succeeded) {
      await waitFor(
        async () => hasClusterValue(cluster.nodes, key, { key }),
        {
          description: `successful failover write converges for ${key}`,
          onTimeout: () => collectReplicationStatus(cluster.nodes)
        }
      )

      const history = await follower.getHistory(key)
      assert.equal(history.length, 1)
      assert.equal(history[0].opId, operation.opId)
    }

    for (const key of failedKeys) {
      assert.equal(await follower.get(key), null)
    }
  } finally {
    await cluster.closeAll()
  }
})

test("HTTP writes fail while durability is unavailable and recover after a follower returns", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 800
  })
  const servers = []

  try {
    await cluster.startAll()

    await waitFor(
      async () => cluster.nodes.every((node) => node.status.knownHeartbeats.length >= 3),
      {
        description: "cluster convergence before HTTP durability test",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const leaderId = currentLeaderId(cluster)
    const leader = cluster.record(leaderId).node
    const offlineFollowers = liveFollowerIds(cluster, leaderId)

    await waitFor(
      async () => cluster.nodes.every((node) => node.currentLeader() === leaderId),
      {
        description: "leader convergence before HTTP durability test",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )

    const server = new HolepunchHttpServer({
      node: leader,
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

    await leader.put("hash:http-baseline", { baseline: true })

    await Promise.all(offlineFollowers.map((nodeId) => cluster.stopNode(nodeId)))

    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return offlineFollowers.every((nodeId) => status.feeds[nodeId]?.alive === false)
      },
      {
        description: "HTTP test waits for durability loss",
        onTimeout: () => leader.getReplicationStatus()
      }
    )

    const baseUrl = `http://${server.address.address}:${server.address.port}`
    const blockedPut = await fetch(`${baseUrl}/kv/hash:http-blocked?keyspace=default`, {
      method: "PUT",
      headers: {
        authorization: "Bearer writer",
        "content-type": "application/json"
      },
      body: JSON.stringify({ value: { blocked: true } })
    })
    assert.equal(blockedPut.status, 500)
    assert.match((await blockedPut.json()).error, /Durability requirement not met/)

    const blockedDelete = await fetch(`${baseUrl}/kv/hash:http-baseline?keyspace=default`, {
      method: "DELETE",
      headers: {
        authorization: "Bearer writer"
      }
    })
    assert.equal(blockedDelete.status, 500)
    assert.match((await blockedDelete.json()).error, /Durability requirement not met/)

    await cluster.restartNode(offlineFollowers[0])

    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return status.feeds[offlineFollowers[0]]?.alive === true && status.feeds[offlineFollowers[0]]?.connectedPeers > 0
      },
      {
        description: "HTTP durability recovers when follower returns",
        onTimeout: () => leader.getReplicationStatus()
      }
    )

    const recoveredPut = await fetch(`${baseUrl}/kv/hash:http-blocked?keyspace=default`, {
      method: "PUT",
      headers: {
        authorization: "Bearer writer",
        "content-type": "application/json"
      },
      body: JSON.stringify({ value: { blocked: false } })
    })
    assert.equal(recoveredPut.status, 200)

    await waitFor(
      async () => {
        const follower = cluster.record(offlineFollowers[0]).node
        return (await follower.get("hash:http-blocked"))?.value?.blocked === false
      },
      {
        description: "recovered HTTP write reaches restarted follower",
        onTimeout: () => collectReplicationStatus(cluster.nodes)
      }
    )
  } finally {
    await Promise.allSettled(servers.map((server) => server.close()))
    await cluster.closeAll()
  }
})

test("deterministic churn preserves convergence and write outcome invariants", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 4,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 900,
    durability: {
      requiredFollowerAcks: 1,
      timeoutMs: 750
    }
  })

  try {
    await cluster.startAll()
    await waitForClusterConvergence(cluster)

    const stepResults = []
    const steps = [
      {
        label: "baseline-write",
        run: async () => writeOnCurrentLeader(cluster, "hash:churn-0", { step: 0 })
      },
      {
        label: "stop-follower",
        run: async () => {
          const leaderId = currentLeaderId(cluster)
          await cluster.stopNode(liveFollowerIds(cluster, leaderId)[0])
          return { ok: true }
        }
      },
      {
        label: "degraded-write",
        run: async () => writeOnCurrentLeader(cluster, "hash:churn-1", { step: 1 })
      },
      {
        label: "restart-follower",
        run: async () => {
          const stopped = cluster.records.find((record) => !record.node)
          await cluster.restartNode(stopped.identity.publicKeyId)
          await waitForClusterConvergence(cluster)
          return { ok: true }
        }
      },
      {
        label: "stop-leader",
        run: async () => {
          const leaderId = currentLeaderId(cluster)
          await cluster.stopNode(leaderId)
          return { ok: true }
        }
      },
      {
        label: "transient-failover-write",
        run: async () => {
          const follower = cluster.nodes[0]
          try {
            const operation = await follower.put("hash:churn-2", { step: 2 })
            return { ok: true, key: "hash:churn-2", operation }
          } catch (error) {
            return { ok: false, key: "hash:churn-2", error: String(error) }
          }
        }
      },
      {
        label: "post-failover-write",
        run: async () => {
          let result = null
          await waitFor(
            async () => {
              if (!cluster.nodes.every((node) => node.currentLeader() !== null)) return false

              try {
                result = await writeOnCurrentLeader(cluster, "hash:churn-3", { step: 3 })
                return true
              } catch {
                return false
              }
            },
            {
              description: "post-failover durable write during churn",
              onTimeout: () => collectReplicationStatus(cluster.nodes)
            }
          )
          return result
        }
      },
      {
        label: "restart-old-leader",
        run: async () => {
          const stopped = cluster.records.find((record) => !record.node)
          await cluster.restartNode(stopped.identity.publicKeyId)
          await waitForClusterConvergence(cluster)
          return { ok: true }
        }
      },
      {
        label: "stop-two-followers",
        run: async () => {
          const leaderId = currentLeaderId(cluster)
          for (const followerId of liveFollowerIds(cluster, leaderId).slice(0, 2)) {
            await cluster.stopNode(followerId)
          }
          return { ok: true }
        }
      },
      {
        label: "durability-blocked-write",
        run: async () => {
          try {
            await writeOnCurrentLeader(cluster, "hash:churn-4", { step: 4 })
            return { ok: true, key: "hash:churn-4" }
          } catch (error) {
            return { ok: false, key: "hash:churn-4", error: String(error) }
          }
        }
      },
      {
        label: "restart-all",
        run: async () => {
          for (const stopped of cluster.records.filter((record) => !record.node)) {
            await cluster.restartNode(stopped.identity.publicKeyId)
          }
          await waitForClusterConvergence(cluster)
          return { ok: true }
        }
      }
    ]

    for (const step of steps) {
      stepResults.push({ label: step.label, ...(await step.run()) })
    }

    await waitForClusterConvergence(cluster)

    for (const result of stepResults.filter((entry) => entry.key && entry.ok && entry.operation)) {
      await waitFor(
        async () => hasClusterValue(cluster.nodes, result.key, { step: Number(result.key.slice(-1)) }),
        {
          description: `final convergence for ${result.key}`,
          onTimeout: () => collectReplicationStatus(cluster.nodes)
        }
      )
    }

    for (const result of stepResults.filter((entry) => entry.key && entry.ok === false)) {
      assert.equal(await cluster.nodes[0].get(result.key), null)
    }

    await assertClusterInvariants(cluster)
  } finally {
    await cluster.closeAll()
  }
})

test("full-cluster cold restart from persisted data directories rebuilds state and accepts new writes", { concurrency: false }, async () => {
  const initialCluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000
  })

  let restartedCluster = null

  try {
    await initialCluster.startAll()
    await waitForClusterConvergence(initialCluster)

    await writeOnCurrentLeader(initialCluster, "hash:cold-1", { cold: 1 })
    await writeOnCurrentLeader(initialCluster, "hash:cold-2", { cold: 2 })
    await currentLeaderNode(initialCluster).delete("hash:cold-2")

    await waitFor(
      async () => {
        const values = await Promise.all(initialCluster.nodes.map((node) => node.get("hash:cold-2")))
        return values.every((value) => value?.deleted === true)
      },
      {
        description: "pre-restart tombstone convergence",
        onTimeout: () => collectReplicationStatus(initialCluster.nodes)
      }
    )

    await initialCluster.closeNodes()
    await initialCluster.testnet.destroy()

    restartedCluster = await createSwarmCluster({
      identities: initialCluster.identities,
      authorizedNodes: initialCluster.authorizedNodes,
      dataDirs: initialCluster.records.map((record) => record.dataDir),
      clusterId: initialCluster.options.clusterId,
      topicSalt: initialCluster.options.topicSalt,
      encryptionKey: initialCluster.encryptionKey,
      heartbeatIntervalMs: initialCluster.options.heartbeatIntervalMs,
      heartbeatTtlMs: initialCluster.options.heartbeatTtlMs,
      identityLabels: initialCluster.records.map((record) => record.label)
    })

    await restartedCluster.startAll()
    await waitForClusterConvergence(restartedCluster)

    await waitFor(
      async () => hasClusterValue(restartedCluster.nodes, "hash:cold-1", { cold: 1 }),
      {
        description: "cold restart preserves live value",
        onTimeout: () => collectReplicationStatus(restartedCluster.nodes)
      }
    )
    await waitFor(
      async () => {
        const values = await Promise.all(restartedCluster.nodes.map((node) => node.get("hash:cold-2")))
        return values.every((value) => value?.deleted === true)
      },
      {
        description: "cold restart preserves tombstone",
        onTimeout: () => collectReplicationStatus(restartedCluster.nodes)
      }
    )

    const operation = await writeOnCurrentLeader(restartedCluster, "hash:cold-3", { cold: 3 })
    assert.ok(operation.ok)

    await waitFor(
      async () => hasClusterValue(restartedCluster.nodes, "hash:cold-3", { cold: 3 }),
      {
        description: "post-cold-restart write converges",
        onTimeout: () => collectReplicationStatus(restartedCluster.nodes)
      }
    )

    await assertClusterInvariants(restartedCluster)
  } finally {
    if (restartedCluster) {
      await restartedCluster.closeNodes()
      await restartedCluster.destroyResources()
    } else {
      await initialCluster.destroyResources()
    }
  }
})

function currentLeaderId(cluster) {
  return cluster.identities.map((identity) => identity.publicKeyId).sort()[0]
}

function liveFollowerIds(cluster, leaderId) {
  return cluster.records
    .map((record) => record.identity.publicKeyId)
    .filter((nodeId) => nodeId !== leaderId)
}

async function hasClusterValue(nodes, key, expected) {
  try {
    await assertClusterValue(nodes, key, expected)
    return true
  } catch {
    return false
  }
}

async function delay(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitForClusterConvergence(cluster) {
  await waitFor(
    async () => cluster.nodes.every((node) => node.status.knownHeartbeats.length >= cluster.nodes.length),
    {
      description: "cluster heartbeat convergence",
      onTimeout: () => collectReplicationStatus(cluster.nodes)
    }
  )
  await waitFor(
    async () => {
      const leaderId = currentLeaderId({ identities: cluster.nodes.map((node) => node.options.identity) })
      return cluster.nodes.every((node) => node.currentLeader() === leaderId)
    },
    {
      description: "cluster leader convergence",
      onTimeout: () => collectReplicationStatus(cluster.nodes)
    }
  )
}

function currentLeaderNode(cluster) {
  const elected = cluster.nodes.find((node) => node.currentLeader() === node.options.identity.publicKeyId)
  if (elected) return elected

  const leaderId = currentLeaderId({ identities: cluster.nodes.map((node) => node.options.identity) })
  return cluster.record(leaderId).node
}

async function writeOnCurrentLeader(cluster, key, value) {
  const leader = currentLeaderNode(cluster)
  const operation = await leader.put(key, value)
  return { ok: true, key, operation }
}

async function assertClusterInvariants(cluster) {
  const statuses = await collectReplicationStatus(cluster.nodes)

  for (const status of Object.values(statuses)) {
    for (const feed of Object.values(status.feeds)) {
      assert.ok(feed.applied <= feed.length)
      assert.ok(feed.lag >= 0)
    }
  }
}
