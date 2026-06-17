import assert from "node:assert/strict"
import test from "node:test"

import { assertClusterValue, collectReplicationStatus, waitFor } from "./helpers/eventual.js"
import { createSwarmCluster } from "./helpers/swarm-cluster.js"

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
    const survivingFollowerId = liveFollowerIds(cluster, leaderId).at(-1)

    await Promise.all(offlineFollowers.map((nodeId) => cluster.stopNode(nodeId)))

    const operation = await cluster.record(survivingFollowerId).node.put("hash:degraded-five", { degraded: true })
    assert.equal(operation.actor, leaderId)

    const liveNodes = cluster.nodes
    await waitFor(
      async () => hasClusterValue(liveNodes, "hash:degraded-five", { degraded: true }),
      {
        description: "live-node convergence while two followers are offline",
        onTimeout: () => collectReplicationStatus(liveNodes)
      }
    )

    const leaderStatus = await cluster.record(leaderId).node.getReplicationStatus()
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
