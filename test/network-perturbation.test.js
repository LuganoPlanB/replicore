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

test("HTTP writes fail while durability is unavailable and recover after a follower returns", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 800
  })
  const servers = []

  try {
    await cluster.startAll()

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
