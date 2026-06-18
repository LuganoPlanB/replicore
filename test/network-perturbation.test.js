import assert from "node:assert/strict"
import test from "node:test"

import { createPromotionCredential, HolepunchHttpServer, validateLogLink } from "../src/index.js"
import {
  assertClusterValue,
  collectClusterDiagnostics,
  collectReplicationStatus,
  waitFor,
  waitForNoChange
} from "./helpers/eventual.js"
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
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const leaderId = currentLeaderId(cluster)
    await waitFor(
      async () => cluster.nodes.every((node) => node.currentLeader() === leaderId),
      {
        description: "five-node leader convergence",
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return liveFollowerIds(cluster, leaderId).some(
          (nodeId) => status.feeds[nodeId]?.alive === true && status.feeds[nodeId]?.connectedPeers > 0
        )
      },
      {
        description: "five-node delete durability precondition",
        onTimeout: () => leader.getReplicationStatus()
      }
    )

    let deleteOperation = null
    await waitFor(
      async () => {
        try {
          deleteOperation = await leader.delete("hash:five-node")
          return true
        } catch {
          return false
        }
      },
      {
        description: "five-node durable delete",
        onTimeout: () => leader.getReplicationStatus()
      }
    )
    assert.ok(deleteOperation)

    await waitFor(
      async () => {
        const values = await Promise.all(cluster.nodes.map((node) => node.get("hash:five-node")))
        return values.every((value) => value?.deleted === true)
      },
      {
        description: "five-node delete convergence",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    for (const status of Object.values(await collectReplicationStatus(cluster.nodes))) {
      assert.equal(Object.keys(status.feeds).length, 5)
      assert.equal(Object.keys(status.heartbeats).length, 5)
      assert.equal(status.leader, leaderId)
    }

    const history = await leader.getHistory("hash:five-node")
    assert.ok(history.some((entry) => entry.opId === operation.opId && entry.type === "put"))
    assert.ok(history.some((entry) => entry.opId === deleteOperation.opId && entry.type === "delete"))
    assert.equal(history.at(-1)?.type, "delete")
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
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const leaderId = currentLeaderId(cluster)
    await waitFor(
      async () => cluster.nodes.every((node) => node.currentLeader() === leaderId),
      {
        description: "initial five-node leader convergence",
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster, liveNodes)
      }
    )

    const leaderStatus = await leader.getReplicationStatus()
    assert.ok(leaderStatus.lastDurableSequence >= operation.seq)

    await Promise.all(offlineFollowers.map((nodeId) => cluster.restartNode(nodeId)))

    await waitFor(
      async () => cluster.nodes.every((node) => node.status.knownHeartbeats.length >= cluster.identities.length),
      {
        description: "five-node reconvergence after follower restart",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:degraded-five", { degraded: true }),
      {
        description: "restarted follower catch-up",
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await waitFor(
      async () => {
        const standby = cluster.record(standbyId).node
        return (await standby.get("hash:standby-before"))?.value?.standby === "baseline"
      },
      {
        description: "standby catches up to pre-start writes",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await leader.put("hash:standby-after", { standby: "joined" })

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:standby-after", { standby: "joined" }),
      {
        description: "all nodes converge after standby joins",
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(initialCluster)
      }
    )

    const initialLeaderId = currentLeaderId(initialCluster)
    const initialLeader = initialCluster.record(initialLeaderId).node
    await initialLeader.put("hash:before-add", { phase: "before" })

    await waitFor(
      async () => hasClusterValue(initialCluster.nodes, "hash:before-add", { phase: "before" }),
      {
        description: "baseline replication before node addition",
        onTimeout: () => collectClusterDiagnostics(initialCluster)
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

    const expandedLeaderId = currentLeaderId(expandedCluster)
    await expandedCluster.startNode(expandedLeaderId)
    for (const identity of expandedCluster.identities) {
      if (identity.publicKeyId === expandedLeaderId) continue
      await expandedCluster.startNode(identity.publicKeyId)
    }

    await waitFor(
      async () => expandedCluster.nodes.every((node) => node.status.knownHeartbeats.length >= 4),
      {
        description: "expanded four-node convergence after restart",
        onTimeout: () => collectClusterDiagnostics(expandedCluster)
      }
    )

    await waitFor(
      async () => hasClusterValue(expandedCluster.nodes, "hash:before-add", { phase: "before" }),
      {
        description: "new node catches up to existing state",
        onTimeout: () => collectClusterDiagnostics(expandedCluster)
      }
    )

    const expandedLeader = expandedCluster.record(expandedLeaderId).node
    await expandedLeader.put("hash:after-add", { phase: "after" })

    await waitFor(
      async () => hasClusterValue(expandedCluster.nodes, "hash:after-add", { phase: "after" }),
      {
        description: "post-add write converges to all four nodes",
        onTimeout: () => collectClusterDiagnostics(expandedCluster)
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

test("joint-consensus learner promotion blocks when only one side of the joint quorum is available", { concurrency: false }, async () => {
  const identities = createIdentities(4, ["leader", "follower-1", "follower-2", "standby"])
  const learnerIdentity = identities[3]
  const cluster = await createSwarmCluster({
    identities,
    membership: {
      version: 0,
      voters: identities.slice(0, 3).map((identity) => identity.publicKeyId),
      learners: [learnerIdentity.publicKeyId],
      removed: []
    },
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000
  })

  try {
    await cluster.startAll()
    await waitForClusterConvergence(cluster)

    const leader = currentLeaderNode(cluster)
    const leaderId = leader.options.identity.publicKeyId
    const leaderIdentity = cluster.record(leaderId).identity

    await leader.put("hash:promotion-before", { phase: "before" })
    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:promotion-before", { phase: "before" }),
      {
        description: "learner catches up before promotion",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const learner = cluster.record(learnerIdentity.publicKeyId).node
    const credential = createPromotionCredential({
      payload: {
        v: 1,
        type: "replicore.promotion",
        clusterId: cluster.options.clusterId,
        membershipVersion: 0,
        learnerNodeId: learnerIdentity.publicKeyId,
        learnerNoisePublicKey: learner.transportIdentity.publicKeyHex,
        targetRole: "voter",
        issuedAt: "2026-06-18T12:00:00.000Z",
        expiresAt: "2026-06-18T13:00:00.000Z",
        nonce: "joint-promotion-blocked",
        signerNodeId: leaderIdentity.publicKeyId
      },
      signerSecretKey: leaderIdentity.secretKey
    })

    const oldFollowerIds = identities
      .slice(0, 3)
      .map((identity) => identity.publicKeyId)
      .filter((nodeId) => nodeId !== leaderId)
      .sort()
    await cluster.partitionGroups([
      [leaderId, oldFollowerIds[0]],
      [oldFollowerIds[1], learnerIdentity.publicKeyId]
    ])

    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return status.network.policyActive && status.connections === 1
      },
      {
        description: "promotion test partition settles",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await assert.rejects(
      leader.commitPromotionCredential(credential),
      /(Timed out waiting for follower acknowledgement|Durability requirement not met)/
    )

    const blockedStatus = await leader.getReplicationStatus()
    assert.equal(blockedStatus.membership.version, 0)
    assert.equal(blockedStatus.membership.joint, null)
    assert.equal(
      blockedStatus.membership.learners.some((entry) => entry.nodeId === learnerIdentity.publicKeyId),
      true
    )

    await cluster.healPartition()
    await waitForClusterConvergence(cluster)
    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return identities
          .slice(0, 3)
          .map((identity) => identity.publicKeyId)
          .filter((nodeId) => nodeId !== leaderId)
          .every((nodeId) => status.feeds[nodeId]?.connectedPeers > 0)
      },
      {
        description: "promotion leader regains enough live connectivity after heal",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const committed = await leader.commitPromotionCredential(credential)
    assert.equal(committed.version, 1)
    assert.equal(committed.joint, null)
    assert.equal(committed.voters.some((entry) => entry.nodeId === learnerIdentity.publicKeyId), true)

    await waitFor(
      async () => cluster.nodes.every((node) => node.getWritersStatus().membership.some((entry) => (
        entry.nodeId === learnerIdentity.publicKeyId && entry.role === "voter"
      ))),
      {
        description: "promotion final membership converges",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )
  } finally {
    await cluster.closeAll()
  }
})

test("joint-consensus voter removal blocks when only one side of the joint quorum is available", { concurrency: false }, async () => {
  const identities = createIdentities(4, ["leader", "follower-1", "follower-2", "follower-3"])
  const cluster = await createSwarmCluster({
    identities,
    membership: {
      version: 0,
      voters: identities.map((identity) => identity.publicKeyId),
      learners: [],
      removed: []
    },
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000
  })

  try {
    await cluster.startAll()
    await waitForClusterConvergence(cluster)

    const leader = currentLeaderNode(cluster)
    const leaderId = leader.options.identity.publicKeyId
    const removalTargetId = identities
      .map((identity) => identity.publicKeyId)
      .filter((nodeId) => nodeId !== leaderId)
      .sort()
      .at(-1)
    const retainedFollowerId = identities
      .map((identity) => identity.publicKeyId)
      .filter((nodeId) => nodeId !== leaderId && nodeId !== removalTargetId)
      .sort()[0]
    const isolatedCompanionId = identities
      .map((identity) => identity.publicKeyId)
      .filter((nodeId) => ![leaderId, retainedFollowerId, removalTargetId].includes(nodeId))
      .sort()[0]

    await cluster.partitionGroups([
      [leaderId, retainedFollowerId],
      [removalTargetId, isolatedCompanionId]
    ])

    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return status.network.policyActive && status.connections === 1
      },
      {
        description: "removal test partition settles",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await assert.rejects(
      leader.removeVoter(removalTargetId),
      /(Timed out waiting for follower acknowledgement|Durability requirement not met)/
    )

    const blockedStatus = await leader.getReplicationStatus()
    assert.equal(blockedStatus.membership.version, 0)
    assert.equal(blockedStatus.membership.joint, null)
    assert.equal(
      blockedStatus.membership.voters.some((entry) => entry.nodeId === removalTargetId),
      true
    )

    await cluster.healPartition()
    await waitForClusterConvergence(cluster)
    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return identities
          .map((identity) => identity.publicKeyId)
          .filter((nodeId) => nodeId !== leaderId)
          .every((nodeId) => status.feeds[nodeId]?.connectedPeers > 0)
      },
      {
        description: "removal leader regains enough live connectivity after heal",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const committed = await leader.removeVoter(removalTargetId)
    assert.equal(committed.version, 1)
    assert.equal(committed.joint, null)
    assert.equal(committed.removed.some((entry) => entry.nodeId === removalTargetId), true)
    assert.equal(committed.voters.some((entry) => entry.nodeId === removalTargetId), false)
  } finally {
    await cluster.closeAll()
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
        onTimeout: () => collectClusterDiagnostics(initialCluster)
      }
    )

    const leaderId = currentLeaderId(initialCluster)
    const leader = initialCluster.record(leaderId).node
    await leader.put("hash:replacement-before", { replacement: "before" })

    await waitFor(
      async () => hasClusterValue(initialCluster.nodes, "hash:replacement-before", { replacement: "before" }),
      {
        description: "baseline replication before replacement",
        onTimeout: () => collectClusterDiagnostics(initialCluster)
      }
    )

    const retainedFollowerIdentity = initialCluster.identities.find((identity) => identity.publicKeyId !== leaderId)
    assert.ok(retainedFollowerIdentity)
    const retainedIdentities = [
      initialCluster.record(leaderId).identity,
      retainedFollowerIdentity
    ]
    const retiredIdentity = initialCluster.identities.find(
      (identity) => !retainedIdentities.some((retained) => retained.publicKeyId === identity.publicKeyId)
    )
    assert.ok(retiredIdentity)
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
      dataDirs: retainedIdentities.map((identity) => initialCluster.record(identity.publicKeyId).dataDir),
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

    await waitForClusterConvergence(replacementCluster)

    await waitFor(
      async () => hasClusterValue(replacementCluster.nodes, "hash:replacement-before", { replacement: "before" }),
      {
        description: "replacement node catches up to prior state",
        onTimeout: () => collectClusterDiagnostics(replacementCluster)
      }
    )

    const replacementLeaderId = currentLeaderId(replacementCluster)
    const replacementLeader = replacementCluster.record(replacementLeaderId).node

    await waitFor(
      async () => {
        const status = await replacementLeader.getReplicationStatus()
        return liveFollowerIds(replacementCluster, replacementLeaderId).some(
          (nodeId) => status.feeds[nodeId]?.alive === true && status.feeds[nodeId]?.connectedPeers > 0
        )
      },
      {
        description: "replacement cluster regains durable follower reachability",
        onTimeout: () => collectClusterDiagnostics(replacementCluster)
      }
    )

    await waitForDurableClusterWrite(
      replacementCluster,
      "hash:replacement-after",
      { replacement: "after" },
      "post-replacement durable write"
    )

    await waitFor(
      async () => hasClusterValue(replacementCluster.nodes, "hash:replacement-after", { replacement: "after" }),
      {
        description: "post-replacement write converges",
        onTimeout: () => collectClusterDiagnostics(replacementCluster)
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

test("node replacement catches up a retained stale node after long absence", { concurrency: false }, async () => {
  const initialCluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 1000
  })

  let replacementCluster = null

  try {
    await initialCluster.startAll()
    await waitForClusterConvergence(initialCluster)

    const leaderId = currentLeaderId(initialCluster)
    const leader = initialCluster.record(leaderId).node
    const staleRetainedIdentity = initialCluster.identities.find((identity) => identity.publicKeyId !== leaderId)
    assert.ok(staleRetainedIdentity)
    const retainedIdentities = [
      initialCluster.record(leaderId).identity,
      staleRetainedIdentity
    ]
    const retiredIdentity = initialCluster.identities.find(
      (identity) => !retainedIdentities.some((retained) => retained.publicKeyId === identity.publicKeyId)
    )
    assert.ok(retiredIdentity)

    const baselineHistory = await leader.put("hash:replacement-history", { phase: "before-absence" })
    await leader.put("hash:replacement-delete", { phase: "before-delete" })

    await waitFor(
      async () =>
        hasClusterValue(initialCluster.nodes, "hash:replacement-history", { phase: "before-absence" }) &&
        hasClusterValue(initialCluster.nodes, "hash:replacement-delete", { phase: "before-delete" }),
      {
        description: "baseline replication before stale retained follower goes offline",
        onTimeout: () => collectClusterDiagnostics(initialCluster)
      }
    )

    await initialCluster.stopNode(staleRetainedIdentity.publicKeyId)

    const absentHistory = await leader.put("hash:replacement-history", { phase: "during-absence" })
    const absentDelete = await leader.delete("hash:replacement-delete")

    await waitFor(
      async () => {
        const liveNodes = initialCluster.nodes
        const historyValues = await Promise.all(liveNodes.map((node) => node.get("hash:replacement-history")))
        const deletedValues = await Promise.all(liveNodes.map((node) => node.get("hash:replacement-delete")))
        return (
          historyValues.every((value) => value?.value?.phase === "during-absence") &&
          deletedValues.every((value) => value?.deleted === true)
        )
      },
      {
        description: "live nodes converge while retained follower is absent",
        onTimeout: () => collectClusterDiagnostics(initialCluster)
      }
    )

    const replacementIdentity = createIdentities(1, ["replacement-stale"])[0]
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
      dataDirs: retainedIdentities.map((identity) => initialCluster.record(identity.publicKeyId).dataDir),
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
    await waitForClusterConvergence(replacementCluster)

    await waitFor(
      async () => {
        const values = await Promise.all(
          replacementCluster.nodes.map(async (node) => ({
            history: await node.get("hash:replacement-history"),
            deleted: await node.get("hash:replacement-delete")
          }))
        )
        return (
          values.every((value) => value.history?.value?.phase === "during-absence") &&
          values.every((value) => value.deleted?.deleted === true)
        )
      },
      {
        description: "replacement cluster converges after stale retained node rejoins",
        onTimeout: () => collectClusterDiagnostics(replacementCluster)
      }
    )

    const staleRetainedNode = replacementCluster.record(staleRetainedIdentity.publicKeyId).node
    const historyEntries = await staleRetainedNode.getHistory("hash:replacement-history")
    assert.deepEqual(
      historyEntries.map((entry) => entry.type),
      ["put", "put"]
    )
    assert.equal(historyEntries[0].opId, baselineHistory.opId)
    assert.equal(historyEntries[1].opId, absentHistory.opId)

    const deletedEntries = await staleRetainedNode.getHistory("hash:replacement-delete")
    assert.deepEqual(
      deletedEntries.map((entry) => entry.type),
      ["put", "delete"]
    )
    assert.equal(deletedEntries[1].opId, absentDelete.opId)

    await assertClusterInvariants(replacementCluster)
  } finally {
    if (replacementCluster) {
      await replacementCluster.closeNodes()
      await replacementCluster.destroyResources()
    }
    await initialCluster.destroyResources()
  }
})

test("mismatched membership config blocks degraded writes conservatively", { concurrency: false }, async () => {
  const identities = createIdentities(3, ["leader", "follower-1", "follower-2"])
  const authorizedNodes = identities.map((identity) => ({
    nodeId: identity.publicKeyId,
    publicKey: identity.publicKey,
    feedKey: identity.feedKey
  }))
  const sortedNodeIds = identities.map((identity) => identity.publicKeyId).sort()
  const leaderId = sortedNodeIds[0]
  const staleFollowerId = sortedNodeIds[1]
  const fullyConfiguredFollowerId = sortedNodeIds[2]

  const cluster = await createSwarmCluster({
    identities,
    authorizedNodes,
    authorizedNodesByNodeId: {
      [staleFollowerId]: authorizedNodes.filter((node) => node.nodeId !== fullyConfiguredFollowerId)
    },
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 900,
    durability: {
      requiredFollowerAcks: 1,
      timeoutMs: 750
    }
  })

  try {
    await cluster.startAll()

    const leader = cluster.record(leaderId).node
    const staleFollower = cluster.record(staleFollowerId).node
    const fullyConfiguredFollower = cluster.record(fullyConfiguredFollowerId).node

    await waitFor(
      async () => leader.currentLeader() === leaderId,
      {
        description: "baseline leader before mismatched rollout outage",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const staleWriters = staleFollower.getWritersStatus()
    assert.equal(staleWriters.authorizedNodes.length, 2)
    assert.notEqual(
      staleWriters.membershipFingerprint,
      leader.getWritersStatus().membershipFingerprint
    )

    await waitFor(
      async () => {
        const staleStatus = await staleFollower.getReplicationStatus()
        const fullStatus = await fullyConfiguredFollower.getReplicationStatus()
        return (
          staleStatus.membership.mismatchedNodeIds.includes(leaderId) &&
          fullStatus.membership.mismatchedNodeIds.includes(staleFollowerId)
        )
      },
      {
        description: "membership mismatch becomes observable before leader loss",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const staleStatusBeforeOutage = await staleFollower.getReplicationStatus()
    assert.equal(
      staleStatusBeforeOutage.membership.peerFingerprints[leaderId],
      leader.getWritersStatus().membershipFingerprint
    )
    assert.deepEqual(staleStatusBeforeOutage.membership.mismatchedNodeIds, [leaderId])

    await leader.put("hash:mismatch-before", { phase: "before" })
    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:mismatch-before", { phase: "before" }),
      {
        description: "baseline replication before mismatched leader loss",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await cluster.stopNode(leaderId)

    await waitFor(
      async () => staleFollower.currentLeader() === staleFollowerId,
      {
        description: "stale-config follower promotes itself after leader loss",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await assert.rejects(
      staleFollower.put("hash:mismatch-blocked", { blocked: true }),
      /(Durability requirement not met|Timed out waiting for follower acknowledgement|Timed out forwarding write request)/
    )

    assert.equal(await staleFollower.get("hash:mismatch-blocked"), null)

    await waitFor(
      async () => fullyConfiguredFollower.currentLeader() === staleFollowerId,
      {
        description: "fully configured follower still routes to stale-config leader",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const fullStatusDuringOutage = await fullyConfiguredFollower.getReplicationStatus()
    assert.deepEqual(fullStatusDuringOutage.membership.mismatchedNodeIds, [staleFollowerId])

    await assert.rejects(
      fullyConfiguredFollower.put("hash:mismatch-blocked-forwarded", { blocked: true }),
      /(Durability requirement not met|Timed out waiting for follower acknowledgement|Timed out forwarding write request)/
    )
    assert.equal(await fullyConfiguredFollower.get("hash:mismatch-blocked-forwarded"), null)

    const restartedLeader = await cluster.restartNode(leaderId)
    await waitFor(
      async () =>
        restartedLeader.currentLeader() === leaderId &&
        fullyConfiguredFollower.currentLeader() === leaderId,
      {
        description: "fully configured nodes recover stable leader after restart",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    assert.equal(staleFollower.getWritersStatus().authorizedNodes.length, 2)
    assert.deepEqual(
      (await restartedLeader.getReplicationStatus()).membership.mismatchedNodeIds,
      [staleFollowerId]
    )

    await waitFor(
      async () => {
        const status = await restartedLeader.getReplicationStatus()
        return (
          status.feeds[fullyConfiguredFollowerId]?.alive === true &&
          status.feeds[fullyConfiguredFollowerId]?.connectedPeers > 0
        )
      },
      {
        description: "fully configured follower becomes reachable after leader restart",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await restartedLeader.put("hash:mismatch-after", { phase: "after" })

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:mismatch-after", { phase: "after" }),
      {
        description: "writes recover after consistent leader returns",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await assertClusterInvariants(cluster)
  } finally {
    await cluster.closeAll()
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )
    assert.equal(duringFailover.actor, failoverLeaderId)

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:leader-during", { phase: "during" }),
      {
        description: "surviving nodes replicate failover write",
        onTimeout: () => collectClusterDiagnostics(cluster)
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
    await waitFor(
      async () => {
        const settledLeader = cluster.record(settledLeaderId).node
        if (settledLeader.currentLeader() !== settledLeaderId) return false

        const status = await settledLeader.getReplicationStatus()
        return liveFollowerIds(cluster, settledLeaderId).some(
          (nodeId) => status.feeds[nodeId]?.alive === true && status.feeds[nodeId]?.connectedPeers > 0
        )
      },
      {
        description: "agreed leader regains a durable follower before post-restart write",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const settledLeader = cluster.record(settledLeaderId).node
    let afterRecoveryAttempt = 0
    let afterRecovery = null
    await waitFor(
      async () => {
        const recoveryKey = `hash:leader-after-${++afterRecoveryAttempt}`
        try {
          afterRecovery = {
            key: recoveryKey,
            operation: await settledLeader.put(recoveryKey, { phase: "after" })
          }
          return true
        } catch {
          return false
        }
      },
      {
        description: "agreed leader accepts post-restart write",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await waitFor(
      async () => hasClusterValue(cluster.nodes, afterRecovery.key, { phase: "after" }),
      {
        description: "post-restart write converges to all nodes",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const beforeHistory = await restartedLeader.getHistory("hash:leader-before")
    const duringHistory = await restartedLeader.getHistory("hash:leader-during")
    const afterHistory = await restartedLeader.getHistory(afterRecovery.key)

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

test("subgroup partition exposes active policy and blocks cross-group links", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 900
  })

  try {
    await cluster.startAll()
    await waitForClusterConvergence(cluster)

    const isolatedNodeId = currentLeaderId(cluster)
    const connectedNodeIds = liveFollowerIds(cluster, isolatedNodeId)

    await cluster.partitionGroups([[isolatedNodeId], connectedNodeIds])

    await waitFor(
      async () => {
        const statuses = await collectReplicationStatus(cluster.nodes)
        const isolatedStatus = statuses[isolatedNodeId]
        return (
          isolatedStatus.connections === 0 &&
          connectedNodeIds.every(
            (nodeId) =>
              isolatedStatus.network.peers[nodeId]?.allowed === false &&
              isolatedStatus.network.peers[nodeId]?.connected === false
          ) &&
          connectedNodeIds.every(
            (nodeId) =>
              statuses[nodeId].network.peers[isolatedNodeId]?.allowed === false &&
              statuses[nodeId].network.peers[isolatedNodeId]?.connected === false
          )
        )
      },
      {
        description: "subgroup partition reports policy and blocks the isolated node",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const diagnostics = await collectClusterDiagnostics(cluster)
    assert.deepEqual(diagnostics.partitionGroups, [
      [isolatedNodeId],
      [...connectedNodeIds].sort()
    ])

    await cluster.healPartition()
    await waitForClusterConvergence(cluster)
    assert.ok(cluster.nodes.every((node) => node.currentLeader() === currentLeaderId(cluster)))
  } finally {
    await cluster.closeAll()
  }
})

test("isolated leader blocks minority writes while connected followers continue and heal cleanly", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 5,
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

    await waitFor(
      async () => {
        const status = await originalLeader.getReplicationStatus()
        return liveFollowerIds(cluster, originalLeaderId).some(
          (nodeId) => status.feeds[nodeId]?.alive === true && status.feeds[nodeId]?.connectedPeers > 0
        )
      },
      {
        description: "partition baseline durability precondition",
        onTimeout: () => originalLeader.getReplicationStatus()
      }
    )

    await originalLeader.put("hash:partition-before", { phase: "before" })
    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:partition-before", { phase: "before" }),
      {
        description: "baseline convergence before live isolation",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await cluster.isolateNode(originalLeaderId)

    await waitFor(
      async () => {
        const status = await originalLeader.getReplicationStatus()
        return status.connections === 0 && originalLeader.currentLeader() === originalLeaderId
      },
      {
        description: "isolated leader loses all live peers",
        onTimeout: () => originalLeader.getReplicationStatus()
      }
    )

    const connectedFollowers = cluster.nodes.filter(
      (node) => node.options.identity.publicKeyId !== originalLeaderId
    )
    const expectedConnectedLeaderId = connectedFollowers
      .map((node) => node.options.identity.publicKeyId)
      .sort()[0]

    await waitFor(
      async () => connectedFollowers.every((node) => node.currentLeader() === expectedConnectedLeaderId),
      {
        description: "connected side elects next live leader during isolation",
        onTimeout: () => collectClusterDiagnostics(cluster, connectedFollowers)
      }
    )

    const connectedLeader = cluster.record(expectedConnectedLeaderId).node
    await waitFor(
      async () => {
        const status = await connectedLeader.getReplicationStatus()
        return connectedFollowers
          .map((node) => node.options.identity.publicKeyId)
          .filter((nodeId) => nodeId !== expectedConnectedLeaderId)
          .some((nodeId) => status.feeds[nodeId]?.alive === true && status.feeds[nodeId]?.connectedPeers > 0)
      },
      {
        description: "connected side keeps a reachable follower during leader isolation",
        onTimeout: () => connectedLeader.getReplicationStatus()
      }
    )

    await assert.rejects(
      originalLeader.put("hash:partition-blocked", { blocked: true }),
      /(Durability requirement not met|Timed out waiting for follower acknowledgement)/
    )
    assert.equal(await originalLeader.get("hash:partition-blocked"), null)
    assert.deepEqual(await originalLeader.getHistory("hash:partition-blocked"), [])

    const duringIsolation = await connectedLeader.put("hash:partition-during", { phase: "during" })
    assert.equal(duringIsolation.actor, expectedConnectedLeaderId)

    await waitFor(
      async () => hasClusterValue(connectedFollowers, "hash:partition-during", { phase: "during" }),
      {
        description: "connected side continues durable writes during isolation",
        onTimeout: () => collectClusterDiagnostics(cluster, connectedFollowers)
      }
    )

    await cluster.healNode(originalLeaderId)
    await waitForClusterConvergence(cluster)
    assert.ok(cluster.nodes.every((node) => node.currentLeader() === originalLeaderId))

    await waitFor(
      async () => {
        const current = await originalLeader.get("hash:partition-during")
        return current?.value?.phase === "during"
      },
      {
        description: "healed leader catches up to connected-side write",
        onTimeout: () => originalLeader.getReplicationStatus()
      }
    )

    for (const node of cluster.nodes) {
      assert.equal(await node.get("hash:partition-blocked"), null)
    }

    await waitFor(
      async () => {
        const status = await originalLeader.getReplicationStatus()
        return liveFollowerIds(cluster, originalLeaderId).some(
          (nodeId) => status.feeds[nodeId]?.alive === true && status.feeds[nodeId]?.connectedPeers > 0
        )
      },
      {
        description: "old leader regains durable follower reachability after heal",
        onTimeout: () => originalLeader.getReplicationStatus()
      }
    )

    const afterHeal = await originalLeader.put("hash:partition-after", { phase: "after" })
    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:partition-after", { phase: "after" }),
      {
        description: "post-heal write converges to full cluster",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const isolatedHistory = await originalLeader.getHistory("hash:partition-during")
    assert.equal(isolatedHistory.length, 1)
    assert.equal(isolatedHistory[0].opId, duringIsolation.opId)
    await assertFeedChainValid(originalLeader, expectedConnectedLeaderId)
    assert.equal(afterHeal.actor, originalLeaderId)
    assert.ok(afterHeal.opId)

    await assertClusterInvariants(cluster)
  } finally {
    await cluster.closeAll()
  }
})

test(
  "subgroup partition blocks minority durability while majority continues and old leader heals cleanly",
  { concurrency: false },
  async () => {
    const cluster = await createSwarmCluster({
      size: 5,
      heartbeatIntervalMs: 100,
      heartbeatTtlMs: 900,
      durability: {
        requiredFollowerAcks: 2,
        timeoutMs: 750
      }
    })

    try {
      await cluster.startAll()
      await waitForClusterConvergence(cluster)

      const originalLeaderId = currentLeaderId(cluster)
      const originalLeader = cluster.record(originalLeaderId).node
      const minorityNodeIds = [originalLeaderId]
      const majorityNodeIds = cluster.records
        .map((record) => record.identity.publicKeyId)
        .filter((nodeId) => !minorityNodeIds.includes(nodeId))
      const majorityNodes = majorityNodeIds.map((nodeId) => cluster.record(nodeId).node)

      await waitForDurableWriteFrom(
        originalLeader,
        "hash:subgroup-before",
        { phase: "before" },
        "baseline durable write before subgroup partition"
      )
      await waitFor(
        async () => hasClusterValue(cluster.nodes, "hash:subgroup-before", { phase: "before" }),
        {
          description: "baseline convergence before subgroup partition",
          onTimeout: () => collectClusterDiagnostics(cluster)
        }
      )

      await cluster.partitionGroups([minorityNodeIds, majorityNodeIds])

      await waitFor(
        async () =>
          minorityNodeIds.every((nodeId) => cluster.record(nodeId).node.currentLeader() === originalLeaderId),
        {
          description: "minority subgroup keeps the original leader",
          onTimeout: () =>
            collectClusterDiagnostics(cluster, minorityNodeIds.map((nodeId) => cluster.record(nodeId).node))
        }
      )

      const majorityLeaderId = [...majorityNodeIds].sort()[0]
      await waitFor(
        async () => majorityNodes.every((node) => node.currentLeader() === majorityLeaderId),
        {
          description: "majority subgroup elects its local leader",
          onTimeout: () => collectClusterDiagnostics(cluster, majorityNodes)
        }
      )

      const majorityLeader = cluster.record(majorityLeaderId).node
      await waitFor(
        async () => {
          const status = await majorityLeader.getReplicationStatus()
          return majorityNodeIds
            .filter((nodeId) => nodeId !== majorityLeaderId)
            .some((nodeId) => status.network.peers[nodeId]?.connected === true)
        },
        {
          description: "majority subgroup keeps a reachable follower",
          onTimeout: () => majorityLeader.getReplicationStatus()
        }
      )

      await assert.rejects(
        originalLeader.put("hash:subgroup-blocked", { blocked: true }),
        /(Durability requirement not met|Timed out waiting for follower acknowledgement)/
      )

      const duringPartition = await majorityLeader.put("hash:subgroup-during", { phase: "during" })
      assert.equal(duringPartition.actor, majorityLeaderId)

      await waitFor(
        async () => hasClusterValue(majorityNodes, "hash:subgroup-during", { phase: "during" }),
        {
          description: "majority subgroup continues durable writes during partition",
          onTimeout: () => collectClusterDiagnostics(cluster, majorityNodes)
        }
      )

      assert.equal(await originalLeader.get("hash:subgroup-during"), null)

      await cluster.healPartition()
      await waitForClusterConvergence(cluster)
      assert.ok(cluster.nodes.every((node) => node.currentLeader() === originalLeaderId))

      await waitFor(
        async () => {
          const current = await originalLeader.get("hash:subgroup-during")
          return current?.value?.phase === "during"
        },
        {
          description: "old leader catches up after partition heal",
          onTimeout: () => originalLeader.getReplicationStatus()
        }
      )

      await waitFor(
        async () => {
          const status = await originalLeader.getReplicationStatus()
          return liveFollowerIds(cluster, originalLeaderId).some(
            (nodeId) => status.feeds[nodeId]?.alive === true && status.feeds[nodeId]?.connectedPeers > 0
          )
        },
        {
          description: "old leader regains durable follower reachability",
          onTimeout: () => originalLeader.getReplicationStatus()
        }
      )

      const afterHeal = await waitForDurableWriteFrom(
        originalLeader,
        "hash:subgroup-after",
        { phase: "after" },
        "post-heal durable write from the original leader"
      )
      assert.equal(afterHeal.actor, originalLeaderId)

      await waitFor(
        async () => hasClusterValue(cluster.nodes, "hash:subgroup-after", { phase: "after" }),
        {
          description: "post-heal write converges to the full cluster",
          onTimeout: () => collectClusterDiagnostics(cluster)
        }
      )

      for (const node of cluster.nodes) {
        assert.equal(await node.get("hash:subgroup-blocked"), null)
      }

      const history = await originalLeader.getHistory("hash:subgroup-during")
      assert.equal(history.length, 1)
      assert.equal(history[0].opId, duringPartition.opId)
      await assertFeedChainValid(originalLeader, majorityLeaderId)

      await assertClusterInvariants(cluster)
    } finally {
      await cluster.closeAll()
    }
  }
)

test("timed-out local append stays uncommitted across leader restart and later healthy writes", { concurrency: false }, async () => {
  const identities = createIdentities(3, ["timeout-leader", "timeout-follower-a", "timeout-follower-b"])
  const leaderId = identities.map((identity) => identity.publicKeyId).sort()[0]
  const ackDelayMsByNodeId = Object.fromEntries(
    identities
      .filter((identity) => identity.publicKeyId !== leaderId)
      .map((identity) => [identity.publicKeyId, 1000])
  )

  const cluster = await createSwarmCluster({
    identities,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 900,
    ackDelayMsByNodeId,
    durability: {
      requiredFollowerAcks: 1,
      timeoutMs: 300
    }
  })

  try {
    await cluster.startAll()
    await waitForClusterConvergence(cluster)

    const originalLeader = cluster.record(leaderId).node
    await waitFor(
      async () => cluster.nodes.every((node) => node.currentLeader() === leaderId),
      {
        description: "leader convergence before timed-out local append test",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await assert.rejects(
      originalLeader.put("hash:timed-out-local", { blocked: true }),
      /Timed out waiting for follower acknowledgement/
    )

    await waitFor(
      async () => (await originalLeader.getReplicationStatus()).feeds[leaderId].staged.count === 1,
      {
        description: "timed-out local append is retained only as staged state",
        onTimeout: () => originalLeader.getReplicationStatus()
      }
    )

    assert.equal(await originalLeader.get("hash:timed-out-local"), null)
    assert.deepEqual(await originalLeader.getHistory("hash:timed-out-local"), [])
    assert.ok((await originalLeader.createSnapshot()).entries.every((entry) => !String(entry.key).includes("/staged/")))

    const restartedLeader = await cluster.restartNode(leaderId)
    await waitFor(
      async () => (await restartedLeader.getReplicationStatus()).feeds[leaderId].staged.count === 1,
      {
        description: "staged timed-out append survives leader restart",
        onTimeout: () => restartedLeader.getReplicationStatus()
      }
    )

    assert.equal(await restartedLeader.get("hash:timed-out-local"), null)
    assert.deepEqual(await restartedLeader.getHistory("hash:timed-out-local"), [])

    cluster.options.ackDelayMsByNodeId = {}
    for (const nodeId of identities.map((identity) => identity.publicKeyId).filter((nodeId) => nodeId !== leaderId)) {
      await cluster.restartNode(nodeId)
    }

    await waitForClusterConvergence(cluster)
    await waitFor(
      async () => cluster.nodes.every((node) => node.currentLeader() === leaderId),
      {
        description: "cluster reconverges after removing artificial ack delay",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const recovered = await restartedLeader.put("hash:timed-out-recovered", { recovered: true })
    assert.equal(recovered.actor, leaderId)

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:timed-out-recovered", { recovered: true }),
      {
        description: "healthy durable write converges after timed-out append scenario",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    for (const node of cluster.nodes) {
      assert.equal(await node.get("hash:timed-out-local"), null)
      assert.deepEqual(await node.getHistory("hash:timed-out-local"), [])
    }

    await assertClusterInvariants(cluster)
  } finally {
    await cluster.closeAll()
  }
})

test("isolated follower serves stale reads until heal and status shows stale connectivity", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 900
  })

  try {
    await cluster.startAll()
    await waitForClusterConvergence(cluster)

    const leaderId = currentLeaderId(cluster)
    const leader = cluster.record(leaderId).node
    const isolatedFollowerId = liveFollowerIds(cluster, leaderId)[0]
    const isolatedFollower = cluster.record(isolatedFollowerId).node

    await leader.put("hash:stale-value", { phase: "before" })
    await leader.put("hash:stale-delete", { phase: "before-delete" })
    await waitFor(
      async () =>
        hasClusterValue(cluster.nodes, "hash:stale-value", { phase: "before" }) &&
        hasClusterValue(cluster.nodes, "hash:stale-delete", { phase: "before-delete" }),
      {
        description: "baseline replication before follower isolation",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await cluster.isolateNode(isolatedFollowerId)

    await waitFor(
      async () => {
        const status = await isolatedFollower.getReplicationStatus()
        return (
          status.connections === 0 &&
          status.feeds[leaderId]?.connectedPeers === 0 &&
          status.readStatus.staleReadsPossible === true &&
          status.readStatus.reason === "leader-unreachable"
        )
      },
      {
        description: "isolated follower loses live connectivity",
        onTimeout: () => isolatedFollower.getReplicationStatus()
      }
    )

    await leader.put("hash:stale-value", { phase: "after" })
    await leader.delete("hash:stale-delete")

    await waitFor(
      async () => {
        const liveNodes = cluster.nodes.filter((node) => node.options.identity.publicKeyId !== isolatedFollowerId)
        const current = await Promise.all([
          ...liveNodes.map((node) => node.get("hash:stale-value")),
          ...liveNodes.map((node) => node.get("hash:stale-delete"))
        ])
        return (
          current.slice(0, liveNodes.length).every((value) => value?.value?.phase === "after") &&
          current.slice(liveNodes.length).every((value) => value?.deleted === true)
        )
      },
      {
        description: "connected nodes converge while follower is stale",
        onTimeout: () => collectClusterDiagnostics(cluster, cluster.nodes.filter((node) => node.options.identity.publicKeyId !== isolatedFollowerId))
      }
    )

    const staleValue = await isolatedFollower.get("hash:stale-value")
    const staleDelete = await isolatedFollower.get("hash:stale-delete")
    assert.deepEqual(staleValue?.value, { phase: "before" })
    assert.deepEqual(staleDelete?.value, { phase: "before-delete" })

    await waitFor(
      async () => {
        const status = await isolatedFollower.getReplicationStatus()
        return (
          status.feeds[leaderId]?.alive === false &&
          status.readStatus.staleReadsPossible === true &&
          status.readStatus.reason === "no-live-peer-connections"
        )
      },
      {
        description: "isolated follower marks leader heartbeat stale after TTL",
        onTimeout: () => isolatedFollower.getReplicationStatus()
      }
    )

    await cluster.healNode(isolatedFollowerId)
    await waitForClusterConvergence(cluster)

    await waitFor(
      async () => {
        const healedValue = await isolatedFollower.get("hash:stale-value")
        const healedDelete = await isolatedFollower.get("hash:stale-delete")
        return healedValue?.value?.phase === "after" && healedDelete?.deleted === true
      },
      {
        description: "healed follower catches up after stale-read window",
        onTimeout: () => isolatedFollower.getReplicationStatus()
      }
    )

    await assertClusterInvariants(cluster)
  } finally {
    await cluster.closeAll()
  }
})

test("bootstrap outage after discovery does not break writes for already connected peers", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 900
  })

  try {
    await cluster.startAll()
    await waitForClusterConvergence(cluster)

    const leaderId = currentLeaderId(cluster)
    const leader = cluster.record(leaderId).node
    const baselineStatus = await leader.getReplicationStatus()
    const baselinePeerIds = [...baselineStatus.knownPeerNodeIds].sort()
    assert.ok(baselinePeerIds.length > 0)

    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return liveFollowerIds(cluster, leaderId).some(
          (nodeId) => status.feeds[nodeId]?.alive === true && status.feeds[nodeId]?.connectedPeers > 0
        )
      },
      {
        description: "bootstrap outage baseline durability precondition",
        onTimeout: () => leader.getReplicationStatus()
      }
    )

    await cluster.testnet.destroy()

    const operation = await leader.put("hash:bootstrap-outage", { outage: true })
    assert.equal(operation.actor, leaderId)

    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return (
          status.connections > 0 &&
          status.readStatus.staleReadsPossible === false &&
          [...status.knownPeerNodeIds].sort().join(",") === baselinePeerIds.join(",")
        )
      },
      {
        description: "connected peers remain attached after bootstrap outage",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:bootstrap-outage", { outage: true }),
      {
        description: "connected peers continue replicating after bootstrap outage",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await assertClusterInvariants(cluster)
  } finally {
    cluster.testnet = null
    await cluster.closeAll()
  }
})

test("restarted follower stays disconnected while bootstrap remains unavailable", { concurrency: false }, async () => {
  const cluster = await createSwarmCluster({
    size: 3,
    heartbeatIntervalMs: 100,
    heartbeatTtlMs: 900
  })

  try {
    await cluster.startAll()
    await waitForClusterConvergence(cluster)

    const leaderId = currentLeaderId(cluster)
    const leader = cluster.record(leaderId).node
    const followerIds = liveFollowerIds(cluster, leaderId)
    const restartingFollowerId = followerIds[1]
    const survivingFollowerId = followerIds[0]
    const baselineLeaderStatus = await leader.getReplicationStatus()
    assert.ok(baselineLeaderStatus.knownPeerNodeIds.length > 0)

    await leader.put("hash:bootstrap-restart-before", { phase: "before" })
    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:bootstrap-restart-before", { phase: "before" }),
      {
        description: "baseline replication before follower restart during bootstrap outage",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await cluster.testnet.destroy()
    cluster.testnet = null
    await cluster.stopNode(restartingFollowerId)

    await waitFor(
      async () => {
        const status = await leader.getReplicationStatus()
        return (
          status.feeds[survivingFollowerId]?.alive === true &&
          status.feeds[survivingFollowerId]?.connectedPeers > 0 &&
          status.feeds[restartingFollowerId]?.alive === false
        )
      },
      {
        description: "surviving follower remains durable after bootstrap loss and follower stop",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    await leader.put("hash:bootstrap-restart-during", { phase: "during" })
    await waitFor(
      async () =>
        hasClusterValue(
          cluster.nodes.filter((node) => node.options.identity.publicKeyId !== restartingFollowerId),
          "hash:bootstrap-restart-during",
          { phase: "during" }
        ),
      {
        description: "connected peers continue writing while bootstrap remains unavailable",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const restartedFollower = await cluster.restartNode(restartingFollowerId)

    await waitFor(
      async () => {
        const status = await restartedFollower.getReplicationStatus()
        return (
          status.connections === 0 &&
          status.knownPeerNodeIds.length === 0 &&
          status.feeds[leaderId]?.connectedPeers === 0 &&
          status.feeds[survivingFollowerId]?.connectedPeers === 0 &&
          status.readStatus.staleReadsPossible === true &&
          status.readStatus.reason === "no-live-peer-connections" &&
          restartedFollower.currentLeader() === restartingFollowerId
        )
      },
      {
        description: "restarted follower stays disconnected without bootstrap rediscovery",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const staleDuringValue = await waitForNoChange(
      async () => restartedFollower.get("hash:bootstrap-restart-during"),
      {
        stableMs: 500,
        timeoutMs: 3000,
        intervalMs: 100
      }
    )
    assert.equal(staleDuringValue, null)

    await leader.put("hash:bootstrap-restart-after", { phase: "after" })
    await waitFor(
      async () =>
        hasClusterValue(
          cluster.nodes.filter((node) => node.options.identity.publicKeyId !== restartingFollowerId),
          "hash:bootstrap-restart-after",
          { phase: "after" }
        ),
      {
        description: "connected peers still converge after follower restart without bootstrap",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const staleAfterValue = await waitForNoChange(
      async () => restartedFollower.get("hash:bootstrap-restart-after"),
      {
        stableMs: 500,
        timeoutMs: 3000,
        intervalMs: 100
      }
    )
    assert.equal(staleAfterValue, null)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const baselineLeaderId = currentLeaderId(cluster)
    const baselineLeader = cluster.record(baselineLeaderId).node
    await baselineLeader.put("hash:rolling-0", { cycle: 0 })

    await waitFor(
      async () => hasClusterValue(cluster.nodes, "hash:rolling-0", { cycle: 0 }),
      {
        description: "baseline write before rolling restarts",
        onTimeout: () => collectClusterDiagnostics(cluster)
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
          onTimeout: () => collectClusterDiagnostics(cluster)
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
          onTimeout: () => collectClusterDiagnostics(cluster)
        }
      )
      assert.ok(writeResult)

      await waitFor(
        async () => hasClusterValue(cluster.nodes, `hash:rolling-${index + 1}`, { cycle: index + 1 }),
        {
          description: `write convergence after restart ${index + 1}`,
          onTimeout: () => collectClusterDiagnostics(cluster)
        }
      )
    }
  } finally {
    await cluster.closeAll()
  }
})

test("follower write forwarding fails transiently near failover and recovers after leader TTL shifts", { concurrency: false }, async () => {
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    for (const node of cluster.nodes) {
      assert.equal(await node.get(targetKey), null)
      assert.deepEqual(await node.getHistory(targetKey), [])
    }

    const recovered = await follower.put(targetKey, { recovered: true })
    assert.equal(recovered.actor, follower.currentLeader())

    await waitFor(
      async () => hasClusterValue(cluster.nodes, targetKey, { recovered: true }),
      {
        description: "post-failover forwarded write converges",
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    for (const node of cluster.nodes) {
      const history = await node.getHistory(targetKey)
      assert.equal(history.length, 1)
      assert.equal(history[0].opId, recovered.opId)
    }
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )
    assert.ok(recovered.opId)

    for (const { key, operation } of succeeded) {
      await waitFor(
        async () => hasClusterValue(cluster.nodes, key, { key }),
        {
          description: `successful failover write converges for ${key}`,
          onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
      }
    )

    const leaderId = currentLeaderId(cluster)
    const leader = cluster.record(leaderId).node
    const offlineFollowers = liveFollowerIds(cluster, leaderId)

    await waitFor(
      async () => cluster.nodes.every((node) => node.currentLeader() === leaderId),
      {
        description: "leader convergence before HTTP durability test",
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        onTimeout: () => collectClusterDiagnostics(cluster)
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
        checkRecoveryAfter: true,
        run: async () => waitForDurableClusterWrite(cluster, "hash:churn-0", { step: 0 }, "baseline churn write")
      },
      {
        label: "stop-follower",
        checkRecoveryAfter: true,
        run: async () => {
          const leaderId = currentLeaderId(cluster)
          await cluster.stopNode(liveFollowerIds(cluster, leaderId)[0])
          return { ok: true }
        }
      },
      {
        label: "degraded-write",
        checkRecoveryAfter: true,
        run: async () => waitForDurableClusterWrite(cluster, "hash:churn-1", { step: 1 }, "degraded churn write")
      },
      {
        label: "restart-follower",
        checkRecoveryAfter: true,
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
        checkRecoveryAfter: true,
        run: async () => {
          let result = null
          result = await waitForDurableClusterWrite(
            cluster,
            "hash:churn-3",
            { step: 3 },
            "post-failover durable write during churn"
          )
          return result
        }
      },
      {
        label: "restart-old-leader",
        checkRecoveryAfter: true,
        run: async () => {
          const stopped = cluster.records.find((record) => !record.node)
          await cluster.restartNode(stopped.identity.publicKeyId)
          await waitForClusterConvergence(cluster)
          return { ok: true }
        }
      },
      {
        label: "stop-two-followers",
        checkRecoveryAfter: true,
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
        checkRecoveryAfter: true,
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
        checkRecoveryAfter: true,
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
      const result = { label: step.label, ...(await step.run()) }
      stepResults.push(result)

      if (step.checkRecoveryAfter) {
        await assertRecoveryWindowInvariants(cluster, step.label)
      }

      if (result.key && result.ok === false) {
        assert.equal(await cluster.nodes[0].get(result.key), null)
      }
    }

    await waitForClusterConvergence(cluster)

    for (const result of stepResults.filter((entry) => entry.key && entry.ok && entry.operation)) {
      await waitFor(
        async () => hasClusterValue(cluster.nodes, result.key, { step: Number(result.key.slice(-1)) }),
        {
          description: `final convergence for ${result.key}`,
          onTimeout: () => collectClusterDiagnostics(cluster)
        }
      )
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
        onTimeout: () => collectClusterDiagnostics(initialCluster)
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

    const restartedLeaderId = currentLeaderId(restartedCluster)
    await restartedCluster.startNode(restartedLeaderId)
    for (const identity of restartedCluster.identities) {
      if (identity.publicKeyId === restartedLeaderId) continue
      await restartedCluster.startNode(identity.publicKeyId)
    }
    await waitForClusterConvergence(restartedCluster)

    await waitFor(
      async () => hasClusterValue(restartedCluster.nodes, "hash:cold-1", { cold: 1 }),
      {
        description: "cold restart preserves live value",
        onTimeout: () => collectClusterDiagnostics(restartedCluster)
      }
    )
    await waitFor(
      async () => {
        const values = await Promise.all(restartedCluster.nodes.map((node) => node.get("hash:cold-2")))
        return values.every((value) => value?.deleted === true)
      },
      {
        description: "cold restart preserves tombstone",
        onTimeout: () => collectClusterDiagnostics(restartedCluster)
      }
    )

    const operation = await writeOnCurrentLeader(restartedCluster, "hash:cold-3", { cold: 3 })
    assert.ok(operation.ok)

    await waitFor(
      async () => hasClusterValue(restartedCluster.nodes, "hash:cold-3", { cold: 3 }),
      {
        description: "post-cold-restart write converges",
        onTimeout: () => collectClusterDiagnostics(restartedCluster)
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
    async () => {
      const statuses = await Promise.all(cluster.nodes.map((node) => node.getReplicationStatus()))
      return statuses.every((status) => Object.keys(status.heartbeats).length >= status.membership.voters.length)
    },
    {
      description: "cluster heartbeat convergence",
      onTimeout: () => collectClusterDiagnostics(cluster)
    }
  )
  await waitFor(
    async () => {
      const leaders = cluster.nodes.map((node) => node.currentLeader()).filter(Boolean)
      return leaders.length === cluster.nodes.length && new Set(leaders).size === 1
    },
    {
      description: "cluster leader convergence",
      onTimeout: () => collectClusterDiagnostics(cluster)
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

async function waitForDurableClusterWrite(cluster, key, value, description) {
  let result = null
  await waitFor(
    async () => {
      if (!cluster.nodes.every((node) => node.currentLeader() !== null)) return false

      try {
        result = await writeOnCurrentLeader(cluster, key, value)
        return true
      } catch {
        return false
      }
    },
    {
      description,
      onTimeout: () => collectClusterDiagnostics(cluster)
    }
  )
  return result
}

async function waitForDurableWriteFrom(node, key, value, description) {
  let operation = null
  await waitFor(
    async () => {
      try {
        operation = await node.put(key, value)
        return true
      } catch {
        return false
      }
    },
    {
      description,
      onTimeout: () => node.getReplicationStatus()
    }
  )
  return operation
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

async function assertFeedChainValid(node, sourceNodeId) {
  const core = node.feedCores.get(sourceNodeId)
  let previous = null

  for (let slot = 0; slot < core.length; slot += 1) {
    const operation = await core.get(slot)
    validateLogLink(operation, previous, slot)
    previous = operation
  }
}

async function assertRecoveryWindowInvariants(cluster, label) {
  await waitFor(
    async () => {
      if (cluster.nodes.length === 0) return false
      const leaders = cluster.nodes.map((node) => node.currentLeader()).filter(Boolean)
      return leaders.length === cluster.nodes.length && new Set(leaders).size === 1
    },
    {
      description: `leader agreement after ${label}`,
      onTimeout: () => collectClusterDiagnostics(cluster)
    }
  )

  const statuses = await collectReplicationStatus(cluster.nodes)
  const agreedLeader = cluster.nodes[0].currentLeader()
  assert.ok(agreedLeader, `expected a leader after ${label}`)

  for (const status of Object.values(statuses)) {
    assert.equal(status.leader, agreedLeader, `leader mismatch after ${label}`)
    assert.deepEqual(status.membership.mismatchedNodeIds, [], `membership mismatch after ${label}`)

    for (const feed of Object.values(status.feeds)) {
      assert.ok(feed.applied <= feed.length, `applied exceeds length after ${label}`)
      assert.ok(feed.lag >= 0, `negative lag after ${label}`)
    }
  }
}
