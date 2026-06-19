import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import assert from "node:assert/strict"
import { createHash, randomBytes } from "node:crypto"

import createTestnet from "hyperdht/testnet.js"

import {
  generateIdentity,
  HolepunchSwarmNode
} from "../src/index.js"
import { waitFor } from "./helpers/eventual.js"

try {
  await withStep("removed voter membership scenario", run, { timeoutMs: 75_000 })
  process.exit(0)
} catch (error) {
  console.error(error)
  process.exit(1)
}

async function run() {
  const testnet = await createTestnet(3)
  const dirs = []
  const nodes = []

  try {
    const clusterSecret = Buffer.from(
      "5555555555555555555555555555555555555555555555555555555555555555",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const leaderIdentity = generateIdentity(seed("removed-restart-leader"))
    const followerIdentity = generateIdentity(seed("removed-restart-follower"))
    const removedIdentity = generateIdentity(seed("removed-restart-target"))
    const authorizedNodes = [leaderIdentity, followerIdentity, removedIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const leaderNode = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "removed-restart-cluster",
      clusterSecret,
      machineId: "removed-restart-leader",
      identity: leaderIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    const followerNode = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "removed-restart-cluster",
      clusterSecret,
      machineId: "removed-restart-follower",
      identity: followerIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    const removedNode = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "removed-restart-cluster",
      clusterSecret,
      machineId: "removed-restart-target",
      identity: removedIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })

    nodes.push(leaderNode, followerNode, removedNode)
    await withStep("start initial voters", async () => {
      await leaderNode.start()
      await followerNode.start()
      await removedNode.start()
    })

    const leaderId = [leaderIdentity.publicKeyId, followerIdentity.publicKeyId, removedIdentity.publicKeyId]
      .sort()[0]
    const leader = nodes.find((node) => node.options.identity.publicKeyId === leaderId)
    const removalTargetId = removedIdentity.publicKeyId
    const removalTarget = nodes.find((node) => node.options.identity.publicKeyId === removalTargetId)
    const retainedFollower = nodes.find((node) =>
      node.options.identity.publicKeyId !== leaderId && node.options.identity.publicKeyId !== removalTargetId
    )
    assert.ok(removalTarget)
    assert.ok(retainedFollower)

    await withStep("elect initial leader", () =>
      waitFor(async () => nodes.every((node) => node.currentLeader() === leaderId))
    )
    await withStep("write before removal", () =>
      leader.put("hash:removed-before", { value: "before-removal" })
    )

    const removedMembership = await withStep("remove voter", () => leader.removeVoter(removalTargetId))
    assert.equal(removedMembership.removed.some((entry) => entry.nodeId === removalTargetId), true)
    await withStep("leader observes removed membership", () =>
      waitFor(async () => {
        const status = await leader.getReplicationStatus()
        return (
          status.membership.joint === null &&
          status.membership.current.removed.includes(removalTargetId)
        )
      })
    )

    await withStep("target observes removed role", () =>
      waitFor(async () => {
        const status = await removalTarget.getReplicationStatus()
        return status.membership.localRole === "removed"
      })
    )

    await withStep("removed target rejects writes", () =>
      assert.rejects(
        removalTarget.put("hash:removed-write", { value: "forbidden" }),
        (error) => error?.code === "READ_ONLY_LEARNER"
      )
    )

    await withStep("close retained follower", () => retainedFollower.close())
    nodes.splice(nodes.indexOf(retainedFollower), 1)

    await withStep("leader sees single connection", () =>
      waitFor(async () => {
        const status = await leader.getReplicationStatus()
        return status.connections === 1
      })
    )

    await withStep("leader blocks durability without retained voter", () =>
      assert.rejects(
        leader.put("hash:removed-durability", { value: "blocked" }),
        /Durability requirement not met/
      )
    )

    const removedDataDir = removalTarget.options.dataDir
    const removedMachineId = removalTarget.options.machineId
    await withStep("close removed target", () => removalTarget.close())
    nodes.splice(nodes.indexOf(removalTarget), 1)

    const restartedRemovedNode = new HolepunchSwarmNode({
      dataDir: removedDataDir,
      clusterId: "removed-restart-cluster",
      clusterSecret,
      machineId: removedMachineId,
      identity: removedIdentity,
      authorizedNodes,
      encryptionKey,
      bootstrap: testnet.bootstrap
    })
    await withStep("restart removed target", () => restartedRemovedNode.start())
    nodes.push(restartedRemovedNode)

    await withStep("leader keeps restarted target removed", () =>
      waitFor(async () => {
        const status = await leader.getReplicationStatus()
        return (
          status.membership.joint === null &&
          status.membership.current.removed.includes(removalTargetId) &&
          !status.membership.current.learners.includes(removalTargetId)
        )
      })
    )
    await withStep("restarted removed target rejects writes", () =>
      assert.rejects(
        restartedRemovedNode.put("hash:removed-restart-write", { value: "still-forbidden" }),
        (error) =>
          error?.code === "READ_ONLY_LEARNER" ||
          /No current leader is available/.test(error?.message ?? "") ||
          /Durability requirement not met/.test(error?.message ?? "")
      )
    )

    const leaderStatus = await leader.getReplicationStatus()
    assert.equal(
      leaderStatus.membership.removed.some((entry) => entry.nodeId === removalTargetId),
      true
    )
    assert.equal(
      leaderStatus.membership.learners.some((entry) => entry.nodeId === removalTargetId),
      false
    )
  } finally {
    await Promise.allSettled(nodes.map((node) => withStep("close node during cleanup", () => node.close(), { timeoutMs: 5_000 })))
    await Promise.allSettled(dirs.map((dir) => rm(dir, { recursive: true, force: true })))
    await withStep("destroy testnet", () => testnet.destroy(), { timeoutMs: 5_000 })
  }
}

async function withStep(name, fn, options = {}) {
  const timeoutMs = options.timeoutMs ?? 30_000
  const operation = Promise.resolve().then(fn)
  let timer = null
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`Timed out during ${name} after ${timeoutMs}ms`))
    }, timeoutMs)
    timer.unref?.()
  })

  try {
    return await Promise.race([operation, timeout])
  } finally {
    clearTimeout(timer)
  }
}

function seed(label) {
  return createHash("sha256").update(label).digest()
}

async function tempDir(dirs) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-swarm-membership-"))
  dirs.push(dir)
  return dir
}
