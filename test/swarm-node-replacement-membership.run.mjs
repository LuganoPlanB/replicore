import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import assert from "node:assert/strict"
import { createHash, randomBytes } from "node:crypto"

import createTestnet from "hyperdht/testnet.js"

import {
  createPromotionCredential,
  generateIdentity,
  HolepunchSwarmNode
} from "../src/index.js"
import { waitFor } from "./helpers/eventual.js"

try {
  await withStep("replacement membership scenario", run, { timeoutMs: 90_000 })
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
      "6666666666666666666666666666666666666666666666666666666666666666",
      "hex"
    )
    const encryptionKey = randomBytes(32)
    const durability = {
      requiredFollowerAcks: 1,
      timeoutMs: 30_000
    }
    const leaderIdentity = generateIdentity(seed("replacement-membership-leader"))
    const followerIdentity = generateIdentity(seed("replacement-membership-follower"))
    const retiredIdentity = generateIdentity(seed("replacement-membership-retired"))
    const replacementIdentity = generateIdentity(seed("replacement-membership-new"))
    const initialAuthorizedNodes = [leaderIdentity, followerIdentity, retiredIdentity].map((identity) => ({
      nodeId: identity.publicKeyId,
      publicKey: identity.publicKey,
      feedKey: identity.feedKey
    }))

    const leaderNode = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "replacement-membership-cluster",
      clusterSecret,
      machineId: "replacement-membership-leader",
      identity: leaderIdentity,
      authorizedNodes: initialAuthorizedNodes,
      encryptionKey,
      durability,
      bootstrap: testnet.bootstrap
    })
    const followerNode = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "replacement-membership-cluster",
      clusterSecret,
      machineId: "replacement-membership-follower",
      identity: followerIdentity,
      authorizedNodes: initialAuthorizedNodes,
      encryptionKey,
      durability,
      bootstrap: testnet.bootstrap
    })
    const retiredNode = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "replacement-membership-cluster",
      clusterSecret,
      machineId: "replacement-membership-retired",
      identity: retiredIdentity,
      authorizedNodes: initialAuthorizedNodes,
      encryptionKey,
      durability,
      bootstrap: testnet.bootstrap
    })

    nodes.push(leaderNode, followerNode, retiredNode)
    await withStep("start initial voters", async () => {
      await leaderNode.start()
      await followerNode.start()
      await retiredNode.start()
    })

    const leaderId = [leaderIdentity.publicKeyId, followerIdentity.publicKeyId, retiredIdentity.publicKeyId]
      .sort()[0]
    const leader = nodes.find((node) => node.options.identity.publicKeyId === leaderId)
    const nonLeaderNodes = nodes
      .filter((node) => node.options.identity.publicKeyId !== leaderId)
      .sort((left, right) =>
        left.options.identity.publicKeyId.localeCompare(right.options.identity.publicKeyId)
      )
    const retainedFollower = nonLeaderNodes[0]
    const retiredTarget = nonLeaderNodes[1]
    const retiredTargetId = retiredTarget.options.identity.publicKeyId

    await withStep("elect initial leader", () =>
      waitFor(async () => nodes.every((node) => node.currentLeader() === leaderId))
    )
    await withStep("write before replacement", () =>
      leader.put("hash:replacement-membership-before", { value: "before-replacement" })
    )

    const removal = await withStep("remove retired voter", () => leader.removeVoter(retiredTargetId))
    assert.equal(removal.removed.some((entry) => entry.nodeId === retiredTargetId), true)

    await withStep("retired target observes removed role", () =>
      waitFor(async () => {
        const status = await retiredTarget.getReplicationStatus()
        return status.membership.localRole === "removed"
      })
    )

    const replacement = new HolepunchSwarmNode({
      dataDir: await tempDir(dirs),
      clusterId: "replacement-membership-cluster",
      clusterSecret,
      role: "learner",
      machineId: "replacement-membership-new",
      identity: replacementIdentity,
      authorizedNodes: [],
      encryptionKey,
      durability,
      bootstrap: testnet.bootstrap
    })
    await withStep("start replacement learner", () => replacement.start())
    nodes.push(replacement)

    await withStep("replacement catches up before promotion", () =>
      waitFor(async () => {
        const value = await replacement.get("hash:replacement-membership-before")
        return value?.value?.value === "before-replacement"
      })
    )
    const replacementPromotionWindow = nextCredentialWindow()

    const credential = createPromotionCredential({
      payload: {
        v: 1,
        type: "replicore.promotion",
        clusterId: "replacement-membership-cluster",
        membershipVersion: 1,
        learnerNodeId: replacementIdentity.publicKeyId,
        learnerNoisePublicKey: replacement.transportIdentity.publicKeyHex,
        targetRole: "voter",
        issuedAt: replacementPromotionWindow.issuedAt,
        expiresAt: replacementPromotionWindow.expiresAt,
        nonce: "replacement-membership-promotion",
        signerNodeId: leaderIdentity.publicKeyId
      },
      signerSecretKey: leaderIdentity.secretKey
    })

    await withStep("replacement stores promotion credential", () =>
      replacement.submitPromotionCredential(credential)
    )
    const committed = await withStep("leader commits promotion credential", () =>
      leader.commitPromotionCredential(credential)
    )
    assert.equal(committed.voters.some((entry) => entry.nodeId === replacementIdentity.publicKeyId), true)

    await withStep("replacement observes voter role", () =>
      waitFor(async () => {
        const status = await replacement.getReplicationStatus()
        return status.membership.localRole === "voter"
      })
    )

    await withStep("leader sees replacement peer", () =>
      waitFor(async () => {
        const status = await leader.getReplicationStatus()
        const replacementFeed = status.peerReplication[replacementIdentity.publicKeyId]
        return replacementFeed?.alive === true && replacementFeed?.connectedPeers > 0
      })
    )

    await withStep("leader and replacement see matching membership", () =>
      waitFor(async () => {
        const leaderStatus = await leader.getReplicationStatus()
        const replacementStatus = await replacement.getReplicationStatus()
        return (
          leaderStatus.membership.matchingNodeIds.includes(replacementIdentity.publicKeyId) &&
          replacementStatus.membership.matchingNodeIds.includes(leader.options.identity.publicKeyId)
        )
      })
    )

    await withStep("close retained follower", () => retainedFollower.close())
    nodes.splice(nodes.indexOf(retainedFollower), 1)

    await withStep("replacement remains connected after retained follower stops", () =>
      waitFor(async () => {
        const status = await leader.getReplicationStatus()
        const replacementFeed = status.peerReplication[replacementIdentity.publicKeyId]
        return status.connections === 2 && replacementFeed?.alive === true && replacementFeed?.connectedPeers > 0
      })
    )

    let postReplacementWriteError = null
    await withStep("replacement voter satisfies durability", () =>
      waitFor(async () => {
        try {
          await leader.put("hash:replacement-membership-after", { value: "after-replacement" })
          postReplacementWriteError = null
          return true
        } catch (error) {
          postReplacementWriteError = error
          if (
            /Timed out waiting for follower acknowledgement/.test(error?.message ?? "") ||
            /Durability requirement not met/.test(error?.message ?? "")
          ) {
            return false
          }
          throw error
        }
      }, {
        description: "replacement voter satisfies durability after retained follower stops",
        onTimeout: async () => ({
          lastError: postReplacementWriteError
            ? {
                message: postReplacementWriteError.message,
                stack: postReplacementWriteError.stack
              }
            : null,
          leader: await leader.getReplicationStatus(),
          replacement: await replacement.getReplicationStatus(),
          retired: await retiredTarget.getReplicationStatus()
        })
      }),
      { timeoutMs: 45_000 }
    )
    await withStep("replacement reads post-promotion write", () =>
      waitFor(async () => {
        const value = await replacement.get("hash:replacement-membership-after")
        return value?.value?.value === "after-replacement"
      })
    )

    await withStep("retired target rejects writes", () =>
      assert.rejects(
        retiredTarget.put("hash:replacement-membership-retired", { value: "forbidden" }),
        (error) => error?.code === "READ_ONLY_LEARNER"
      )
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

function nextCredentialWindow() {
  const issuedAt = new Date(Date.now() - 60_000)
  const expiresAt = new Date(Date.now() + 60 * 60 * 1000)
  return {
    issuedAt: issuedAt.toISOString(),
    expiresAt: expiresAt.toISOString()
  }
}

async function tempDir(dirs) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "holepunch-swarm-membership-"))
  dirs.push(dir)
  return dir
}
