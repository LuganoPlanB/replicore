import assert from "node:assert/strict"
import test from "node:test"

import { ConsensusEngine } from "../src/raft-engine.js"

test("leader election emits a self-vote and deterministic timeout ranking", () => {
  let now = 1_000
  const engine = new ConsensusEngine({
    localNodeId: "node-b",
    now: () => now
  })

  const timeout = engine.planElectionTimeout({
    minMs: 100,
    maxMs: 160,
    voterNodeIds: ["node-a", "node-b", "node-c"]
  })
  assert.equal(timeout.timeoutMs, 120)
  assert.equal(timeout.deadlineAt, 1_120)

  const election = engine.startElection({
    currentTerm: 4,
    voterNodeIds: ["node-a", "node-b", "node-c"],
    lastLog: { index: 18, term: 4 },
    membershipVersion: 2
  })

  assert.equal(engine.role, "candidate")
  assert.equal(election.nextTerm, 5)
  assert.deepEqual(election.persistPatch, {
    currentTerm: 5,
    votedFor: "node-b"
  })
  assert.equal(election.requiredVotes, 2)
  assert.deepEqual(election.voteRequest, {
    term: 5,
    candidateNodeId: "node-b",
    lastLogIndex: 18,
    lastLogTerm: 4,
    membershipVersion: 2
  })
})

test("vote state survives restart and prevents a second vote in the same term", () => {
  const first = new ConsensusEngine({ localNodeId: "node-b", now: () => 10 })
  const granted = first.evaluateVoteRequest({
    consensusState: { currentTerm: 7, votedFor: null },
    voterNodeIds: ["node-a", "node-b", "node-c"],
    isLearner: false,
    localLog: { index: 12, term: 6 },
    localMembershipVersion: 1,
    message: {
      term: 7,
      candidateNodeId: "node-a",
      lastLogIndex: 12,
      lastLogTerm: 6,
      membershipVersion: 1
    }
  })
  assert.equal(granted.response.voteGranted, true)
  assert.deepEqual(granted.persistPatch, {
    currentTerm: 7,
    votedFor: "node-a"
  })

  const restarted = new ConsensusEngine({ localNodeId: "node-b", now: () => 11 })
  const denied = restarted.evaluateVoteRequest({
    consensusState: { currentTerm: 7, votedFor: "node-a" },
    voterNodeIds: ["node-a", "node-b", "node-c"],
    isLearner: false,
    localLog: { index: 12, term: 6 },
    localMembershipVersion: 1,
    message: {
      term: 7,
      candidateNodeId: "node-c",
      lastLogIndex: 12,
      lastLogTerm: 6,
      membershipVersion: 1
    }
  })
  assert.equal(denied.response.voteGranted, false)
  assert.equal(denied.response.refusalReason, "already-voted")
})

test("vote request rejects a stale term", () => {
  const engine = new ConsensusEngine({ localNodeId: "node-b", now: () => 10 })
  const denied = engine.evaluateVoteRequest({
    consensusState: { currentTerm: 9, votedFor: null },
    voterNodeIds: ["node-a", "node-b", "node-c"],
    isLearner: false,
    localLog: { index: 6, term: 9 },
    localMembershipVersion: 3,
    message: {
      term: 8,
      candidateNodeId: "node-a",
      lastLogIndex: 6,
      lastLogTerm: 9,
      membershipVersion: 3
    }
  })

  assert.equal(denied.response.voteGranted, false)
  assert.equal(denied.response.term, 9)
  assert.equal(denied.response.refusalReason, "stale-term")
})

test("vote request rejects a stale candidate log", () => {
  const engine = new ConsensusEngine({ localNodeId: "node-b", now: () => 10 })
  const denied = engine.evaluateVoteRequest({
    consensusState: { currentTerm: 4, votedFor: null },
    voterNodeIds: ["node-a", "node-b", "node-c"],
    isLearner: false,
    localLog: { index: 18, term: 4 },
    localMembershipVersion: 1,
    message: {
      term: 5,
      candidateNodeId: "node-a",
      lastLogIndex: 17,
      lastLogTerm: 4,
      membershipVersion: 1
    }
  })

  assert.equal(denied.response.voteGranted, false)
  assert.equal(denied.response.refusalReason, "stale-log")
  assert.deepEqual(denied.persistPatch, {
    currentTerm: 5,
    votedFor: null
  })
})

test("accepted heartbeat resets leader lease and exposes the current leader", () => {
  let now = 5_000
  const engine = new ConsensusEngine({
    localNodeId: "node-b",
    now: () => now
  })

  const observation = engine.observeRemoteOperation({
    nodeId: "node-a",
    voterNodeIds: ["node-a", "node-b", "node-c"],
    currentTerm: 6,
    electionTimeoutMaxMs: 900,
    operation: {
      kind: "heartbeat",
      term: 6,
      heartbeat: {
        observedLeader: "node-a"
      }
    }
  })

  assert.equal(observation.acceptedLeader, true)
  assert.equal(engine.currentLeader({ isLearner: false, heartbeatTtlMs: 100 }), "node-a")
  now += 901
  assert.equal(engine.currentLeader({ isLearner: false, heartbeatTtlMs: 100 }), null)
})

test("higher-term vote request steps down and grants an up-to-date voter", () => {
  const engine = new ConsensusEngine({ localNodeId: "node-b", now: () => 10 })
  const becameLeader = engine.becomeLeader({
    term: 4,
    currentTerm: 4,
    electionTimeoutMaxMs: 500
  })
  assert.equal(becameLeader.becameLeader, true)

  const granted = engine.evaluateVoteRequest({
    consensusState: { currentTerm: 4, votedFor: "node-b" },
    voterNodeIds: ["node-a", "node-b", "node-c"],
    isLearner: false,
    localLog: { index: 12, term: 4 },
    localMembershipVersion: 2,
    message: {
      term: 5,
      candidateNodeId: "node-a",
      lastLogIndex: 12,
      lastLogTerm: 4,
      membershipVersion: 2
    }
  })

  assert.equal(engine.role, "follower")
  assert.equal(granted.response.voteGranted, true)
  assert.deepEqual(granted.persistPatch, {
    currentTerm: 5,
    votedFor: "node-a"
  })
})

test("learner nodes and non-voters cannot receive votes", () => {
  const learnerEngine = new ConsensusEngine({ localNodeId: "node-b", now: () => 10 })
  const learnerDecision = learnerEngine.evaluateVoteRequest({
    consensusState: { currentTerm: 3, votedFor: null },
    voterNodeIds: ["node-a", "node-c"],
    isLearner: true,
    localLog: { index: 0, term: 0 },
    localMembershipVersion: 1,
    message: {
      term: 4,
      candidateNodeId: "node-a",
      lastLogIndex: 0,
      lastLogTerm: 0,
      membershipVersion: 1
    }
  })
  assert.equal(learnerDecision.response.refusalReason, "learner-node")

  const voterEngine = new ConsensusEngine({ localNodeId: "node-b", now: () => 10 })
  const removedDecision = voterEngine.evaluateVoteRequest({
    consensusState: { currentTerm: 3, votedFor: null },
    voterNodeIds: ["node-a", "node-b"],
    isLearner: false,
    localLog: { index: 0, term: 0 },
    localMembershipVersion: 1,
    message: {
      term: 4,
      candidateNodeId: "node-z",
      lastLogIndex: 0,
      lastLogTerm: 0,
      membershipVersion: 1
    }
  })
  assert.equal(removedDecision.response.refusalReason, "candidate-not-voter")
  assert.deepEqual(removedDecision.persistPatch, {
    currentTerm: 4,
    votedFor: null
  })
})

test("membership version mismatch rejects the vote but still records a higher term", () => {
  const engine = new ConsensusEngine({ localNodeId: "node-b", now: () => 10 })
  const denied = engine.evaluateVoteRequest({
    consensusState: { currentTerm: 6, votedFor: null },
    voterNodeIds: ["node-a", "node-b", "node-c"],
    isLearner: false,
    localLog: { index: 9, term: 6 },
    localMembershipVersion: 3,
    message: {
      term: 7,
      candidateNodeId: "node-a",
      lastLogIndex: 9,
      lastLogTerm: 6,
      membershipVersion: 2
    }
  })

  assert.equal(denied.response.voteGranted, false)
  assert.equal(denied.response.refusalReason, "membership-version-mismatch")
  assert.deepEqual(denied.persistPatch, {
    currentTerm: 7,
    votedFor: null
  })
})

test("wrapper-style write authority follows consensus output instead of heartbeat maps", () => {
  const engine = new ConsensusEngine({
    localNodeId: "node-b",
    now: () => 100
  })
  const staleHeartbeatMap = new Map([
    ["node-z", { ts: new Date(0).toISOString() }]
  ])

  engine.noteKnownLeader({
    leaderNodeId: "node-a",
    electionTimeoutMs: 300
  })

  const canAcceptWrites = ({ localNodeId, heartbeatMap }) => {
    assert.ok(heartbeatMap instanceof Map)
    return engine.currentLeader({ isLearner: false, heartbeatTtlMs: 50 }) === localNodeId
  }

  assert.equal(canAcceptWrites({ localNodeId: "node-a", heartbeatMap: staleHeartbeatMap }), true)
  assert.equal(canAcceptWrites({ localNodeId: "node-z", heartbeatMap: staleHeartbeatMap }), false)
})
