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
