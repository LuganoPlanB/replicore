/**
 * Small local Raft core.
 *
 * We intentionally keep this logic in-repo instead of adding a general Raft
 * library. Replicore already owns transport, persistence, membership, and log
 * shape; a generic library here would either hide those boundaries or force
 * awkward adapters around Hypercore-specific state.
 */
export class ConsensusEngine {
  /**
   * @param {{ localNodeId: string, now?: () => number }} options
   */
  constructor({ localNodeId, now = () => Date.now() }) {
    this.localNodeId = localNodeId
    this.now = now
    this.state = {
      role: "follower",
      leaderNodeId: null,
      leaderHeartbeatAt: 0,
      electionDeadlineAt: 0
    }
  }

  get role() {
    return this.state.role
  }

  /**
   * @param {{ isLearner: boolean, heartbeatTtlMs: number }} options
   */
  currentLeader({ isLearner, heartbeatTtlMs }) {
    if (this.state.role === "leader") {
      return this.localNodeId
    }

    if (!this.state.leaderNodeId) {
      return null
    }

    const deadlineAt = isLearner
      ? this.state.leaderHeartbeatAt + heartbeatTtlMs
      : this.state.electionDeadlineAt
    if (this.now() > deadlineAt) {
      return null
    }

    return this.state.leaderNodeId
  }

  /**
   * @param {{ minMs: number, maxMs: number, voterNodeIds: string[] }} options
   */
  planElectionTimeout({ minMs, maxMs, voterNodeIds }) {
    const timeoutMs = nextElectionTimeoutMs({
      minMs,
      maxMs,
      voterNodeIds,
      localNodeId: this.localNodeId,
      leaderNodeId: this.state.leaderNodeId
    })

    this.state.electionDeadlineAt = this.now() + timeoutMs
    return {
      timeoutMs,
      deadlineAt: this.state.electionDeadlineAt
    }
  }

  /**
   * @param {{
   *   currentTerm: number,
   *   voterNodeIds: string[],
   *   lastLog: { index: number, term: number },
   *   membershipVersion: number
   * }} options
   */
  startElection({ currentTerm, voterNodeIds, lastLog, membershipVersion }) {
    const nextTerm = currentTerm + 1
    this.state.role = "candidate"
    this.state.leaderNodeId = null
    this.state.leaderHeartbeatAt = 0
    this.state.electionDeadlineAt = 0

    return {
      nextTerm,
      persistPatch: {
        currentTerm: nextTerm,
        votedFor: this.localNodeId
      },
      requiredVotes: majoritySize(voterNodeIds.length),
      voteRequest: {
        term: nextTerm,
        candidateNodeId: this.localNodeId,
        lastLogIndex: lastLog.index,
        lastLogTerm: lastLog.term,
        membershipVersion
      }
    }
  }

  /**
   * @param {{
   *   consensusState: { currentTerm: number, votedFor: string | null },
   *   voterNodeIds: string[],
   *   isLearner: boolean,
   *   localLog: { index: number, term: number },
   *   localMembershipVersion?: number,
   *   message: {
   *     term?: number,
   *     candidateNodeId?: string,
   *     lastLogIndex?: number,
   *     lastLogTerm?: number,
   *     membershipVersion?: number
   *   }
   * }} options
   */
  evaluateVoteRequest({ consensusState, voterNodeIds, isLearner, localLog, localMembershipVersion, message }) {
    if (isLearner) {
      return this.#voteRefusal(consensusState.currentTerm, "learner-node")
    }

    const term = Number.isInteger(message?.term) ? message.term : -1
    if (term < consensusState.currentTerm) {
      return this.#voteRefusal(consensusState.currentTerm, "stale-term")
    }

    let persistPatch = null
    let votedFor = consensusState.votedFor
    if (term > consensusState.currentTerm) {
      persistPatch = {
        currentTerm: term,
        votedFor: null
      }
      votedFor = null
      this.state.role = "follower"
      this.state.leaderNodeId = null
      this.state.leaderHeartbeatAt = 0
      this.state.electionDeadlineAt = 0
    }

    const candidateNodeId = message?.candidateNodeId
    if (typeof candidateNodeId !== "string" || !voterNodeIds.includes(candidateNodeId)) {
      return this.#voteRefusal(Math.max(term, consensusState.currentTerm), "candidate-not-voter", persistPatch)
    }

    if (
      Number.isInteger(message?.membershipVersion) &&
      Number.isInteger(localMembershipVersion) &&
      message.membershipVersion !== localMembershipVersion
    ) {
      return this.#voteRefusal(Math.max(term, consensusState.currentTerm), "membership-version-mismatch", persistPatch)
    }

    if (votedFor && votedFor !== candidateNodeId) {
      return this.#voteRefusal(Math.max(term, consensusState.currentTerm), "already-voted", persistPatch)
    }

    const candidateLog = {
      term: Number.isInteger(message?.lastLogTerm) ? message.lastLogTerm : -1,
      index: Number.isInteger(message?.lastLogIndex) ? message.lastLogIndex : -1
    }
    if (
      candidateLog.term < localLog.term ||
      (candidateLog.term === localLog.term && candidateLog.index < localLog.index)
    ) {
      return this.#voteRefusal(Math.max(term, consensusState.currentTerm), "stale-log", persistPatch)
    }

    this.state.role = "follower"
    this.state.leaderNodeId = null
    this.state.leaderHeartbeatAt = 0

    return {
      persistPatch: {
        ...(persistPatch ?? {}),
        currentTerm: Math.max(term, consensusState.currentTerm),
        votedFor: candidateNodeId
      },
      response: {
        term: Math.max(term, consensusState.currentTerm),
        voteGranted: true,
        leaderNodeId: this.currentLeader({ isLearner: false, heartbeatTtlMs: 0 }),
        refusalReason: null
      }
    }
  }

  /**
   * @param {{ term: number, currentTerm: number, electionTimeoutMaxMs: number }} options
   */
  becomeLeader({ term, currentTerm, electionTimeoutMaxMs }) {
    if (currentTerm !== term) {
      return { becameLeader: false, persistPatch: null }
    }

    const now = this.now()
    this.state.role = "leader"
    this.state.leaderNodeId = this.localNodeId
    this.state.leaderHeartbeatAt = now
    this.state.electionDeadlineAt = now + electionTimeoutMaxMs

    return {
      becameLeader: true,
      persistPatch: {
        currentTerm: term,
        votedFor: this.localNodeId
      }
    }
  }

  /**
   * @param {{ nextTerm: number, currentTerm: number, leaderNodeId?: string | null }} options
   */
  stepDown({ nextTerm, currentTerm, leaderNodeId = null }) {
    const term = Math.max(nextTerm, currentTerm)
    this.state.role = "follower"
    this.state.leaderNodeId = leaderNodeId
    this.state.leaderHeartbeatAt = leaderNodeId ? this.now() : 0
    this.state.electionDeadlineAt = 0

    return {
      persistPatch: {
        currentTerm: term,
        votedFor: null
      }
    }
  }

  /**
   * @param {{
   *   nodeId: string,
   *   voterNodeIds: string[],
   *   consensusState: { currentTerm: number, votedFor: string | null },
   *   localMembershipVersion?: number,
   *   operation: any,
   *   previousOperation?: any,
   *   electionTimeoutMaxMs: number
   * }} options
   */
  observeRemoteOperation({
    nodeId,
    voterNodeIds,
    consensusState,
    localMembershipVersion,
    operation,
    previousOperation,
    electionTimeoutMaxMs
  }) {
    if (nodeId === this.localNodeId) return { persistPatch: null, acceptedLeader: false }
    if (!voterNodeIds.includes(nodeId)) return { persistPatch: null, acceptedLeader: false }
    if (!operation || !Number.isInteger(operation.term)) return { persistPatch: null, acceptedLeader: false }

    let currentTerm = consensusState.currentTerm
    let votedFor = consensusState.votedFor
    let persistPatch = null
    if (operation.term > currentTerm) {
      const leaderHint = operation.kind === "heartbeat" && operation.heartbeat?.observedLeader === nodeId
        ? nodeId
        : null
      persistPatch = this.stepDown({
        nextTerm: operation.term,
        currentTerm,
        leaderNodeId: leaderHint
      }).persistPatch
      currentTerm = persistPatch.currentTerm
      votedFor = persistPatch.votedFor
    }

    let acceptedLeader = false
    const authority = this.#evaluateHeartbeatAuthority({
      nodeId,
      currentTerm,
      votedFor,
      localMembershipVersion,
      operation,
      previousOperation
    })
    if (authority.accepted) {
      const now = this.now()
      this.state.role = "follower"
      this.state.leaderNodeId = nodeId
      this.state.leaderHeartbeatAt = now
      this.state.electionDeadlineAt = now + electionTimeoutMaxMs
      acceptedLeader = true
    }

    return {
      persistPatch,
      acceptedLeader,
      refusalReason: authority.refusalReason
    }
  }

  /**
   * @param {{ leaderNodeId: string, electionTimeoutMs: number }} options
   */
  noteKnownLeader({ leaderNodeId, electionTimeoutMs }) {
    const now = this.now()
    this.state.role = "follower"
    this.state.leaderNodeId = leaderNodeId
    this.state.leaderHeartbeatAt = now
    this.state.electionDeadlineAt = now + electionTimeoutMs
  }

  /**
   * @param {number} term
   */
  isCandidateForTerm(term) {
    return this.state.role === "candidate" && Number.isInteger(term)
  }

  #voteRefusal(term, refusalReason, persistPatch = null) {
    return {
      persistPatch,
      response: {
        term,
        voteGranted: false,
        leaderNodeId: null,
        refusalReason
      }
    }
  }

  #evaluateHeartbeatAuthority({ nodeId, currentTerm, votedFor, localMembershipVersion, operation, previousOperation }) {
    if (operation.kind !== "heartbeat") {
      return { accepted: false, refusalReason: "not-heartbeat" }
    }

    const heartbeat = operation.heartbeat
    if (!heartbeat || typeof heartbeat !== "object") {
      return { accepted: false, refusalReason: "missing-heartbeat-metadata" }
    }
    if (operation.term < currentTerm) {
      return { accepted: false, refusalReason: "stale-term" }
    }
    if (heartbeat.leaderId !== nodeId || heartbeat.observedLeader !== nodeId) {
      return { accepted: false, refusalReason: "not-leader-heartbeat" }
    }
    if (this.state.leaderNodeId !== nodeId && votedFor !== nodeId) {
      return { accepted: false, refusalReason: "not-elected-leader" }
    }
    if (
      Number.isInteger(localMembershipVersion) &&
      Number.isInteger(heartbeat.membershipVersion) &&
      heartbeat.membershipVersion !== localMembershipVersion
    ) {
      return { accepted: false, refusalReason: "membership-version-mismatch" }
    }

    const expectedPrevIndex = previousOperation ? previousOperation.index : -1
    const expectedPrevTerm = previousOperation ? previousOperation.term : -1
    const expectedPrevHash = previousOperation ? previousOperation.entryHash : null
    if (heartbeat.prevLogIndex !== expectedPrevIndex) {
      return { accepted: false, refusalReason: "prev-index-mismatch" }
    }
    if (heartbeat.prevLogTerm !== expectedPrevTerm) {
      return { accepted: false, refusalReason: "prev-term-mismatch" }
    }
    if (heartbeat.prevLogHash !== expectedPrevHash) {
      return { accepted: false, refusalReason: "prev-hash-mismatch" }
    }

    return { accepted: true, refusalReason: null }
  }
}

/**
 * @param {{ minMs: number, maxMs: number, voterNodeIds: string[], localNodeId: string, leaderNodeId?: string | null }} options
 */
export function nextElectionTimeoutMs({ minMs, maxMs, voterNodeIds, localNodeId, leaderNodeId = null }) {
  if (maxMs <= minMs) return minMs

  const voters = [...voterNodeIds]
    .filter((nodeId) => nodeId !== leaderNodeId)
    .sort()
  const rank = Math.max(0, voters.indexOf(localNodeId))
  const spread = maxMs - minMs
  if (spread === 0) return minMs
  const slot = Math.max(1, Math.floor(spread / Math.max(voters.length, 1)))
  return Math.min(maxMs, minMs + (rank * slot))
}

/**
 * @param {number} size
 */
export function majoritySize(size) {
  return Math.floor(size / 2) + 1
}
