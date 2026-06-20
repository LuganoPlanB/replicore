/**
 * Structured refusal errors for client-facing HTTP and RPC responses.
 *
 * All error-creation logic lives here so that HolepunchSwarmNode
 * stays focused on coordination rather than error formatting.
 */
export class RefusalErrors {
  /**
   * @param {{
   *   consensusState: () => { currentTerm: number, commitIndex: number },
   *   membershipState: () => { current: { version: number } },
   *   splitState: () => { fenced: boolean, reason: string | null, leaderNodeId: string | null, since: string | null },
   *   joinState: () => { recovery?: { leaderHints?: { peerPublicKey?: string | null, httpAddress?: unknown } | null } | null },
   *   role: () => string,
   *   isLeaderReachable: (nodeId: string) => boolean,
   *   getPeerHint: (nodeId: string) => { peerPublicKey?: string | null, httpAddress?: unknown } | null,
   *   getEffectiveVoterNodeIds: () => string[],
   *   network: () => { peerPublicKeyForNodeId?: (nodeId: string) => string | null } | null,
   *   currentLeader: () => string | null,
   *   lastKnownLeaderId: () => string | null,
   *   localNodeId: string
   * }} context
   */
  constructor(context) {
    this.#context = context
  }

  createLearnerWriteError() {
    return this.createRefusalError({
      code: "read-only-learner",
      internalCode: "READ_ONLY_LEARNER",
      statusCode: 403,
      retryable: false,
      message: "This node is a read-only learner and cannot accept or proxy writes"
    })
  }

  createSplitFencedError() {
    return this.createRefusalError({
      code: "split-fenced",
      internalCode: "SPLIT_FENCED",
      statusCode: 503,
      retryable: true,
      message: "This node is split-fenced and is waiting to reconnect to the current leader",
      leaderNodeId: this.#context.lastKnownLeaderId()
    })
  }

  createLeaderUnavailableError(
    message = "No current leader is available",
    leaderNodeId = this.#context.currentLeader() ?? this.#context.lastKnownLeaderId()
  ) {
    return this.createRefusalError({
      code: "leader-unreachable",
      internalCode: "LEADER_UNREACHABLE",
      statusCode: 503,
      retryable: true,
      message,
      leaderNodeId
    })
  }

  createNotWitnessEntrypointError() {
    return this.createRefusalError({
      code: "not-witness-entrypoint",
      internalCode: "NOT_WITNESS_ENTRYPOINT",
      statusCode: 503,
      retryable: true,
      message: "Direct leader-facing CRUD is not supported; send writes to a witness node",
      leaderNodeId: this.#context.currentLeader() ?? this.#context.lastKnownLeaderId()
    })
  }

  createMembershipChangingError(message = "Durability requirement not met: membership mismatch blocks degraded writes") {
    return this.createRefusalError({
      code: "membership-changing",
      internalCode: "MEMBERSHIP_CHANGING",
      statusCode: 503,
      retryable: true,
      message
    })
  }

  createDurabilityUnavailableError(message = "Durability requirement not met: no reachable quorum available") {
    return this.createRefusalError({
      code: "leader-unreachable",
      internalCode: "DURABILITY_UNAVAILABLE",
      statusCode: 503,
      retryable: true,
      message
    })
  }

  normalizeDurabilityWaitError(error) {
    if (
      typeof error?.message === "string" &&
      error.message.startsWith("Timed out waiting for follower acknowledgement")
    ) {
      return this.createDurabilityUnavailableError()
    }
    return error
  }

  createJoinError(code, message, options = {}) {
    return this.createRefusalError({
      code,
      internalCode: options.internalCode ?? code.replace(/-/g, "_").toUpperCase(),
      statusCode: options.statusCode ?? 400,
      retryable: options.retryable ?? false,
      message,
      leaderNodeId: options.leaderNodeId ?? this.#context.currentLeader() ?? this.#context.lastKnownLeaderId()
    })
  }

  refusalReconnectHints(leaderNodeId) {
    const recoveryLeaderHints = this.#context.joinState().recovery?.leaderHints ?? null
    const persistedLeaderHint = leaderNodeId ? this.#context.getPeerHint(leaderNodeId) : null
    const leaderHint = leaderNodeId
      ? {
          nodeId: leaderNodeId,
          peerPublicKey: this.#context.network()?.peerPublicKeyForNodeId?.(leaderNodeId)
            ?? persistedLeaderHint?.peerPublicKey
            ?? recoveryLeaderHints?.peerPublicKey
            ?? null,
          httpAddress: persistedLeaderHint?.httpAddress ?? recoveryLeaderHints?.httpAddress ?? null
        }
      : null
    const witnesses = this.witnessHintEntries(leaderNodeId)

    return {
      leader: leaderHint,
      witnesses
    }
  }

  createRefusalError({ code, internalCode, statusCode, retryable, message, leaderNodeId = this.#context.currentLeader() ?? this.#context.lastKnownLeaderId() }) {
    const error = new Error(message)
    const refusal = {
      code,
      message,
      retryable,
      currentTerm: this.#context.consensusState().currentTerm,
      knownLeaderId: leaderNodeId ?? null,
      leaderReachable: leaderNodeId ? this.#context.isLeaderReachable(leaderNodeId) : false,
      splitStatus: { ...this.#context.splitState() },
      commitIndex: this.#context.consensusState().commitIndex,
      membershipVersion: this.#context.membershipState().current.version,
      role: this.#context.role(),
      reconnectHints: this.refusalReconnectHints(leaderNodeId)
    }
    error.code = internalCode
    error.statusCode = statusCode
    error.leader = leaderNodeId ?? null
    error.splitStatus = { ...this.#context.splitState() }
    error.refusal = refusal
    return error
  }

  #context

  witnessHintEntries(leaderNodeId) {
    return this.#context.getEffectiveVoterNodeIds()
      .filter((nodeId) => nodeId !== this.#context.localNodeId && nodeId !== leaderNodeId)
      .map((nodeId) => {
        const hint = this.#context.getPeerHint(nodeId)
        return {
          nodeId,
          peerPublicKey: this.#context.network()?.peerPublicKeyForNodeId?.(nodeId) ?? hint?.peerPublicKey ?? null,
          httpAddress: hint?.httpAddress ?? null,
          reachable: this.#context.isLeaderReachable(nodeId)
        }
      })
      .filter((hint) => hint.peerPublicKey || hint.httpAddress)
      .sort((left, right) => left.nodeId.localeCompare(right.nodeId))
  }
}
