/**
 * Build serializable node status payloads without embedding domain decisions.
 */
export function buildNodeStatus({
  nodeId,
  role,
  leader,
  knownHeartbeats,
  connections,
  encryptionKeyId,
  feeds
}) {
  return {
    nodeId,
    role,
    leader,
    knownHeartbeats,
    connections,
    encryptionKeyId,
    feeds
  }
}

export function buildReplicationStatus({
  nodeId,
  role,
  leader,
  consensus,
  leaderHealth,
  witnessHealth,
  quorum,
  authoritativeLog,
  peerReplication,
  splitStatus,
  connections,
  lastDurableSequence,
  encryptionKeyId,
  knownPeerNodeIds,
  membership,
  promotion,
  peerCache,
  antiEntropy,
  recentRefusal,
  reelection,
  network,
  readStatus,
  heartbeatByNode
}) {
  return {
    nodeId,
    role,
    leader,
    consensus,
    leaderHealth,
    witnessHealth,
    quorum,
    authoritativeLog,
    peerReplication,
    splitStatus,
    connections,
    lastDurableSequence,
    encryptionKeyId,
    knownPeerNodeIds,
    membership,
    promotion,
    peerCache,
    antiEntropy,
    recentRefusal,
    reelection,
    network,
    readStatus,
    heartbeatByNode
  }
}

export function buildWritersStatus({
  role,
  currentLeader,
  currentTerm,
  membershipVersion,
  revokedNodeIds,
  encryptionKeyId,
  membershipFingerprint,
  membership,
  quorum,
  peerCache,
  recentRefusal,
  authorizedNodes
}) {
  return {
    role,
    currentLeader,
    currentTerm,
    membershipVersion,
    revokedNodeIds,
    encryptionKeyId,
    membershipFingerprint,
    membership,
    quorum,
    peerCache,
    recentRefusal,
    authorizedNodes
  }
}

export function buildLeaderStatus({
  nodeId,
  role,
  currentLeader,
  reachable,
  heartbeat,
  currentTerm,
  membershipVersion,
  splitStatus,
  witnessHealth,
  reelection
}) {
  return {
    nodeId,
    role,
    currentLeader,
    reachable,
    heartbeat,
    currentTerm,
    membershipVersion,
    splitStatus,
    witnessHealth,
    reelection
  }
}
