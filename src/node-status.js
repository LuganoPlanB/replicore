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
  authoritativeLog,
  peerReplication,
  splitStatus,
  connections,
  lastDurableSequence,
  encryptionKeyId,
  knownPeerNodeIds,
  membership,
  promotion,
  network,
  readStatus,
  heartbeatByNode
}) {
  return {
    nodeId,
    role,
    leader,
    consensus,
    authoritativeLog,
    peerReplication,
    splitStatus,
    connections,
    lastDurableSequence,
    encryptionKeyId,
    knownPeerNodeIds,
    membership,
    promotion,
    network,
    readStatus,
    heartbeatByNode
  }
}

export function buildWritersStatus({
  role,
  currentLeader,
  revokedNodeIds,
  encryptionKeyId,
  membershipFingerprint,
  membership,
  authorizedNodes
}) {
  return {
    role,
    currentLeader,
    revokedNodeIds,
    encryptionKeyId,
    membershipFingerprint,
    membership,
    authorizedNodes
  }
}

export function buildLeaderStatus({
  nodeId,
  role,
  currentLeader,
  reachable,
  heartbeat
}) {
  return {
    nodeId,
    role,
    currentLeader,
    reachable,
    heartbeat
  }
}
