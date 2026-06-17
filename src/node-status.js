/**
 * Build serializable node status payloads without embedding domain decisions.
 */
export function buildNodeStatus({
  nodeId,
  leader,
  knownHeartbeats,
  connections,
  encryptionKeyId,
  feeds
}) {
  return {
    nodeId,
    leader,
    knownHeartbeats,
    connections,
    encryptionKeyId,
    feeds
  }
}

export function buildReplicationStatus({
  nodeId,
  leader,
  connections,
  lastDurableSequence,
  encryptionKeyId,
  knownPeerNodeIds,
  membership,
  network,
  readStatus,
  feeds,
  heartbeats
}) {
  return {
    nodeId,
    leader,
    connections,
    lastDurableSequence,
    encryptionKeyId,
    knownPeerNodeIds,
    membership,
    network,
    readStatus,
    feeds,
    heartbeats
  }
}

export function buildWritersStatus({
  currentLeader,
  revokedNodeIds,
  encryptionKeyId,
  membershipFingerprint,
  authorizedNodes
}) {
  return {
    currentLeader,
    revokedNodeIds,
    encryptionKeyId,
    membershipFingerprint,
    authorizedNodes
  }
}

export function buildLeaderStatus({
  nodeId,
  currentLeader,
  reachable,
  heartbeat
}) {
  return {
    nodeId,
    currentLeader,
    reachable,
    heartbeat
  }
}
