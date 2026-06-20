const PEER_HINT_TTL_MS = 15 * 60 * 1000

/**
 * Persisted peer address book that maps node IDs to transport hints.
 *
 * Owns the in-memory Map and consensusBee persistence; the node itself
 * owns the refresh logic (which depends on cross-cutting leadership and
 * membership state).
 */
export class PeerHintManager {
  /**
   * @param {{
   *   consensusBee: import("hyperbee"),
   *   localNodeId: string
   * }} options
   */
  constructor(options) {
    this._consensusBee = options.consensusBee
    this._localNodeId = options.localNodeId
    this.hints = new Map()
  }

  async load() {
    const now = Date.now()
    for await (const entry of this._consensusBee.createReadStream({
      gt: "peer-hints/",
      lt: "peer-hints/~"
    })) {
      const hint = entry.value
      if (!hint?.nodeId) continue
      if (Number.isInteger(hint.expiresAt) && hint.expiresAt <= now) continue
      this.hints.set(hint.nodeId, hint)
    }
  }

  get(nodeId) {
    const hint = this.hints.get(nodeId) ?? null
    if (!hint) return null
    if (Number.isInteger(hint.expiresAt) && hint.expiresAt <= Date.now()) {
      this.hints.delete(nodeId)
      return null
    }
    return hint
  }

  getByMachineId(machineId) {
    if (typeof machineId !== "string" || machineId.length === 0) return null
    for (const hint of this.hints.values()) {
      const current = this.get(hint.nodeId)
      if (current?.machineId === machineId) return current
    }
    return null
  }

  async record(nodeId, patch) {
    const existing = this.get(nodeId)
    const now = Date.now()
    const nextHttpAddress = patch.httpAddress === undefined ? (existing?.httpAddress ?? null) : patch.httpAddress
    const nextPeerPublicKey = patch.peerPublicKey ?? existing?.peerPublicKey ?? null
    const nextMachineId = patch.machineId ?? existing?.machineId ?? null
    const nextRole = patch.role ?? existing?.role ?? null
    if (
      existing &&
      existing.peerPublicKey === nextPeerPublicKey &&
      existing.machineId === nextMachineId &&
      JSON.stringify(existing.httpAddress) === JSON.stringify(nextHttpAddress) &&
      existing.role === nextRole &&
      Number.isInteger(existing.expiresAt) &&
      existing.expiresAt > now + Math.floor(PEER_HINT_TTL_MS / 2)
    ) {
      return
    }
    const next = {
      nodeId,
      peerPublicKey: nextPeerPublicKey,
      machineId: nextMachineId,
      httpAddress: nextHttpAddress,
      role: nextRole,
      seenAt: new Date().toISOString(),
      lastSuccessfulAt: new Date(now).toISOString(),
      expiresAt: now + PEER_HINT_TTL_MS
    }
    this.hints.set(nodeId, next)
    await this._consensusBee.put(`peer-hints/${nodeId}`, next)
  }

  normalizeHttpAddress(address) {
    if (!address || typeof address !== "object") return null
    if (typeof address.address !== "string" || !Number.isInteger(address.port)) return null
    return {
      host: address.address,
      port: address.port
    }
  }
}
