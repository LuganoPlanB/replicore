const DEFAULT_CONSENSUS_STATE = Object.freeze({
  currentTerm: 0,
  votedFor: null,
  commitIndex: -1,
  lastApplied: -1,
  membershipVersion: 0,
  splitFenced: false,
  splitLeaderNodeId: null,
  splitReason: null
})

/**
 * Persist minimal consensus metadata needed for restart safety.
 */
export class ConsensusStateStore {
  /**
   * @param {import("hyperbee").default} bee
   */
  constructor(bee) {
    this.bee = bee
  }

  async load() {
    const state = {}

    for (const [key, fallback] of Object.entries(DEFAULT_CONSENSUS_STATE)) {
      const entry = await this.bee.get(`consensus/${key}`)
      state[key] = entry?.value ?? fallback
    }

    return state
  }

  /**
   * @param {Partial<typeof DEFAULT_CONSENSUS_STATE>} patch
   */
  async save(patch) {
    const current = await this.load()
    const next = { ...current, ...patch }
    const batch = this.bee.batch()

    for (const [key, value] of Object.entries(next)) {
      await batch.put(`consensus/${key}`, value)
    }
    await batch.flush()

    return next
  }
}
