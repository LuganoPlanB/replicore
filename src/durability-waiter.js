/**
 * Track follower acknowledgements for leader durability waits.
 */
export class DurabilityWaiter {
  /**
   * @param {{ feedKey: string, timeoutMs: number, requiredFollowerAcks: number }} options
   */
  constructor(options) {
    this.options = options
    this.waiters = new Map()
    this.lastDurableSequence = -1
  }

  /**
   * @param {number} seq
   * @param {number} required
   */
  async waitFor(seq, required) {
    return this.waitForGroups(seq, [
      {
        eligibleNodeIds: null,
        requiredCount: required
      }
    ])
  }

  /**
   * @param {number} seq
   * @param {Array<{ eligibleNodeIds: string[] | null, requiredCount: number }>} groups
   * @param {string[]} [initialNodeIds]
   */
  async waitForGroups(seq, groups, initialNodeIds = []) {
    const key = this.#waiterKey(seq)
    const existing = this.waiters.get(key) ?? {
      nodes: new Set(),
      groups: groups.map((group) => ({
        eligibleNodeIds: group.eligibleNodeIds ? new Set(group.eligibleNodeIds) : null,
        requiredCount: group.requiredCount
      })),
      resolve: null,
      reject: null,
      timer: null,
      promise: null,
      handledPromise: null
    }

    for (const nodeId of initialNodeIds) {
      existing.nodes.add(nodeId)
    }

    if (!existing.promise) {
      existing.promise = new Promise((resolve, reject) => {
        existing.resolve = resolve
        existing.reject = reject
        existing.timer = setTimeout(() => {
          this.waiters.delete(key)
          reject(new Error(`Timed out waiting for follower acknowledgement for sequence ${seq}`))
        }, this.options.timeoutMs)
      })
      existing.handledPromise = existing.promise.catch((error) => {
        throw error
      })
      this.waiters.set(key, existing)
    }

    if (this.#groupsSatisfied(existing)) {
      clearTimeout(existing.timer)
      this.waiters.delete(key)
      this.lastDurableSequence = Math.max(this.lastDurableSequence, seq)
      return
    }

    return existing.handledPromise
  }

  /**
   * @param {string} nodeId
   * @param {number} seq
   */
  record(nodeId, seq) {
    const waiter = this.waiters.get(this.#waiterKey(seq))
    if (!waiter) return

    waiter.nodes.add(nodeId)
    if (this.#groupsSatisfied(waiter)) {
      clearTimeout(waiter.timer)
      this.waiters.delete(this.#waiterKey(seq))
      this.lastDurableSequence = Math.max(this.lastDurableSequence, seq)
      waiter.resolve()
    }
  }

  rejectAll(error) {
    for (const waiter of this.waiters.values()) {
      clearTimeout(waiter.timer)
      waiter.reject(error)
    }
    this.waiters.clear()
  }

  status() {
    return { lastDurableSequence: this.lastDurableSequence }
  }

  /**
   * @param {number} seq
   */
  #waiterKey(seq) {
    return `${this.options.feedKey}:${seq}`
  }

  /**
   * @param {{ nodes: Set<string>, groups: Array<{ eligibleNodeIds: Set<string> | null, requiredCount: number }> }} waiter
   */
  #groupsSatisfied(waiter) {
    return waiter.groups.every((group) => {
      if (group.requiredCount <= 0) return true

      const matchedCount =
        group.eligibleNodeIds === null
          ? waiter.nodes.size
          : [...waiter.nodes].filter((nodeId) => group.eligibleNodeIds.has(nodeId)).length

      return matchedCount >= group.requiredCount
    })
  }
}
