/**
 * Local derived state over the replicated append-only log.
 */
export class MaterializedView {
  /**
   * @param {import("hyperbee").default} bee
   */
  constructor(bee) {
    this.bee = bee
  }

  /**
   * @param {Record<string, unknown>} operation
   * @param {string} feedKey
   */
  async apply(operation, feedKey) {
    if (operation.kind === "heartbeat") {
      await this.#applyHeartbeat(operation, feedKey)
      return
    }

    const batch = this.bee.batch()
    const keyspace = /** @type {string} */ (operation.keyspace)
    const key = /** @type {string} */ (operation.key)
    const currentKey = `kv/current/${keyspace}/${key}`
    const valueKey = `kv/value/${keyspace}/${key}`
    const historyKey = `kv/history/${keyspace}/${key}/${String(operation.seq).padStart(12, "0")}`
    const progressKey = `feeds/${feedKey}/progress`
    const summary = {
      opId: operation.opId,
      seq: operation.seq,
      ts: operation.ts,
      type: operation.type,
      keyspace,
      key,
      actor: operation.actor,
      expiresAt: operation.expiresAt
    }

    if (operation.type === "put") {
      await batch.put(currentKey, { ...summary, deleted: false })
      await batch.put(valueKey, operation.value)
    } else {
      await batch.put(currentKey, { ...summary, deleted: true })
      await batch.del(valueKey)
    }

    await batch.put(historyKey, summary)
    await batch.put(progressKey, { applied: operation.seq + 1, lastOpId: operation.opId })
    await batch.flush()
  }

  /**
   * @param {Record<string, unknown>} operation
   * @param {string} feedKey
   */
  async #applyHeartbeat(operation, feedKey) {
    const batch = this.bee.batch()
    await batch.put(`system/heartbeats/${operation.actor}`, {
      actor: operation.actor,
      feed: feedKey,
      ts: operation.ts,
      seq: operation.seq,
      observedLeader: operation.heartbeat?.observedLeader ?? null,
      reachableLeader: operation.heartbeat?.reachableLeader ?? false,
      appliedFeeds: operation.heartbeat?.appliedFeeds ?? {},
      membershipFingerprint: operation.heartbeat?.membershipFingerprint ?? null
    })
    await batch.put(`feeds/${feedKey}/progress`, { applied: operation.seq + 1, lastOpId: operation.opId })
    await batch.flush()
  }

  /**
   * @param {string} feedKey
   * @returns {Promise<number>}
   */
  async getApplied(feedKey) {
    const progress = await this.bee.get(`feeds/${feedKey}/progress`)
    return progress?.value?.applied ?? 0
  }

  /**
   * @param {string} keyspace
   * @param {string} key
   * @returns {Promise<null | { metadata: Record<string, unknown>, encryptedValue: unknown }>}
   */
  async getCurrent(keyspace, key) {
    const current = await this.bee.get(`kv/current/${keyspace}/${key}`)
    if (!current) return null

    const value = current.value.deleted
      ? null
      : (await this.bee.get(`kv/value/${keyspace}/${key}`))?.value ?? null

    return {
      metadata: current.value,
      encryptedValue: value
    }
  }

  /**
   * @param {string} keyspace
   * @param {string} key
   * @returns {Promise<Record<string, unknown>[]>}
   */
  async getHistory(keyspace, key) {
    const entries = []
    const range = this.bee.createReadStream({
      gte: `kv/history/${keyspace}/${key}/`,
      lt: `kv/history/${keyspace}/${key}/~`
    })

    for await (const entry of range) {
      entries.push(entry.value)
    }

    return entries
  }

  /**
   * @returns {Promise<Record<string, { actor: string, feed: string, ts: string, seq: number, observedLeader: string | null, reachableLeader: boolean, appliedFeeds: Record<string, number>, membershipFingerprint: string | null }>>}
   */
  async getHeartbeats() {
    const heartbeats = {}
    const range = this.bee.createReadStream({
      gte: "system/heartbeats/",
      lt: "system/heartbeats/~"
    })

    for await (const entry of range) {
      heartbeats[entry.value.actor] = entry.value
    }

    return heartbeats
  }

  async exportSnapshot() {
    return {
      version: 1,
      createdAt: new Date().toISOString(),
      entries: await this.#collectEntries()
    }
  }

  /**
   * @param {{ version: number, entries: Array<{ key: string, value: unknown }> }} snapshot
   */
  async importSnapshot(snapshot) {
    if (snapshot.version !== 1) {
      throw new Error(`Unsupported snapshot version: ${snapshot.version}`)
    }

    const batch = this.bee.batch()
    for (const entry of snapshot.entries) {
      await batch.put(entry.key, entry.value)
    }
    await batch.flush()
  }

  async #collectEntries() {
    const entries = []
    const stream = this.bee.createReadStream()

    for await (const entry of stream) {
      entries.push({
        key: entry.key,
        value: entry.value
      })
    }

    return entries
  }
}
