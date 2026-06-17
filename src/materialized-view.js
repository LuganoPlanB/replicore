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
    await batch.put(
      this.#progressKey(feedKey),
      this.#nextProgressRecord({
        feedKey,
        rawApplied: operation.seq + 1,
        rawLastOpId: operation.opId,
        committedApplied: operation.seq + 1,
        committedLastOpId: operation.opId
      })
    )
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
    await batch.put(
      this.#progressKey(feedKey),
      this.#nextProgressRecord({
        feedKey,
        rawApplied: operation.seq + 1,
        rawLastOpId: operation.opId,
        committedApplied: operation.seq + 1,
        committedLastOpId: operation.opId
      })
    )
    await batch.flush()
  }

  /**
   * @param {string} feedKey
   * @returns {Promise<number>}
   */
  async getApplied(feedKey) {
    return (await this.getFeedProgress(feedKey)).committedApplied
  }

  /**
   * Committed K/V state must read only from committed progress.
   * Raw progress may move ahead once staging is introduced.
   *
   * @param {string} feedKey
   */
  async getRawApplied(feedKey) {
    return (await this.getFeedProgress(feedKey)).rawApplied
  }

  /**
   * Read both feed cursors with backward-compatible fallback from the legacy
   * single `applied` cursor used before committed progress was split out.
   *
   * @param {string} feedKey
   */
  async getFeedProgress(feedKey) {
    const progress = await this.bee.get(this.#progressKey(feedKey))
    return this.#normalizeProgressRecord(progress?.value)
  }

  /**
   * @param {string} feedKey
   * @param {{ applied: number, lastOpId?: string | null }} progress
   */
  async setRawProgress(feedKey, progress) {
    const current = await this.getFeedProgress(feedKey)
    await this.bee.put(
      this.#progressKey(feedKey),
      this.#nextProgressRecord({
        feedKey,
        rawApplied: progress.applied,
        rawLastOpId: progress.lastOpId ?? null,
        committedApplied: current.committedApplied,
        committedLastOpId: current.committedLastOpId
      })
    )
  }

  /**
   * @param {string} feedKey
   * @param {{ applied: number, lastOpId?: string | null }} progress
   */
  async setCommittedProgress(feedKey, progress) {
    const current = await this.getFeedProgress(feedKey)
    await this.bee.put(
      this.#progressKey(feedKey),
      this.#nextProgressRecord({
        feedKey,
        rawApplied: current.rawApplied,
        rawLastOpId: current.rawLastOpId,
        committedApplied: progress.applied,
        committedLastOpId: progress.lastOpId ?? null
      })
    )
  }

  /**
   * Persist a validated-but-uncommitted K/V entry so restart and heal can
   * inspect or later apply it without exposing it as committed state.
   *
   * @param {string} feedKey
   * @param {{
   *   nodeId: string,
   *   source: "local" | "remote",
   *   validation: "valid",
   *   operation: Record<string, unknown>
   * }} staged
   */
  async stageEntry(feedKey, staged) {
    const operation = staged.operation
    if (operation.kind !== "kv") {
      throw new Error("Only K/V operations may be staged")
    }

    await this.bee.put(this.#stagedEntryKey(feedKey, operation.seq), {
      feedKey,
      nodeId: staged.nodeId,
      source: staged.source,
      validation: staged.validation,
      seq: operation.seq,
      opId: operation.opId,
      kind: operation.kind,
      type: operation.type,
      keyspace: operation.keyspace,
      key: operation.key,
      actor: operation.actor,
      ts: operation.ts,
      expiresAt: operation.expiresAt ?? null
    })
  }

  /**
   * @param {string} feedKey
   * @returns {Promise<Record<string, unknown>[]>}
   */
  async getStagedEntries(feedKey) {
    const entries = []
    const range = this.bee.createReadStream({
      gte: `feeds/${feedKey}/staged/`,
      lt: `feeds/${feedKey}/staged/~`
    })

    for await (const entry of range) {
      entries.push(entry.value)
    }

    return entries
  }

  /**
   * @param {string} feedKey
   */
  async getStagedSummary(feedKey) {
    const entries = await this.getStagedEntries(feedKey)
    if (entries.length === 0) {
      return {
        count: 0,
        firstSeq: null,
        lastSeq: null,
        latestOpId: null,
        latestKey: null
      }
    }

    return {
      count: entries.length,
      firstSeq: entries[0].seq,
      lastSeq: entries.at(-1).seq,
      latestOpId: entries.at(-1).opId,
      latestKey: entries.at(-1).key
    }
  }

  /**
   * @param {string} feedKey
   * @param {number} seq
   */
  async deleteStagedEntry(feedKey, seq) {
    await this.bee.del(this.#stagedEntryKey(feedKey, seq))
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
      if (this.#isStagedEntryKey(entry.key)) continue
      entries.push({
        key: entry.key,
        value: entry.value
      })
    }

    return entries
  }

  /**
   * @param {string} feedKey
   */
  #progressKey(feedKey) {
    return `feeds/${feedKey}/progress`
  }

  /**
   * @param {string} feedKey
   * @param {number} seq
   */
  #stagedEntryKey(feedKey, seq) {
    return `feeds/${feedKey}/staged/${String(seq).padStart(12, "0")}`
  }

  /**
   * @param {Record<string, unknown> | undefined} value
   */
  #normalizeProgressRecord(value) {
    return {
      rawApplied: value?.rawApplied ?? 0,
      rawLastOpId: value?.rawLastOpId ?? null,
      committedApplied: value?.committedApplied ?? 0,
      committedLastOpId: value?.committedLastOpId ?? null
    }
  }

  /**
   * @param {{
   *   feedKey: string,
   *   rawApplied: number,
   *   rawLastOpId: string | null,
   *   committedApplied: number,
   *   committedLastOpId: string | null
   * }} next
   */
  #nextProgressRecord(next) {
    return {
      rawApplied: next.rawApplied,
      rawLastOpId: next.rawLastOpId,
      committedApplied: next.committedApplied,
      committedLastOpId: next.committedLastOpId
    }
  }

  /**
   * @param {string} key
   */
  #isStagedEntryKey(key) {
    return key.includes("/staged/")
  }
}
