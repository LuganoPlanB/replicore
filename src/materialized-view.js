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
  async applyCommitted(operation, feedKey) {
    if (operation.kind === "heartbeat") {
      await this.setCommittedProgress(feedKey, {
        applied: operation.seq + 1,
        lastOpId: operation.opId
      })
      return
    }

    if (operation.kind === "membership") {
      await this.setCommittedProgress(feedKey, {
        applied: operation.seq + 1,
        lastOpId: operation.opId
      })
      return
    }

    const batch = this.bee.batch()
    const keyspace = /** @type {string} */ (operation.keyspace)
    const key = /** @type {string} */ (operation.key)
    const currentKey = `kv/current/${keyspace}/${key}`
    const valueKey = `kv/value/${keyspace}/${key}`
    const historyKey = this.#historyKey(keyspace, key, operation)
    const summary = {
      opId: operation.opId,
      seq: operation.seq,
      term: operation.term,
      index: operation.index,
      prevIndex: operation.prevIndex,
      prevHash: operation.prevHash,
      entryHash: operation.entryHash,
      feed: operation.feed,
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
    const current = await this.getFeedProgress(feedKey)
    await batch.put(this.#progressKey(feedKey), this.#nextProgressRecord({
      feedKey,
      rawApplied: current.rawApplied,
      rawLastOpId: current.rawLastOpId,
      committedApplied: operation.seq + 1,
      committedLastOpId: operation.opId
    }))
    await batch.del(this.#stagedEntryKey(feedKey, operation.seq))
    await batch.flush()
  }

  /**
   * Commit a rejected feed slot without applying its staged K/V mutation.
   *
   * @param {Record<string, unknown>} operation
   * @param {string} feedKey
   */
  async skipCommitted(operation, feedKey) {
    const current = await this.getFeedProgress(feedKey)
    const batch = this.bee.batch()
    await batch.put(this.#progressKey(feedKey), this.#nextProgressRecord({
      feedKey,
      rawApplied: current.rawApplied,
      rawLastOpId: current.rawLastOpId,
      committedApplied: operation.seq + 1,
      committedLastOpId: operation.opId
    }))
    if (operation.kind === "kv") {
      const keyspace = /** @type {string} */ (operation.keyspace)
      const key = /** @type {string} */ (operation.key)
      const currentKey = `kv/current/${keyspace}/${key}`
      const valueKey = `kv/value/${keyspace}/${key}`
      const historyKey = this.#historyKey(keyspace, key, operation)
      const currentEntry = await this.bee.get(currentKey)

      await batch.del(historyKey)
      if (currentEntry?.value?.opId === operation.opId) {
        await batch.del(currentKey)
        await batch.del(valueKey)
      }
      await batch.del(this.#stagedEntryKey(feedKey, operation.seq))
    }
    await batch.flush()
  }

  /**
   * @param {Record<string, unknown>} operation
   * @param {string} feedKey
   */
  async applyHeartbeat(operation, feedKey) {
    await this.bee.put(`system/heartbeats/${operation.actor}`, {
      actor: operation.actor,
      feed: feedKey,
      term: operation.term,
      ts: operation.ts,
      seq: operation.seq,
      leaderId: operation.heartbeat?.leaderId ?? null,
      leaderCommitIndex: operation.heartbeat?.leaderCommitIndex ?? -1,
      membershipVersion: operation.heartbeat?.membershipVersion ?? 0,
      prevLogIndex: operation.heartbeat?.prevLogIndex ?? operation.prevIndex,
      prevLogTerm: operation.heartbeat?.prevLogTerm ?? -1,
      prevLogHash: operation.heartbeat?.prevLogHash ?? operation.prevHash ?? null,
      observedLeader: operation.heartbeat?.observedLeader ?? null,
      reachableLeader: operation.heartbeat?.reachableLeader ?? false,
      appliedFeeds: operation.heartbeat?.appliedFeeds ?? {},
      rejectedFeeds: operation.heartbeat?.rejectedFeeds ?? {},
      membershipFingerprint: operation.heartbeat?.membershipFingerprint ?? null,
      httpAddress: operation.heartbeat?.httpAddress ?? null
    })
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
   * Read both feed cursors for one replicated feed.
   * Raw progress may advance ahead of the committed prefix.
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
   *   resolution?: "pending" | "rejected",
   *   operation: Record<string, unknown>
   * }} staged
   */
  async stageEntry(feedKey, staged) {
    const operation = staged.operation
    if (operation.kind !== "kv") {
      throw new Error("Only K/V operations may be staged")
    }
    if (await this.getApplied(feedKey) > operation.seq) return

    const stagedKey = this.#stagedEntryKey(feedKey, operation.seq)
    const existing = await this.getStagedEntry(feedKey, operation.seq)
    await this.bee.put(stagedKey, {
      feedKey,
      nodeId: staged.nodeId,
      source: staged.source,
      validation: staged.validation,
      resolution: existing?.resolution ?? staged.resolution ?? "pending",
      seq: operation.seq,
      term: operation.term,
      index: operation.index,
      prevIndex: operation.prevIndex,
      prevHash: operation.prevHash,
      entryHash: operation.entryHash,
      opId: operation.opId,
      kind: operation.kind,
      type: operation.type,
      keyspace: operation.keyspace,
      key: operation.key,
      actor: operation.actor,
      ts: operation.ts,
      expiresAt: operation.expiresAt ?? null
    })
    if (await this.getApplied(feedKey) > operation.seq) {
      await this.bee.del(stagedKey)
    }
  }

  /**
   * @param {string} feedKey
   * @param {number} seq
   * @returns {Promise<Record<string, unknown> | null>}
   */
  async getStagedEntry(feedKey, seq) {
    const entry = await this.bee.get(this.#stagedEntryKey(feedKey, seq))
    return entry?.value ?? null
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
   * @param {number} seq
   * @param {"pending" | "rejected"} resolution
   */
  async setStagedEntryResolution(feedKey, seq, resolution) {
    if (await this.getApplied(feedKey) > seq) {
      await this.deleteStagedEntry(feedKey, seq)
      return
    }

    const existing = await this.getStagedEntry(feedKey, seq)
    if (!existing) return

    await this.bee.put(this.#stagedEntryKey(feedKey, seq), {
      ...existing,
      resolution
    })
    if (await this.getApplied(feedKey) > seq) {
      await this.deleteStagedEntry(feedKey, seq)
    }
  }

  /**
   * Persist that a feed sequence must advance committed progress without
   * materializing its K/V operation.
   *
   * @param {string} feedKey
   * @param {number} seq
   */
  async markSkippedEntry(feedKey, seq) {
    await this.bee.put(this.#skippedEntryKey(feedKey, seq), { feedKey, seq })
  }

  /**
   * @param {string} feedKey
   * @returns {Promise<number[]>}
   */
  async getSkippedEntries(feedKey) {
    const skipped = []
    const range = this.bee.createReadStream({
      gte: `feeds/${feedKey}/skipped/`,
      lt: `feeds/${feedKey}/skipped/~`
    })

    for await (const entry of range) {
      skipped.push(entry.value.seq)
    }

    return skipped
  }

  /**
   * @param {string} feedKey
   * @param {number} seq
   */
  async isSkippedEntry(feedKey, seq) {
    return Boolean(await this.bee.get(this.#skippedEntryKey(feedKey, seq)))
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
   * Discard derived state for an authoritative suffix that was truncated
   * before commit. Committed progress and user-visible K/V state are preserved.
   *
   * @param {string} feedKey
   * @param {number} keepLength
   */
  async discardUncommittedSuffix(feedKey, keepLength) {
    const current = await this.getFeedProgress(feedKey)
    const nextRawApplied = Math.max(
      current.committedApplied,
      Math.min(current.rawApplied, keepLength)
    )
    const batch = this.bee.batch()
    await batch.put(this.#progressKey(feedKey), this.#nextProgressRecord({
      feedKey,
      rawApplied: nextRawApplied,
      rawLastOpId: nextRawApplied === current.committedApplied ? current.committedLastOpId : current.rawLastOpId,
      committedApplied: current.committedApplied,
      committedLastOpId: current.committedLastOpId
    }))

    for await (const entry of this.bee.createReadStream({
      gte: `feeds/${feedKey}/staged/${String(keepLength).padStart(12, "0")}`,
      lt: `feeds/${feedKey}/staged/~`
    })) {
      await batch.del(entry.key)
    }
    for await (const entry of this.bee.createReadStream({
      gte: `feeds/${feedKey}/skipped/${String(keepLength).padStart(12, "0")}`,
      lt: `feeds/${feedKey}/skipped/~`
    })) {
      await batch.del(entry.key)
    }
    await batch.flush()
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
      if (this.#isSkippedEntryKey(entry.key)) continue
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
   * @param {string} keyspace
   * @param {string} key
   * @param {Record<string, unknown>} operation
   */
  #historyKey(keyspace, key, operation) {
    const paddedIndex = String(operation.index).padStart(12, "0")
    return `kv/history/${keyspace}/${key}/${paddedIndex}/${operation.actor}`
  }

  /**
   * @param {string} feedKey
   * @param {number} seq
   */
  #skippedEntryKey(feedKey, seq) {
    return `feeds/${feedKey}/skipped/${String(seq).padStart(12, "0")}`
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

  /**
   * @param {string} key
   */
  #isSkippedEntryKey(key) {
    return key.includes("/skipped/")
  }
}
