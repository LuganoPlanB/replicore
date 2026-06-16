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
}
