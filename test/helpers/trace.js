const DEFAULT_LIMIT = 200

/**
 * Keep a bounded event timeline for test diagnostics.
 *
 * @param {{ limit?: number, stream?: NodeJS.WritableStream | null }} [options]
 */
export function createTrace(options = {}) {
  const limit = options.limit ?? DEFAULT_LIMIT
  const events = []
  const stream = options.stream ?? (process.env.REPLICORE_TEST_TRACE === "1" ? process.stderr : null)

  return {
    /**
     * Record one timeline event.
     *
     * @param {string} event
     * @param {Record<string, unknown>} [details]
     */
    record(event, details = {}) {
      const entry = {
        ts: new Date().toISOString(),
        event,
        ...details
      }

      events.push(entry)
      if (events.length > limit) events.splice(0, events.length - limit)

      if (stream) {
        stream.write(`[replicore-test] ${entry.ts} ${event} ${JSON.stringify(details)}\n`)
      }
    },

    /**
     * Return a serializable snapshot of the most recent events.
     */
    snapshot() {
      return events.slice()
    }
  }
}
