import assert from "node:assert/strict"

/**
 * Poll an async condition until it succeeds or times out.
 *
 * @param {() => Promise<boolean>} condition
 * @param {{
 *   timeoutMs?: number,
 *   intervalMs?: number,
 *   description?: string,
 *   onTimeout?: (() => Promise<unknown> | unknown)
 * }} [options]
 */
export async function waitFor(condition, options = {}) {
  const timeoutMs = options.timeoutMs ?? 15000
  const intervalMs = options.intervalMs ?? 50
  const started = Date.now()

  while (Date.now() - started < timeoutMs) {
    if (await condition()) return
    await sleep(intervalMs)
  }

  let details = ""
  if (options.onTimeout) {
    const diagnostic = await options.onTimeout()
    if (diagnostic !== undefined) {
      details = `\n${typeof diagnostic === "string" ? diagnostic : JSON.stringify(diagnostic, null, 2)}`
    }
  }

  throw new Error(`Timed out waiting for ${options.description ?? "condition"} after ${timeoutMs}ms${details}`)
}

/**
 * Bound one async operation and attach optional diagnostics to timeout failures.
 *
 * @template T
 * @param {string} description
 * @param {Promise<T>} operation
 * @param {{
 *   timeoutMs?: number,
 *   onTimeout?: (() => Promise<unknown> | unknown)
 * }} [options]
 * @returns {Promise<T>}
 */
export async function withTimeout(description, operation, options = {}) {
  const timeoutMs = options.timeoutMs ?? 15000
  let timer = null

  const timeout = new Promise((_, reject) => {
    timer = setTimeout(async () => {
      let details = ""
      if (options.onTimeout) {
        const diagnostic = await options.onTimeout()
        if (diagnostic !== undefined) {
          details = `\n${typeof diagnostic === "string" ? diagnostic : JSON.stringify(diagnostic, null, 2)}`
        }
      }

      reject(new Error(`Timed out during ${description} after ${timeoutMs}ms${details}`))
    }, timeoutMs)
  })

  try {
    return await Promise.race([operation, timeout])
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Assert that an async value remains unchanged for a stability window.
 *
 * @template T
 * @param {() => Promise<T>} reader
 * @param {{ stableMs?: number, timeoutMs?: number, intervalMs?: number }} [options]
 */
export async function waitForNoChange(reader, options = {}) {
  const stableMs = options.stableMs ?? 300
  const timeoutMs = options.timeoutMs ?? 15000
  const intervalMs = options.intervalMs ?? 50
  const initial = await reader()
  const deadline = Date.now() + timeoutMs
  let stableSince = Date.now()

  while (Date.now() < deadline) {
    await sleep(intervalMs)
    const current = await reader()
    try {
      assert.deepEqual(current, initial)
    } catch {
      stableSince = Date.now()
    }
    if (Date.now() - stableSince >= stableMs) return initial
  }

  throw new Error(`Timed out waiting for value stability after ${timeoutMs}ms`)
}

/**
 * Resolve a condition and assert it eventually becomes true.
 *
 * @param {string} description
 * @param {() => Promise<boolean>} condition
 * @param {Parameters<typeof waitFor>[1]} [options]
 */
export async function expectEventually(description, condition, options = {}) {
  await waitFor(condition, { ...options, description })
}

/**
 * Collect replication status from a set of nodes.
 *
 * @param {Array<import("../../src/index.js").HolepunchSwarmNode>} nodes
 */
export async function collectReplicationStatus(nodes) {
  const statuses = await Promise.all(nodes.map((node) => node.getReplicationStatus()))
  return Object.fromEntries(statuses.map((status) => [status.nodeId, status]))
}

/**
 * Collect cluster status together with recent fixture lifecycle events.
 *
 * @param {{
 *   diagnostics?: (nodes?: Array<import("../../src/index.js").HolepunchSwarmNode>) => Promise<unknown>,
 *   trace?: { snapshot?: () => unknown }
 * }} cluster
 * @param {Array<import("../../src/index.js").HolepunchSwarmNode>} [nodes]
 */
export async function collectClusterDiagnostics(cluster, nodes = cluster.nodes) {
  if (typeof cluster.diagnostics === "function") {
    return cluster.diagnostics(nodes)
  }

  return {
    status: await collectReplicationStatus(nodes),
    trace: cluster.trace?.snapshot?.() ?? []
  }
}

/**
 * Assert that every node exposes the same current value for a key.
 *
 * @param {Array<import("../../src/index.js").HolepunchSwarmNode>} nodes
 * @param {string} key
 * @param {unknown} expected
 * @param {{ keyspace?: string }} [options]
 */
export async function assertClusterValue(nodes, key, expected, options = {}) {
  const values = await Promise.all(nodes.map((node) => node.get(key, options)))

  for (const current of values) {
    assert.deepEqual(current?.value ?? null, expected)
  }
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms))
}
