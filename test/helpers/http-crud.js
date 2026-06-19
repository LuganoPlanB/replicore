import assert from "node:assert/strict"

/**
 * Issue one JSON HTTP request against the Replicore test API.
 *
 * @param {string} url
 * @param {{
 *   method?: string,
 *   token?: string,
 *   body?: unknown,
 *   headers?: Record<string, string>
 * }} [options]
 */
export async function requestJson(url, options = {}) {
  const headers = { ...(options.headers ?? {}) }
  if (options.token) {
    headers.authorization = `Bearer ${options.token}`
  }
  if (options.body !== undefined) {
    headers["content-type"] = headers["content-type"] ?? "application/json"
  }

  const response = await fetch(url, {
    method: options.method ?? "GET",
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body)
  })

  const text = await response.text()
  const payload =
    text.length === 0 ? null : response.headers.get("content-type")?.includes("application/json")
      ? JSON.parse(text)
      : text

  return { status: response.status, payload }
}

/**
 * Perform one witness-facing HTTP put.
 *
 * @param {string} baseUrl
 * @param {string} key
 * @param {unknown} value
 * @param {{ keyspace?: string, token?: string }} [options]
 */
export async function putValue(baseUrl, key, value, options = {}) {
  return requestJson(`${baseUrl}/kv/${encodeURIComponent(key)}?keyspace=${options.keyspace ?? "default"}`, {
    method: "PUT",
    token: options.token ?? "writer",
    body: { value }
  })
}

/**
 * Read one current value through the HTTP API.
 *
 * @param {string} baseUrl
 * @param {string} key
 * @param {{ keyspace?: string, token?: string }} [options]
 */
export async function getValue(baseUrl, key, options = {}) {
  return requestJson(`${baseUrl}/kv/${encodeURIComponent(key)}?keyspace=${options.keyspace ?? "default"}`, {
    token: options.token ?? "reader"
  })
}

/**
 * Perform one HTTP delete.
 *
 * @param {string} baseUrl
 * @param {string} key
 * @param {{ keyspace?: string, token?: string }} [options]
 */
export async function deleteValue(baseUrl, key, options = {}) {
  return requestJson(`${baseUrl}/kv/${encodeURIComponent(key)}?keyspace=${options.keyspace ?? "default"}`, {
    method: "DELETE",
    token: options.token ?? "writer"
  })
}

/**
 * Read one key history through the HTTP API.
 *
 * @param {string} baseUrl
 * @param {string} key
 * @param {{ keyspace?: string, token?: string }} [options]
 */
export async function getHistory(baseUrl, key, options = {}) {
  return requestJson(
    `${baseUrl}/kv/${encodeURIComponent(key)}/history?keyspace=${options.keyspace ?? "default"}`,
    { token: options.token ?? "reader" }
  )
}

/**
 * Assert one write returned a committed operation envelope.
 *
 * @param {{ status: number, payload: unknown }} result
 * @param {{ type?: string, key?: string, keyspace?: string }} [options]
 */
export function expectCommittedOperation(result, options = {}) {
  assert.equal(result.status, 200)
  assert.equal(typeof result.payload, "object")
  assert.ok(result.payload)
  assert.equal(typeof result.payload.opId, "string")
  assert.equal(typeof result.payload.actor, "string")
  if (options.type) assert.equal(result.payload.type, options.type)
  if (options.key) assert.equal(result.payload.key, options.key)
  if (options.keyspace) assert.equal(result.payload.keyspace, options.keyspace)
  return result.payload
}

/**
 * Assert one read returns the expected committed value.
 *
 * @param {{ status: number, payload: unknown }} result
 * @param {unknown} expectedValue
 */
export function expectCommittedValue(result, expectedValue) {
  assert.equal(result.status, 200)
  assert.equal(typeof result.payload, "object")
  assert.ok(result.payload)
  assert.notEqual(result.payload.deleted, true)
  assert.deepEqual(result.payload.value, expectedValue)
  return result.payload
}

/**
 * Assert one read reports committed absence.
 *
 * @param {{ status: number, payload: unknown }} result
 */
export function expectAbsent(result) {
  assert.equal(result.status, 404)
  assert.deepEqual(result.payload, { error: "Not found" })
}

/**
 * Assert one read reports a committed delete marker.
 *
 * @param {{ status: number, payload: unknown }} result
 */
export function expectDeleted(result) {
  assert.equal(result.status, 200)
  assert.equal(typeof result.payload, "object")
  assert.ok(result.payload)
  assert.equal(result.payload.deleted, true)
  assert.equal(result.payload.value, null)
  return result.payload
}

/**
 * Assert one write refusal returns the expected structured payload.
 *
 * @param {{ status: number, payload: unknown }} result
 * @param {{
 *   status?: number | number[],
 *   code?: string | RegExp | Array<string | RegExp>
 * }} [options]
 */
export function expectWriteRefusal(result, options = {}) {
  const allowedStatuses = Array.isArray(options.status)
    ? options.status
    : [options.status ?? 503]
  assert.ok(allowedStatuses.includes(result.status), `Unexpected refusal status ${result.status}`)
  assert.equal(typeof result.payload, "object")
  assert.ok(result.payload)
  assert.equal(typeof result.payload.error, "string")
  if (options.code !== undefined) {
    const codes = Array.isArray(options.code) ? options.code : [options.code]
    assert.ok(
      codes.some((expected) =>
        expected instanceof RegExp ? expected.test(result.payload.code ?? "") : result.payload.code === expected
      ),
      `Unexpected refusal code ${result.payload.code}`
    )
  }
  return result.payload
}

/**
 * Assert one refusal includes reconnect hints that point to witness nodes.
 *
 * @param {unknown} payload
 * @param {{ witnessPort?: number }} [options]
 */
export function expectRedirectHints(payload, options = {}) {
  assert.equal(typeof payload, "object")
  assert.ok(payload)
  assert.equal(Array.isArray(payload.reconnectHints?.witnesses), true)
  assert.ok(payload.reconnectHints.witnesses.length > 0)
  if (options.witnessPort !== undefined) {
    assert.ok(
      payload.reconnectHints.witnesses.some((hint) => hint.httpAddress?.port === options.witnessPort),
      `Missing witness redirect hint for port ${options.witnessPort}`
    )
  }
}

/**
 * Assert one history response contains the expected committed operation sequence.
 *
 * @param {{ status: number, payload: unknown }} result
 * @param {Array<{ type?: string, opId?: string }>} expectedEntries
 */
export function expectHistoryOps(result, expectedEntries) {
  assert.equal(result.status, 200)
  assert.equal(typeof result.payload, "object")
  assert.ok(result.payload)
  assert.equal(Array.isArray(result.payload.history), true)
  assert.equal(result.payload.history.length, expectedEntries.length)

  for (const [index, expected] of expectedEntries.entries()) {
    const entry = result.payload.history[index]
    assert.equal(typeof entry.opId, "string")
    assert.equal(typeof entry.actor, "string")
    if (expected.type) assert.equal(entry.type, expected.type)
    if (expected.opId) assert.equal(entry.opId, expected.opId)
  }

  return result.payload.history
}
