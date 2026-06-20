const KEYSPACE_PATTERN = /^[A-Za-z0-9._:-]+$/

/**
 * Ensure a route body is a plain JSON object.
 *
 * @param {unknown} value
 * @param {string} label
 * @returns {Record<string, unknown>}
 */
export function requirePlainObject(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw badRequest(`${label} must be an object`)
  }
  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) {
    throw badRequest(`${label} must be an object`)
  }
  return /** @type {Record<string, unknown>} */ (value)
}

/**
 * Reject unknown keys in one route body.
 *
 * @param {Record<string, unknown>} value
 * @param {string[]} allowedKeys
 */
export function rejectUnknownKeys(value, allowedKeys) {
  const allowed = new Set(allowedKeys)
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) {
      throw badRequest(`Unknown field: ${key}`)
    }
  }
}

/**
 * Require one string field with conservative optional constraints.
 *
 * @param {unknown} value
 * @param {string} field
 * @param {{ minLength?: number, maxLength?: number, pattern?: RegExp, trim?: boolean }} [options]
 */
export function requireString(value, field, options = {}) {
  if (typeof value !== "string") {
    throw badRequest(`${field} must be a string`)
  }

  const normalized = options.trim === false ? value : value.trim()
  const minLength = options.minLength ?? 1
  const maxLength = options.maxLength ?? Number.POSITIVE_INFINITY
  if (normalized.length < minLength || normalized.length > maxLength) {
    throw badRequest(`${field} must be ${describeLength(minLength, maxLength)}`)
  }
  if (options.pattern && !options.pattern.test(normalized)) {
    throw badRequest(`${field} has an invalid format`)
  }
  return normalized
}

/**
 * Validate one keyspace string.
 *
 * @param {unknown} value
 */
export function validateKeyspace(value) {
  return requireString(value, "keyspace", {
    minLength: 1,
    maxLength: 128,
    pattern: KEYSPACE_PATTERN
  })
}

/**
 * Validate one K/V key.
 *
 * @param {unknown} value
 */
export function validateKvKey(value) {
  const key = requireString(value, "key", {
    minLength: 1,
    maxLength: 512,
    trim: false
  })
  if (key.includes("/")) {
    throw badRequest("key must not contain /")
  }
  if ([...key].some((char) => isControlCharacter(char))) {
    throw badRequest("key must not contain control characters")
  }
  return key
}

/**
 * Validate one JSON value recursively with bounded shape.
 *
 * @param {unknown} value
 * @param {{ maxDepth?: number, maxStringLength?: number }} [options]
 */
export function validateJsonValue(value, options = {}) {
  const limits = {
    maxDepth: options.maxDepth ?? 8,
    maxStringLength: options.maxStringLength ?? 4096
  }
  return validateJsonValueAtDepth(value, limits, 0)
}

function validateJsonValueAtDepth(value, limits, depth) {
  if (depth > limits.maxDepth) {
    throw badRequest(`value exceeds maximum depth of ${limits.maxDepth}`)
  }
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number"
  ) {
    return value
  }
  if (typeof value === "string") {
    if (value.length > limits.maxStringLength) {
      throw badRequest(`value strings must be at most ${limits.maxStringLength} characters`)
    }
    return value
  }
  if (Array.isArray(value)) {
    return value.map((entry) => validateJsonValueAtDepth(entry, limits, depth + 1))
  }

  const object = requirePlainObject(value, "value")
  const normalized = {}
  for (const [key, entry] of Object.entries(object)) {
    normalized[key] = validateJsonValueAtDepth(entry, limits, depth + 1)
  }
  return normalized
}

function describeLength(minLength, maxLength) {
  if (Number.isFinite(maxLength)) {
    if (minLength === maxLength) return `exactly ${minLength} characters`
    return `between ${minLength} and ${maxLength} characters`
  }
  return `at least ${minLength} characters`
}

function isControlCharacter(char) {
  const code = char.charCodeAt(0)
  return code < 0x20 || code === 0x7f
}

function badRequest(message) {
  const error = new Error(message)
  error.statusCode = 400
  error.code = "INVALID_REQUEST"
  return error
}
