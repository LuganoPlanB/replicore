/**
 * Canonicalize JSON-like values using a stable key order so the same payload
 * always produces the same bytes before hashing or signing.
 *
 * @param {unknown} value
 * @returns {string}
 */
export function canonicalize(value) {
  return canonicalizeValue(value)
}

function canonicalizeValue(value) {
  if (value === null) return "null"

  switch (typeof value) {
    case "boolean":
      return value ? "true" : "false"
    case "number":
      if (!Number.isFinite(value)) {
        throw new TypeError("Canonicalization does not support non-finite numbers")
      }
      return JSON.stringify(value)
    case "string":
      return JSON.stringify(value)
    case "object":
      if (Array.isArray(value)) {
        return `[${value.map(canonicalizeValue).join(",")}]`
      }
      return `{${Object.keys(value)
        .sort()
        .map((key) => `${JSON.stringify(key)}:${canonicalizeValue(value[key])}`)
        .join(",")}}`
    default:
      throw new TypeError(`Canonicalization does not support ${typeof value}`)
  }
}
