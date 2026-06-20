/**
 * Build the default security headers for JSON responses.
 *
 * @param {{
 *   contentSecurityPolicy?: string,
 *   strictTransportSecurity?: string | null
 * }} [options]
 */
export function securityHeadersFor(options = {}) {
  const headers = {
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
    "x-frame-options": "DENY",
    "content-security-policy": options.contentSecurityPolicy
      ?? "default-src 'none'; frame-ancestors 'none'; base-uri 'none'"
  }

  if (options.strictTransportSecurity) {
    headers["strict-transport-security"] = options.strictTransportSecurity
  }

  return headers
}

/**
 * Send one JSON response with consistent headers.
 *
 * @param {import("node:http").ServerResponse} res
 * @param {number} statusCode
 * @param {unknown} payload
 * @param {{
 *   headers?: Record<string, string>,
 *   strictTransportSecurity?: string | null,
 *   contentSecurityPolicy?: string
 * }} [options]
 */
export function sendJson(res, statusCode, payload, options = {}) {
  const body = JSON.stringify(payload)
  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
    "connection": "close",
    ...securityHeadersFor(options),
    ...(options.headers ?? {})
  })
  res.end(body)
}

/**
 * Send one JSON error response with consistent headers.
 *
 * @param {import("node:http").ServerResponse} res
 * @param {number} statusCode
 * @param {Record<string, unknown>} payload
 * @param {{
 *   headers?: Record<string, string>,
 *   strictTransportSecurity?: string | null,
 *   contentSecurityPolicy?: string
 * }} [options]
 */
export function sendError(res, statusCode, payload, options = {}) {
  sendJson(res, statusCode, payload, options)
}
