/**
 * Read and parse one JSON request body with a bounded byte budget.
 *
 * @param {AsyncIterable<Buffer | string>} req
 * @param {{
 *   maxBytes: number,
 *   contentType?: string,
 *   allowEmpty?: boolean
 * }} options
 */
export async function readJsonBody(req, options) {
  const contentType = options.contentType ?? ""
  const allowEmpty = options.allowEmpty ?? true

  if (!contentType.startsWith("application/json")) {
    if (Number.parseInt(req.headers?.["content-length"] ?? "0", 10) === 0 && !contentType && allowEmpty) {
      return {}
    }
    const error = new Error("Unsupported Media Type")
    error.code = "UNSUPPORTED_MEDIA_TYPE"
    error.statusCode = 415
    throw error
  }

  const contentLength = Number.parseInt(req.headers?.["content-length"], 10)
  if (Number.isFinite(contentLength) && contentLength > options.maxBytes) {
    const error = new Error("Request body too large")
    error.code = "PAYLOAD_TOO_LARGE"
    error.statusCode = 413
    throw error
  }

  let totalSize = 0
  const chunks = []
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    totalSize += buffer.length
    if (totalSize > options.maxBytes) {
      const error = new Error("Request body too large")
      error.code = "PAYLOAD_TOO_LARGE"
      error.statusCode = 413
      throw error
    }
    chunks.push(buffer)
  }

  if (chunks.length === 0) {
    return allowEmpty ? {} : invalidJsonBodyError()
  }

  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"))
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw invalidJsonBodyError()
    }
    throw error
  }
}

function invalidJsonBodyError() {
  const error = new Error("Invalid JSON body")
  error.code = "INVALID_JSON"
  error.statusCode = 400
  return error
}
