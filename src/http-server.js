import http from "node:http"
import { readJsonBody } from "./http/body.js"
import {
  rejectUnknownKeys,
  requirePlainObject,
  validateJsonValue,
  validateKvKey,
  validateKeyspace
} from "./http/validation.js"

/**
 * Minimal authorized HTTP surface for the swarm node.
 */
export class HolepunchHttpServer {
  /**
   * @param {{
   *   node: import("./node.js").HolepunchSwarmNode,
   *   host?: string,
   *   port?: number,
   *   auth?: {
   *     tokens: Record<string, {
   *       admin?: boolean,
   *       readKeyspaces?: string[],
   *       writeKeyspaces?: string[]
   *     }>
   *   }
   * }} options
   */
  constructor(options) {
    this.options = {
      host: "127.0.0.1",
      port: 0,
      auth: { tokens: {} },
      maxBodySize: 64 * 1024,
      rateLimit: options?.rateLimit ?? {
        writes: { max: 60, windowMs: 60_000 },
        admin: { max: 10, windowMs: 60_000 },
        reads: { max: 600, windowMs: 60_000 }
      },
      ...options
    }
    this.server = null
    this.sockets = new Set()
    this.rateLimitBuckets = new Map()
  }

  async start() {
    if (this.server) return

    this.server = http.createServer((req, res) => {
      void this.#handle(req, res)
    })
    this.server.on("connection", (socket) => {
      this.sockets.add(socket)
      socket.once("close", () => this.sockets.delete(socket))
    })

    await new Promise((resolve) => {
      this.server.listen(this.options.port, this.options.host, resolve)
    })
    await this.options.node.setHttpAddress(this.address)
  }

  async close() {
    if (!this.server) return
    const server = this.server
    server.closeIdleConnections?.()
    await new Promise((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()))
      server.closeAllConnections?.()
      for (const socket of this.sockets) socket.destroy()
    })
    this.server = null
    this.sockets.clear()
    await this.options.node.setHttpAddress(null)
  }

  get address() {
    const address = this.server?.address()
    if (!address || typeof address === "string") return null
    return address
  }

  async #handle(req, res) {
    try {
      const url = new URL(req.url, "http://127.0.0.1")
      const match = url.pathname.match(/^\/kv\/([^/]+)(\/history)?$/)

      if (match) {
        const key = validateKvKey(decodeURIComponent(match[1]))
        const keyspace = validateKeyspace(url.searchParams.get("keyspace") ?? "default")

        if (req.method === "GET" && match[2] === "/history") {
          this.#authorize(req, keyspace, "read")
          this.#checkRateLimit(req, "reads")
          return this.#json(res, 200, {
            key,
            keyspace,
            history: await this.options.node.getHistory(key, { keyspace })
          })
        }

        if (req.method === "GET") {
          this.#authorize(req, keyspace, "read")
          this.#checkRateLimit(req, "reads")
          const value = await this.options.node.get(key, { keyspace })
          if (!value) return this.#json(res, 404, { error: "Not found" })
          return this.#json(res, 200, value)
        }

        if (req.method === "PUT") {
          this.#authorize(req, keyspace, "write")
          this.#checkRateLimit(req, "writes")
          const body = requirePlainObject(await this.#readJson(req), "Request body")
          rejectUnknownKeys(body, ["value"])
          if (!Object.hasOwn(body, "value")) {
            const error = new Error("Request body must include value")
            error.code = "INVALID_REQUEST"
            error.statusCode = 400
            throw error
          }
          const value = validateJsonValue(body.value)
          await this.options.node.qualifyClientWriteEntrypoint()
          const operation = await this.options.node.put(key, value, { keyspace })
          return this.#json(res, 200, operation)
        }

        if (req.method === "DELETE") {
          this.#authorize(req, keyspace, "write")
          this.#checkRateLimit(req, "writes")
          await this.options.node.qualifyClientWriteEntrypoint()
          const operation = await this.options.node.delete(key, { keyspace })
          return this.#json(res, 200, operation)
        }
      }

      if (req.method === "GET" && url.pathname === "/status/replication") {
        this.#checkRateLimit(req, "reads")
        return this.#json(res, 200, await this.options.node.getReplicationStatus())
      }

      if (req.method === "GET" && url.pathname === "/status/writers") {
        this.#checkRateLimit(req, "reads")
        return this.#json(res, 200, this.options.node.getWritersStatus())
      }

      if (req.method === "GET" && url.pathname === "/status/leader") {
        this.#checkRateLimit(req, "reads")
        return this.#json(res, 200, await this.options.node.getLeaderStatus())
      }

      if (req.method === "GET" && url.pathname === "/admin/snapshot") {
        this.#authorizeAdmin(req)
        this.#checkRateLimit(req, "admin")
        return this.#json(res, 200, await this.options.node.createSnapshot())
      }

      if (req.method === "POST" && url.pathname === "/admin/snapshot/import") {
        this.#authorizeAdmin(req)
        this.#checkRateLimit(req, "admin")
        const body = await this.#readJson(req, 1024 * 1024)
        await this.options.node.restoreSnapshot(body)
        return this.#json(res, 200, { ok: true })
      }

      if (req.method === "POST" && url.pathname === "/admin/encryption/rotate") {
        this.#authorizeAdmin(req)
        this.#checkRateLimit(req, "admin")
        const body = await this.#readJson(req)
        return this.#json(res, 200, {
          ok: true,
          ...this.options.node.rotateEncryptionKey(body.keyId)
        })
      }

      return this.#json(res, 404, { error: "Not found" })
    } catch (error) {
      const status = error?.statusCode ?? 500
      const extraHeaders = error?.retryAfter ? { "retry-after": String(error.retryAfter) } : undefined
      const isInternalError = status >= 500 && !error?.refusal
      const payload = isInternalError
        ? { error: "Internal server error", code: "INTERNAL_ERROR" }
        : this.#errorPayload(error)
      if (isInternalError) {
        console.error("http internal error", { status, code: error?.code, message: error?.message?.slice(0, 200) })
      }
      return this.#json(res, status, payload, extraHeaders)
    }
  }

  #authorize(req, keyspace, mode) {
    const grants = this.#tokenGrants(req)
    if (!grants) {
      const error = new Error("Unauthorized")
      error.code = "UNAUTHORIZED"
      error.statusCode = 401
      throw error
    }

    const allowed = mode === "read" ? grants.readKeyspaces ?? [] : grants.writeKeyspaces ?? []
    if (!(allowed.includes("*") || allowed.includes(keyspace))) {
      const error = new Error("Forbidden")
      error.code = "FORBIDDEN"
      error.statusCode = 403
      throw error
    }
  }

  #authorizeAdmin(req) {
    const grants = this.#tokenGrants(req)
    if (!grants) {
      const error = new Error("Unauthorized")
      error.code = "UNAUTHORIZED"
      error.statusCode = 401
      throw error
    }

    if (grants.admin !== true) {
      const error = new Error("Forbidden")
      error.code = "FORBIDDEN"
      error.statusCode = 403
      throw error
    }
  }

  #tokenGrants(req) {
    const header = req.headers.authorization ?? ""
    const token = header.startsWith("Bearer ") ? header.slice(7) : null
    return token ? this.options.auth.tokens[token] ?? null : null
  }

  #checkRateLimit(req, category) {
    if (Object.keys(this.options.auth.tokens).length === 0) return

    const token = this.#extractToken(req)
    if (!token) return

    const limits = this.options.rateLimit ?? { reads: { max: 600, windowMs: 60_000 } }
    const defaults = { max: 600, windowMs: 60_000 }
    const limit = limits[category] ?? limits.reads ?? defaults
    const { max, windowMs } = limit

    let buckets = this.rateLimitBuckets.get(token)
    if (!buckets) {
      buckets = { writes: [], admin: [], reads: [] }
      this.rateLimitBuckets.set(token, buckets)
    }

    const timestamps = buckets[category]
    const now = Date.now()
    while (timestamps.length > 0 && timestamps[0] < now - windowMs) {
      timestamps.shift()
    }

    if (timestamps.length >= max) {
      const retryAfter = Math.ceil((timestamps[0] + windowMs - now) / 1000)
      const error = new Error("Too Many Requests")
      error.code = "TOO_MANY_REQUESTS"
      error.statusCode = 429
      error.retryAfter = retryAfter
      throw error
    }

    timestamps.push(now)
  }

  #extractToken(req) {
    const header = req.headers.authorization ?? ""
    return header.startsWith("Bearer ") ? header.slice(7) : null
  }

  async #readJson(req, maxSize = this.options.maxBodySize) {
    return readJsonBody(req, {
      maxBytes: maxSize,
      contentType: req.headers["content-type"] ?? "",
      allowEmpty: true
    })
  }

  #errorPayload(error) {
    const message = error instanceof Error ? error.message : String(error)
    const payload = {
      error: message
    }
    if (error?.code) payload.code = error.code
    if (error?.refusal) {
      payload.code = error.refusal.code
      payload.message = error.refusal.message
      payload.retryable = error.refusal.retryable
      payload.reconnectHints = error.refusal.reconnectHints
      if (process.env.DEBUG) {
        console.debug("refusal detail", {
          currentTerm: error.refusal.currentTerm,
          knownLeaderId: error.refusal.knownLeaderId,
          leaderReachable: error.refusal.leaderReachable,
          splitStatus: error.refusal.splitStatus,
          commitIndex: error.refusal.commitIndex,
          membershipVersion: error.refusal.membershipVersion,
          role: error.refusal.role
        })
      }
    }
    return payload
  }

  #json(res, statusCode, payload, extraHeaders) {
    const body = JSON.stringify(payload)
    res.writeHead(statusCode, {
      "content-type": "application/json; charset=utf-8",
      "content-length": Buffer.byteLength(body),
      "connection": "close",
      "x-content-type-options": "nosniff",
      ...(extraHeaders ?? {})
    })
    res.end(body)
  }
}
