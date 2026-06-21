import { readFileSync } from "node:fs"
import http from "node:http"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { readJsonBody } from "./http/body.js"
import { createFixedWindowRateLimiter } from "./http/rate-limit.js"
import { sendError, sendJson } from "./http/response.js"
import {
  rejectUnknownKeys,
  requirePlainObject,
  validateJsonValue,
  validateKvKey,
  validateKeyspace
} from "./http/validation.js"

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")

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
   *       read?: string[],
   *       write?: string[]
   *     }>
   *   }
   * }} options
   */
  constructor(options) {
   this.options = {
      host: "127.0.0.1",
      port: 0,
      auth: { tokens: {} },
      logger: console,
      maxBodySize: 64 * 1024,
      rateLimit: options?.rateLimit ?? {
        all: { max: 300, windowMs: 60_000 },
        writes: { max: 60, windowMs: 60_000 },
        admin: { max: 10, windowMs: 60_000 },
        reads: { max: 600, windowMs: 60_000 }
      },
      ...options
    }
    this.server = null
    this.sockets = new Set()
    this.rateLimiters = {
      all: createFixedWindowRateLimiter(this.options.rateLimit.all ?? { max: 300, windowMs: 60_000 }),
      writes: createFixedWindowRateLimiter(this.options.rateLimit.writes ?? { max: 60, windowMs: 60_000 }),
      admin: createFixedWindowRateLimiter(this.options.rateLimit.admin ?? { max: 10, windowMs: 60_000 }),
      reads: createFixedWindowRateLimiter(this.options.rateLimit.reads ?? { max: 600, windowMs: 60_000 })
    }
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
          return this.#sendJson(res, 200, {
            key,
            keyspace,
            history: await this.options.node.getHistory(key, { keyspace })
          })
        }

        if (req.method === "GET") {
          this.#authorize(req, keyspace, "read")
          this.#checkRateLimit(req, "reads")
          const value = await this.options.node.get(key, { keyspace })
          if (!value) return this.#sendJson(res, 404, { error: "Not found" })
          return this.#sendJson(res, 200, value)
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
          return this.#sendJson(res, 200, operation)
        }

        if (req.method === "DELETE") {
          this.#authorize(req, keyspace, "write")
          this.#checkRateLimit(req, "writes")
          await this.options.node.qualifyClientWriteEntrypoint()
          const operation = await this.options.node.delete(key, { keyspace })
          return this.#sendJson(res, 200, operation)
        }
      }

      if (req.method === "GET" && url.pathname === "/status/replication") {
        this.#checkRateLimit(req, "reads")
        return this.#sendJson(res, 200, await this.options.node.getReplicationStatus())
      }

      if (req.method === "GET" && url.pathname === "/status/writers") {
        this.#checkRateLimit(req, "reads")
        return this.#sendJson(res, 200, this.options.node.getWritersStatus())
      }

      if (req.method === "GET" && url.pathname === "/status/leader") {
        this.#checkRateLimit(req, "reads")
        return this.#sendJson(res, 200, await this.options.node.getLeaderStatus())
      }

      if (req.method === "GET" && url.pathname === "/admin/snapshot") {
        this.#authorizeAdmin(req)
        this.#checkRateLimit(req, "admin")
        return this.#sendJson(res, 200, await this.options.node.createSnapshot())
      }

      if (req.method === "POST" && url.pathname === "/admin/snapshot/import") {
        this.#authorizeAdmin(req)
        this.#checkRateLimit(req, "admin")
        const body = requirePlainObject(await this.#readJson(req, 1024 * 1024), "Request body")
        if (
          !Array.isArray(body.entries)
          && (!body.content || typeof body.content !== "object" || Array.isArray(body.content))
        ) {
          const error = new Error("Request body must look like a snapshot")
          error.code = "INVALID_REQUEST"
          error.statusCode = 400
          throw error
        }
        await this.options.node.restoreSnapshot(body)
        return this.#sendJson(res, 200, { ok: true })
      }

      if (req.method === "POST" && url.pathname === "/admin/encryption/rotate") {
        this.#authorizeAdmin(req)
        this.#checkRateLimit(req, "admin")
        const body = requirePlainObject(await this.#readJson(req), "Request body")
        rejectUnknownKeys(body, ["keyId"])
        const keyId = validateKeyspace(body.keyId)
        return this.#sendJson(res, 200, {
          ok: true,
          ...this.options.node.rotateEncryptionKey(keyId)
        })
      }

      if (req.method === "GET" && url.pathname === "/docs") {
        return this.#serveStatic(res, "docs/scalar.html", "text/html; charset=utf-8")
      }

      if (req.method === "GET" && url.pathname === "/openapi.yaml") {
        return this.#serveStatic(res, "docs/openapi.yaml", "application/yaml; charset=utf-8")
      }

      return this.#sendJson(res, 404, { error: "Not found" })
    } catch (error) {
      const status = error?.statusCode ?? 500
      const extraHeaders = error?.rateLimitHeaders
        ?? (error?.retryAfter ? { "retry-after": String(error.retryAfter) } : undefined)
      const isInternalError = status >= 500 && !error?.refusal
      const payload = isInternalError
        ? { error: "Internal server error", code: "INTERNAL_ERROR" }
        : this.#errorPayload(error)
      if (isInternalError) {
        this.options.logger?.error?.("http internal error", { status, code: error?.code, error })
      }
      return this.#sendError(res, status, payload, extraHeaders)
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

    const allowed = mode === "read" ? grants.read ?? [] : grants.write ?? []
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
    const remoteAddress = req.socket?.remoteAddress ?? "unknown"
    const globalResult = this.rateLimiters.all(remoteAddress)
    if (!globalResult.allowed) {
      this.#logRateLimit(req, "all")
      throw this.#createRateLimitError(globalResult, this.options.rateLimit.all ?? { max: 300, windowMs: 60_000 })
    }

    const limiter = this.rateLimiters[category]
    if (!limiter) return

    const result = limiter(remoteAddress)
    if (!result.allowed) {
      this.#logRateLimit(req, category)
      throw this.#createRateLimitError(result, this.options.rateLimit[category] ?? { max: 0, windowMs: 0 })
    }
  }

  #logRateLimit(req, category) {
    this.options.logger?.warn?.("http rate limited", {
      method: req.method,
      path: new URL(req.url, "http://127.0.0.1").pathname,
      remoteAddress: req.socket?.remoteAddress ?? "unknown",
      limitName: category
    })
  }

  #createRateLimitError(result, config) {
    const error = new Error("Too Many Requests")
    error.code = "TOO_MANY_REQUESTS"
    error.statusCode = 429
    error.retryAfter = result.retryAfterSeconds
    error.rateLimitHeaders = {
      "retry-after": String(result.retryAfterSeconds),
      "ratelimit-limit": String(config.max ?? 0),
      "ratelimit-remaining": String(result.remaining),
      "ratelimit-reset": String(Math.ceil(result.resetAt / 1000))
    }
    return error
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

  #sendJson(res, statusCode, payload) {
    sendJson(res, statusCode, payload)
  }

  #serveStatic(res, relativePath, contentType) {
    try {
      const filePath = path.resolve(ROOT, relativePath)
      const content = readFileSync(filePath)
      res.writeHead(200, {
        "content-type": contentType,
        "content-length": String(content.length),
        "x-content-type-options": "nosniff"
      })
      res.end(content)
      return true
    } catch (err) {
      if (err?.code === "ENOENT") {
        this.#sendJson(res, 404, { error: "Not found" })
        return true
      }
      this.options.logger?.error?.("serve static failed", { relativePath, err })
      sendError(res, 500, { error: "Internal server error", code: "INTERNAL_ERROR" })
      return true
    }
  }

  #sendError(res, statusCode, payload, extraHeaders) {
    sendError(res, statusCode, payload, { headers: extraHeaders })
  }
}
