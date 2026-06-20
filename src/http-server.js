import http from "node:http"

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
      ...options
    }
    this.server = null
    this.sockets = new Set()
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
        const key = decodeURIComponent(match[1])
        const keyspace = url.searchParams.get("keyspace") ?? "default"

        if (req.method === "GET" && match[2] === "/history") {
          this.#authorize(req, keyspace, "read")
          return this.#json(res, 200, {
            key,
            keyspace,
            history: await this.options.node.getHistory(key, { keyspace })
          })
        }

        if (req.method === "GET") {
          this.#authorize(req, keyspace, "read")
          const value = await this.options.node.get(key, { keyspace })
          if (!value) return this.#json(res, 404, { error: "Not found" })
          return this.#json(res, 200, value)
        }

        if (req.method === "PUT") {
          this.#authorize(req, keyspace, "write")
          await this.options.node.qualifyClientWriteEntrypoint()
          const body = await this.#readJson(req)
          const operation = await this.options.node.put(key, body.value, { keyspace })
          return this.#json(res, 200, operation)
        }

        if (req.method === "DELETE") {
          this.#authorize(req, keyspace, "write")
          await this.options.node.qualifyClientWriteEntrypoint()
          const operation = await this.options.node.delete(key, { keyspace })
          return this.#json(res, 200, operation)
        }
      }

      if (req.method === "GET" && url.pathname === "/status/replication") {
        return this.#json(res, 200, await this.options.node.getReplicationStatus())
      }

      if (req.method === "GET" && url.pathname === "/status/writers") {
        return this.#json(res, 200, this.options.node.getWritersStatus())
      }

      if (req.method === "GET" && url.pathname === "/status/leader") {
        return this.#json(res, 200, await this.options.node.getLeaderStatus())
      }

      if (req.method === "GET" && url.pathname === "/admin/snapshot") {
        this.#authorizeAdmin(req)
        return this.#json(res, 200, await this.options.node.createSnapshot())
      }

      if (req.method === "POST" && url.pathname === "/admin/snapshot/import") {
        this.#authorizeAdmin(req)
        const body = await this.#readJson(req, 1024 * 1024)
        await this.options.node.restoreSnapshot(body)
        return this.#json(res, 200, { ok: true })
      }

      if (req.method === "POST" && url.pathname === "/admin/encryption/rotate") {
        this.#authorizeAdmin(req)
        const body = await this.#readJson(req)
        return this.#json(res, 200, {
          ok: true,
          ...this.options.node.rotateEncryptionKey(body.keyId)
        })
      }

      return this.#json(res, 404, { error: "Not found" })
    } catch (error) {
      const status = error?.statusCode ?? 500
      return this.#json(res, status, this.#errorPayload(error))
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

  async #readJson(req, maxSize = this.options.maxBodySize) {
    const contentLength = Number.parseInt(req.headers["content-length"], 10)
    if (Number.isFinite(contentLength) && contentLength > maxSize) {
      const error = new Error("Request body too large")
      error.code = "PAYLOAD_TOO_LARGE"
      error.statusCode = 413
      throw error
    }

    let totalSize = 0
    const chunks = []
    for await (const chunk of req) {
      totalSize += chunk.length
      if (totalSize > maxSize) {
        const error = new Error("Request body too large")
        error.code = "PAYLOAD_TOO_LARGE"
        error.statusCode = 413
        throw error
      }
      chunks.push(chunk)
    }
    if (chunks.length === 0) return {}
    return JSON.parse(Buffer.concat(chunks).toString("utf8"))
  }

  #errorPayload(error) {
    const message = error instanceof Error ? error.message : String(error)
    const payload = {
      error: message
    }
    if (error?.code) payload.code = error.code
    if (error?.refusal) {
      payload.refusal = error.refusal
      payload.code = error.refusal.code
      payload.message = error.refusal.message
      payload.retryable = error.refusal.retryable
      payload.currentTerm = error.refusal.currentTerm
      payload.knownLeaderId = error.refusal.knownLeaderId
      payload.leaderReachable = error.refusal.leaderReachable
      payload.splitStatus = error.refusal.splitStatus
      payload.commitIndex = error.refusal.commitIndex
      payload.membershipVersion = error.refusal.membershipVersion
      payload.role = error.refusal.role
      payload.reconnectHints = error.refusal.reconnectHints
    }
    return payload
  }

  #json(res, statusCode, payload) {
    const body = JSON.stringify(payload)
    res.writeHead(statusCode, {
      "content-type": "application/json; charset=utf-8",
      "content-length": Buffer.byteLength(body),
      "connection": "close"
    })
    res.end(body)
  }
}
