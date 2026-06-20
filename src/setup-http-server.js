import http from "node:http"

/**
 * Minimal local-only HTTP surface for setup mode before a node is configured.
 */
export class SetupHttpServer {
  /**
   * @param {{
   *   host?: string,
   *   port?: number,
   *   state?: () => Promise<unknown> | unknown
   * }} [options]
   */
  constructor(options = {}) {
    this.options = {
      host: "127.0.0.1",
      port: 0,
      state: () => ({ mode: "setup" }),
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
  }

  get address() {
    const address = this.server?.address()
    if (!address || typeof address === "string") return null
    return address
  }

  async #handle(req, res) {
    try {
      const url = new URL(req.url, "http://127.0.0.1")

      if (req.method === "GET" && url.pathname === "/setup/state") {
        return this.#json(res, 200, await this.options.state())
      }

      return this.#json(res, 404, { error: "Not found" })
    } catch (error) {
      return this.#json(res, error?.statusCode ?? 500, {
        error: error instanceof Error ? error.message : String(error)
      })
    }
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
