import http from "node:http"

import { deriveMachineId, MACHINE_ID_PURPOSE } from "./cluster-secret.js"
import { normalizeSetupMachineIdInput } from "./setup-validation.js"

/**
 * Minimal local-only HTTP surface for setup mode before a node is configured.
 */
export class SetupHttpServer {
  /**
   * @param {{
   *   host?: string,
   *   port?: number,
   *   state?: () => Promise<unknown> | unknown,
   *   deriveMachineId?: typeof deriveMachineId,
   *   loadDraft?: () => Promise<unknown>,
   *   saveDraft?: (draft: unknown) => Promise<unknown>
   * }} [options]
   */
  constructor(options = {}) {
    this.options = {
      host: "127.0.0.1",
      port: 0,
      state: () => ({ mode: "setup" }),
      deriveMachineId,
      loadDraft: null,
      saveDraft: null,
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

      if (req.method === "POST" && url.pathname === "/setup/derive-machine-id") {
        const input = normalizeSetupMachineIdInput(await this.#readJson(req))
        const machineId = await this.options.deriveMachineId(input)
        return this.#json(res, 200, {
          machineId: machineId.toString("hex"),
          kdf: "argon2d",
          purpose: MACHINE_ID_PURPOSE
        })
      }

      if (url.pathname === "/setup/draft") {
        this.#requireDraftPersistence()

        if (req.method === "GET") {
          try {
            return this.#json(res, 200, { draft: await this.options.loadDraft() })
          } catch (error) {
            if (error?.code === "ENOENT") {
              error.statusCode = 404
              error.message = "Setup draft not found"
            }
            throw error
          }
        }

        if (req.method === "POST") {
          return this.#json(res, 200, {
            draft: await this.options.saveDraft(await this.#readJson(req))
          })
        }
      }

      return this.#json(res, 404, { error: "Not found" })
    } catch (error) {
      return this.#json(res, error?.statusCode ?? 500, {
        error: error instanceof Error ? error.message : String(error)
      })
    }
  }

  #requireDraftPersistence() {
    if (!this.options.loadDraft || !this.options.saveDraft) {
      const error = new Error("Setup draft storage is unavailable")
      error.statusCode = 409
      throw error
    }
  }

  async #readJson(req) {
    const chunks = []

    for await (const chunk of req) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
    }

    if (chunks.length === 0) {
      return {}
    }

    try {
      return JSON.parse(Buffer.concat(chunks).toString("utf8"))
    } catch {
      const error = new Error("Request body must be valid JSON")
      error.statusCode = 400
      throw error
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
