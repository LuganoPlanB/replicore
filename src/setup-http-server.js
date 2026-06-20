import http from "node:http"
import { readFile } from "node:fs/promises"
import path from "node:path"

import { deriveMachineId, MACHINE_ID_PURPOSE } from "./cluster-secret.js"
import { readJsonBody } from "./http/body.js"
import { sendError, sendJson } from "./http/response.js"
import { listNetworkInterfaces } from "./setup/network-interfaces.js"
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
   *   listNetworkInterfaces?: typeof listNetworkInterfaces,
   *   uiRoot?: string | null,
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
      listNetworkInterfaces,
      uiRoot: null,
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
        return this.#sendJson(res, 200, await this.options.state())
      }

      if (req.method === "POST" && url.pathname === "/setup/derive-machine-id") {
        const input = normalizeSetupMachineIdInput(await this.#readJson(req, 16 * 1024))
        const machineId = await this.options.deriveMachineId(input)
        return this.#sendJson(res, 200, {
          machineId: machineId.toString("hex"),
          kdf: "argon2d",
          purpose: MACHINE_ID_PURPOSE
        })
      }

      if (req.method === "GET" && url.pathname === "/setup/interfaces") {
        return this.#sendJson(res, 200, {
          interfaces: await this.options.listNetworkInterfaces()
        })
      }

      if (url.pathname === "/setup/draft") {
        this.#requireDraftPersistence()

        if (req.method === "GET") {
          try {
            return this.#sendJson(res, 200, { draft: await this.options.loadDraft() })
          } catch (error) {
            if (error?.code === "ENOENT") {
              return this.#sendJson(res, 200, { draft: null })
            }
            throw error
          }
        }

        if (req.method === "POST") {
          return this.#sendJson(res, 200, {
            draft: await this.options.saveDraft(await this.#readJson(req, 64 * 1024))
          })
        }
      }

      if (req.method === "GET") {
        const asset = await this.#readStaticAsset(url.pathname)
        if (asset) {
          return this.#send(res, 200, asset.contentType, asset.body)
        }
      }

      return this.#sendJson(res, 404, { error: "Not found" })
    } catch (error) {
      return this.#sendError(res, error?.statusCode ?? 500, {
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

  async #readStaticAsset(pathname) {
    if (!this.options.uiRoot) return null

    const uiRoot = path.resolve(this.options.uiRoot)
    const decodedPath = decodeURIComponent(pathname)
    const relativePath =
      decodedPath === "/" ? "index.html" : decodedPath.replace(/^\/+/, "")
    const candidatePath = path.resolve(uiRoot, relativePath)

    if (!candidatePath.startsWith(`${uiRoot}${path.sep}`) && candidatePath !== path.join(uiRoot, "index.html")) {
      return null
    }

    try {
      return {
        body: await readFile(candidatePath),
        contentType: contentTypeForPath(candidatePath)
      }
    } catch (error) {
      if (error?.code !== "ENOENT") throw error
    }

    if (!decodedPath.includes(".")) {
      const indexPath = path.join(uiRoot, "index.html")

      try {
        return {
          body: await readFile(indexPath),
          contentType: "text/html; charset=utf-8"
        }
      } catch (error) {
        if (error?.code !== "ENOENT") throw error
      }

      return {
        body: Buffer.from(fallbackSetupHtml(), "utf8"),
        contentType: "text/html; charset=utf-8"
      }
    }

    return null
  }

  async #readJson(req, maxBytes) {
    return readJsonBody(req, {
      maxBytes,
      contentType: req.headers["content-type"] ?? "",
      allowEmpty: true
    })
  }

  #sendJson(res, statusCode, payload) {
    sendJson(res, statusCode, payload)
  }

  #sendError(res, statusCode, payload) {
    sendError(res, statusCode, payload)
  }

  #send(res, statusCode, contentType, body) {
    res.writeHead(statusCode, {
      "content-type": contentType,
      "content-length": Buffer.byteLength(body),
      "connection": "close"
    })
    res.end(body)
  }
}

function contentTypeForPath(filePath) {
  switch (path.extname(filePath)) {
    case ".html":
      return "text/html; charset=utf-8"
    case ".css":
      return "text/css; charset=utf-8"
    case ".js":
      return "text/javascript; charset=utf-8"
    case ".json":
      return "application/json; charset=utf-8"
    case ".svg":
      return "image/svg+xml"
    default:
      return "application/octet-stream"
  }
}

function fallbackSetupHtml() {
  return [
    "<!doctype html>",
    "<html lang=\"en\">",
    "<head>",
    "<meta charset=\"utf-8\" />",
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />",
    "<title>Replicore Setup</title>",
    "<link rel=\"icon\" href=\"data:,\" />",
    "</head>",
    "<body>",
    "<main><h1>Replicore Setup</h1><p>Setup UI assets are not built yet.</p></main>",
    "</body>",
    "</html>"
  ].join("")
}
