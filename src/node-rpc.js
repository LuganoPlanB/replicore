const RPC_EXTENSION = "planb-cleard-rpc-v1"

/**
 * Manage internal Hypercore RPC extensions for forwarded writes and follower
 * acknowledgements.
 */
export class NodeRpcRouter {
  /**
   * @param {{
   *   localNodeId: string,
   *   timeoutMs: number,
   *   ackDelayMs?: number,
   *   onWriteRequest: (message: { request: any }) => Promise<unknown>,
   *   onWriteAck: (nodeId: string, seq: number) => void
   * }} options
   */
  constructor(options) {
    this.options = options
    this.extensions = new Map()
    this.inflightRequests = new Map()
    this.requestId = 0
    this.closed = false
  }

  /**
   * @param {string} nodeId
   * @param {import("hypercore")} core
   */
  register(nodeId, core) {
    const extension = core.registerExtension(RPC_EXTENSION, {
      encoding: "json",
      onmessage: async (message, peer) => {
        try {
          if (message.type === "write-request") {
            const result = await this.options.onWriteRequest(message)
            this.#extensionFor(message.from)?.send(
              { type: "write-response", requestId: message.requestId, ok: true, result },
              peer
            )
            return
          }

          if (message.type === "write-response") {
            this.#resolveRequest(message)
            return
          }

          if (message.type === "write-ack") {
            this.options.onWriteAck(message.from, message.seq)
          }
        } catch (error) {
          if (message.type !== "write-request") return

          this.#extensionFor(message.from)?.send(
            {
              type: "write-response",
              requestId: message.requestId,
              ok: false,
              error: error instanceof Error ? error.message : String(error)
            },
            peer
          )
        }
      }
    })

    this.extensions.set(nodeId, extension)
    return extension
  }

  /**
   * @param {{ targetNodeId: string, peer: any, request: any }} options
   */
  async forwardWrite({ targetNodeId, peer, request }) {
    if (this.closed) {
      throw new Error("Node is closing")
    }

    const extension = this.#extensionFor(targetNodeId)
    const requestId = `${this.options.localNodeId}-${++this.requestId}`
    const response = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.inflightRequests.delete(requestId)
        reject(new Error(`Timed out forwarding write request ${requestId}`))
      }, this.options.timeoutMs)
      this.inflightRequests.set(requestId, { resolve, reject, timer })
    })
    response.catch(() => {})

    extension?.send(
      {
        type: "write-request",
        requestId,
        from: this.options.localNodeId,
        request
      },
      peer
    )

    return response
  }

  /**
   * @param {{ targetNodeId: string, peer: any, seq: number }} options
   */
  async sendAck({ targetNodeId, peer, seq }) {
    if (this.options.ackDelayMs) {
      await new Promise((resolve) => setTimeout(resolve, this.options.ackDelayMs))
    }
    if (this.closed) return

    this.#extensionFor(targetNodeId)?.send(
      {
        type: "write-ack",
        from: this.options.localNodeId,
        feedKey: targetNodeId,
        seq
      },
      peer
    )
  }

  close(error = new Error("Node is closing")) {
    this.closed = true

    for (const pending of this.inflightRequests.values()) {
      clearTimeout(pending.timer)
      pending.reject(error)
    }
    this.inflightRequests.clear()

    for (const extension of this.extensions.values()) {
      extension.destroy()
    }
    this.extensions.clear()
  }

  /**
   * @param {string} nodeId
   */
  #extensionFor(nodeId) {
    return this.extensions.get(nodeId) ?? null
  }

  /**
   * @param {{ requestId: string, ok: boolean, result?: unknown, error?: string }} message
   */
  #resolveRequest(message) {
    const pending = this.inflightRequests.get(message.requestId)
    if (!pending) return

    clearTimeout(pending.timer)
    this.inflightRequests.delete(message.requestId)
    if (message.ok) pending.resolve(message.result)
    else pending.reject(new Error(message.error))
  }
}
