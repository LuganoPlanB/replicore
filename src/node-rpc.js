const RPC_EXTENSION = "planb-cleard-rpc-v1"

/**
 * Manage internal Hypercore RPC extensions for forwarded writes, leader-log
 * append notifications, and follower acknowledgements.
 */
export class NodeRpcRouter {
  /**
 * @param {{
 *   localNodeId: string,
 *   timeoutMs: number,
 *   ackDelayMs?: number,
 *   onPeerIdentity?: (nodeId: string, peer: any) => void,
 *   onWriteRequest: (message: { request: any }) => Promise<unknown>,
 *   onVoteRequest?: (message: { term: number, candidateNodeId: string, lastLogIndex: number, lastLogTerm: number, membershipVersion?: number }) => Promise<unknown>,
 *   onAppendEntries?: (message: { term: number, leaderNodeId: string, prevLogIndex: number, prevLogTerm: number, prevLogHash: string | null, logLength: number, entryHash: string | null, leaderCommitIndex: number }) => Promise<void>,
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
          if (typeof message?.from === "string") {
            this.options.onPeerIdentity?.(message.from, peer)
          }

          if (message.type === "hello") {
            return
          }

          if (message.type === "write-request") {
            const result = await this.options.onWriteRequest(message)
            this.#extensionFor(message.from)?.send(
              { type: "write-response", requestId: message.requestId, ok: true, result },
              peer
            )
            return
          }

          if (message.type === "vote-request") {
            const result = await this.options.onVoteRequest?.(message)
            this.#extensionFor(message.from)?.send(
              { type: "vote-response", requestId: message.requestId, ok: true, result },
              peer
            )
            return
          }

          if (message.type === "append-entries") {
            await this.options.onAppendEntries?.(message)
            return
          }

          if (message.type === "write-response") {
            this.#resolveRequest(message)
            return
          }

          if (message.type === "vote-response") {
            this.#resolveRequest(message)
            return
          }

          if (message.type === "write-ack") {
            this.options.onWriteAck(message.from, message.seq)
          }
        } catch (error) {
          if (message.type !== "write-request" && message.type !== "vote-request") return

          this.#extensionFor(message.from)?.send(
            {
              type: message.type === "vote-request" ? "vote-response" : "write-response",
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
    return this.#sendRequest(
      extension,
      peer,
      {
        type: "write-request",
        from: this.options.localNodeId,
        request
      },
      { timeoutMessage: (requestId) => `Timed out forwarding write request ${requestId}` }
    )
  }

  /**
   * @param {{ targetNodeId: string, peer: any, request: { term: number, candidateNodeId: string, lastLogIndex: number, lastLogTerm: number, membershipVersion?: number } }} options
   */
  async requestVote({ targetNodeId, peer, request }) {
    if (this.closed) {
      throw new Error("Node is closing")
    }

    const extension = this.#extensionFor(targetNodeId)
    return this.#sendRequest(
      extension,
      peer,
      {
        type: "vote-request",
        from: this.options.localNodeId,
        ...request
      },
      { timeoutMessage: (requestId) => `Timed out forwarding vote request ${requestId}` }
    )
  }

  /**
   * @param {{
   *   targetNodeId: string,
   *   peer: any,
   *   request: {
   *     term: number,
   *     leaderNodeId: string,
   *     prevLogIndex: number,
   *     prevLogTerm: number,
   *     prevLogHash: string | null,
   *     logLength: number,
   *     entryHash: string | null,
   *     leaderCommitIndex: number
   *   }
   * }} options
   */
  sendAppendEntries({ targetNodeId, peer, request }) {
    if (this.closed) return

    this.#extensionFor(targetNodeId)?.send(
      {
        type: "append-entries",
        from: this.options.localNodeId,
        ...request
      },
      peer
    )
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

  /**
   * @param {{ targetNodeId: string, peer: any }} options
   */
  sendHello({ targetNodeId, peer }) {
    if (this.closed) return

    this.#extensionFor(targetNodeId)?.send(
      {
        type: "hello",
        from: this.options.localNodeId
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
   * Track one RPC request until timeout or response.
   *
   * @param {{ send?: (message: any, peer: any) => void } | null} extension
   * @param {any} peer
   * @param {Record<string, unknown>} message
   * @param {{ timeoutMessage: (requestId: string) => string }} options
   */
  #sendRequest(extension, peer, message, options) {
    const requestId = `${this.options.localNodeId}-${++this.requestId}`
    if (!extension || typeof extension.send !== "function") {
      const error = new Error(`${options.timeoutMessage(requestId)}: RPC extension unavailable`)
      error.code = "RPC_EXTENSION_UNAVAILABLE"
      error.retryable = true
      return Promise.reject(error)
    }

    const response = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.inflightRequests.delete(requestId)
        reject(new Error(options.timeoutMessage(requestId)))
      }, this.options.timeoutMs)
      this.inflightRequests.set(requestId, { resolve, reject, timer })
    })
    response.catch(() => {})

    extension.send(
      {
        ...message,
        requestId
      },
      peer
    )

    return response
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
