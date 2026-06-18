import Hyperswarm from "hyperswarm"

/**
 * Manage live swarm connections and optional test-only connection filtering.
 */
export class SwarmNetwork {
  /**
   * @param {{
   *   bootstrap?: Array<string | { host: string, port: number }>,
   *   topic: Buffer,
   *   keyPair?: { publicKey: Buffer, secretKey: Buffer },
   *   localNodeId: string,
   *   authorizedNodes: Array<{ nodeId: string }>,
   *   isRevokedNode: (nodeId: string) => boolean,
   *   replicateConnection: (conn: any) => void,
   *   networkPolicy?: { allowedNodeIds?: string[], allowConnection?: (localNodeId: string, remoteNodeId: string) => boolean } | null
   * }} options
   */
  constructor(options) {
    this.options = options
    this.swarm = null
    this.discovery = null
    this.connections = new Set()
    this.remoteNodeIdsByConnectionKey = new Map()
    this.observedNodeIdsByConnectionKey = new Map()
    this.networkPolicy = options.networkPolicy ?? null
  }

  async start() {
    if (this.swarm) return

    this.swarm = new Hyperswarm({
      ...(this.options.bootstrap ? { bootstrap: this.options.bootstrap } : {}),
      ...(this.options.keyPair ? { keyPair: this.options.keyPair } : {})
    })
    this.swarm.on("connection", (conn) => {
      this.connections.add(conn)
      conn.once("close", () => {
        this.connections.delete(conn)
      })
      this.#enforceConnectionPolicyForKey(this.#connectionKey(conn.remotePublicKey))
      this.options.replicateConnection(conn)
    })

    this.discovery = this.swarm.join(this.options.topic, { client: true, server: true })
    await this.discovery.flushed()
  }

  async suspend() {
    if (!this.swarm) return

    for (const conn of this.connections) {
      conn.destroy()
    }
    this.connections.clear()

    if (this.discovery) {
      await this.discovery.destroy()
      this.discovery = null
    }

    await this.swarm.destroy()
    this.swarm = null
  }

  async resume() {
    if (this.swarm) return
    await this.start()
  }

  setPolicy(networkPolicy = null) {
    this.networkPolicy = networkPolicy
    this.#enforceConnectionPolicy()
  }

  /**
   * @param {boolean} added
   * @param {string} nodeId
   * @param {{ remotePublicKey?: Buffer, stream?: { remotePublicKey?: Buffer } }} peer
   */
  trackPeer(added, nodeId, peer) {
    const connectionKey = this.#connectionKey(peer.stream?.remotePublicKey ?? peer.remotePublicKey)
    if (!connectionKey || !added) return

    const existing = this.remoteNodeIdsByConnectionKey.get(connectionKey)
    if (!existing) {
      this.remoteNodeIdsByConnectionKey.set(connectionKey, nodeId)
    }

    this.#enforceConnectionPolicyForKey(connectionKey)
  }

  /**
   * Observe the application-level node ID currently using a live transport
   * connection. This is stronger than feed-based peer inference because an
   * unknown learner may replicate voter feeds without being that voter.
   *
   * @param {string} nodeId
   * @param {{ stream?: { remotePublicKey?: Buffer }, remotePublicKey?: Buffer }} peer
   */
  observePeerIdentity(nodeId, peer) {
    const connectionKey = this.#connectionKey(peer.stream?.remotePublicKey ?? peer.remotePublicKey)
    if (!connectionKey) return

    this.observedNodeIdsByConnectionKey.set(connectionKey, nodeId)
    this.#enforceConnectionPolicyForKey(connectionKey)
  }

  clear() {
    this.remoteNodeIdsByConnectionKey.clear()
    this.observedNodeIdsByConnectionKey.clear()
  }

  get connectionCount() {
    return this.connections.size
  }

  get knownPeerPublicKeys() {
    return [...this.connections].map((conn) => conn.remotePublicKey.toString("hex"))
  }

  networkStatus() {
    const learnerCandidates = []
    const connectedNodeIds = new Set(
      [...this.connections].flatMap((conn) => {
        const connectionKey = this.#connectionKey(conn.remotePublicKey)
        const remoteNodeId = this.observedNodeIdsByConnectionKey.get(connectionKey)
        if ((!remoteNodeId || !this.#isAuthorizedNode(remoteNodeId)) && connectionKey) {
          learnerCandidates.push(connectionKey)
        }
        return remoteNodeId ? [remoteNodeId] : []
      })
    )

    const peers = {}
    for (const node of this.options.authorizedNodes) {
      if (node.nodeId === this.options.localNodeId || this.options.isRevokedNode(node.nodeId)) continue
      peers[node.nodeId] = {
        allowed: this.#isConnectionAllowed(node.nodeId),
        connected: connectedNodeIds.has(node.nodeId)
      }
    }

    return {
      policyActive: Boolean(this.networkPolicy),
      allowedNodeIds: this.#allowedNodeIds(),
      learnerCandidates: learnerCandidates.sort(),
      peers
    }
  }

  /**
   * @param {string} nodeId
   */
  isNodeConnected(nodeId) {
    for (const conn of this.connections) {
      const connectionKey = this.#connectionKey(conn.remotePublicKey)
      if (this.observedNodeIdsByConnectionKey.get(connectionKey) === nodeId) {
        return true
      }
    }
    return false
  }

  #allowedNodeIds() {
    return this.options.authorizedNodes
      .map((node) => node.nodeId)
      .filter((nodeId) => nodeId !== this.options.localNodeId)
      .filter((nodeId) => !this.options.isRevokedNode(nodeId))
      .filter((nodeId) => this.#isConnectionAllowed(nodeId))
      .sort()
  }

  /**
   * @param {string | null} nodeId
   */
  #isConnectionAllowed(nodeId) {
    if (!nodeId) return true
    if (!this.networkPolicy?.allowConnection) return true
    return this.networkPolicy.allowConnection(this.options.localNodeId, nodeId) !== false
  }

  #enforceConnectionPolicy() {
    for (const conn of this.connections) {
      this.#enforceConnectionPolicyForKey(this.#connectionKey(conn.remotePublicKey))
    }
  }

  /**
   * @param {string | null} connectionKey
   */
  #enforceConnectionPolicyForKey(connectionKey) {
    if (!connectionKey) return

    const remoteNodeId =
      this.observedNodeIdsByConnectionKey.get(connectionKey) ??
      this.remoteNodeIdsByConnectionKey.get(connectionKey)
    if (!remoteNodeId || this.#isConnectionAllowed(remoteNodeId)) return

    for (const conn of this.connections) {
      if (this.#connectionKey(conn.remotePublicKey) === connectionKey) {
        conn.destroy()
      }
    }
  }

  /**
   * @param {Buffer | null | undefined} publicKey
   */
  #connectionKey(publicKey) {
    return publicKey ? publicKey.toString("hex") : null
  }

  /**
   * @param {string} nodeId
   */
  #isAuthorizedNode(nodeId) {
    return this.options.authorizedNodes.some((node) => node.nodeId === nodeId)
  }
}
