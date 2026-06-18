import Protomux from "protomux"
import c from "compact-encoding"

export const JOIN_CONTROL_PROTOCOL = "planb-cleard-join-control-v1"

/**
 * Manage one connection-level control channel for signed join requests and
 * single responses on live Holepunch streams.
 */
export class JoinControl {
  /**
   * @param {{
   *   onChannelOpen?: (session: {
   *     conn: any,
   *     remotePublicKeyHex: string,
   *     sendRequest: (message: any) => void,
   *     sendResponse: (message: any) => void
   *   }) => void,
   *   onJoinRequest?: (session: {
   *     conn: any,
   *     remotePublicKeyHex: string,
   *     sendRequest: (message: any) => void,
   *     sendResponse: (message: any) => void
   *   }, message: any) => void,
   *   onJoinResponse?: (session: {
   *     conn: any,
   *     remotePublicKeyHex: string,
   *     sendRequest: (message: any) => void,
   *     sendResponse: (message: any) => void
   *   }, message: any) => void
   * }} options
   */
  constructor(options) {
    this.options = options
    this.channels = new Set()
  }

  /**
   * @param {any} conn
   */
  attachConnection(conn) {
    const mux = Protomux.from(conn)
    const channel = mux.createChannel({
      protocol: JOIN_CONTROL_PROTOCOL,
      userData: { conn },
      onopen: () => {
        this.options.onChannelOpen?.(session)
      },
      onclose: () => {
        this.channels.delete(channel)
      }
    })
    if (!channel) return null

    const remotePublicKeyHex = conn.remotePublicKey.toString("hex")
    const session = {
      conn,
      remotePublicKeyHex,
      sendRequest: (message) => request.send(message),
      sendResponse: (message) => response.send(message)
    }
    const request = channel.addMessage({
      encoding: c.json,
      onmessage: (message) => {
        this.options.onJoinRequest?.(session, message)
      }
    })
    const response = channel.addMessage({
      encoding: c.json,
      onmessage: (message) => {
        this.options.onJoinResponse?.(session, message)
      }
    })

    this.channels.add(channel)
    channel.open()
    return session
  }

  close() {
    for (const channel of this.channels) {
      channel.close()
    }
    this.channels.clear()
  }
}
