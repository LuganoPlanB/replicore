import test from "node:test"
import assert from "node:assert/strict"

import { NodeRpcRouter } from "../src/node-rpc.js"

test("forwardWrite and requestVote share request lifecycle cleanup", async () => {
  const sentMessages = []
  let onmessage = null
  const router = new NodeRpcRouter({
    localNodeId: "local-node",
    timeoutMs: 100,
    onWriteRequest: async () => ({ ok: true }),
    onVoteRequest: async () => ({ ok: true }),
    onWriteAck() {}
  })

  router.register("remote-node", {
    registerExtension(_name, handlers) {
      onmessage = handlers.onmessage
      return {
        send(message, peer) {
          sentMessages.push({ message, peer })
        },
        destroy() {}
      }
    }
  })

  const peer = { id: "peer-a" }
  const writePromise = router.forwardWrite({
    targetNodeId: "remote-node",
    peer,
    request: { action: "put", key: "hash:test" }
  })

  assert.equal(router.inflightRequests.size, 1)
  const writeRequestId = sentMessages[0].message.requestId
  await onmessage({
    type: "write-response",
    requestId: writeRequestId,
    ok: true,
    result: { committed: true }
  }, peer)
  assert.deepEqual(await writePromise, { committed: true })
  assert.equal(router.inflightRequests.size, 0)

  const votePromise = router.requestVote({
    targetNodeId: "remote-node",
    peer,
    request: {
      term: 2,
      candidateNodeId: "local-node",
      lastLogIndex: 5,
      lastLogTerm: 2,
      membershipVersion: 1
    }
  })

  assert.equal(router.inflightRequests.size, 1)
  const voteRequestId = sentMessages[1].message.requestId
  await onmessage({
    type: "vote-response",
    requestId: voteRequestId,
    ok: true,
    result: { voteGranted: true }
  }, peer)
  assert.deepEqual(await votePromise, { voteGranted: true })
  assert.equal(router.inflightRequests.size, 0)
})

test("timed out RPC requests remove inflight state before rejecting", async () => {
  const router = new NodeRpcRouter({
    localNodeId: "local-node",
    timeoutMs: 10,
    onWriteRequest: async () => ({ ok: true }),
    onVoteRequest: async () => ({ ok: true }),
    onWriteAck() {}
  })

  router.register("remote-node", {
    registerExtension() {
      return {
        send() {},
        destroy() {}
      }
    }
  })

  const request = router.forwardWrite({
    targetNodeId: "remote-node",
    peer: { id: "peer-a" },
    request: { action: "put", key: "hash:test" }
  })

  assert.equal(router.inflightRequests.size, 1)
  await assert.rejects(request, /Timed out forwarding write request/)
  assert.equal(router.inflightRequests.size, 0)
})
