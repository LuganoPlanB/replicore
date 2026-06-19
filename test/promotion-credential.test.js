import test from "node:test"
import assert from "node:assert/strict"

import {
  createPromotionCredential,
  generateIdentity,
  validatePromotionCredential
} from "../src/index.js"

function basePayload(overrides = {}) {
  return {
    v: 1,
    type: "replicore.promotion",
    clusterId: "cluster-a",
    membershipVersion: 3,
    learnerNodeId: "learner-node",
    learnerNoisePublicKey: "noise-public-key",
    targetRole: "voter",
    issuedAt: "2026-06-18T10:00:00.000Z",
    expiresAt: "2026-06-18T11:00:00.000Z",
    nonce: "nonce-1",
    signerNodeId: "signer-node",
    ...overrides
  }
}

function baseContext(signerIdentity, overrides = {}) {
  return {
    clusterId: "cluster-a",
    membershipVersion: 3,
    learnerNodeId: "learner-node",
    learnerNoisePublicKey: "noise-public-key",
    authorizedNodes: [
      {
        nodeId: "signer-node",
        publicKey: signerIdentity.publicKey
      }
    ],
    now: new Date("2026-06-18T10:30:00.000Z"),
    isCaughtUp: true,
    ...overrides
  }
}

test("validatePromotionCredential accepts a valid signed credential", () => {
  const signerIdentity = generateIdentity(Buffer.alloc(32, 1))
  const credential = createPromotionCredential({
    payload: basePayload(),
    signerSecretKey: signerIdentity.secretKey
  })

  const summary = validatePromotionCredential(credential, baseContext(signerIdentity))
  assert.equal(summary.signerNodeId, "signer-node")
  assert.equal(summary.targetRole, "voter")
})

test("validatePromotionCredential rejects expired credentials", () => {
  const signerIdentity = generateIdentity(Buffer.alloc(32, 2))
  const credential = createPromotionCredential({
    payload: basePayload({ expiresAt: "2026-06-18T10:15:00.000Z" }),
    signerSecretKey: signerIdentity.secretKey
  })

  assert.throws(
    () => validatePromotionCredential(credential, baseContext(signerIdentity)),
    /expired/
  )
})

test("validatePromotionCredential rejects credentials from non-voters", () => {
  const signerIdentity = generateIdentity(Buffer.alloc(32, 3))
  const credential = createPromotionCredential({
    payload: basePayload(),
    signerSecretKey: signerIdentity.secretKey
  })

  assert.throws(
    () =>
      validatePromotionCredential(credential, {
        ...baseContext(signerIdentity),
        authorizedNodes: []
      }),
    /authorized voter/
  )
})

test("validatePromotionCredential rejects wrong learner identity or role", () => {
  const signerIdentity = generateIdentity(Buffer.alloc(32, 4))

  const wrongLearner = createPromotionCredential({
    payload: basePayload({ learnerNodeId: "other-learner" }),
    signerSecretKey: signerIdentity.secretKey
  })
  assert.throws(
    () => validatePromotionCredential(wrongLearner, baseContext(signerIdentity)),
    /learnerNodeId/
  )

  const wrongRole = createPromotionCredential({
    payload: basePayload({ targetRole: "learner" }),
    signerSecretKey: signerIdentity.secretKey
  })
  assert.throws(
    () => validatePromotionCredential(wrongRole, baseContext(signerIdentity)),
    /targetRole/
  )
})

test("validatePromotionCredential rejects wrong membership version and replay", () => {
  const signerIdentity = generateIdentity(Buffer.alloc(32, 5))
  const credential = createPromotionCredential({
    payload: basePayload(),
    signerSecretKey: signerIdentity.secretKey
  })

  assert.throws(
    () =>
      validatePromotionCredential(credential, {
        ...baseContext(signerIdentity),
        membershipVersion: 4
      }),
    /membershipVersion/
  )

  assert.throws(
    () =>
      validatePromotionCredential(credential, {
        ...baseContext(signerIdentity),
        seenNonces: new Set(["nonce-1"])
      }),
    /nonce was already submitted/
  )
})

test("validatePromotionCredential rejects invalid signatures and promotion before catch-up", () => {
  const signerIdentity = generateIdentity(Buffer.alloc(32, 6))
  const credential = createPromotionCredential({
    payload: basePayload(),
    signerSecretKey: signerIdentity.secretKey
  })

  assert.throws(
    () =>
      validatePromotionCredential(
        {
          ...credential,
          signature: "x".repeat(credential.signature.length)
        },
        baseContext(signerIdentity)
      ),
    /signature is invalid/
  )

  assert.throws(
    () =>
      validatePromotionCredential(credential, {
        ...baseContext(signerIdentity),
        isCaughtUp: false
      }),
    /catch up/
  )
})
