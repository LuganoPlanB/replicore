import test from "node:test"
import assert from "node:assert/strict"

import { createFixedWindowRateLimiter } from "../src/http/rate-limit.js"

test("fixed-window rate limiter allows requests up to the limit", () => {
  let currentTime = 1_000
  const limiter = createFixedWindowRateLimiter({
    max: 2,
    windowMs: 60_000,
    now: () => currentTime
  })

  assert.deepEqual(limiter("127.0.0.1"), {
    allowed: true,
    remaining: 1,
    resetAt: 61_000,
    retryAfterSeconds: 0
  })
  assert.deepEqual(limiter("127.0.0.1"), {
    allowed: true,
    remaining: 0,
    resetAt: 61_000,
    retryAfterSeconds: 0
  })
})

test("fixed-window rate limiter blocks once the limit is reached and resets after the window", () => {
  let currentTime = 5_000
  const limiter = createFixedWindowRateLimiter({
    max: 1,
    windowMs: 10_000,
    now: () => currentTime
  })

  assert.equal(limiter("10.0.0.1").allowed, true)
  const blocked = limiter("10.0.0.1")
  assert.equal(blocked.allowed, false)
  assert.equal(blocked.remaining, 0)
  assert.equal(blocked.resetAt, 15_000)
  assert.equal(blocked.retryAfterSeconds, 10)

  currentTime = 15_001
  const reset = limiter("10.0.0.1")
  assert.equal(reset.allowed, true)
  assert.equal(reset.remaining, 0)
  assert.equal(reset.resetAt, 25_001)
})

test("fixed-window rate limiter tracks keys independently and prunes old windows", () => {
  let currentTime = 0
  const limiter = createFixedWindowRateLimiter({
    max: 1,
    windowMs: 1_000,
    now: () => currentTime
  })

  assert.equal(limiter("10.0.0.1").allowed, true)
  assert.equal(limiter("10.0.0.2").allowed, true)
  assert.equal(limiter("10.0.0.1").allowed, false)

  currentTime = 2_000
  assert.equal(limiter("10.0.0.1").allowed, true)
  assert.equal(limiter("10.0.0.2").allowed, true)
})
