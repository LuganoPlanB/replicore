import test from "node:test"
import assert from "node:assert/strict"

import {
  rejectUnknownKeys,
  requirePlainObject,
  requireString,
  validateJsonValue,
  validateKvKey,
  validateKeyspace
} from "../src/http/validation.js"

test("requirePlainObject accepts plain objects and rejects arrays or null", () => {
  assert.deepEqual(requirePlainObject({ ok: true }, "body"), { ok: true })
  assert.throws(() => requirePlainObject(null, "body"), /body must be an object/)
  assert.throws(() => requirePlainObject([], "body"), /body must be an object/)
})

test("rejectUnknownKeys rejects fields outside the allowed set", () => {
  assert.doesNotThrow(() => rejectUnknownKeys({ value: 1 }, ["value"]))
  assert.throws(() => rejectUnknownKeys({ value: 1, extra: true }, ["value"]), /Unknown field: extra/)
})

test("requireString enforces type, length, trimming, and pattern", () => {
  assert.equal(
    requireString("  alpha  ", "keyId", { minLength: 1, maxLength: 16, pattern: /^[a-z]+$/ }),
    "alpha"
  )
  assert.throws(() => requireString(1, "keyId"), /keyId must be a string/)
  assert.throws(() => requireString(" ", "keyId"), /keyId must be between 1 and Infinity characters|at least 1 characters/)
  assert.throws(
    () => requireString("abc/def", "keyId", { pattern: /^[A-Za-z0-9._:-]+$/ }),
    /keyId has an invalid format/
  )
})

test("validateKeyspace accepts the expected identifier pattern", () => {
  assert.equal(validateKeyspace("default"), "default")
  assert.equal(validateKeyspace("admin.logs:v1"), "admin.logs:v1")
  assert.throws(() => validateKeyspace(""), /keyspace must be/)
  assert.throws(() => validateKeyspace("bad space"), /keyspace has an invalid format/)
  assert.throws(() => validateKeyspace("x".repeat(129)), /keyspace must be/)
})

test("validateKvKey rejects slash, control characters, and overlong values", () => {
  assert.equal(validateKvKey("hash:http"), "hash:http")
  assert.throws(() => validateKvKey("bad/key"), /key must not contain \//)
  assert.throws(() => validateKvKey("bad\u0000key"), /key must not contain control characters/)
  assert.throws(() => validateKvKey("x".repeat(513)), /key must be/)
})

test("validateJsonValue accepts JSON primitives, arrays, and objects", () => {
  assert.equal(validateJsonValue(null), null)
  assert.deepEqual(validateJsonValue({ a: [1, true, "ok"] }), { a: [1, true, "ok"] })
})

test("validateJsonValue rejects functions, deep nesting, and overlong strings", () => {
  assert.throws(() => validateJsonValue({ bad: () => {} }), /value must be an object/)
  assert.throws(
    () => validateJsonValue({ a: { b: { c: { d: 1 } } } }, { maxDepth: 2 }),
    /value exceeds maximum depth of 2/
  )
  assert.throws(
    () => validateJsonValue("x".repeat(9), { maxStringLength: 8 }),
    /value strings must be at most 8 characters/
  )
})
