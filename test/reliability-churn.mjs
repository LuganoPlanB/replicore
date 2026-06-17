import { spawnSync } from "node:child_process"

const rounds = Number(process.env.REPLICORE_TEST_ROUNDS ?? process.env.RELIABILITY_ROUNDS ?? "2")
const timeoutMs = Number(process.env.REPLICORE_TEST_TIMEOUT_MS ?? "180000")
const testConcurrency = process.env.REPLICORE_NODE_TEST_CONCURRENCY ?? "1"
const pattern =
  process.env.REPLICORE_TEST_PATTERN ??
  "offline leader|isolated leader|isolated follower|concurrent writes|bootstrap outage|restarted follower stays disconnected|follower write forwarding|deterministic churn"
const file = process.env.REPLICORE_TEST_FILE ?? "test/network-perturbation.test.js"

for (let round = 1; round <= rounds; round += 1) {
  process.stdout.write(
    `round ${round}/${rounds}: timeoutMs=${timeoutMs} testConcurrency=${testConcurrency} pattern=${pattern}\n`
  )

  const result = spawnSync(
    process.execPath,
    ["--test", "--test-concurrency", testConcurrency, "--test-name-pattern", pattern, file],
    {
      encoding: "utf8",
      timeout: timeoutMs
    }
  )

  if (result.stdout) process.stdout.write(result.stdout)
  if (result.stderr) process.stderr.write(result.stderr)

  if (result.error?.name === "Error" && result.error.message.includes("ETIMEDOUT")) {
    process.stderr.write(
      `reliability runner timed out after ${timeoutMs}ms: ${process.execPath} --test --test-concurrency ${testConcurrency} --test-name-pattern ${pattern} ${file}\n`
    )
  }

  if (result.status !== 0) {
    process.exit(result.status ?? 1)
  }
}
