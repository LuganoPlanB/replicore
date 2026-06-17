import { spawnSync } from "node:child_process"

const rounds = Number(process.env.RELIABILITY_ROUNDS ?? "2")
const pattern =
  "offline leader|isolated leader|isolated follower|concurrent writes|deterministic churn"

for (let round = 1; round <= rounds; round += 1) {
  process.stdout.write(`round ${round}/${rounds}: ${pattern}\n`)

  const result = spawnSync(
    process.execPath,
    ["--test", "--test-concurrency=1", "--test-name-pattern", pattern, "test/network-perturbation.test.js"],
    { encoding: "utf8" }
  )

  if (result.stdout) process.stdout.write(result.stdout)
  if (result.stderr) process.stderr.write(result.stderr)

  if (result.status !== 0) {
    process.exit(result.status ?? 1)
  }
}
