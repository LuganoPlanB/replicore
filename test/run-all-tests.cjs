const { spawnSync } = require("node:child_process")

const testRuns = [
  ["test/config-loader.test.js"],
  ["test/swarm-node.test.js"],
  [
    "--test-name-pattern",
    "five-node static membership|five-node cluster stays durable|single surviving node",
    "test/network-perturbation.test.js"
  ],
  ["--test-name-pattern", "pre-authorized standby", "test/network-perturbation.test.js"],
  ["--test-name-pattern", "planned node addition", "test/network-perturbation.test.js"],
  [
    "--test-name-pattern",
    "node replacement|offline follower|offline leader|isolated leader|rolling restarts",
    "test/network-perturbation.test.js"
  ],
  [
    "--test-name-pattern",
    "follower write forwarding|concurrent writes|HTTP writes fail|deterministic churn|full-cluster cold restart",
    "test/network-perturbation.test.js"
  ]
]

for (const testRun of testRuns) {
  process.stdout.write(`\n> node --test --test-concurrency=1 ${testRun.join(" ")}\n`)
  const result = spawnSync(process.execPath, ["--test", "--test-concurrency=1", ...testRun], {
    encoding: "utf8"
  })

  if (result.stdout) process.stdout.write(result.stdout)
  if (result.stderr) process.stderr.write(result.stderr)

  if (result.status !== 0) {
    process.exit(result.status ?? 1)
  }
}
