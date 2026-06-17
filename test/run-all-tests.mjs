import { spawnSync } from "node:child_process"

const testFiles = [
  "test/config-loader.test.js",
  "test/swarm-node.test.js",
  "test/network-perturbation.test.js"
]

for (const testFile of testFiles) {
  const result = spawnSync(process.execPath, ["--test", "--test-concurrency=1", testFile], {
    encoding: "utf8"
  })

  if (result.stdout) process.stdout.write(result.stdout)
  if (result.stderr) process.stderr.write(result.stderr)

  if (result.status !== 0) {
    process.exit(result.status ?? 1)
  }
}
