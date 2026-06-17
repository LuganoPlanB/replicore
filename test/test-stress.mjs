import { spawn } from "node:child_process"

const rounds = Number(process.env.REPLICORE_TEST_ROUNDS ?? "1")
const workers = Number(process.env.REPLICORE_TEST_CONCURRENCY ?? "1")
const timeoutMs = Number(process.env.REPLICORE_TEST_TIMEOUT_MS ?? "120000")
const testConcurrency = process.env.REPLICORE_NODE_TEST_CONCURRENCY ?? "1"
const pattern =
  process.env.REPLICORE_TEST_PATTERN ??
  "offline leader|isolated leader|concurrent writes|deterministic churn"
const file = process.env.REPLICORE_TEST_FILE ?? "test/network-perturbation.test.js"

for (let round = 1; round <= rounds; round += 1) {
  process.stdout.write(
    `round ${round}/${rounds}: workers=${workers} timeoutMs=${timeoutMs} testConcurrency=${testConcurrency} pattern=${pattern}\n`
  )

  const results = await Promise.all(
    Array.from({ length: workers }, (_, index) => runWorker(round, index + 1))
  )
  const failed = results.find((result) => result.exitCode !== 0)
  if (failed) {
    process.exit(failed.exitCode ?? 1)
  }
}

async function runWorker(round, worker) {
  const args = ["--test", "--test-concurrency", testConcurrency]
  if (pattern) args.push("--test-name-pattern", pattern)
  args.push(file)

  const child = spawn(process.execPath, args, {
    stdio: ["ignore", "pipe", "pipe"],
    env: process.env
  })
  const prefix = `[r${round}/w${worker}]`

  child.stdout?.on("data", (chunk) => {
    process.stdout.write(prefixLines(`${prefix} `, String(chunk)))
  })
  child.stderr?.on("data", (chunk) => {
    process.stderr.write(prefixLines(`${prefix} ERR `, String(chunk)))
  })

  const timer = setTimeout(() => {
    process.stderr.write(`${prefix} ERR timed out after ${timeoutMs}ms: ${process.execPath} ${args.join(" ")}\n`)
    child.kill("SIGTERM")
    setTimeout(() => child.kill("SIGKILL"), 5000).unref()
  }, timeoutMs)
  timer.unref()

  const result = await new Promise((resolve) => {
    child.once("exit", (code, signal) => {
      clearTimeout(timer)
      resolve({ code, signal })
    })
  })

  if (result.code !== 0) {
    process.stderr.write(
      `${prefix} ERR exited with code=${result.code ?? "null"} signal=${result.signal ?? "null"}: ${process.execPath} ${args.join(" ")}\n`
    )
  }

  return {
    worker,
    exitCode: result.code ?? (result.signal ? 1 : 0),
    signal: result.signal
  }
}

function prefixLines(prefix, text) {
  return text
    .split(/(?<=\n)/)
    .map((line) => (line.length > 0 ? `${prefix}${line}` : line))
    .join("")
}
