import { spawn } from "node:child_process"

const timeoutMs = Number(process.env.REPLICORE_TEST_STEP_TIMEOUT_MS ?? "180000")
const steps = [
  {
    label: "raft-engine",
    args: ["--test", "--test-concurrency=1", "test/raft-engine.test.js"]
  },
  {
    label: "config-loader",
    args: ["--test", "--test-concurrency=1", "test/config-loader.test.js"]
  },
  {
    label: "swarm-node",
    args: ["--test", "--test-concurrency=1", "test/swarm-node.test.js"]
  },
  {
    label: "perturbation-a",
    args: [
      "--test",
      "--test-concurrency=1",
      "--test-name-pattern",
      "five-node static membership|five-node cluster stays durable|single surviving node",
      "test/network-perturbation.test.js"
    ]
  },
  {
    label: "perturbation-b",
    args: [
      "--test",
      "--test-concurrency=1",
      "--test-name-pattern",
      "pre-authorized standby|planned node addition",
      "test/network-perturbation.test.js"
    ]
  },
  {
    label: "perturbation-c",
    args: [
      "--test",
      "--test-concurrency=1",
      "--test-name-pattern",
      "node replacement|mismatched membership|offline follower|offline leader|isolated leader|isolated follower|bootstrap outage|restarted follower stays disconnected|rolling restarts|subgroup partition",
      "test/network-perturbation.test.js"
    ]
  },
  {
    label: "perturbation-d",
    args: [
      "--test",
      "--test-concurrency=1",
      "--test-name-pattern",
      "follower write forwarding|concurrent writes|HTTP writes fail|deterministic churn|full-cluster cold restart",
      "test/network-perturbation.test.js"
    ]
  }
]

for (const step of steps) {
  process.stdout.write(`step ${step.label}: ${process.execPath} ${step.args.join(" ")}\n`)
  const exitCode = await runStep(step)
  if (exitCode !== 0) {
    process.exit(exitCode)
  }
}

async function runStep(step) {
  const child = spawn(process.execPath, step.args, {
    stdio: ["ignore", "pipe", "pipe"],
    env: process.env
  })

  child.stdout?.on("data", (chunk) => {
    process.stdout.write(prefixLines(`[${step.label}] `, String(chunk)))
  })
  child.stderr?.on("data", (chunk) => {
    process.stderr.write(prefixLines(`[${step.label} ERR] `, String(chunk)))
  })

  const timer = setTimeout(() => {
    process.stderr.write(
      `[${step.label} ERR] timed out after ${timeoutMs}ms: ${process.execPath} ${step.args.join(" ")}\n`
    )
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
      `[${step.label} ERR] exited with code=${result.code ?? "null"} signal=${result.signal ?? "null"}\n`
    )
  }

  return result.code ?? (result.signal ? 1 : 0)
}

function prefixLines(prefix, text) {
  return text
    .split(/(?<=\n)/)
    .map((line) => (line.length > 0 ? `${prefix}${line}` : line))
    .join("")
}
