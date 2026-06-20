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
    label: "swarm-node-a",
    args: [
      "--test",
      "--test-concurrency=1",
      "--test-name-pattern",
      "leader operations replicate|new and restarted followers|reconnected follower truncates|followers forward writes|history keeps actor audit|startup election converges|leader-only loss|two-node leader loss|leader loss plus a second|leader writes require|follower heartbeat diagnostics|consensus state persists|authorized HTTP API|HTTP CRUD failure|acknowledged HTTP CRUD",
      "test/swarm-node.test.js"
    ]
  },
  {
    label: "swarm-node-b",
    args: [
      "--test",
      "--test-concurrency=1",
      "--test-name-pattern",
      "same-secret unknown peers|init-cluster voter can admit|independently initialized clusters|learner can join through|learner catches up for reads|healed follower converges|live learner connection|learner can store a valid promotion|learner HTTP CRUD|wrong-secret nodes do not discover",
      "test/swarm-node.test.js"
    ]
  },
  {
    label: "swarm-node-removed",
    args: ["test/swarm-node-removed-membership.run.mjs"]
  },
  {
    label: "swarm-node-replacement",
    args: ["test/swarm-node-replacement-membership.run.mjs"]
  },
  {
    label: "swarm-node-c",
    args: [
      "--test",
      "--test-concurrency=1",
      "--test-name-pattern",
      "snapshot restore rejects tampered|operation validation rejects mismatched|operation validation rejects inconsistent|operation validation rejects revoked|logical log link validation|sync rejects a feed entry with a bad|sync rejects a feed entry with a corrupted|encryption rotation preserves|fresh node can restore current state|restored node can serve snapshot reads|replication status exposes staged|concurrent leader appends|follower keeps a replicated write staged|committed feed progress survives|staged delete stays out|closing a leader rejects|HTTP body size limit|HTTP error payload suppresses",
      "test/swarm-node.test.js"
    ]
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
      "node replacement|mismatched membership|offline follower|offline leader|isolated leader|isolated follower|bootstrap outage|restarted follower keeps cached peer hints|rolling restarts|subgroup partition",
      "test/network-perturbation.test.js"
    ]
  },
  {
    label: "perturbation-d",
    args: [
      "--test",
      "--test-concurrency=1",
      "--test-name-pattern",
      "follower write forwarding|concurrent writes|HTTP witness CRUD|deterministic churn|full-cluster cold restart",
      "test/network-perturbation.test.js"
    ]
  }
]

for (const step of steps) {
  const command = step.cmd ?? process.execPath
  process.stdout.write(`step ${step.label}: ${command} ${step.args.join(" ")}\n`)
  const exitCode = await runStep(step)
  if (exitCode !== 0) {
    process.exit(exitCode)
  }
}

async function runStep(step) {
  const command = step.cmd ?? process.execPath
  const child = spawn(command, step.args, {
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
      `[${step.label} ERR] timed out after ${timeoutMs}ms: ${command} ${step.args.join(" ")}\n`
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
