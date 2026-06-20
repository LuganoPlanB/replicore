import { spawn } from "node:child_process"
import { readFile } from "node:fs/promises"

const timeoutMs = Number(process.env.REPLICORE_TEST_STEP_TIMEOUT_MS ?? "180000")

// Network-heavy perturbation scenarios run in isolated child processes so a
// single lifecycle stall cannot pin the whole test harness. Shard patterns
// must resolve to discovered test titles so renamed tests cannot silently drop
// out of CI coverage.
const shards = [
  {
    label: "raft-engine",
    file: "test/raft-engine.test.js",
    args: ["--test", "--test-concurrency=1"]
  },
  {
    label: "config-loader",
    file: "test/config-loader.test.js",
    args: ["--test", "--test-concurrency=1"]
  },
  {
    label: "http-validation",
    file: "test/http-validation.test.js",
    args: ["--test", "--test-concurrency=1"]
  },
  {
    label: "swarm-node-a",
    file: "test/swarm-node.test.js",
    args: ["--test", "--test-concurrency=1"],
    patterns: [
      "leader operations replicate to followers and rebuild after restart",
      "new and restarted followers read the same authoritative leader-log prefix",
      "a reconnected follower truncates a divergent authoritative tail and replays the exact leader suffix",
      "followers forward writes to the computed leader and become split-fenced when that leader disappears",
      "history keeps actor audit data and blocks new committed entries while a survivor is split-fenced",
      "startup election converges on a single leader with a persisted term",
      "leader-only loss in a three-voter cluster elects a replacement after witness verification",
      "two-node leader loss stays split-fenced and does not autonomously reelect",
      "leader loss plus a second missing voter keeps the remaining voters split-fenced",
      "leader writes require a voter majority, not just one follower acknowledgement",
      "follower heartbeat diagnostics do not grant leader authority",
      "consensus state persists votedFor across restart",
      "authorized HTTP API forwards writes and exposes status routes",
      "HTTP CRUD failure before commit stays absent after leader restart",
      "acknowledged HTTP CRUD survives leader restart with one committed history entry"
    ]
  },
  {
    label: "swarm-node-b",
    file: "test/swarm-node.test.js",
    args: ["--test", "--test-concurrency=1"],
    patterns: [
      "same-secret unknown peers are surfaced as learner candidates without joining membership",
      "an init-cluster voter can admit and replicate to a secret-first learner",
      "a duplicate machine identity is rejected during learner admission",
      "two independently initialized clusters with the same secret do not auto-merge",
      "a learner can join through the leader control channel, catch up, and later become a live voter",
      "a learner catches up for reads, stays out of quorum, and rejects writes",
      "a healed follower converges by recovery pull without requiring a fresh leader write",
      "a live learner connection does not satisfy voter durability after a follower stops",
      "a learner can store a valid promotion credential without becoming a voter yet",
      "learner HTTP CRUD stays read-only while serving caught-up reads",
      "wrong-secret nodes do not discover or mirror cluster CRUD state"
    ]
  },
  {
    label: "swarm-node-removed",
    cmd: process.execPath,
    args: ["test/swarm-node-removed-membership.run.mjs"]
  },
  {
    label: "swarm-node-replacement",
    cmd: process.execPath,
    args: ["test/swarm-node-replacement-membership.run.mjs"]
  },
  {
    label: "swarm-node-c",
    file: "test/swarm-node.test.js",
    args: ["--test", "--test-concurrency=1"],
    patterns: [
      "snapshot restore rejects tampered integrity metadata",
      "operation validation rejects mismatched feed metadata",
      "operation validation rejects inconsistent logical log metadata",
      "operation validation rejects revoked writers",
      "logical log link validation rejects previous hash mismatch",
      "sync rejects a feed entry with a bad previous hash",
      "sync rejects a feed entry with a corrupted signature",
      "encryption rotation preserves existing reads and exposes revoked writer state",
      "a fresh node can restore current state from a snapshot",
      "a restored node can serve snapshot reads before rejoin and later catch up under degraded topology",
      "replication status exposes staged entries without exposing committed CRUD state",
      "concurrent leader appends keep signed sequence equal to feed slot",
      "a follower keeps a replicated write staged until the leader advertises the commit watermark",
      "committed feed progress survives follower restart after watermark-driven apply",
      "a staged delete stays out of reads, history, and snapshots until committed",
      "closing a leader rejects a delayed durability wait without leaving a live timer behind",
      "HTTP body size limit enforces maximum request body size through Content-Length header",
      "HTTP malformed JSON returns 400 without calling node handlers and later valid requests still work",
      "HTTP error payload suppresses internal cluster state in refusal responses",
      "HTTP rate limiting returns 429 after exceeding per-token write budget"
    ]
  },
  {
    label: "perturbation-a",
    file: "test/network-perturbation.test.js",
    args: ["--test", "--test-concurrency=1"],
    patterns: [
      "five-node static membership supports forwarding, replication, and deletes",
      "five-node cluster stays durable when two non-leader followers go offline",
      "single surviving node serves reads but blocks writes until a follower returns"
    ]
  },
  {
    label: "perturbation-b",
    file: "test/network-perturbation.test.js",
    args: ["--test", "--test-concurrency=1"],
    patterns: [
      "pre-authorized standby node can join later and catch up without config changes",
      "planned node addition works after full-cluster restart with expanded membership",
      "joint-consensus learner promotion blocks when only one side of the joint quorum is available",
      "joint-consensus voter removal blocks when only one side of the joint quorum is available"
    ]
  },
  {
    label: "perturbation-c",
    file: "test/network-perturbation.test.js",
    args: ["--test", "--test-concurrency=1"],
    patterns: [
      "node replacement via revocation and new identity restores service without hot membership changes",
      "node replacement catches up a retained stale node after long absence",
      "mismatched membership config blocks degraded writes conservatively",
      "offline follower misses writes, then catches up with full history after restart",
      "offline leader yields failover writes and catches up cleanly after restart",
      "subgroup partition exposes active policy and blocks cross-group links",
      "isolated leader blocks writes on both sides until heal and followers become split-fenced",
      "subgroup partition blocks writes on both sides until heal when the leader side is lost",
      "isolated follower serves stale reads until heal and status shows stale connectivity",
      "bootstrap outage after discovery does not break writes for already connected peers",
      "restarted follower keeps cached peer hints but stays disconnected while bootstrap remains unavailable",
      "rolling restarts across a four-node cluster preserve availability and convergence"
    ]
  },
  {
    label: "perturbation-d",
    file: "test/network-perturbation.test.js",
    args: ["--test", "--test-concurrency=1"],
    patterns: [
      "follower write forwarding pauses during leader loss and recovers after replacement or return",
      "split-fenced follower restart preserves write refusal until the leader returns",
      "concurrent writes during split fencing do not create accepted operations before the leader returns",
      "failed local write stays uncommitted across leader restart and later healthy writes",
      "HTTP witness CRUD keeps writes on the leader-connected side during a split",
      "deterministic churn preserves convergence and write outcome invariants",
      "full-cluster cold restart from persisted data directories rebuilds state and accepts new writes"
    ]
  }
]

const requiredTestsByFile = new Map([
  [
    "test/network-perturbation.test.js",
    [
      "offline follower misses writes, then catches up with full history after restart",
      "offline leader yields failover writes and catches up cleanly after restart",
      "isolated leader blocks writes on both sides until heal and followers become split-fenced",
      "isolated follower serves stale reads until heal and status shows stale connectivity",
      "bootstrap outage after discovery does not break writes for already connected peers",
      "restarted follower keeps cached peer hints but stays disconnected while bootstrap remains unavailable",
      "rolling restarts across a four-node cluster preserve availability and convergence"
    ]
  ],
  [
    "test/swarm-node.test.js",
    [
      "a restored node can serve snapshot reads before rejoin and later catch up under degraded topology"
    ]
  ]
])

const excludedTestsByFile = new Map()

const discoveredTestsByFile = new Map()
for (const shard of shards) {
  if (!shard.file || discoveredTestsByFile.has(shard.file)) continue
  discoveredTestsByFile.set(shard.file, await discoverTestTitles(shard.file))
}

const selectedTitlesByShard = validateShards(shards, discoveredTestsByFile)
printShardSummary(shards, selectedTitlesByShard)

for (const shard of shards) {
  const command = shard.cmd ?? process.execPath
  const args = buildArgs(shard)
  process.stdout.write(`step ${shard.label}: ${command} ${args.join(" ")}\n`)
  const exitCode = await runStep(shard, args, selectedTitlesByShard.get(shard.label) ?? [])
  if (exitCode !== 0) {
    process.exit(exitCode)
  }
}

function buildArgs(shard) {
  const args = [...shard.args]
  if (shard.patterns?.length) {
    args.push("--test-name-pattern", buildExactPattern(shard.patterns))
  }
  if (shard.file) {
    args.push(shard.file)
  }
  return args
}

function buildExactPattern(patterns) {
  const escapedPatterns = patterns.map((pattern) => escapeRegex(pattern))
  return `^(?:${escapedPatterns.join("|")})$`
}

function validateShards(shards, discoveredTestsByFile) {
  const selectedTitlesByShard = new Map()
  const shardCoverageByFile = new Map()

  for (const shard of shards) {
    const discoveredTitles = discoveredTestsByFile.get(shard.file) ?? []
    if (!shard.file) continue

    if (!shard.patterns?.length) {
      selectedTitlesByShard.set(shard.label, discoveredTitles)
      continue
    }

    const matcher = new RegExp(buildExactPattern(shard.patterns))
    const selectedTitles = discoveredTitles.filter((title) => matcher.test(title))

    if (selectedTitles.length === 0) {
      failValidation(
        `Shard ${shard.label} matches no tests in ${shard.file}.`,
        [
          `pattern: ${buildExactPattern(shard.patterns)}`,
          `available tests: ${formatTitleList(discoveredTitles)}`
        ]
      )
    }

    selectedTitlesByShard.set(shard.label, selectedTitles)

    let coverage = shardCoverageByFile.get(shard.file)
    if (!coverage) {
      coverage = new Map()
      shardCoverageByFile.set(shard.file, coverage)
    }
    for (const title of selectedTitles) {
      const shardLabels = coverage.get(title) ?? []
      shardLabels.push(shard.label)
      coverage.set(title, shardLabels)
    }
  }

  for (const [file, requiredTitles] of requiredTestsByFile) {
    const discoveredTitles = discoveredTestsByFile.get(file) ?? []
    const coverage = shardCoverageByFile.get(file) ?? new Map()

    for (const title of requiredTitles) {
      if (!discoveredTitles.includes(title)) {
        failValidation(`Required test title is not declared in ${file}.`, [`missing: ${title}`])
      }
      if (!coverage.has(title)) {
        failValidation(`Required test title is not selected by any shard for ${file}.`, [`missing: ${title}`])
      }
    }
  }

  for (const [file, coverage] of shardCoverageByFile) {
    const discoveredTitles = discoveredTestsByFile.get(file) ?? []
    const excludedTitles = excludedTestsByFile.get(file) ?? new Map()
    const duplicates = []
    for (const [title, shardLabels] of coverage) {
      if (shardLabels.length > 1) {
        duplicates.push(`${title} -> ${shardLabels.join(", ")}`)
      }
      if (excludedTitles.has(title)) {
        failValidation(`Excluded test title is still selected by a shard for ${file}.`, [`${title} -> ${shardLabels.join(", ")}`])
      }
    }
    if (duplicates.length > 0) {
      failValidation(`Duplicate shard selection detected for ${file}.`, duplicates)
    }

    const missingTitles = discoveredTitles.filter((title) => !coverage.has(title) && !excludedTitles.has(title))
    if (missingTitles.length > 0) {
      failValidation(`Discovered tests are not assigned to any shard for ${file}.`, missingTitles)
    }
  }

  return selectedTitlesByShard
}

async function discoverTestTitles(file) {
  const source = await readFile(file, "utf8")
  const titles = []
  let offset = 0

  while (offset < source.length) {
    const testIndex = source.indexOf("test(", offset)
    if (testIndex === -1) break
    let cursor = testIndex + "test(".length

    while (cursor < source.length && /\s/.test(source[cursor])) {
      cursor += 1
    }

    const quote = source[cursor]
    if (quote !== '"' && quote !== "'" && quote !== "`") {
      offset = cursor
      continue
    }

    const parsed = parseQuotedString(source, cursor)
    if (parsed !== null) {
      titles.push(parsed.value)
      offset = parsed.nextIndex
      continue
    }

    offset = cursor + 1
  }

  return titles
}

function parseQuotedString(source, startIndex) {
  const quote = source[startIndex]
  let cursor = startIndex + 1
  let value = ""

  while (cursor < source.length) {
    const char = source[cursor]

    if (char === "\\") {
      const next = source[cursor + 1]
      if (next === undefined) return null
      value += char + next
      cursor += 2
      continue
    }

    if (char === quote) {
      try {
        return {
          value: JSON.parse(`"${escapeJsonString(value)}"`),
          nextIndex: cursor + 1
        }
      } catch {
        return null
      }
    }

    value += char
    cursor += 1
  }

  return null
}

function escapeJsonString(value) {
  return value
    .replaceAll("\\", "\\\\")
    .replaceAll('"', '\\"')
    .replaceAll("\n", "\\n")
    .replaceAll("\r", "\\r")
    .replaceAll("\t", "\\t")
    .replaceAll("\f", "\\f")
    .replaceAll("\b", "\\b")
}

function printShardSummary(shards, selectedTitlesByShard) {
  for (const shard of shards) {
    const selectedTitles = selectedTitlesByShard.get(shard.label)
    const summary = [
      `[suite] ${shard.label}:`,
      shard.file ?? shard.args[0],
      `selected=${describeSelection(shard, selectedTitles)}`,
      `timeout=${timeoutMs}ms`
    ]
    process.stdout.write(`${summary.join(" ")}\n`)
  }

  for (const [file, excludedTitles] of excludedTestsByFile) {
    for (const [title, reason] of excludedTitles) {
      process.stdout.write(`[suite] excluded: ${file} :: ${title} :: ${reason}\n`)
    }
  }
}

function describeSelection(shard, selectedTitles) {
  if (!shard.file) return "script"
  if (!shard.patterns?.length) return `all(${selectedTitles?.length ?? 0})`
  return String(selectedTitles?.length ?? 0)
}

async function runStep(shard, args, selectedTitles) {
  const command = shard.cmd ?? process.execPath
  const child = spawn(command, args, {
    stdio: ["ignore", "pipe", "pipe"],
    env: process.env
  })

  child.stdout?.on("data", (chunk) => {
    process.stdout.write(prefixLines(`[${shard.label}] `, String(chunk)))
  })
  child.stderr?.on("data", (chunk) => {
    process.stderr.write(prefixLines(`[${shard.label} ERR] `, String(chunk)))
  })

  const timer = setTimeout(() => {
    process.stderr.write(
      `[${shard.label} ERR] timed out after ${timeoutMs}ms: ${command} ${args.join(" ")}\n`
    )
    printSelectedTitles(shard.label, selectedTitles)
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
      `[${shard.label} ERR] exited with code=${result.code ?? "null"} signal=${result.signal ?? "null"}\n`
    )
    printSelectedTitles(shard.label, selectedTitles)
  }

  return result.code ?? (result.signal ? 1 : 0)
}

function printSelectedTitles(label, selectedTitles) {
  if (!selectedTitles.length) return
  process.stderr.write(`[${label} ERR] selected tests:\n`)
  for (const title of selectedTitles) {
    process.stderr.write(`[${label} ERR] - ${title}\n`)
  }
}

function failValidation(message, details) {
  process.stderr.write(`[suite ERR] ${message}\n`)
  for (const detail of details) {
    process.stderr.write(`[suite ERR] ${detail}\n`)
  }
  process.exit(1)
}

function formatTitleList(titles) {
  return titles.length > 0 ? titles.join(" | ") : "<none>"
}

function escapeRegex(value) {
  return value.replace(/[|\\{}()[\]^$+*?.-]/g, "\\$&")
}

function prefixLines(prefix, text) {
  return text
    .split(/(?<=\n)/)
    .map((line) => (line.length > 0 ? `${prefix}${line}` : line))
    .join("")
}
