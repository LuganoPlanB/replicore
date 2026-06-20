# AGENTS.md

Repository guidance for coding agents.

## Scope

This repo is a minimal Holepunch / Hypercore K/V swarm prototype. Treat it as a prototype that should stay small, readable, and easy to run locally.

## Current Architecture

- One writable Hypercore feed per node
- Signed append-only operations as the source of truth
- Hyperbee as derived local state only
- Leader-only K/V writes
- Followers replicate, serve reads, and forward writes
- Heartbeats determine the current leader
- Durability requires leader append plus one follower acknowledgement
- `HolepunchSwarmNode` accepts `useHeartbeatDurability: false` to prevent
  follower heartbeats from short-circuiting the durability-waiter timeout.
  Tests that inject artificial `ackDelayMs` should set this to `false` so the
  explicit ack timeout fires deterministically.

Do not redesign this into Redis, SQL clustering, or general multi-writer consensus unless the user explicitly asks for that.

## Working Rules

- Prefer the smallest viable change.
- Keep dependencies minimal. Use built-in Node modules unless a new package is justified.
- Preserve the current prototype shape: library-first core, small `bin/` launchers, JSON configs, focused tests.
- Use `apply_patch` for manual edits.
- Do not commit `.gestalt/`.
- Do not touch `holepunch-stack/`. It is vendored reference material, not project code.

## Key Files

- [src/node.js](C:/Users/denis/devel/planb-cleard/src/node.js:1): main swarm node logic
- [src/http-server.js](C:/Users/denis/devel/planb-cleard/src/http-server.js:1): HTTP API
- [src/materialized-view.js](C:/Users/denis/devel/planb-cleard/src/materialized-view.js:1): derived Hyperbee state
- [src/operation.js](C:/Users/denis/devel/planb-cleard/src/operation.js:1): operation schema, signing, validation
- [src/config-loader.js](C:/Users/denis/devel/planb-cleard/src/config-loader.js:1): runtime config loading
- [bin/run-node.js](C:/Users/denis/devel/planb-cleard/bin/run-node.js:1): node launcher
- [bin/run-bootstrap.js](C:/Users/denis/devel/planb-cleard/bin/run-bootstrap.js:1): local DHT bootstrap launcher

## Testing

Run:

```powershell
npm test
```

When changing behavior around replication, forwarding, leader election, durability, or snapshots, update or add tests in [test](C:/Users/denis/devel/planb-cleard/test:1).

The default suite groups perturbation scenarios into smaller `node --test` runs through the `npm test` script so they do not all share one Node test process. The live-isolation helpers are in [test/helpers/swarm-cluster.js](C:/Users/denis/devel/planb-cleard/test/helpers/swarm-cluster.js:1) and rely on the node-level networking hooks in [src/node.js](C:/Users/denis/devel/planb-cleard/src/node.js:1).

Use `isolateNode()` when the scenario needs full networking suspension for one process. Use `partitionGroups()` when the scenario needs live subgroup filtering that keeps nodes running and preserves only in-group links. Stay with these built-in hooks before reaching for external chaos tooling.

For longer local churn checks, run:

```powershell
npm run test:reliability
```

The reliability runner accepts the same bounded env controls used elsewhere:
`REPLICORE_TEST_ROUNDS`, `REPLICORE_TEST_TIMEOUT_MS`, `REPLICORE_TEST_PATTERN`,
`REPLICORE_TEST_FILE`, and `REPLICORE_NODE_TEST_CONCURRENCY`.

For bounded stress and timeout reproduction, run:

```powershell
node test/test-stress.mjs
```

Use `REPLICORE_TEST_TRACE=1` to stream fixture lifecycle events during a failing run. The default `npm test` entrypoint now uses `test/run-suite.mjs` so each grouped child test process has its own timeout. Do not add external chaos tooling or child-process dependencies unless the built-in runner and trace path prove insufficient.

## Docs and Plans

- Top-level human docs belong in `README.md`.
- Local execution examples belong in `examples/local/`.
- Planning work lives under `.gestalt/` and is intentionally untracked.

## Known Gaps

These are expected missing pieces, not accidental omissions:

- production-grade auth
- writer revocation
- key rotation
- log pruning / archival
- production backup lifecycle
- deployment packaging

If you add any of these, keep the implementation incremental and do not silently broaden the system model.
