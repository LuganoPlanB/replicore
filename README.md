# planb-cleard

Minimal Holepunch / Hypercore K/V swarm prototype.

This repository implements a small multi-node service with these properties:

- Each node has its own writable Hypercore feed.
- The application state is a signed append-only operation log.
- A local Hyperbee is the derived K/V view, not the source of truth.
- Only the current leader writes K/V operations.
- Followers replicate feeds, serve reads, and forward writes to the leader.
- Writes are considered successful only after leader-local append plus one follower acknowledgement.

The current implementation is a prototype. It is useful for local runs and architecture validation, not production deployment.

## Current Features

- Signed K/V operations and signed heartbeat records
- Deterministic leader selection from live heartbeats
- Follower-to-leader write forwarding over Hypercore extensions
- Derived Hyperbee current view and history view
- Static bearer-token ACLs by keyspace
- HTTP routes for CRUD and status
- Snapshot export/import for current-state restore
- Admin snapshot export/import endpoints and CLI
- Static writer revocation on restart/reload config
- Keyring-based value encryption with live active-key rotation
- Config-driven node launcher and local bootstrap helper

## Not Implemented Yet

- Real production auth such as JWT issuer integration, mTLS, or signed HTTP requests
- Dynamic writer membership changes through the replicated log
- Node identity rotation
- Log pruning and feed rotation
- Backup archive lifecycle
- Production deployment packaging

## Requirements

- Node.js 25 or newer
- npm

## Install

```powershell
npm install
```

## Local Run

Start a local HyperDHT bootstrap node:

```powershell
npm run start:bootstrap
```

Start three swarm nodes in separate terminals:

```powershell
npm run start:node -- examples/local/node-1.json
npm run start:node -- examples/local/node-2.json
npm run start:node -- examples/local/node-3.json
```

Each node prints a `node-ready` JSON object with:

- `nodeId`
- `feedKey`
- `dataDir`
- HTTP bind address
- currently observed leader

## CRUD

Write:

```powershell
curl -X PUT "http://127.0.0.1:3001/kv/hash:abc?keyspace=default" `
  -H "authorization: Bearer writer" `
  -H "content-type: application/json" `
  -d "{\"value\":{\"hello\":\"world\"}}"
```

Read:

```powershell
curl "http://127.0.0.1:3002/kv/hash:abc?keyspace=default" `
  -H "authorization: Bearer reader"
```

Delete:

```powershell
curl -X DELETE "http://127.0.0.1:3003/kv/hash:abc?keyspace=default" `
  -H "authorization: Bearer writer"
```

History:

```powershell
curl "http://127.0.0.1:3001/kv/hash:abc/history?keyspace=default" `
  -H "authorization: Bearer reader"
```

## Status

Replication:

```powershell
curl "http://127.0.0.1:3001/status/replication"
```

Writers:

```powershell
curl "http://127.0.0.1:3001/status/writers"
```

Leader:

```powershell
curl "http://127.0.0.1:3001/status/leader"
```

## Snapshots

Export a snapshot from a live node:

```powershell
npm run snapshot -- export http://127.0.0.1:3001 admin .\tmp\snapshot.json
```

Import a snapshot into a live node:

```powershell
npm run snapshot -- import http://127.0.0.1:3001 admin .\tmp\snapshot.json
```

Rotate the active encryption key on a live node:

```powershell
curl -X POST "http://127.0.0.1:3001/admin/encryption/rotate" `
  -H "authorization: Bearer admin" `
  -H "content-type: application/json" `
  -d "{\"keyId\":\"next\"}"
```

The example configs include an `admin` token for these routes.

## Config

Example configs live in [examples/local/README.md](C:/Users/denis/devel/planb-cleard/examples/local/README.md:1) and `examples/local/node-{1,2,3}.json`.

The node launcher expects:

- `dataDir`
- `clusterId`
- `topicSalt`
- `identitySeed`
- either `authorizedNodeSeeds` or `authorizedNodes`
- either `encryptionKey` or `encryption`
- optional `revokedNodeIds`
- `bootstrap`
- `http`
- `auth`

`identitySeed` and `authorizedNodeSeeds` are hex-encoded 32-byte seeds. The launcher derives stable node identities and feed keys from them.

Simple encryption config:

```json
{
  "encryptionKey": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
```

Keyring config with rotation support:

```json
{
  "encryption": {
    "currentKeyId": "primary",
    "keys": {
      "primary": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "next": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    }
  }
}
```

Revoked writers remain part of the explicit cluster membership record, but nodes stop replicating from them and reject new operations signed by them after restart with updated config.

## Project Layout

- [bin/run-bootstrap.js](C:/Users/denis/devel/planb-cleard/bin/run-bootstrap.js:1): local HyperDHT bootstrap helper
- [bin/run-node.js](C:/Users/denis/devel/planb-cleard/bin/run-node.js:1): config-driven node runner
- [src/node.js](C:/Users/denis/devel/planb-cleard/src/node.js:1): swarm node, replication, forwarding, leadership, durability
- [src/http-server.js](C:/Users/denis/devel/planb-cleard/src/http-server.js:1): minimal authorized HTTP surface
- [src/materialized-view.js](C:/Users/denis/devel/planb-cleard/src/materialized-view.js:1): derived Hyperbee state
- [src/operation.js](C:/Users/denis/devel/planb-cleard/src/operation.js:1): operation creation, signing, validation
- [src/config-loader.js](C:/Users/denis/devel/planb-cleard/src/config-loader.js:1): JSON config loading
- [test](C:/Users/denis/devel/planb-cleard/test:1): integration and config tests

## Tests

```powershell
npm test
```

Longer local reliability pass:

```powershell
npm run test:reliability
```

The tests currently cover:

- replication and restart recovery
- leader failover and follower forwarding
- single-node isolation and connected-side continuation
- stale reads on isolated followers until rejoin
- HTTP CRUD and status routes
- operation validation
- snapshot restore
- config loading

## Reliability Semantics

The current test suite verifies these non-malicious failure behaviors:

- follower crashes and restarts catch up from replicated feeds
- former leaders can disappear, a new leader can write, and the old leader can rejoin and catch up
- a single isolated node can continue serving local reads but cannot make durable writes
- a connected subset with a live leader plus at least one follower can continue writing
- isolated followers can serve stale reads until they heal and catch up
- already-connected peers continue writing after bootstrap disappears
- writes may fail transiently during failover windows while leader view and reachability converge
- snapshot restore and persisted data directories recover current state after severe outage or full restart

Not covered:

- malevolent or Byzantine nodes
- dynamic membership through the replicated log
- restart-time rediscovery while bootstrap remains unavailable
- production-grade consensus semantics
- production auth, backup lifecycle, or deployment packaging

## Notes

- `.gestalt/` is intentionally ignored and should remain out of commits.
- `holepunch-stack/` is vendored reference material and is also ignored.
- `data/` is local runtime state and is ignored.
