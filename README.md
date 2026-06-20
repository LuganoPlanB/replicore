# Replicore

Resilient multi-node K/V storage for high availability, based on the Holepunch / Hypercore stack.

Replicore is an app running on remote servers to form a dynamically extensible network of nodes self-governed by Raft leadership elections.


The nodes may have three roles:
2. Notary: is the primary node by election, the only one who can write
3. Witness: eligible as candidate for next elections, read-only and transparent proxy to the Notary on writes
4. Archivist: needs authorization to become a Witness and provides read-only access.

Replicore implements a small multi-node service with these properties:
- The data is stored as append-only feed that that can reconcile after network splits.
- The K/V database is a derived view im Hyperbee from a Hypercore feed of signed entries. 
- Witness nodes replicate feeds, qualify join requests, serve reads, forward writes to the Notary.
- Writes are considered successful only after a Notary write and one Witness acknowledges.

The must be minimum two nodes for functioning.

Replicore is intended for non-Byzantine production use only within the boundaries stated in this README.

## Current Features

- Ease of use.
- Post-Quantum resistant join protocol.
- Noise protocol for internal node communication.
- Config-less DHT based networking setup.
- Signed K/V operations and signed heartbeat records
- Quorum based election, Raft like
- Witness to Notary write forwarding over Hypercore extensions
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
- Node identity rotation
- Log pruning and feed rotation
- Backup archive lifecycle
- Production deployment packaging
- Shared-secret compromise rotation and automatic cluster-wide recovery
- Byzantine fault tolerance

## Requirements

- Node.js 24.6 or newer
- npm

## Releases

Releases are managed by GitHub Actions with Release Please.

Commits on `main` are interpreted with Conventional Commits:

- `fix: ...` creates a patch release.
- `feat: ...` creates a minor release.
- `feat!: ...`, `fix!: ...`, or a `BREAKING CHANGE:` footer creates a major release.

When releasable commits are present, the release workflow opens or updates a
release PR that bumps `package.json`, updates `package-lock.json`, and writes
`CHANGELOG.md`. Merging that PR creates the `vX.Y.Z` tag and GitHub release.

## Install

```powershell
npm install
```

## Local Run

Start a local HyperDHT bootstrap node:

```powershell
npm run start:bootstrap
```

For a fresh cluster, bootstrap one explicit initializer:

```powershell
npm run start:node -- examples/local/init-node.json
npm run start:node -- examples/local/init-joiner.json
```

`initCluster: true` is the only supported secret-first voter bootstrap path.
Another node with the same `clusterSecret` must join as a learner instead of
implicitly creating a second voter cluster.

Start three swarm nodes in separate terminals:

```powershell
npm run start:node -- examples/local/node-1.json
npm run start:node -- examples/local/node-2.json
npm run start:node -- examples/local/node-3.json
```

Those three files are the current bootstrap-voter example. They still use
`compatibilityMode: "legacy-static-membership"` to show a pre-expanded local
cluster. The explicit fresh-cluster bootstrap path is `examples/local/init-node.json`.

Start a fourth node and let it join as a learner without editing the existing
voter configs:

```powershell
npm run start:node -- examples/local/joiner.json
```

Each node prints a `node-ready` JSON object with:

- `nodeId`
- `feedKey`
- `dataDir`
- HTTP bind address
- currently observed leader

`clusterSecret` is the shared discovery and admission root. Replicore derives:

- the Holepunch topic from `clusterSecret + clusterId`
- a cluster-scoped `machineId` from keyed Argon2d over `clusterSecret + machineIdentity`
- the Noise transport key from `clusterSecret + machineId`
- the join-signing key from `clusterSecret + machineId`

Committed voter authority does not come from the secret alone. A joining node
starts as a learner and only becomes a voter after committed membership
promotion.

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

Example configs live in `examples/local/README.md` and `examples/local/node-{1,2,3}.json`.

The node launcher expects:

- `dataDir`
- `clusterId`
- `clusterSecret`
- `identitySeed`
- optional `machineIdentity` (or legacy `machineId`)
- either:
  - `initCluster: true` for the first secret-first voter in a brand-new cluster
  - `compatibilityMode: "legacy-static-membership"` plus `authorizedNodeSeeds` or `authorizedNodes` for bootstrap voters
  - `role: "learner"` with no static membership fields for a secret-first joining node
- either `encryptionKey` or `encryption`
- optional `revokedNodeIds`
- `bootstrap`
- `http`
- `auth`

`identitySeed` and `authorizedNodeSeeds` are hex-encoded 32-byte seeds. The
launcher derives stable Replicore signing identities and feed keys from them.
`machineIdentity` is the local machine-specific input used to derive the
cluster-scoped transport `machineId`, Noise key, and join key.

Legacy static membership remains supported for the initial voter set while the
project keeps a local-demo path for pre-expanded clusters. Fresh clusters should
prefer the explicit `initCluster: true` bootstrap plus learner join and
promotion. Joining nodes should use the secret-first learner config shown in
`examples/local/joiner.json`.

Replicore binds each `dataDir` to one cluster secret hash, one local signing
identity, and one bootstrap mode. Restarting a populated directory with a
different secret, different node identity, or `initCluster: true` after that
directory already joined another cluster fails closed.

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

## Migration

If you already have static configs:

- keep `compatibilityMode: "legacy-static-membership"` for the existing voter set
- replace `topicSalt` with `clusterSecret` if needed
- rename `machineId` to `machineIdentity` when convenient; the old field still loads
- use `initCluster: true` only for a brand-new single-voter cluster
- use a learner config like `examples/local/joiner.json` for new joining nodes instead of editing every existing node config by hand

## Project Layout

- `bin/run-bootstrap.js`: local HyperDHT bootstrap helper
- `bin/run-node.js`: config-driven node runner
- `src/node.js`: swarm node, replication, forwarding, leadership, durability
- `src/http-server.js`: minimal authorized HTTP surface
- `src/materialized-view.js`: derived Hyperbee state
- `src/operation.js`: operation creation, signing, validation
- `src/config-loader.js`: JSON config loading
- `test`: integration and config tests

## Tests

```powershell
npm test
```

Longer local reliability pass:

```powershell
npm run test:reliability
```

Example bounded reliability profile:

```powershell
$env:REPLICORE_TEST_ROUNDS=2
$env:REPLICORE_TEST_TIMEOUT_MS=180000
$env:REPLICORE_TEST_PATTERN="offline leader|isolated leader|isolated follower|concurrent writes|bootstrap outage|restarted follower stays disconnected|follower write forwarding|deterministic churn"
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
- new same-secret nodes join as learners first and only become voters after committed promotion
- wrong-secret nodes do not discover, mirror, or join the cluster
- a single isolated node can continue serving local reads but cannot make durable writes
- only one connected side with the live leader and quorum can continue writing
- isolated followers can serve stale reads until they heal and catch up
- already-connected peers continue writing after bootstrap disappears
- if a follower restarts while bootstrap remains unavailable, it starts locally but does not rediscover peers from scratch, stays disconnected, and does not catch up to writes made while it was away
- learner promotion, voter removal, and node replacement go through committed membership changes
- divergent membership views remain observable through membership fingerprints, and degraded writes may stay blocked until configs converge again
- writes may fail transiently during failover windows while leader view and reachability converge
- snapshot restore and persisted data directories recover current state after severe outage or full restart

## Production Boundaries

Replicore currently guarantees these properties for non-malicious failures:

- committed writes are crash-safe across restart and leader replacement
- only one partition may accept durable writes at a time; minority or split-fenced sides reject writes
- `clusterSecret` gates discovery and first admission, but voter authority still requires committed membership
- new nodes join as read-only learners, catch up, and can later be promoted to voters through quorum commit
- stale reads are explicit in runtime status when a node is isolated or split-fenced
- recovery is explicit: healed nodes reconcile to the committed leader log, and snapshot restore validates integrity metadata before import

Operators must still treat these as hard limits:

- no Byzantine or malicious-node tolerance
- compromise of the shared secret requires manual rotation and operational recovery
- clients that need freshest reads must use a leader-connected path or otherwise require a strong-read policy above the current API
- do not initialize two independent clusters with the same `clusterSecret`; a fresh cluster must use one explicit `initCluster: true` voter and all other same-secret nodes must join as learners

Not covered:

- malevolent or Byzantine nodes
- automatic shared-secret rotation and compromise recovery
- production auth, backup lifecycle, or deployment packaging
