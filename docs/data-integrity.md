# Data Integrity Rules

Replicore's first production requirement is simple: only one partition may
accept durable writes.

The implementation must preserve these invariants:

1. Only a quorum-elected leader in the current term may accept writes.
2. A write succeeds only after quorum replication and commit.
3. Nodes apply only committed operations to the materialized K/V view.
4. Stale leaders and minority partitions reject writes with structured errors.
5. Membership changes are committed operations.
6. New nodes start as non-voting learners and do not affect quorum until a
   committed promotion.

## Consensus Shape

Replicore should adopt a minimal Raft-like quorum-commit model for metadata and
K/V operations.

The target protocol is intentionally narrow:

- one leader per term
- majority quorum for votes and commits
- append-entries style replication
- persistent `currentTerm` and `votedFor`
- persistent `commitIndex` and `lastApplied`
- log matching before append acceptance
- membership changes committed through the log

This replaces heartbeat-only leader choice for production durability. It does
not require a full external consensus dependency if the local implementation can
stay small and readable.

## Read Semantics

Replicore should keep two read modes:

- local reads: available from any node, may be stale, and must report that
  staleness is possible
- strong reads: available only after leader or quorum confirmation against the
  committed log

Until strong reads exist, the API and status responses must not imply
linearizable reads. Existing stale-read metadata should remain explicit and be
extended rather than removed.

## Non-Goals

This production path does not include:

- multi-writer conflict merge
- accepting durable writes on both sides of a split
- Byzantine fault tolerance
- full PKI or external trust infrastructure
- automatic recovery from shared-secret compromise
- custom NAT traversal or deployment-specific networking
- external database coordination

The common cluster secret is for discovery and read-only learner admission only.
It is not a defense against a node that already knows the secret and behaves
maliciously.

## Config Modes

The checked-in JSON config files under `examples/local/` are temporary
compatibility fixtures, not the target production path.

- `compatibilityMode: "legacy-static-membership"` means the file predeclares a
  static voter set with `authorizedNodeSeeds` or `authorizedNodes`.
- Legacy static membership is useful for local demos, tests, and staged
  migration only.
- Production-ready operation should use `clusterSecret`, secret-derived
  Holepunch discovery, persisted transport identity, learner admission, and
  signed promotion rather than a pre-edited static voter file.

Do not mix legacy static membership config with learner admission or later
dynamic membership in the same running cluster.

## Protocol Vocabulary

Use these terms consistently in code, tests, and docs:

- `Argon2id-based domain-separated KDF`: derivation from the operator-facing
  `clusterSecret`. Do not call this HKDF; HKDF is a different construction.
- `secret-derived Holepunch topic`: the Hyperswarm topic derived from
  `clusterSecret`.
- `Noise identity classification`: identifying a connected transport peer by
  its Holepunch/Noise public key and matching it to Replicore membership state.
- `learner`: a same-secret peer that may replicate committed data and serve
  read-only local queries, but cannot vote, lead, proxy durable writes, or
  accept durable writes.
- `promotion credential`: a user-approved signed object that allows a learner
  to be considered for voter promotion. The learner is not a voter until the
  membership path accepts the promotion.

Do not add a custom HMAC pre-admission handshake. Discovery is gated by the
secret-derived topic, transport identity comes from Noise identity
classification, and voter authority comes only from committed membership.

## Secret Derivation Defaults

Use `hash-wasm` Argon2id for production secret derivation. The defaults are
chosen for startup/config-time work, not per-request use:

- variant: `argon2id`
- version: Argon2 v1.3, as exposed by `hash-wasm`
- memory: `65536` KiB
- iterations: `3`
- parallelism: `1`
- output: binary, with explicit caller-selected length
- topic output length: `32` bytes
- Noise seed output length: `32` bytes

The KDF input is the raw `clusterSecret`. The salt is ASCII and includes the
purpose label and context:

```text
replicore:kdf:v1:<purpose-label>:<context>
```

Use these initial purpose labels:

- `replicore:dht-topic:v1`
- `replicore:noise-node-key:v1`

For the DHT topic, the context is the stable cluster identifier. For the
machine-specific Noise key, the context is the machine identity input, defaulting
to `/etc/machine-id` where available, with explicit config overrides for tests
and non-Linux environments. Persist the derived identity in the node data dir
and fail closed if it changes unexpectedly.

## Promotion Credential V1

Use a simple canonical JSON payload signed with the existing Replicore node
signing key. The signature is over `canonicalize(payload)` bytes.

Payload fields:

- `v`: `1`
- `type`: `replicore.promotion`
- `clusterId`: cluster identifier
- `membershipVersion`: expected membership version
- `learnerNodeId`: Replicore node ID being promoted
- `learnerNoisePublicKey`: hex Noise public key being promoted
- `targetRole`: `voter`
- `issuedAt`: ISO-8601 timestamp
- `expiresAt`: ISO-8601 timestamp
- `nonce`: random base64url string
- `signerNodeId`: signing voter/admin node ID

Wire object:

```json
{
  "payload": {
    "v": 1,
    "type": "replicore.promotion",
    "clusterId": "example",
    "membershipVersion": 1,
    "learnerNodeId": "hex-node-id",
    "learnerNoisePublicKey": "hex-noise-public-key",
    "targetRole": "voter",
    "issuedAt": "2026-06-17T00:00:00.000Z",
    "expiresAt": "2026-06-18T00:00:00.000Z",
    "nonce": "base64url-random",
    "signerNodeId": "hex-signer-node-id"
  },
  "signature": "base64url-signature"
}
```

Validation must check the signature, signer authority, expiry, target learner
identity, expected membership version, and replay by nonce or credential hash.
