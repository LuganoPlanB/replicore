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

The common cluster secret is for discovery and admission only. It is not a
defense against a node that already knows the secret and behaves maliciously.
