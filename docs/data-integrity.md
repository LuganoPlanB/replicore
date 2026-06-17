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
