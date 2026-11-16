# Architecture Decision Records

One file per decision: context, the decision, the alternatives considered and
the consequences. Numbers 0000–0015 follow the ADR backlog in docs/PLAN.md and
are written in the phase where each decision first matters; decisions that
come up during implementation take the next free number from 0016.

| ADR | Decision | Status |
| --- | --- | --- |
| [0000](0000-scope.md) | Scope: single-key operations with CAS | Accepted |
| [0001](0001-replication-model.md) | One Raft group per shard | Accepted |
| [0002](0002-partitioning.md) | Fixed hash shards placed by consistent hashing | Accepted |
| [0005](0005-compaction.md) | Size-tiered compaction over time-ordered runs of adjacent tables | Accepted |
| [0006](0006-manifest-format.md) | Manifest rewritten in full with an atomic rename | Accepted |
| [0016](0016-configuration-sources.md) | Configuration sources and precedence | Accepted |
| [0017](0017-memtable-concurrency.md) | Memtable: insert-only skiplist, single writer, lock-free readers | Accepted |
| [0018](0018-batch-sequence-numbers.md) | One sequence number per batch; last write wins inside a batch; one log record per batch in group commit | Accepted (revised in Phase 2) |
| [0019](0019-wal-format-and-recovery.md) | WAL format, segments and torn-tail vs corruption rules | Accepted |
| [0020](0020-vfs-fault-model.md) | Filesystem interface and the simulated power-loss model | Accepted (revised in Phase 3) |
| [0021](0021-node-identity-and-bootstrap-membership.md) | Persistent random node IDs and a static bootstrap peer list with ping-based failure detection | Accepted |
| [0022](0022-internal-transport.md) | Internal gRPC transport: separate port, one connection per peer, fail fast, keepalives | Accepted |
