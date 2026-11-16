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
| [0016](0016-configuration-sources.md) | Configuration sources and precedence | Accepted |
| [0017](0017-memtable-concurrency.md) | Memtable: insert-only skiplist, single writer, lock-free readers | Accepted |
| [0018](0018-batch-sequence-numbers.md) | One sequence number per batch | Accepted |
