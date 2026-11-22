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
| [0003](0003-raft-structure.md) | Raft as a deterministic core driven by a runner; pre-vote tie-break; per-node message batching; the leader writes while it replicates | Accepted (extended in Phase 7, the Phase 8 follow-up and Phase 10) |
| [0004](0004-engine-per-shard.md) | One storage engine per shard replica, sharing a node-wide block cache and compaction pool | Accepted |
| [0005](0005-compaction.md) | Size-tiered compaction over time-ordered runs of adjacent tables | Accepted |
| [0006](0006-manifest-format.md) | Manifest rewritten in full with an atomic rename | Accepted |
| [0007](0007-read-consistency.md) | ReadIndex reads by default, stale reads opt-in, lease reads later | Accepted |
| [0008](0008-idempotency.md) | Client sessions in replicated state; a pool of sessions per client | Accepted |
| [0009](0009-conflict-resolution.md) | Log order plus compare-and-set; version = log index | Accepted |
| [0010](0010-durability-path.md) | The Raft log is a replica's only WAL (since Phase 10; two logs before); commit index not fsynced; log compaction and snapshot install | Accepted (revised in Phases 7, 8 and 10) |
| [0011](0011-cluster-metadata.md) | Meta Raft group inside dkv; conditional transactions; directory polling | Accepted |
| [0012](0012-membership-changes.md) | Single-server changes with learners, applied on append; node IDs as members; leader transfer; rebalancer steps | Accepted |
| [0013](0013-failure-detection.md) | Heartbeats to the meta leader as soft state; only decisions replicated | Accepted |
| [0014](0014-role-of-postgresql.md) | PostgreSQL holds dashboard history only, outside the data path; the console works without it | Accepted |
| [0015](0015-dashboard-api.md) | Read-only admin HTTP on nodes; the console polls them and serves REST and server-sent events | Accepted (revised by 0029) |
| [0016](0016-configuration-sources.md) | Configuration sources and precedence | Accepted |
| [0017](0017-memtable-concurrency.md) | Memtable: insert-only skiplist, single writer, lock-free readers | Accepted |
| [0018](0018-batch-sequence-numbers.md) | One sequence number per batch; last write wins inside a batch; one log record per batch in group commit | Accepted (revised in Phase 2) |
| [0019](0019-wal-format-and-recovery.md) | WAL format, segments and torn-tail vs corruption rules | Accepted |
| [0020](0020-vfs-fault-model.md) | Filesystem interface and the simulated power-loss model | Accepted (revised in Phase 3) |
| [0021](0021-node-identity-and-bootstrap-membership.md) | Persistent random node IDs and a static bootstrap peer list with ping-based failure detection | Accepted (revised in Phase 7) |
| [0022](0022-internal-transport.md) | Internal gRPC transport: separate port, one connection per peer, fail fast, keepalives | Accepted |
| [0023](0023-placement.md) | Placement on a consistent hash ring (by node ID over eligible nodes since Phase 7); stated balance tolerance | Accepted (revised in Phase 7) |
| [0024](0024-routing.md) | Routing with one-hop forwarding, NotOwner hints and jittered retries | Accepted (revised in Phases 6 and 7) |
| [0025](0025-fault-injection-and-linearizability-checking.md) | Fault injection at the transport seam (in-process) and through the docker CLI; histories checked by Porcupine with versions in the model; scenarios prove their faults took effect | Accepted (revised in the Phase 8 follow-up) |
| [0026](0026-disruptive-servers-and-simulator-liveness.md) | Keep the CheckQuorum lease as is (it already implements dissertation §4.2.3); no simulated membership changes without PreVote; seed 51674 kept as a regression seed | Accepted |
| [0027](0027-client-admin-failover.md) | Admin requests try the nodes in turn, moving on from one the client cannot connect to within its share of the deadline or that answers `Unavailable`; once connected, the request has all the time left | Accepted |
| [0028](0028-failover-bound.md) | The failover gate's "about two election timeouts" are two of the longest randomized timeouts, `4 × election_ticks × tick` (800 ms in `TestLeaderFailover`) | Accepted |
| [0029](0029-console-missed-polls.md) | The console shows a node down at once when its poll fails, and only after `console.down_after_misses` polls in a row (default 3) when they time out | Accepted |
