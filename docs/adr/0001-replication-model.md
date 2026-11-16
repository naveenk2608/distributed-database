# ADR 0001: Replication model

Status: Accepted (Phase 1; implemented in Phase 6)

## Context

dkv must keep every acknowledged write through the failure of a minority of
a shard's replicas and serve linearizable reads by default. It must also
scale writes across nodes, so a single leader for the whole keyspace is not
enough.

## Decision

Each shard is an independent Raft group ("multi-Raft"), with RF replicas
(default 3) on different nodes. Raft orders and replicates every write; the
LSM engine on each replica is only the state machine that committed commands
are applied to. One more Raft group, the meta group, stores membership and
placement (Phase 7).

## Alternatives considered

- **Leaderless quorums (Dynamo, Cassandra).** Writes go to any W of N
  replicas and reads to any R. Highly available under partitions, but
  replicas can accept conflicting writes, so the system needs last-write-wins
  timestamps or vector clocks, read repair, hinted handoff and anti-entropy,
  and it is not linearizable without extra machinery. Running it beside Raft
  would create two sources of truth.
- **One Raft group for everything.** Simplest correct design, but every write
  goes through one leader and one log, so write throughput does not scale
  with nodes.
- **Primary–backup with a lease or external coordinator.** Cheap in the happy
  path, but failover safety depends on the coordinator and on clocks; getting
  fencing right is as hard as Raft without Raft's proofs.

## Consequences

- Strong consistency per key with a single write path; no conflict merging.
- Writes cost one majority round trip plus an fsync on each replica in it.
- Many groups multiply heartbeats and fsyncs; mitigated with a modest fixed
  shard count, one ticker per node and per-node message batching.
- Requires a deterministic, well-tested Raft core (ADR 0003, Phase 6).
