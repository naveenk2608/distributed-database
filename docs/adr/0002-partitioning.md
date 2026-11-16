# ADR 0002: Partitioning

Status: Accepted (Phase 1; implemented in Phase 5)

## Context

Keys must be spread over nodes so load and data are balanced, and replicas
must be movable when nodes join or fail without rewriting keys one by one.

## Decision

A fixed number of shards S (default 64) is chosen at bootstrap. A key's shard
is `xxhash64(key) mod S`. Shards, not keys, are placed on nodes with a
consistent hash ring with virtual nodes, which maps each shard to RF nodes
deterministically from the membership list. Keys never move between shards;
only whole shard replicas move, using Raft snapshots and membership changes.

## Alternatives considered

- **Range partitioning with automatic splits (CockroachDB, TiKV, Bigtable).**
  Supports ordered scans and splits hot ranges, but needs split/merge
  protocols, range descriptors and a range-lookup index. Too much for v1
  (ADR 0000).
- **Per-key token ring (Cassandra, Dynamo).** Every node owns token ranges
  and keys move when membership changes, which suits leaderless replication
  but not one Raft group per unit of placement.
- **Redis Cluster's 16,384 hash slots.** Similar to fixed shards, but 16k
  Raft groups would be far too many; S is kept in the tens to low hundreds.

## Consequences

- Even load for uniform keys, and simple, deterministic placement that Phase
  5 can run without a metadata service.
- S cannot change after bootstrap, and one hot key cannot be split.
- No ordered scans across the keyspace.
