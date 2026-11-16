# ADR 0004: One storage engine per shard replica

Status: Accepted (Phase 5)

## Context

A cluster node holds replicas of many shards. Their data could live in one
shared engine with shard-prefixed keys, or in one engine per replica.

## Decision

Each shard replica a node holds gets its own LSM engine in
`<storage.dir>/shards/NNNN/`, with its own WAL, memtables, tables and
manifest. The engines on a node share one block cache
(`storage.block_cache_bytes`) and one compaction pool
(`storage.compaction_concurrency`). A standalone node keeps a single engine
in `<storage.dir>`; a `CLUSTER` marker (cluster name, shard count,
replication factor) keeps the two layouts from being mixed and refuses a
change of shard count or replication factor once data exists.

## Alternatives considered

- **One engine per node with shard-prefixed keys.** One WAL and one fsync
  stream per node, fewer files, better compaction batching. But moving a
  replica (Phase 7) means scanning a key range out of a shared engine,
  dropping a shard means range deletes and compaction to reclaim space, a
  snapshot cannot be a plain directory checkpoint, and sequence numbers
  (versions = Raft indexes per shard) would collide across shards.

## Consequences

- A shard's data is a directory: a Raft snapshot is an engine checkpoint
  of that directory, and moving or dropping a replica is a file operation.
- Each engine has its own WAL, so a node fsyncs once per shard with writes
  in flight; group commit works per engine only. Phase 10 measures this,
  and the plan's stretch goal is one shared Raft log per node.
- Memory: each engine has its own memtables, so a node holding H replicas
  can use up to H × memtable_bytes × (1 + max immutable memtables) under a
  write burst. The memtable size is per engine.
- Versions are per shard: two keys in different shards can have the same
  version.
