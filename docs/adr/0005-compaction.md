# ADR 0005: Size-tiered compaction over time-ordered runs

Status: Accepted (Phase 3)

## Context

Flushes produce a steady stream of small tables. Without merging, every read
of an absent key checks every table (bloom filters only reduce the cost),
space grows with every overwrite and delete, and tombstones live forever.
The plan asks for size-tiered compaction first, with tombstone removal,
write backpressure, a rate limiter and a shared worker pool, and treats
leveled compaction as a stretch goal.

## Decision

Tables are kept in a list ordered by age, newest first, and their sequence
ranges never overlap: flushes add tables at the newest end in sequence
order, and a compaction merges only **adjacent** tables, replacing them in
place with one output table. This is RocksDB's "universal" form of
size-tiered compaction. It lets a point read stop at the first table that
holds any version of the key visible to it, exactly as with memtables.

A compaction is picked in this order:

1. The newest run of at least `CompactionMinTables` (4) adjacent tables
   whose largest is at most `CompactionSizeRatio` (2) times the smallest,
   capped at `CompactionMaxTables` (10). Tables smaller than a quarter
   memtable count as that size, so stray small tables (a manual flush, the
   startup flush) do not split runs.
2. If there are `SlowdownTables` (20) or more tables, the newest ones,
   whatever their sizes.
3. If the tables above the oldest hold more bytes than the oldest one
   (space amplification over 100%), all of them.

Merging keeps an entry unless a newer version of the same key is visible to
every live snapshot, and drops a tombstone at or below the oldest snapshot
only when the run includes the oldest table (nothing older can be hidden by
it). This is LevelDB's rule; it keeps every version newer than the oldest
snapshot, even ones no snapshot needs. Inputs are deleted only after the new
manifest is durable and the last reader of the old state has let go.

Backpressure: past `SlowdownTables` each write group is delayed 1 ms; past
`StopTables` (30), or with `MaxImmutableMemtables` (2) memtables waiting for
a flush, a write that needs a new memtable waits. `CompactionBytesPerSec`
rate-limits compaction output, and a `CompactionPool` shared by every engine
on a node caps concurrent compactions.

## Alternatives considered

- **Classic size tiering over arbitrary similar-sized tables** (Cassandra's
  STCS). Merging non-adjacent tables makes sequence ranges overlap, so a read
  could no longer stop at the first table with a visible version and would
  have to consult every table whose range covers the key, then pick the
  newest. Adjacency costs little: flush order already sorts tables by size
  tier.
- **Leveled compaction** (LevelDB, RocksDB default). Bounds read and space
  amplification much better, at the price of more write amplification and a
  more complex picker with per-level key-range bookkeeping. Kept as the
  stretch goal; Phase 10 can measure both on the same workload.
- **Exact snapshot-aware garbage collection** (keep, per snapshot stripe,
  only the newest version). Saves space under many long-lived snapshots;
  dkv's snapshots are short (reads, checkpoints), so the simpler rule wins.

## Consequences

- Tombstones and overwritten versions are dropped only by compactions that
  reach the oldest table; rule 3 makes sure that happens once enough data
  piles up above it.
- Space amplification can reach about 2x before rule 3 fires, and write
  amplification grows with the number of tiers (log base 4 of the data size
  over the memtable size).
- The Phase 3 model test runs a million operations with background flushes
  and compactions and restarts and matches a Go map; a mutation check shows
  the gate tests catch a tombstone dropped too early and snapshots ignored by
  compaction (docs/PROGRESS.md).
