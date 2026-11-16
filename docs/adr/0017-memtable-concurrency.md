# ADR 0017: Memtable concurrency

Status: Accepted (Phase 1)

## Context

The memtable holds the newest writes and is read by every GET. The plan keys
entries by `userKey | seq | kind`, so read snapshots and Raft-index versions
work, and the Phase 1 gate requires a randomized skiplist test with
concurrent readers to pass under the race detector.

## Decision

- The skiplist is **insert-only**. A newer version of a key is a new entry
  with a higher sequence number, and a delete is a tombstone entry. Nothing
  is updated or unlinked in place.
- **Writers are serialized** by a mutex inside the memtable; the engine
  serializes writes anyway (later through group commit).
- **Readers never lock.** Every tower link is an `atomic.Pointer`. A new
  node is fully built, its own links are set, and only then is it linked
  into each level from the bottom up. Under the Go memory model, a reader
  that loads a link therefore sees a fully built node.
- The engine publishes a `visibleSeq` only after a whole batch is in the
  memtable, and reads without a snapshot read at `visibleSeq`, so a batch is
  visible all at once or not at all.

## Alternatives considered

- **RWMutex around the whole skiplist.** Simple, but every read contends
  with writers for the lock. The earlier prototype also held the lock only
  while creating an iterator, which the race detector flagged as a data race.
- **Lock-free concurrent inserts (CAS on links, as in Pebble's arenaskl).**
  Allows parallel writers, but is much harder to get right and unnecessary
  while the commit pipeline serializes writes.
- **Arena allocation.** One big byte slice for nodes, keys and values gives
  cheap allocation and exact sizing, but needs unsafe offset arithmetic.
  Deferred until profiling in Phase 10 shows GC pressure from the memtable.
- **A sorted B-tree or `map` plus sorted keys.** Simpler, but no lock-free
  reads.

## Consequences

- Reads scale with cores and never block on writes.
- Memory grows with every version until the memtable is flushed (Phase 3);
  in Phase 1 the in-memory engine grows without bound.
- The fixed RNG seed makes tower shape reproducible for debugging; it
  affects only speed, never results.
