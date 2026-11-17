# ADR 0009: Conflict resolution by log order plus compare-and-set

Status: Accepted (Phase 6)

## Context

Concurrent writers to one key need a defined outcome, and clients that
read-modify-write need a way not to lose each other's updates.

## Decision

- **Raft log order decides.** Each shard's writes are totally ordered by
  its log; the last committed write wins. A key's version is the log index
  of the entry that last wrote it, so versions are unique within a shard
  and increase with every write to it.
- **Compare-and-set** (`CompareAndSet(key, value, expected_version)`) is
  decided when the entry is applied, against the applied state: it writes
  only if the key's current version equals `expected_version` (0 meaning
  the key must not exist), and returns the version afterwards, the new one
  on success or the current one otherwise. Every replica decides alike.
- Versions are compared only within one key; they are not timestamps and
  say nothing about order across shards.

## Alternatives considered

- **Last-write-wins by timestamp.** Needs synchronized clocks and loses
  writes silently when clocks disagree.
- **Vector clocks with siblings** (Dynamo). Pushes merging to every client;
  unnecessary once a single leader orders writes.
- **CRDTs.** Only for data types with a merge function; dkv stores opaque
  bytes.

## Consequences

- Read-modify-write is safe with CAS in a loop: read value and version,
  compute, CAS with that version, retry on mismatch.
- A CAS proposed on an old leader that loses its leadership before commit
  is either never applied (the client gets `UNAVAILABLE` and may retry with
  the same request ID) or applied once (ADR 0008).
- Version numbers jump by more than one between writes to a key, and a
  shard's no-op entries consume indexes too.
