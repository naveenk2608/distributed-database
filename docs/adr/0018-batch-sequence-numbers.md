# ADR 0018: One sequence number per batch

Status: Accepted (Phase 1)

## Context

`lsm.DB.Apply(batch, seq)` writes a batch atomically. The plan makes a key's
version equal to the Raft log index of the write that produced it, and
passes that index as `seq` in clustered mode. From Phase 6 one Raft command
can write more than one key: a PUT also updates the client session table, so
a single log entry produces a batch with at least two operations.

The first Phase 1 implementation gave operation i of a batch the sequence
number `seq+i`, as LevelDB does. An independent review found that this
breaks the Raft mapping: a two-operation batch at index 10 uses 10 and 11, so
the next entry at index 11 is rejected as not increasing, and the session row
gets a version that is not its log index.

## Decision

Every operation in a batch gets the batch's sequence number. If a batch
writes the same key more than once, only the last operation for that key is
inserted (a `Set` then `Delete` of one key deletes it). `Apply` returns that
sequence number, which is the version of every key the batch wrote. In
standalone mode the engine uses `lastSeq + 1` per batch.

## Alternatives considered

- **Consecutive numbers per operation (LevelDB).** Lets two writes to one
  key in one batch both exist as separate versions, which nothing in dkv
  needs. It breaks "version = log index" for multi-key commands, or forces
  a mapping such as `seq = index × k` that wastes sequence space and leaks
  into every version clients see.
- **Reject batches that repeat a key.** Simpler, but pushes deduplication
  onto every caller. Keeping the last write matches what applying the
  operations one by one would produce.
- **Decouple version from sequence number** (store the version in the
  value). Costs space per entry and complicates read snapshots, which are
  sequence numbers.

## Consequences

- Versions equal Raft log indexes exactly, including for the session
  table, and consecutive log entries are always accepted.
- The WAL record format from the plan (`seq | count | ops`) still fits: one
  sequence number per record.
- Within a batch, all keys share one version, which is what a snapshot sees
  anyway since batches are atomic.
