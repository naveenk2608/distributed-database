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

## Revision (Phase 2): last write wins inside the batch, one log record per batch

Status: Accepted (Phase 2)

Two consequences of one sequence number per batch needed to be made
explicit once batches reach the write-ahead log.

**Repeated keys.** Two operations on one key in one batch would have
identical internal keys (`key | seq | kind` with the same seq), and the
memtable can store only one. Phase 1 dropped the earlier operation when the
batch was applied. From Phase 2 the rule lives in `Batch` itself: `Set` or
`Delete` on a key the batch already writes replaces that operation. A batch
therefore never holds two operations on one key, `Count` is the number of
keys written, the log record contains exactly what is applied, and the
decoder rejects a record with a repeated key as corruption.
`TestBatchLastOpWins` covers put+put, put+delete, delete+put and
put+delete+put on one key, with and without an earlier value, before and
after the batch is replayed from the log.

Alternatives: keep deduplicating at apply time (the log would hold
operations that are never applied, and replay would have to repeat the rule
exactly); reject batches that repeat a key (pushes the rule onto every
caller, including the Phase 6 apply path, which writes a value and a
session row and must never fail on its own input).

**Group commit.** The plan describes LevelDB-style group commit as
combining concurrent batches "into one WAL record". That would put several
writers' batches under one sequence number, so unrelated writes would share
a version, and it cannot work at all for caller-supplied sequence numbers
(Raft indexes), which differ per batch. Instead, the group leader assigns
each batch its own sequence number, encodes each as its own record, writes
all of the group's records with one `write` call, and covers them with one
`fsync` before any writer in the group returns.
`TestGroupCommitOneRecordPerBatchOneSync` holds the first writer inside
fsync while 15 others queue, then checks for exactly two fsyncs and one
record per batch with distinct, ordered sequence numbers.

Cost: a 9-byte record header and 12-byte batch header per batch instead of
per group, which is negligible next to the fsync that is still shared.
Alternative considered: a combined record holding several `(seq, ops)`
pairs. It saves those few bytes but complicates the format and recovery
(a torn combined record loses several writers' batches at once) for no
measurable gain.
