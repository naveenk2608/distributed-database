# ADR 0019: WAL format, segments and recovery rules

Status: Accepted (Phase 2)

## Context

Phase 2 makes the engine durable. The log must detect torn and corrupted
records, recover automatically from a crash, and refuse to start when it
finds damage a crash cannot explain (the Phase 2 gate). Phase 6 reuses the
same package for the Raft log, so the format must not know about batches.

## Decision

**Format.** A log is a directory of segment files named `%06d.wal` (wider
once numbers pass 999999; only the canonical spelling is recognized). Each
segment is a sequence of records:

    record := crc32c(4) | length(4) | type(1) | payload(length)

Little-endian integers. The CRC-32C covers length, type and payload. Type 0
is reserved and never written, so a zero-filled region never decodes as a
record. The engine's batch record (type 1) has the payload
`seq(8) | count(4) | { kind(1) | klen(uvarint) | key | vlen(uvarint) | value }*`.
There is no block structure: records are contiguous, and a record larger
than one disk sector simply spans sectors.

**Writing.** `Writer.Append` writes any number of records with one `write`
call; `Writer.Sync` fsyncs. A new segment's directory entry is fsynced
before any record in it can be acknowledged. `Rotate` fsyncs the old segment
before anything is written to the new one. A failed write or fsync makes the
writer permanently unusable: after a failed fsync the kernel may have
dropped dirty pages, so a retry could report success for lost data.

**Recovery.** `Recover` replays segments in ascending order. Because older
segments are fully synced before the next one starts, a crash can only
damage the end of the newest segment. A damaged record (short header, length
past the end of the file, checksum mismatch, type 0) is therefore treated as
a torn write only if it is in the newest segment and no valid record starts
anywhere after it. Recovery then truncates the segment at that record and
fsyncs the truncation (otherwise the garbage could come back after a second
crash, when newer segments exist and it would look like corruption). Any
other damage returns a `CorruptionError` naming the file and offset, and
the engine refuses to open without modifying anything. After recovery the
engine always starts a new segment instead of appending to the old one.

## Alternatives considered

- **LevelDB's 32 KiB blocks with fragmented records.** Lets a reader resync
  after a damaged block. dkv does not want to skip damage, so the extra
  format complexity buys nothing.
- **Treat every damaged record as the end of the log** (RocksDB's
  point-in-time recovery mode). Never refuses to start, but silently drops
  every acknowledged write after a bit flip in the middle of the log.
- **Treat any damage as corruption.** Then an ordinary crash during a write
  would stop the node until an operator intervened.
- **Append-after-recovery** into the old newest segment. Saves a file per
  restart but mixes pre- and post-crash writes in one file and makes the
  "torn writes are only at the end" argument harder.

## Consequences

- An acknowledged record that later suffers bit rot is detected and stops
  startup, unless it is the very last record of the newest segment: that
  damage is indistinguishable from a torn write, so the record is dropped
  (`TestDamagedLastRecordIsATornTail` documents this).
- If a disk persists unsynced pages out of order, a crash could leave a
  damaged record followed by a valid unsynced one. The rule then reports
  corruption: a false alarm that loses no data but needs an operator. The
  fault model (ADR 0020) does not simulate out-of-order page writes.
- Detecting a valid record after damage scans byte by byte; cheap checks
  (type byte, plausible length) reject almost every offset before a CRC is
  computed, so this costs little even on large segments.
- Until Phase 3 adds flushes, no segment is ever deleted, so the log and the
  time to recover grow without bound.
