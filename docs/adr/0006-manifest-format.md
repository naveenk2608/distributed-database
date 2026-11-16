# ADR 0006: Manifest written in full with an atomic rename

Status: Accepted (Phase 3)

## Context

The engine needs one durable record of which tables are live, which log
segments are still needed, the next file number, and the highest sequence
number the tables hold (the applied Raft index in clustered mode). Every
flush and compaction changes it, and a crash at any point must leave either
the old or the new version, never a mixture.

## Decision

The manifest is rewritten in full on every change: encode it, write it to
`MANIFEST.tmp`, fsync the file, rename it over `MANIFEST`, fsync the
directory. The encoding starts with a magic string and ends with a CRC-32C
over everything before it (docs/storage-format.md). A table is fsynced, and
its directory entry fsynced, before a manifest names it; files a manifest
makes obsolete (old tables, flushed log segments) are deleted only after it
is durable. At startup, `MANIFEST.tmp` and any table the manifest does not
name are deleted as leftovers. A failed manifest write deletes nothing,
because the rename may already have reached the disk; the engine stops
taking writes and the next Open sorts it out from whichever manifest it
finds.

## Alternatives considered

- **Append-only version-edit log** (LevelDB's MANIFEST). Each change appends
  a small delta, so cost does not grow with the number of tables. It needs
  its own torn-tail handling, periodic rewriting to stay short, and replay
  logic. With small per-shard engines the full manifest is a few hundred
  bytes to a few kilobytes, so a full rewrite is simple and cheap enough.
- **No manifest; derive the live set from the files present.** Cannot tell a
  finished table from one a crashed flush left behind, and cannot record the
  flushed sequence number.

## Consequences

- One manifest write costs two fsyncs (file and directory) per flush or
  compaction, which is small next to writing the table itself.
- The crash test that crashes at every filesystem operation of a flush and
  compaction workload, including every step of the recovery that follows,
  passes. Removing the directory fsync after the rename, or deleting
  compaction inputs before the new manifest is durable, makes it fail
  (docs/PROGRESS.md).
