# Storage format

Byte-level layout of everything one storage engine keeps on disk. All
integers are little-endian unless stated otherwise; `uvarint` is Go's
`encoding/binary` unsigned varint. Every checksum is CRC-32C (Castagnoli).

## Directory

One engine per directory (one per shard replica from Phase 5):

| File | Contents |
| --- | --- |
| `LOCK` | Empty. Held with `flock` while the engine is open, so two processes can never open one directory. |
| `MANIFEST` | The live tables and how much of the log they cover. Rewritten in full on every change. |
| `MANIFEST.tmp` | A manifest being written. A leftover one is deleted at startup. |
| `NNNNNN.wal` | Write-ahead log segments, numbered upwards. Segments below the manifest's log number are deleted. |
| `NNNNNN.sst` | Sorted tables. A table the manifest does not name is a leftover from an interrupted flush or compaction and is deleted at startup. |

Numbers are zero-padded to six digits and widen beyond 999999. Only the
canonical spelling is recognized (`1.wal` is not a segment).

## Internal keys

Every entry is keyed by `userKey | trailer`, where the 8-byte trailer packs
the sequence number (high 56 bits) and the kind (low 8 bits: `0` delete,
`1` set). Entries sort by user key ascending, then trailer descending, so the
newest version of a key comes first. A key's version is the sequence number
of the batch that wrote it: the engine's own counter in standalone mode, the
Raft log index in clustered mode. Every operation in a batch shares the
batch's sequence number, and a batch holds at most one operation per key
(ADR 0018).

## Write-ahead log

```text
segment := record*
record  := crc32c(4) | length(4) | type(1) | payload(length)
```

The CRC covers `length | type | payload`. Type `0` is reserved and never
written, so zero-filled space never decodes as a record. Records are
contiguous; there is no block structure. Group commit writes several records
with one `write` call and covers them with one `fsync`.

Type `1` is a batch:

```text
payload := seq(8) | count(4) | op{count}
op      := kind(1) | klen(uvarint) | key | vlen(uvarint) | value
```

A delete has `vlen = 0`. Keys within a batch are unique; a decoder rejects a
duplicate, an unknown kind, a delete with a value, a count that cannot fit,
and trailing bytes.

Recovery reads segments in order. Damage (short header, length past the end,
CRC mismatch, type 0) is a torn write, and is truncated and fsynced, only if
it is in the newest segment and no valid record starts anywhere after it.
Anything else stops startup with the file and offset (ADR 0019).

## SSTable

```text
[data block 0] ... [data block N]
[filter block]
[index block]
[properties block]
[footer]
```

Every block is `payload | compression(1) | crc32c(4)`; the CRC covers the
payload and the compression byte. Compression `0` (none) is the only value
defined.

### Data and index blocks

```text
block   := entry* | restart(4)* | numRestarts(4)
entry   := shared(uvarint) | unshared(uvarint) | vlen(uvarint) | key[shared:] | value
```

A key is stored as the bytes it does not share with the previous key in the
block. Every 16th entry in a data block (every entry in the index block) is a
restart point that stores its whole key; `restart` lists their offsets. A
seek binary-searches the restart points, then scans forward. Keys in blocks
are encoded internal keys: `userKey | trailer(8)`.

Data blocks end after roughly 4 KiB (configurable). The index block has one
entry per data block: the block's last key, and its handle
`offset(8) | size(8)` as the value. `size` excludes the 5-byte trailer.

### Filter block

```text
filter := bits(m/8 bytes) | k(1)
```

A bloom filter over the table's user keys (each distinct user key once).
A key is hashed once with FNV-1a followed by the SplitMix64 finalizer, and
the `k` probe positions are `h + i*delta mod m` with `delta = h>>33 | h<<31`.
`k = round(bitsPerKey * ln 2)`; at 10 bits per key the expected
false-positive rate is 0.82%. The hash is part of the format. An empty
filter matches everything.

### Properties block

```text
props := len(uvarint) | smallestUserKey | len(uvarint) | largestUserKey |
         minSeq | maxSeq | numEntries | numDeletions |
         rawKeyBytes | rawValueBytes | numDataBlocks | filterBitsPerKey   (uvarints)
```

### Footer (60 bytes)

```text
footer := filterHandle(16) | indexHandle(16) | propertiesHandle(16) |
          formatVersion(1) | reserved(3) | magic(8)
```

The magic number is `0x31747373766b64` ("dkvsst1" read little-endian);
the format version is `1`. Opening a table verifies the footer and the CRCs
of the filter, index and properties blocks; data blocks are verified when
they are read.

## Manifest

```text
manifest := "DKVMANI1" | body | crc32c(4)
body     := nextFileNum | logNum | flushedSeq | numTables | table{numTables}      (uvarints)
table    := fileNum | size | minSeq | maxSeq | entries | deletions |
            len | smallestUserKey | len | largestUserKey                         (uvarints, bytes)
```

The CRC covers everything before it. Tables are listed newest first. Their
sequence ranges never overlap, which lets a read stop at the first table
holding a visible version of a key (ADR 0005).

- `nextFileNum`: the next table number; numbers are never reused.
- `logNum`: log segments below it are fully flushed and deleted.
- `flushedSeq`: every batch at or below it is in the tables. In clustered
  mode this is the applied Raft index the tables hold.

Writing a manifest: write `MANIFEST.tmp`, fsync it, rename it over
`MANIFEST`, fsync the directory (ADR 0006). A table is fsynced, and its
directory entry fsynced, before any manifest names it. Files a new manifest
makes obsolete are deleted only after it is durable.

## Recovery

1. Read `MANIFEST` (none: an empty engine, or one from before Phase 3).
2. Delete `MANIFEST.tmp`, tables the manifest does not name, and log
   segments below `logNum`.
3. Open every listed table and check its size and footer against the
   manifest; a missing or damaged table stops startup.
4. Replay log segments from `logNum`, skipping batches at or below
   `flushedSeq`, into a memtable.
5. Flush that memtable to a new table, write a manifest whose `logNum` is a
   fresh segment, start that segment, and delete the replayed ones.

## Checkpoints

`DB.Checkpoint(dir)` flushes the memtables, hard-links every live table into
`dir`, and writes a manifest there with `flushedSeq` set to the sequence
number the copy holds. A checkpoint has no log; opening it gives a working
engine. Phase 7 streams checkpoints as Raft snapshots.
