# ADR 0020: Filesystem interface and the simulated power-loss model

Status: Accepted (Phase 2)

## Context

The Phase 2 gate requires that every write acknowledged under `SyncAlways`
survives a simulated power loss. Real power-loss testing needs special
hardware; a SIGKILL test (also in the gate) does not lose the page cache, so
it cannot catch a missing fsync. The project rules allow test doubles only at I/O
seams, and the filesystem is the storage engine's only one.

## Decision

`internal/storage/vfs` defines a small `FS` interface (create, open,
read-write open, remove, rename, link, mkdir, list, directory fsync, lock)
and a `File` interface (read, read-at, write, sync, truncate, size). `vfs.Default`
is the operating system. The engine, WAL and (from Phase 3) SSTables and
manifest use only this interface.

`vfs.MemFS` is an in-memory filesystem that tracks durability separately
from contents:

- A file's contents are durable as of its last `Sync`.
- A directory's entries (create, rename, link, remove) are durable as of its
  last `SyncDir`. A file whose entry was never made durable vanishes on a
  crash even if its data was synced, which is what forces the code to fsync
  parent directories.
- `CrashImage(rng)` returns the filesystem a restarted process would see.
  With a nil rng it holds exactly the durable state. With an rng, a file
  whose unsynced change only appended may keep a prefix of the appended
  bytes (none, all, or a random amount), and the last surviving bytes may be
  garbage, like a partly written sector. The same seed gives the same image.
- `Crash` and `CrashAfter(n)` make every later operation fail with
  `ErrCrashed`, so a test can crash at any chosen mutating operation and
  enumerate them all.
- Directories created by `MkdirAll` are durable immediately (a simplification;
  the engine fsyncs the parent anyway).
- `Lock` uses `flock` on the OS, which the kernel releases when a process
  dies, so a SIGKILLed engine never leaves a stale lock.

## Alternatives considered

- **Wrap the OS filesystem and drop data on "crash"** (copy synced state
  aside). Slower, and directory-entry durability cannot be modeled without
  reimplementing it anyway.
- **dm-flakey / LazyFS style block-device or FUSE fault injection.** The most
  realistic, but needs root and Linux-only tooling in CI and is slow; worth
  revisiting for the Docker chaos suite.
- **Only SIGKILL tests.** Cannot detect a missing fsync, because the page
  cache survives a process crash.

## Consequences

- Durability tests are fast and deterministic per seed: the gate test runs
  150 concurrent power-loss scenarios and a crash at every one of a
  workload's filesystem operations, including during recovery itself.
- A mutation check (deleting each fsync in turn) is caught by these tests,
  recorded in docs/PROGRESS.md.
- Not modeled: out-of-order persistence of unsynced pages within a file,
  metadata-only fsync (`fdatasync`) differences, and filesystems that lie
  about fsync. Those are covered only by the assumptions stated in
  docs/PLAN.md ("provided the disks honor fsync").

## Revision (Phase 3): unsynced directory changes survive as an in-order prefix

Status: Accepted (Phase 3)

The Phase 3 mutation check found a blind spot. Deleting a compaction's input
tables *before* writing the new manifest is a real bug: on a journaling
filesystem, a periodic journal commit can persist the unlinks while the
manifest rename is still unwritten, leaving the old manifest pointing at
deleted tables. The original model made a directory's changes durable only
all together at the next `SyncDir`, so no crash image ever separated the
unlinks from the rename, and the tests passed with the bug in place.

`MemFS` now records each directory's unsynced changes in order, and
`CrashImage` with an rng keeps an in-order prefix of them (none, all, or a
random number), with a same-directory rename as one atomic change. This
matches what ext4 and XFS guarantee for metadata: changes may become durable
before any fsync, but in the order they were made. With the stronger model
every crash test still passes, and the mutation is caught by
`TestCrashDuringFlushAndCompaction` and `TestPowerLossKeepsAcknowledgedWrites`.

Still not modeled: filesystems that reorder metadata changes (ext4 with
journaling disabled), and the subtleties of `rename` over an existing file
on filesystems without `auto_da_alloc`.
