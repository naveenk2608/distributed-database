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
