# Progress

Only what is implemented AND tested is listed here. See docs/PLAN.md for the full plan.

## Phase 1: Skeleton and single-node in-memory store

Status: **done.** All three gate items passed on 2026-11-16.

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| Randomized skiplist test against a Go map passes under `-race` with concurrent readers | `internal/storage/memtable` `TestConcurrentReadersMatchModel` (one writer, 8 readers doing point reads and full scans, each checked against an immutable Go-map model at the latest published sequence number) and `TestModelRandomized`. Passed 8 repeated `-race` runs with different seeds. Mutation check: publishing a node before its links, reloading links in `findGE`, or publishing a batch early all make the tests fail. | Passed |
| `dkvctl put/get/delete` works against a running server | `test/e2e` `TestDkvctlAgainstRunningServer` builds the real binaries, starts `dkv-server` as a process, runs put, get, overwrite, delete, get-missing (exit 1) and delete-missing, then checks a clean exit on SIGTERM. Also run by hand on 2026-11-16: `dkvctl put foo bar` (OK, version 1), `get foo` (bar), `delete foo` (deleted, version 2), `get foo` (not found, exit 1); the server exited 0 on SIGTERM after a graceful shutdown. | Passed |
| CI is green | `.github/workflows/ci.yml` runs `go mod tidy -diff`, `go mod verify`, `make proto-check` and `make check` on ubuntu-latest. CI run 43924884382 on main at d2b3bba (Phase 1 and 2 code; `check` and `crash` jobs) succeeded. | Passed |

`make check` (gofmt, go vet, golangci-lint v2.15.0, `go test -race ./...`) passes locally.

### Implemented and tested

- **Build and API:** Go module, Makefile (`check`, `build`, `proto`, `proto-check`, `tools`, with pinned tool versions), buf lint and codegen, `proto/dkv/v1/kv.proto` (Put, Get, Delete) with generated code committed under `gen/`.
- **`internal/config`:** defaults < YAML < `DKV_*` env < flags. Strict about unknown YAML keys, env vars and flags; `Validate` reports every problem; no default addresses, ports or paths. A reflection test keeps flags and env vars in sync with the struct (ADR 0016).
- **`internal/obs`:** slog logger, JSON or text, configurable level.
- **`internal/storage/ikey`:** internal keys `userKey | seq(56 bits) | kind(8 bits)`, sorted by user key ascending, then newest first.
- **`internal/storage/memtable`:** insert-only skiplist; writers serialized, readers lock-free via atomic links (ADR 0017). Rejects a duplicate (key, seq) of any kind.
- **`internal/storage/lsm`:** in-memory `DB`. Each batch is applied atomically at one sequence number, engine-assigned or caller-supplied, strictly increasing with gaps allowed; a repeated key in a batch keeps its last write (ADR 0018). Also read snapshots, an iterator over live keys, and `Close`. Tested by a randomized model test with snapshots, and by a concurrency test checking that snapshot reads and iterators never see part of a batch.
- **`internal/server`:** gRPC KV service with key and value limits and status-code mapping. Graceful shutdown bounded by `server.shutdown_timeout`; the server stops itself if serving fails. Tested over real loopback gRPC with goleak.
- **`pkg/client`:** Put, Get (`ErrNotFound`), Delete. Accepts responses up to the server's largest configurable value; a 6 MiB value round trip is tested.
- **Binaries:** `cmd/dkv-server`; `cmd/dkvctl` (put/get/delete; exit 0, 1 for not found, 2 for errors, including failed stdout writes).
- **Architecture checks:** golangci-lint `depguard` rules ban third-party Raft and storage engines, allow PostgreSQL only in `cmd/dkv-console`, and keep `internal/storage` free of gRPC, protobuf, config and cluster packages (probed by adding a forbidden import).
- **Docs:** ADRs 0000 (scope), 0001 (replication model), 0002 (partitioning), 0016 (configuration), 0017 (memtable concurrency), 0018 (one sequence number per batch); README quick start; example config `deploy/config/dkv-server.example.yaml` (loaded by a test).

### Known limitations and deferrals

- **No persistence yet.** The engine is in-memory only, so data is lost on exit, and the memtable grows without bound. The WAL arrives in Phase 2, flush in Phase 3.
- **Deferred `internal/obs` parts.** Prometheus metrics and the cluster event log are listed under Phase 1 in the component table, but the Phase 1 roadmap only asks for slog logging. They will be added with the components they measure.
- **Engine API differs from the PLAN sketch:**
  - `OpenInMemory()` takes no options; `Open(dir, opts)` arrives with the WAL.
  - `Apply` returns the sequence number it used.
  - `NewIterator` returns an error once the DB is closed.
  - `Snapshot` has no `Close`/release yet. Phase 3 compaction needs one so it can keep versions that open snapshots still see.
- **Not tested:** the forced-close path when graceful shutdown exceeds `server.shutdown_timeout`. Nothing in Phase 1 can hold a request open long enough to trigger it without a test double.
- **Not available yet:** `make sim`, `make test-integration` and `make chaos`. They are added in the phases that create those suites (6, 4 and 8).

## Phase 2: WAL, persistence and crash recovery

Status: **done.** All three gate items passed locally on 2026-11-16; CI run 43924884382 on main at d2b3bba (`check` and `crash` jobs) is green.

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| Under simulated power loss, every write acknowledged with `SyncAlways` survives | `internal/storage/lsm` `TestPowerLossKeepsAcknowledgedWrites`: 150 seeds × 3 rounds of 4 concurrent writers on `vfs.MemFS`, a crash at a random filesystem operation, torn unsynced tails, recovery, more writes. Each batch writes a unique marker key, so the test learns which batches survived and at which sequence number; it checks that every acknowledged batch survived at its acknowledged version, that surviving sequence numbers are exactly 1..LastSeq (the log is a prefix), and that the full state, versions included, equals replaying those batches. Last run: 44,656 acknowledged batches, all present. `TestCrashAtEveryStep` crashes at each of the 125 filesystem operations of a sequential workload (Open, rotations, Close) with exact and torn images, then crashes again at every step of the recovery: 1,550 recoveries checked. | Passed |
| 100 or more kill -9 cycles lose no acknowledged write | `test/crash` `TestKill9Cycles` via `make crash`: a child process (the test binary) writes with `SyncAlways` from 4 goroutines and prints each write before and after its acknowledgment; the parent sends SIGKILL after a random delay (a quarter of the time while the child is still starting or recovering), reopens the engine and checks every key (acknowledged puts present at their version, acknowledged deletes absent, in-flight writes either way, no phantom keys). One directory accumulates across cycles. Last run: 150 cycles, 302,880 acknowledged writes, all present; 32 kills during startup or recovery. `make check` runs 20 cycles under `-race`; CI runs `make crash` as its own job. | Passed |
| A corrupted record in the middle of the log stops startup with a clear error | `internal/storage/lsm` `TestCorruptRecordMidLogStopsOpen` (damage in an older segment, and in the middle of the newest one): `Open` fails with `ErrCorruption`, the message names the segment file and offset and says "refusing to start", the file is untouched, and a second attempt gives the same error rather than a lock error. `internal/storage/wal` `TestCorruptionIsReportedNotRepaired` covers payload, length-field and CRC damage. `test/e2e` `TestCorruptLogStopsStartup` runs the real `dkv-server` on a damaged log: exit status 1, the log line names the file and offset, and the port is never opened. | Passed |

Mutation check (each change made by hand, gate tests run, change reverted):

| Mutation | Caught by |
| --- | --- |
| No fsync before acknowledging under `SyncAlways` | `TestPowerLossKeepsAcknowledgedWrites`, `TestCrashAtEveryStep` |
| No directory fsync after creating a segment | both of the above and `TestPowerLossKeepsSyncedRecords` (wal) |
| No fsync of the old segment on rotation | `TestPowerLossKeepsSyncedRecords` (under `SyncAlways` every group is already synced before rotation, so only the WAL-level test can see it) |
| No fsync of the torn-tail truncation | all three power-loss tests |
| Every damaged record treated as a torn tail | `TestCorruptRecordMidLogStopsOpen` |
| Publishing the sequence number before the batch is in the memtable | `TestConcurrentReadsSeeWholeBatches` ("saw a partial batch") |

### Implemented and tested

- **`internal/storage/vfs`:** `FS`/`File` interfaces, `vfs.Default` (OS, `flock` directory lock), and `MemFS`, which tracks file and directory-entry durability separately, simulates power loss with optional torn appends (`CrashImage`), and can crash at the Nth mutating operation (ADR 0020). One conformance test runs on both.
- **`internal/storage/wal`:** numbered segments of `crc32c | length | type | payload` records; `Writer.Append` writes a group with one `write` call, `Sync`, `Rotate` (syncs the old segment first), sticky errors after a failed write or fsync; `Recover` truncates a torn tail only at the end of the newest segment when no valid record follows, and otherwise returns a `CorruptionError` naming file and offset (ADR 0019). Fuzzed (`FuzzRecover`).
- **`internal/storage/lsm`:** `Open(dir, Options)` with directory lock, recovery and a fresh segment per open; LevelDB-style group commit where each batch is its own record with its own sequence number and one fsync covers the group (ADR 0018 revision); `SyncAlways`, `SyncInterval` (background goroutine owned by the DB, stopped by `Close`) and `SyncNever`; memtable rotation together with the WAL segment; reads and iterators across the active and immutable memtables through a heap-based merging iterator; sticky failure after a log error. Last write wins inside a batch, enforced by `Batch` (ADR 0018 revision); batch payloads are fuzzed (`FuzzDecodeBatch`).
- **`internal/config`:** `storage.dir` (required), `storage.sync`, `storage.sync_interval`, `storage.memtable_bytes`, with validation.
- **`cmd/dkv-server`:** opens and recovers the engine before binding the port. `test/e2e` `TestDataSurvivesKill9AndRestart` checks that writes survive SIGKILL of the real server and that versions continue.
- **Tooling:** `make crash` (CYCLES, default 150), `make fuzz` (FUZZTIME), CI `crash` job.
- **Docs:** ADR 0018 revised (last write wins in `Batch`; one record per batch in group commit), ADRs 0019 and 0020.

### Known limitations

- **The WAL and the memtables grow without bound** until Phase 3 adds flushes: no segment is ever deleted, every restart replays the whole log, and all data stays in memory. This is the planned one-phase limitation. (Resolved in Phase 3: flushed segments are deleted, and recovery replays at most the unflushed memtables.)
- `SyncInterval` is accepted in config; the plan allows it in standalone mode only, which is the only mode so far. Phase 6 must reject it for replicas.
- Damage to the very last record of the newest segment is indistinguishable from a torn write, so that record is dropped (documented by `TestDamagedLastRecordIsATornTail`; ADR 0019).
- The fault model does not simulate out-of-order persistence of unsynced pages or disks that ignore fsync (ADR 0020).
- `/tmp` on the development machine is tmpfs, where fsync is free, so tests using `t.TempDir()` do not measure fsync cost. Durability correctness is covered by `MemFS`; fsync cost will matter for the Phase 10 benchmarks, which must run on a real disk.
- Not done: an independent second review of the Phase 2 diff against the plan.

## Phase 3: SSTables, LSM tree, indexing and compaction

Status: **done.** All three gate items passed locally on 2026-11-16 (`make storage-gate`).

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| A model test of one million random operations, with flushes, compactions and restarts interleaved, matches a Go map exactly | `internal/storage/lsm` `TestModelMillionOps` via `make storage-gate`: 1,000,000 operations (55% single puts, 15% deletes, 5% multi-key batches, 25% point reads checked against the map, values and versions both) on 20,000 keys, with 32 KiB memtables so background flushes and compactions run constantly, 10 clean restarts, up to 3 live snapshots checked against frozen copies of the map, and full scans every 100,000 operations. Last run: 250,351 point reads and 179 snapshot checks matched; 3,415 flushes, 1,073 compactions. `make check` runs 200,000 operations under `-race`; CI runs the full size in its `storage-gate` job. | Passed |
| The measured bloom filter false-positive rate is close to the configured one | `internal/storage/sstable` `TestBloomFalsePositiveRate`: 100,000 keys, 1,000,000 absent probes, no false negatives; at the default 10 bits per key 0.84% measured vs 0.82% expected (6, 14 and 20 bits per key: 6.2% vs 5.6%, 0.13% vs 0.12%, 0.008% vs 0.007%); the test allows 25%. `TestTableFilterRate` measures through a real table's `Get`: 0.78% of 200,000 absent lookups passed the filter. | Passed |
| A crash injected at every step of flush and compaction recovers to a consistent state | `internal/storage/lsm` `TestCrashDuringFlushAndCompaction`: a workload that writes, flushes, compacts (including a final bottom compaction) runs in manual mode so every run performs the same filesystem operations; the test crashes at each of the 412 operations, recovers from the exact durable state and from a torn image, and for every 7th point also crashes at every step of the recovery. Every recovery checks that each acknowledged batch survived at its version, that the surviving sequence numbers are exactly a prefix, and that the whole state matches. Last run: 1,827 recoveries. The Phase 2 power-loss test now also crashes during real background flushes and compactions. | Passed |

Mutation check (each change made by hand, gate tests run, change reverted; a test counts only if it fails by name):

| Mutation | Caught by |
| --- | --- |
| Tombstones dropped by a compaction that does not reach the oldest table | `TestModelMillionOps`, `TestPowerLossKeepsAcknowledgedWrites`, `TestTombstoneOutlivesNonBottomCompaction` |
| Compaction ignores live snapshots | `TestModelMillionOps`, `TestSnapshotsKeepVersionsThroughCompaction` |
| No fsync of a new table before the manifest names it | `TestCrashDuringFlushAndCompaction`, `TestPowerLossKeepsAcknowledgedWrites` |
| Log segments deleted before the manifest that flushed them is durable | `TestCrashDuringFlushAndCompaction`, `TestPowerLossKeepsAcknowledgedWrites` |
| No directory fsync after the manifest rename | `TestCrashDuringFlushAndCompaction` |
| Iterator keeps a key that aliases a table block | `TestCrashDuringFlushAndCompaction`, `TestModelMillionOps`, `TestPowerLossKeepsAcknowledgedWrites`, `TestReopenModel` |
| Compaction inputs deleted before the new manifest is written | `TestCrashDuringFlushAndCompaction`, `TestPowerLossKeepsAcknowledgedWrites`. This one was first **missed**: the fault model made all of a directory's changes durable together. `MemFS` now keeps an in-order prefix of unsynced directory changes, as a journaling filesystem may (ADR 0020 revision), and every crash test still passes. |

### Implemented and tested

- **`internal/storage/sstable`:** writer and reader for the plan's format: ~4 KiB data blocks with prefix-compressed keys and restart points, one bloom filter per table over user keys (FNV-1a + SplitMix64, double hashing), index, properties and footer; CRC-32C on every block, verified on read. Byte-capped LRU block cache shared through `Options.BlockCache`. Tests: round trip and seeks against a sorted model, point reads at random snapshots, corruption of data/metadata blocks and truncation, cache eviction, `FuzzOpenAndScan`.
- **`internal/storage/lsm`:**
  - Background flush of immutable memtables to tables; a flush deletes the log segments it covered once the manifest is durable.
  - Manifest rewritten in full via temp file, fsync, rename, directory fsync (ADR 0006); recovery deletes leftovers (`MANIFEST.tmp`, unlisted tables, flushed segments), refuses to start on a missing or damaged table or manifest, replays the log above the flushed sequence number and flushes it.
  - Reads through memtables and then tables newest first, stopping at the first visible version; a merging iterator over all of them; reference-counted read states, so files are closed and deleted only after the last reader lets go.
  - Size-tiered compaction of adjacent runs with LevelDB's snapshot-aware garbage collection and bottom-only tombstone removal (ADR 0005); rate limiter; `CompactionPool` to share a limit across engines.
  - Write slowdown and stalls (too many tables, too many memtables awaiting flush); read snapshots with `Release`; `Checkpoint(dir)` by hard links; `Flush`; `Metrics`.
- **`internal/storage/vfs`:** `MemFS` keeps an in-order prefix of unsynced directory changes in crash images.
- **Docs:** `docs/storage-format.md`; ADRs 0005 and 0006; ADR 0020 revised.
- **Tooling:** `make storage-gate`, `make bench`, `FuzzOpenAndScan` in `make fuzz`, CI `storage-gate` job.

Other tests: snapshots keep their versions through compaction and lose them after release; a tombstone survives a compaction that does not reach the oldest table; the compaction picker's rules; checkpoints are hard links (`os.SameFile`) and reopen at the right sequence number after the source compacts its tables away; writes stall while a flush is held and resume when it finishes; the rate limiter bounds compaction speed; flushes delete covered log segments; readers using snapshots never see half a batch while flushes and compactions replace tables underneath them.

Microbenchmarks (`make bench`, i3-1115G4, `t.TempDir()` on tmpfs, so fsync is free and these show engine overhead only): apply with `SyncNever` 2.2 µs; parallel apply with `SyncAlways` 3.4 µs; get from the memtable 0.69 µs; get from a table 4.5 µs (9 allocations, from building an iterator per lookup); get of an absent key 0.27 µs; scanning 100,000 keys 26 ms.

### Known limitations

- Engine options beyond the Phase 2 ones (block cache size, compaction thresholds and rate) are not in `internal/config` yet; the node uses the defaults. Phase 5 adds per-node settings when one node runs many engines that share a cache and a compaction pool.
- Leveled compaction, the plan's stretch goal, is not implemented.
- A table `Get` allocates an iterator per lookup, and a full scan of a table larger than the block cache allocates a block per read; both are candidates for the Phase 10 profiling work.
- Not done: an independent second review of the Phase 3 diff against the plan.

