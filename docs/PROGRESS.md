# Progress

Only what is implemented AND tested is listed here. See docs/PLAN.md for the full plan.

## Phase 1: Skeleton and single-node in-memory store

Status: **done.** All three gate items passed on 2026-11-16.

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| Randomized skiplist test against a Go map passes under `-race` with concurrent readers | `internal/storage/memtable` `TestConcurrentReadersMatchModel` (one writer, 8 readers doing point reads and full scans, each checked against an immutable Go-map model at the latest published sequence number) and `TestModelRandomized`. Passed 8 repeated `-race` runs with different seeds. Mutation check: publishing a node before its links, reloading links in `findGE`, or publishing a batch early all make the tests fail. | Passed |
| `dkvctl put/get/delete` works against a running server | `test/e2e` `TestDkvctlAgainstRunningServer` builds the real binaries, starts `dkv-server` as a process, runs put, get, overwrite, delete, get-missing (exit 1) and delete-missing, then checks a clean exit on SIGTERM. Also run by hand on 2026-11-16: `dkvctl put foo bar` (OK, version 1), `get foo` (bar), `delete foo` (deleted, version 2), `get foo` (not found, exit 1); the server exited 0 on SIGTERM after a graceful shutdown. | Passed |
| CI is green | `.github/workflows/ci.yml` runs `go mod tidy -diff`, `go mod verify`, `make proto-check` and `make check` on ubuntu-latest. Run 43891361720 on e88b4f0 (final Phase 1 code) and run 43890290667 on ddb74f7 both succeeded. | Passed |

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

Status: **done.** All three gate items passed locally on 2026-11-16.

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

- **The WAL and the memtables grow without bound** until Phase 3 adds flushes: no segment is ever deleted, every restart replays the whole log, and all data stays in memory. This is the planned one-phase limitation.
- `SyncInterval` is accepted in config; the plan allows it in standalone mode only, which is the only mode so far. Phase 6 must reject it for replicas.
- Damage to the very last record of the newest segment is indistinguishable from a torn write, so that record is dropped (documented by `TestDamagedLastRecordIsATornTail`; ADR 0019).
- The fault model does not simulate out-of-order persistence of unsynced pages or disks that ignore fsync (ADR 0020).
- `/tmp` on the development machine is tmpfs, where fsync is free, so tests using `t.TempDir()` do not measure fsync cost. Durability correctness is covered by `MemFS`; fsync cost will matter for the Phase 10 benchmarks, which must run on a real disk.
- Not done: an independent second review of the Phase 2 diff against the plan.
