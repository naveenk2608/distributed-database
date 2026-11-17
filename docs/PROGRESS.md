# Progress

Only what is implemented AND tested is listed here. See docs/PLAN.md for the full plan.

## Needs from you

- **Docker.** Docker Desktop is installed on Windows but its engine is not running, and
  this WSL distro has no Docker integration (`docker` says "could not be found in this
  WSL 2 distro"; `docker.exe` cannot reach `dockerDesktopLinuxEngine`). Please start Docker
  Desktop and enable Settings → Resources → WSL integration for this distro. Blocked until
  then: the Phase 4 Compose gate (`make compose-gate`, which runs
  `deploy/compose/gate.sh`). The Dockerfile and Compose file are written; the same check
  runs with three real `dkv-server` processes in `test/e2e`. The Phase 8 Docker chaos
  scenarios (`make chaos`) cannot run here either; they run in the nightly workflow on
  GitHub's runners, and passed there (Phase 8).
- **A decision on simulator seed 51674** (Phase 8, "First nightly run"): one of 100,000
  schedules fails the simulator's liveness check, in a configuration dkv never runs
  (PreVote off). Making it pass means changing what the simulator requires, which the rules
  leave to you; the options and my recommendation are listed there. Until then the nightly
  `sim` job fails at that seed.

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

Status: **done.** All three gate items passed locally on 2026-11-16 (`make storage-gate`); CI run 43929763379 on main at 2d13d1d (`check`, `crash` and `storage-gate` jobs) is green.

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

## Phase 4: Multi-node communication and cluster formation

Status: **done except the Docker Compose half of gate item 1**, which needs Docker (see
"Needs from you"). Everything else passed locally on 2026-11-16; CI run 43932653619 on main
at 2e82939 (`check`, `crash`, `storage-gate`) is green.

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| Three nodes form a cluster from config, in-process | `test/integration` `TestThreeNodesFormACluster` (`make test-integration`): `nodetest.Start` runs three full nodes on ephemeral loopback ports with separate data directories and one bootstrap peer list; every node reports all three UP, with the node ID each actually has, itself marked as self. Formed in 29 ms. | Passed |
| Three nodes form a cluster from config, in Compose | `deploy/compose/docker-compose.yml` (three nodes configured only through `DKV_*` variables) and `deploy/compose/gate.sh` (`make compose-gate`) are written. Not run: Docker is unavailable here. The exact Compose environment was smoke-tested with the real binary outside Docker (the node recognized itself by its advertise address and reported unresolvable peers DOWN after the window). As a stand-in, `test/e2e` `TestThreeProcessesFormACluster` runs three real `dkv-server` processes from one peer list and checks with `dkvctl status` that each sees all three UP. | **Blocked (Docker)** |
| Status reports a killed peer as down within the configured window | `test/integration` `TestKilledPeerIsReportedDownWithinWindow`: with `down_after` 1 s and 100 ms pings, both survivors report the stopped node DOWN 994–996 ms after it stopped; the test requires between window − interval − 50 ms and window + 300 ms, and that the survivors stay UP. `internal/cluster` `TestDownWindowIsExact` (fake clock) checks UP at exactly the window and DOWN 1 ns later. `test/e2e` `TestThreeProcessesFormACluster` SIGKILLs a real process: reported DOWN 3.2 s after the kill with a 2 s window, measured through repeated `dkvctl` runs, each a separate race-built process. | Passed (in-process and real processes; Compose blocked) |
| Transport tests cover timeouts and reconnects | `internal/transport`: `TestDeadlineIsEnforced` (peer accepts the call and never answers), `TestSilentPeerTimesOut` (TCP accepted, no HTTP/2), `TestUnreachablePeerFailsFast` (closed port → `Unavailable` at once), `TestReconnectAfterPeerRestart` (calls fail while the peer is down; the same pooled connection recovers 66 ms after the peer returns on the same address), `TestPoolSharesOneConnectionPerPeer`. | Passed |

### Implemented and tested

- **Protocol:** `proto/dkv/v1/cluster.proto` (`InternalService.Ping`, `NodeInfo`) and
  `admin.proto` (`AdminService.ClusterStatus`, `MemberStatus`, `MemberState`), generated code
  committed.
- **`internal/config`:** `cluster.name`, `internal_addr`, `advertise_addr`, `peers` (YAML list
  or comma-separated env/flag), `ping_interval`, `ping_timeout`, `down_after`;
  `transport.dial_timeout`, `keepalive_time`, `keepalive_timeout`. Validation: peers need an
  internal address, a wildcard internal address needs an advertise address, no port 0 or
  duplicates in peers, timing constraints. With neither peers nor an internal address the
  node is standalone, as before.
- **`internal/transport`:** internal gRPC server with keepalive parameters and enforcement
  policy; `Pool` with one lazily created connection per peer, jittered backoff capped at
  1 s, keepalives, message size limits (ADR 0022).
- **`internal/cluster`:** `Membership` pings each bootstrap peer from its own goroutine,
  derives UP/DOWN/UNKNOWN from the last answer's time, rejects nodes of another cluster,
  detects itself listed under another address, logs state changes; `PingService`
  (ADR 0021).
- **`internal/node`:** composition root shared by `dkv-server` and tests; persistent node
  ID in `<storage.dir>/NODE_ID` (atomic, fsynced; a damaged file stops startup).
  `nodetest` runs in-process clusters with stop and restart on the same addresses.
- **`dkvctl status`** and `client.ClusterStatus`.
- **Deployment:** `deploy/docker/Dockerfile` (multi-stage, static binaries, distroless
  nonroot), `deploy/compose/docker-compose.yml`, `deploy/compose/gate.sh`, `.dockerignore`;
  `make docker-build`, `compose-up`, `compose-down`, `compose-gate`, `test-integration`.
- **Docs:** ADRs 0021 and 0022.

Other tests: a restarted node rejoins with its original ID (`TestRestartedPeerRejoinsWithItsID`);
a node configured with another cluster name is never counted as a member and the reason is
shown (`TestNodeOfAnotherClusterIsNotAMember`); config rules and peer-list parsing; node ID
persistence and damaged-ID detection; `dkvctl status` output.

### Known limitations

- Membership is static (the bootstrap list) and each node's view is its own; there is no
  join, decommission or agreed liveness until the meta group in Phase 7.
- The KV API still uses one local engine per node; sharding and routing arrive in Phase 5.
- The Docker image and Compose file have never been built or run (Docker unavailable).
- Not done: an independent second review of the Phase 4 diff against the plan.

## Phase 5: Consistent hashing, sharding and data placement

Status: **done.** All three gate items passed locally on 2026-11-16; CI run 43936155645 on main
at c7e1b66 (`check`, `crash`, `storage-gate`) is green.

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| A balance test shows shard counts per node within a stated tolerance of the mean | `internal/placement` `TestBalance`. Stated tolerance (ADR 0023): the worst node's replica count is within `3·sqrt((1−p)/(S·p)) + 5%` of the mean, p = RF/N, for at least 95% of random member sets. Checked over 1,000 random member sets for each of S ∈ {64, 256} × N ∈ {3, 4, 5, 7, 10}, RF 3. Default S = 64: p95 0% / 20.8% / 27.6% / 38.5% / 56.3% against tolerances 5% / 26.7% / 35.6% / 48.3% / 62.3%; S = 256: 0% / 14.1% / 17.2% / 23.0% / 28.9% against 5% / 15.8% / 20.3% / 26.7% / 33.6%. | Passed |
| Adding a node to N nodes moves close to 1/(N+1) of shard assignments and no more | `internal/placement` `TestAddingANodeMovesOnlyItsShare`, 400 random clusters per N: on every trial every changed replica set gains only the new member and loses one old member, and primaries move only to the new member (nothing shuffles between old members). Average moved: 25.1%, 20.0%, 16.6%, 12.4%, 10.0% for N = 3, 4, 5, 7, 9 against optimal 25.0%, 20.0%, 16.7%, 12.5%, 10.0% (required within 10%). | Passed |
| Any node accepts any key and the request reaches the owner | `test/integration` `TestAnyNodeAcceptsAnyKey`: 4 in-process nodes, 16 shards, RF 2. Every node computes the same placement as `placement.Compute`; 400 keys are written through the four nodes in turn and read back through every node; afterwards each shard's primary has applied exactly the writes to that shard's keys, each secondary none, and nodes outside the replica set hold no replica. `test/e2e` `TestThreeProcessesFormACluster` writes through one real process and reads through another. `internal/router` tests: requests reach the owner via any node, a NotOwner hint is followed to the right node, a non-owner refuses forwarded requests, a dead owner fails within the caller's deadline. | Passed |

### Implemented and tested

- **`internal/placement`:** `ShardOf` (xxhash64 mod S); hash ring with 128 virtual nodes per member; deterministic replica sets of RF distinct members (first is primary), independent of member order; `Fingerprint` (ADR 0023).
- **`internal/router`:** serves a key's shard locally when this node is the primary, otherwise forwards with `InternalService.Forward`; one hop per attempt; NotOwner answers with an owner hint; retries on `Unavailable` and NotOwner with jittered exponential backoff until the caller's deadline, at most 10 attempts (ADR 0024).
- **`internal/kvstore`:** per-engine Put/Get/Delete shared by the standalone backend and the router; engine errors become gRPC status codes (`Unavailable`, `DataLoss`).
- **`internal/node`:** cluster mode opens one engine per shard replica held in `<storage.dir>/shards/NNNN/` with a node-wide block cache and compaction pool (ADR 0004), a node directory lock, and a `CLUSTER` marker that refuses a changed cluster name, shard count or replication factor and keeps standalone and cluster layouts apart.
- **`internal/config`:** `cluster.shards` (default 64), `cluster.replication_factor` (default 3), `storage.block_cache_bytes`, `storage.compaction_concurrency`; a cluster node's advertise address must appear in `cluster.peers`, and RF cannot exceed the peer count.
- **Membership:** nodes exchange a placement fingerprint; a peer with a different one is flagged in status.
- **Protocol and tools:** `InternalService.Forward`, `AdminService.ShardMap`, `dkvctl shards`, `client.ShardMap`.
- **Docs:** ADRs 0004, 0023, 0024.

Other tests: a shard's keys fail within the deadline while its owner is down and all other keys keep working (`TestOwnerDownAffectsOnlyItsShards`); data survives restarting every node (`TestShardDataSurvivesRestart`); a changed shard count, or a standalone start on cluster data, is refused (`TestClusterSettingsAreFixedAtBootstrap`); a misconfigured node's placement is flagged (`TestPlacementMismatchIsVisible`); `dkv-server` starts and stops as a one-node cluster (`TestRunAsSingleNodeCluster`, which would have caught a startup panic found by the e2e test).

### Known limitations

- Only a shard's primary serves it; while it is down its keys are unavailable. Phase 6 replaces this with Raft groups on the same replica sets.
- A retried write can be applied twice if a forward fails after the owner applied it; Phase 6 sessions make retries exactly-once.
- Placement members are configured addresses; changing the peer list changes placement without moving data (Phase 7).
- With the default 64 shards, primaries can be quite uneven (the busiest node may serve about twice its share); Phase 7's leader balancing addresses it.
- Not done: an independent second review of the Phase 5 diff against the plan.

## Phase 6: Raft consensus and replication

Status: **done.** All three gate items passed locally on 2026-11-16; CI run 43984531065 on main
at 8c7def4 (`check`, `crash`, `storage-gate`, `sim`) is green.

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| At least 10,000 seeded simulator schedules with zero violations of Election Safety, Log Matching, Leader Completeness and State Machine Safety; a failing seed replays identically | `internal/sim` via `make sim SEEDS=10000`: each schedule draws 3 or 5 nodes, drop rate 0–30%, duplicates 0–10%, delays up to 5 steps (so reordering), crashes that lose everything not fsynced (including commit-index updates written without fsync), restarts, and partitions, for 400 steps. Election Safety and Leader Completeness are checked whenever a leader is elected, State Machine Safety whenever an entry commits or applies, ReadIndex safety (a confirmed read index is never below an index committed before the read began) whenever a read is confirmed, and Log Matching across all live logs every 10 steps and at the end; then 300 calm steps must elect a leader and apply a new proposal on every node. Last run: 10,000 schedules in 81.5 s, zero violations; 23,843 leaders, 3,046,831 commit advances, 32,761 crashes, 22,421 partitions, 318,190 reads confirmed, 34,191,767 messages delivered and 3,748,186 dropped. `TestReplayIsIdentical` runs 50 seeds twice each and requires identical trace hashes (and different hashes for different seeds); a failing seed is rerun with `-first-seed`. `make check` runs 1,000 seeds under `-race`; CI runs 10,000 in its `sim` job. | Passed |
| A three-node RF=3 cluster keeps serving after its leader is killed, with a new leader within about two election timeouts and no acknowledged write lost | `test/integration` `TestLeaderFailover`: 3 in-process nodes, 4 shards, 20 ms ticks (election timeout 200–400 ms). Four writers run through the survivors' public APIs; the leader of shard 0 is stopped; every shard it led must get a new leader within 2 election timeouts + 10 ticks (600 ms) of the stop; writes continue; then every acknowledged write is read back (linearizable) through a survivor, and after the stopped node restarts, through it and from its own replicas. 40 runs under `-race`: slowest new leader 203–394 ms per run (median 309 ms); 316 ms in a full parallel `go test -race ./...`. `test/e2e` `TestSIGKILLedLeaderIsReplaced`: three real `dkv-server` processes; the shard-0 leader gets SIGKILL after 40 acknowledged writes; a new leader is seen within the limit (2 timeouts + 25 ticks, as it polls each survivor over gRPC from another process); 40 more writes succeed; all 80 read back from the survivors and, after restart on the same data directory, from the killed node. 5 runs: 214–359 ms. | Passed |
| A retried PUT is applied exactly once | `test/integration` `TestRetriedPutAppliedOnce`: (1) a client interceptor lets the first Put reach the cluster and be applied, then reports `Unavailable` as if the response were lost; the client retries with the same session and request ID and gets the version the first try was applied at, and a read returns that version; (2) a Put through a follower, then the shard's leader is stopped, then the identical request is sent again: it returns the original version and the value is the first write's, not the retry's; a request ID older than the session's latest is refused with `FAILED_PRECONDITION`. `internal/replica` `TestRetriedWriteAppliedOnce` and `TestStateSurvivesRestart` (the session table survives restarting every replica). | Passed |

Mutation checks (each deliberate bug applied alone, then the named tests run; scripts kept
outside the repository):

| Deliberate bug | Caught by |
| --- | --- |
| Commit old-term entries by counting replicas (Raft paper figure 8) | `TestFigure8` |
| Vote for anyone, any number of times | simulator: Election Safety |
| Ignore the candidate's log freshness | simulator: Leader Completeness; `TestMustSync` |
| Accept appends without the consistency check | `TestFigure8`, `TestSimultaneousPreCandidatesDoNotSplit` |
| Commit without a quorum | `TestFigure8`, `TestConflictHintsConvergeQuickly`, simulator: Leader Completeness |
| ReadIndex without a heartbeat round | `TestReadIndexNeedsAQuorum`, simulator: ReadIndex safety |
| A leader ignores a higher term | simulator: liveness |
| A vote not fsynced before replying | `TestMustSync` |
| Followers ignore their lease for pre-votes | `TestPreVoteKeepsTermStableAcrossPartition`, `TestPreCandidateAsksAgainAfterLease` |
| No CheckQuorum step-down | `TestCheckQuorumLeaderStepsDown` |
| No pre-vote tie-break | `TestSimultaneousPreCandidatesDoNotSplit`, `TestPreCandidateAsksAgainAfterLease` |
| Tie-break winner does not ask again | `TestPreCandidateAsksAgainAfterLease` |
| Applied entries not treated as committed on restart | `TestRestartWithAppliedBeyondCommit` |
| Retries applied again (no session check) | `TestRetriedWriteAppliedOnce`, `TestStateSurvivesRestart`, `TestRetriedPutAppliedOnce` |
| Session record not written with the write | same three tests |
| Client draws a new request ID for each retry | `TestRetriedPutAppliedOnce` |
| Raft log not fsynced | `raftlog` `TestPowerLossKeepsSavedState` |

An earlier version of `TestFigure8` passed with the figure-8 bug in place (the leader never
learned that a majority held the old entry); it was reworked until it failed for that bug.

### Implemented and tested

- **`internal/raft`** (ADR 0003): deterministic core with `Tick`, `Step`, `Propose`,
  `ReadIndex`, `Ready`/`Advance`; randomized election timeouts; PreVote; CheckQuorum with
  follower leases; leader no-op and the current-term commit rule; conflict hints;
  probe/replicate flow control with up to 8 appends in flight and stall detection;
  batched ReadIndex; `Ready.MustSync` (no fsync for commit-only changes); restart with the
  state machine's applied index; pre-vote tie-break with re-ask; heartbeats on a
  free-running tick count so a node's groups heartbeat together.
- **`internal/raft/raftlog`:** Raft log and HardState on the WAL format; suffix
  replacement; optional fsync per save; power-loss and corruption tests.
- **`internal/sim`:** seeded simulator and invariant checker described above.
- **`internal/replica`:** one goroutine per replica running persist → send → apply →
  release reads; engine at `shards/NNNN/kv`, Raft log at `shards/NNNN/raft`; apply at
  sequence number = log index; client sessions in the engine with leader-stamped expiry
  (ADR 0008); compare-and-set decided at apply (ADR 0009); linearizable reads by ReadIndex
  and stale reads with `min_version` (ADR 0007); not-leader errors with a leader hint;
  writes in flight when leadership is lost fail with `UNAVAILABLE` (safe to retry);
  leadership changes logged.
- **`internal/node`:** opens one replica per shard held, one node-wide Raft ticker, and a
  Raft transport that batches all replicas' messages per destination node into one
  `InternalService.Raft` call (bounded queues; plain heartbeats wait up to 2 ms for
  company); shard map with leader, term, role, commit and applied index. `nodetest`
  clusters default to 8 shards and 20 ms ticks, and log to stderr with `NODETEST_LOG=1`.
- **`internal/router`** (ADR 0024, revised): leader cache, local replica first, NotLeader
  hints followed at once, rotation through the replica set while no leader is known,
  stale reads served locally, up to 12 attempts.
- **`internal/config`:** `raft.tick_interval` (50 ms), `raft.election_ticks` (10),
  `raft.heartbeat_ticks` (2), `raft.session_ttl` (1 h), with validation.
- **Protocol:** `raft.proto` (`Command`, `RaftMessage`, `RaftRequest`), `CompareAndSet`,
  `Consistency` and `min_version` on reads, `client_id`/`request_id` on writes, applied
  index in read responses, Raft fields in `ShardInfo`.
- **`pkg/client`:** several node addresses, retries on `UNAVAILABLE` with jittered
  backoff, a pool of sessions so concurrent writes each have one, `CompareAndSet`,
  `GetStale`, `Options` with dial options.
- **`dkvctl`:** `cas`, `--stale`, `--min-version`, comma-separated `--addr`; `shards` shows
  role, leader, term, commit and applied index.
- **CI:** new `sim` job (`make sim SEEDS=10000`).
- **Docs:** ADRs 0003, 0007, 0008, 0009, 0010; 0022 and 0024 revised.

Other tests: a minority of nodes down keeps every shard writable
(`TestMinorityDownKeepsEveryShardWorking`); a majority down fails within the client's
deadline (`TestMajorityDownFailsWithinDeadline`); the Phase 5 any-node test now also checks
every replica applied every write, through stale reads served by each replica's own node;
router tests run real replica groups over an in-process transport (leader routing via any
node, hint following, NotOwner answers, local stale reads, leader errors not retried, dead
replica sets failing within the deadline); replica tests (replicated operations, CAS,
restart, writes after the leader stops); config validation for the new keys; `dkvctl cas`
and stale reads.

### Known limitations

- The Raft log is never truncated and there are no snapshots; a replica far behind
  catches up from the full log. Phase 7.
- Replica sets are fixed by the static placement; no membership changes, learners or
  leader transfer (Phase 7). Raft member IDs are advertise addresses.
- Leaders are not balanced across nodes.
- Idle Raft groups cost CPU: about 0.06 of a core per node for an idle three-node
  cluster with 64 shards and 50 ms ticks (release build). Under the race detector a
  test cluster with 64 shards and 20 ms ticks used about 1.2 cores, which slowed a
  timing test in the Phase 4 suite; test clusters now default to 8 shards.
- Each write fsyncs the Raft log and, with `storage.sync = always`, the engine WAL (ADR
  0010; single-log path in Phase 10).
- No test yet combines a running replica with a simulated power loss; the reasoning is in
  ADR 0010 and Phase 8's fault injection covers it.
- Session records are never deleted, only overwritten; a standalone node does not
  deduplicate retries. No lease reads.
- Failover times were measured on a WSL2 machine under the race detector, in process and
  with three local processes; not on separate machines.
- The Docker Compose gate is still blocked (see "Needs from you").
- Not done: an independent second review of the Phase 6 diff against the plan.

## Phase 7: Snapshots, failure detection, membership changes and recovery

Status: **done.** All four gate items passed locally on 2026-11-17; CI run 44023535159 on main
at aae1e71 (`check`, `crash`, `storage-gate`, `sim`) is green.

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| A node down past the dead timeout has its replicas recreated elsewhere and every shard returns to RF voters | `test/integration` `TestDeadNodeReplicasAreRecreated`: 4 nodes, 8 shards plus the meta group, RF 3, `liveness.dead_after` 1.5 s. One node is stopped; the test waits until every group's leader reports 3 voters, none of them the stopped node, and no learners; the stopped node must be recorded dead, nothing may move before the dead timeout, the descriptors must drop it, and every acknowledged write must read back. Four runs (three under `-race`): every group had 3 live voters again 2.2–3.1 s after the stop, with the node a voter in 5 to 9 of the 9 groups. | Passed |
| A wiped node rejoins and catches up through snapshots | `TestWipedNodeRejoinsThroughSnapshots`: 3 nodes, 4 shards, `raft.compact_entries` 50; 400 writes, then the test waits until every shard's log is compacted. One node is stopped, its data directory deleted, and it is restarted with the same configuration. It comes back with a new node ID, sees from its peers' pings that the cluster was bootstrapped without it, and joins; its old identity is declared dead and removed. Every group then has 3 voters including the new ID and not the old one; the node's own replicas serve every acknowledged write through stale reads. Four runs: back in every group 2.1–2.5 s after the restart, each time after installing 4 snapshots. | Passed |
| Adding a node rebalances with write unavailability limited to brief leader-transfer pauses, measured and reported | `TestAddedNodeTakesItsShare`: 3 nodes, 8 shards; four writers run through the client during the whole rebalance. A fourth node joins with `cluster.join`; the test waits until every group's voters equal the placement the ring computes over the four nodes (no learners) and until no node leads two more groups than another, with the new node leading at least one. Every write must succeed (the client retries a leader that is handing over) and none may take over 2 s; every acknowledged write reads back through the new node. Four runs: placement reached in 0.86–1.1 s; 298–661 writes during the rebalance, 0 failed; write latency p99 57–96 ms, max 89–396 ms; 0–3 writes over 100 ms. | Passed |
| Losing meta quorum stops rebalancing but not reads or writes | `TestMetaQuorumLossStopsRebalancingNotData`: 4 nodes, 8 shards, `dead_after` 1 s. Every node drops the meta group's Raft traffic in both directions (a transport filter, the fault-injection hook), and the test waits until the meta group has no leader. A data node is then stopped. For three dead timeouts, writes and linearizable reads to keys of all shards keep succeeding, and no group changes its voters. When the filter is removed, the meta group elects a leader, waits a full dead timeout, and the stopped node is replaced in every group. Four runs: 64–176 writes and their reads during the outage; replaced 1.6–1.9 s after recovery. | Passed |

Mutation checks (each deliberate bug applied alone; the named tests fail):

| Deliberate bug | Caught by |
| --- | --- |
| The planner removes a group's leader without transferring leadership first | `meta` `TestPlanConvergesSafely`, `TestNewNodeGetsItsShareAndLeaders` |
| The planner promotes a learner that has not caught up | `TestLearnerWaitsUntilCaughtUp` |
| The planner adds a learner before removing a surplus voter | `TestPlanConvergesSafely`, `TestNewNodeGetsItsShareAndLeaders`, `TestDecommission` |
| A new meta leader declares nodes dead without waiting a full timeout | `TestLiveness` |
| A removed voter stops campaigning before its removal commits | `raft` `TestRemovedNodeCampaignsUntilItsRemovalCommits` |
| A non-voter's timer reset keeps it inside its lease forever | `TestLearnerLeaseExpires` |
| The engine is switched to a snapshot before the Raft log records it | `replica` `TestCrashDuringSnapshotInstall` |
| An interrupted snapshot install is not finished on restart | `TestCrashDuringSnapshotInstall` |
| A wiped node bootstraps instead of joining | `TestWipedNodeRejoinsThroughSnapshots` |
| Leaders never send snapshots | `TestWipedNodeRejoinsThroughSnapshots` |

The thirteen Raft mutations of Phase 6 were rerun on the Phase 7 core and are all still
caught (some now also by the membership tests).

Also tried: writing a staged snapshot's completion marker before moving its files was not
caught, and is not a bug: the marker is only consulted after the Raft log records the
snapshot, which happens after both steps finish in the same goroutine.

The simulator (`make sim SEEDS=10000`) now also changes membership (adding spare nodes that
start uninitialized, promoting, removing), transfers leadership, compacts logs and installs
snapshots. Last run: 10,000 schedules in 53 s, zero violations: 36,089 leaders, 2,907,248
commit advances, 33,585 crashes, 22,207 partitions, 306,049 reads confirmed, 12,397
membership changes, 18,009 leader transfers, 100,004 compactions, 11,695 snapshots
installed. It found two liveness bugs while the work was in progress: a removed leader that
lost leadership before its removal committed could never be replaced (fixed by letting it
campaign, Raft thesis §4.2.2), and a learner whose leader died stayed inside its lease
forever (fixed by not resetting its timer); each has a unit test that fails without the fix.

### Implemented and tested

- **`internal/raft`** (ADR 0003, 0012): log compaction (`Compact`, `SnapshotAt`), snapshot
  requests and installs (`MsgSnap`, `ReportSnapshot`, `Ready.Snapshot`), single-server
  membership changes with learners applied on append (one at a time, only after the
  leader's first commit), a bootstrap entry recording the initial voters, uninitialized
  replicas, removed voters campaigning until their removal commits, removed leaders stepping
  down on commit, leader transfer with `MsgTimeoutNow`.
- **`internal/raft/raftlog`:** checkpoint records for compaction and snapshot installs,
  written as the only record of a new segment before older segments are deleted; power loss
  at each of the seven filesystem operations recovers the old or the new log.
- **`internal/replica`:** log compaction after flushing the engine; snapshots sent from engine
  checkpoints by a background goroutine and received into a staging directory, marked
  complete, recorded in the Raft log and then made the engine through the `ENGINE` pointer
  file, with interrupted installs finished on restart; conditional multi-key transactions
  (`Txn`); prefix scans; membership changes and leader transfer; status with membership,
  progress and transfer counters; `Destroy`.
- **`internal/meta`** (ADR 0011–0013): meta records and keys, the initial layout, the bootstrap
  transaction, liveness tracking, and the rebalance planner as a pure function, with a seeded
  property test (300 random clusters) that it converges safely.
- **`internal/node`:** node IDs as Raft member IDs and a directory of addresses and
  descriptors (refreshed from the meta group, cached in `DIRECTORY`); bootstrap once every
  bootstrap peer has answered, or join through `cluster.join` or through the peers when the
  cluster exists without this node; replicas reopened from disk, created for a group's leader
  on demand, and deleted once the group's leader no longer lists the node; heartbeats to the
  meta leader; on the meta leader, meta bootstrap, liveness decisions, group status
  collection and the rebalancer's steps; `InstallSnapshot` streaming; a Raft transport filter
  for fault-injection tests.
- **`internal/router`:** groups addressed by node ID, the meta group routed like a shard for
  internal transactions and scans, local replicas created at run time.
- **Protocol:** `meta.proto` (node records, descriptors, settings), `OP_TXN`, snapshot fields
  on Raft messages, `InstallSnapshot`, `NodeStatus`, `ChangeReplicas`, `TransferLeader`,
  `Join`, `Heartbeat`, `AdminService.Decommission`, node state and suspicion in member status.
- **Config:** `cluster.join`, `cluster.meta_replicas`, `raft.compact_entries`,
  `liveness.heartbeat_interval`, `liveness.suspect_after`, `liveness.dead_after`,
  `rebalance.interval`, with validation (a cluster node needs peers or join).
- **Tools:** `dkvctl decommission <node-id>` and `client.Decommission`; `dkvctl status` shows
  each node's state from the meta group.
- **Docs:** ADRs 0011, 0012, 0013; 0003, 0010, 0021, 0023 and 0024 revised.

Other tests: decommissioning drains a node, marks it decommissioned, and the node then
deletes its replicas (`TestDecommissionDrainsNode`); a learner catches up by snapshot after
compaction and survives a restart, leader transfer moves leadership to a named voter
(`replica` tests); a snapshot install is crash-tested at each of 61 filesystem operations;
conf-change encoding, overwritten membership entries being undone, one change at a time,
compaction rules (`raft` tests); `vfs.RemoveAll` and `WriteFileDurably`; config validation for
the new keys.

### Known limitations

- Nodes poll the meta group every rebalance interval rather than watching a stream; routing
  can be stale for that long (covered by NotOwner hints and retries).
- Bootstrapping needs every bootstrap peer reachable once.
- A node's directory knows a joined node only through the meta group; pings cover only the
  bootstrap peers, so status shows joined nodes without ping data.
- Snapshots are streamed whole on every attempt (no resume), and one at a time per follower.
- Only the meta leader knows which nodes are suspect; other nodes' status does not show it.
- Leader balancing stops when no single transfer within a group narrows the gap by two or
  more; the leader count per node can differ by two.
- The Docker Compose gate is still blocked (see "Needs from you").
- Not done: an independent second review of the Phase 7 diff against the plan.


## Phase 8: Fault injection and distributed integration testing

Status: **done.** Both gate items passed locally on 2026-11-17 with the in-process backend, and
the Docker backend's scenarios passed on GitHub's runners (nightly run 44050943445). CI run
44050875076 on main at 51cf806 (`check`, `crash`, `storage-gate`, `sim`) is green. The first
nightly run found two problems in earlier phases' code, below: one fixed, one waiting on a
decision.

The full suite also passes on the Phase 8 cluster layer: `go test -p 2 -count=1 ./...` ran all
31 packages (27 with tests) in 3 m 18 s with no failure. At the default parallelism the run was
killed for lack of memory (exit 137) on this 3 GB machine; run it here with `-p 2`
(`GOFLAGS=-p=2 make check`).

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| Every scenario in the failure matrix passes the linearizability check across repeated runs | `test/linearizability` `TestFailureMatrix` (`make lin LIN_RUNS=5`): 3 nodes, 4 shards plus the meta group, 20 ms ticks, `dead_after` 2 s. Per run, four clients (three pinned to one node each, one using all) issue puts, gets, deletes and compare-and-sets with unique values on 8 keys for 3 s of faults plus recovery; every operation is recorded with its invocation and response time, failed and timed-out ones as unknown outcomes; then every key is read once more and every such read must succeed. Porcupine checks the history against a per-key model that includes versions. One scenario per failure-matrix row (table below); 14 scenarios × 5 runs = 70 runs, all linearizable, in 5 m 33 s. Also run once each under `-race` inside `make check`. `TestPowerLossOfMajorityLosesNothing` (`storage.sync` always and interval): one node partitioned away while writes go on, the power of the other two cut (their replicas on `vfs.MemFS`, so only fsynced data survives), then everything restarted: linearizable, no acknowledged write lost. The Docker backend (`test/chaos`, `make chaos`: SIGKILL, SIGSTOP, iptables, tc netem against a three-node Compose cluster) cannot run here; on GitHub's runners (nightly run 44050943445, `CHAOS_RUNS=2`, 10 s of faults) all 12 of its scenarios × 2 runs = 24 histories were linearizable, 2,573–13,493 operations each. | Passed |
| A deliberately broken configuration fails the checker | `TestNegativeControlFailsTheChecker`: the same power loss with `storage.sync = never`, which now also skips the Raft log's fsyncs (ADR 0010). The two powered-off nodes come back without the writes they acknowledged; the partitioned node leads with older data; the checker must return Illegal, and does in 5 of 5 runs (seed 42, 1,129–1,788 operations each). | Passed |

| Failure-matrix row | Scenario | Nemesis (3 s of faults) | 5 runs: operations / unknown outcomes / injector |
| --- | --- | --- | --- |
| Kill a shard leader | `kill-leader` | stop a random shard's leader, restart it 600 ms later, repeat | 2,419–3,283 / 17–22 |
| Kill a minority | `kill-minority` | stop a random node for 1 s, repeat | 1,799–3,528 / 17–35 |
| Kill a majority | `kill-majority` | stop two nodes; stale reads from the survivor must answer; restart after 1 s | 633–2,247 / 36–45 |
| Isolate the leader in a minority | `isolate-leader` | cut a random shard's leader off from both peers for 1 s, repeat | 881–2,464 / 2–3 / 491–811 calls blocked |
| (same, leader cannot notice) | `paused-leader` | as above, with the leader's Raft clock stopped for 1.5 s | 1,230–1,790 / 2–3 / 520–785 blocked |
| Flapping or one-way partitions | `random-partitions`, `flapping-one-way` | random two-sided splits for 700 ms; random one-way blocks every 150 ms, healed a third of the time | 1,749–2,324 / 2 / 711–979 blocked; 693–2,126 / 1–5 / 495–823 blocked |
| Restart a node | `rolling-restart` | stop and restart each node in turn | 2,146–3,215 / 19–21 |
| Wipe a node's disk | `wipe-node` | stop a node, delete its data, restart it (new ID, snapshots) | 3,144–4,939 / 6–8 |
| Slow disk or high network latency | `slow-disk`, `slow-network` | 30 ms more per fsync on one node; 2–27 ms per call | 395–2,137 / 0; 1,067–1,324 / 0 / 1,456–1,502 delayed |
| Lost, duplicated or reordered messages | `lossy-network` | 10% loss, 10% duplication, 0–10 ms jitter on every call | 1,259–1,569 / 0 / 277–318 lost, 289–339 duplicated |
| Meta group loses quorum | `meta-quorum-loss` | every node drops the meta group's Raft traffic | 3,573–4,763 / 0 |
| Client retries after a timeout | `client-retries` | clients send a third of their writes twice, discarding the first answer | 2,548–3,381 / 0 |
| Power loss of a majority | `TestPowerLossOfMajorityLosesNothing` | see the gate table | `always` and `interval`: 5 of 5 runs each linearizable |

A run's operations vary with which node a fault hits (slow disk costs throughput only when
the slow node leads or is needed for a quorum). Every one of the 70 runs found the history
linearizable and every final read succeeded.

Mutation checks (each deliberate bug applied alone; the named scenarios run three times each):

| Deliberate bug | Caught by |
| --- | --- |
| ReadIndex answers at once, without the heartbeat round that confirms leadership | `paused-leader` (2 of 3 runs Illegal); `isolate-leader` did not catch it (0 of 3), as expected: the isolated leader steps down by CheckQuorum before a new leader can commit |
| Retried writes applied again (session table not consulted) | `client-retries` and `lossy-network`, 6 of 6 runs Illegal |
| Votes granted without the log up-to-date check | `kill-leader` and `rolling-restart` (1 of 3 runs each); `kill-minority`, `isolate-leader` and `random-partitions` 0 of 3: caught, but rarely per run |
| Raft log not fsynced under `storage.sync = always` | `TestPowerLossOfMajorityLosesNothing` (both subtests): the restarted nodes refuse to open ("state machine applied 124, beyond last persisted entry 0"), before the checker runs |

The checker's model was tightened during the phase: compare-and-set on an absent key can apply
only when it expects version 0, and after a write with an unknown outcome the key's previous
version stays a lower bound for every later version. Five new histories the old model wrongly
accepted are now in `TestModelRejectsViolations`.

### Implemented and tested

- **`internal/fault`** (ADR 0025): the in-process fault injector, a `transport.Faults`: a
  partition table of directed links (two-sided partitions, isolating one node, one-way blocks),
  per-link and default loss, duplication, delay and jitter from a seeded generator, `Heal`,
  counters; random two-sided splits. Tests: partition and block rules, loss and duplication
  rates, delay bounds, and a real gRPC call through the transport being blocked, delayed and
  duplicated.
- **`internal/transport`:** client interceptors that ask `Config.Faults` about every unary and
  streaming call between nodes (drop as `Unavailable`, delay, send a unary call twice).
- **`internal/node`:** `WithFaults`, `WithFS` (replica storage on any `vfs.FS`), `PauseTicks`
  (stops the node's Raft clock); `storage.sync = never` passes `UnsafeNoSync` to replicas.
- **`internal/replica`:** `Config.UnsafeNoSync` skips the Raft log's fsyncs.
- **`internal/storage/vfs`:** `Slow`, a filesystem whose file and directory syncs take longer
  (`TestSlowDelaysEverySync`).
- **`internal/node/nodetest`:** `StartWith` and `Options` (faults, per-node `MemFS`, a filesystem
  wrapper), `PowerLoss` (crash a member's `MemFS` and keep only its durable image).
- **`test/linearizability`:** the history recorder, the Porcupine model (per-key partitions,
  versions, unknown outcomes), the shared `Workload`, the failure matrix, the power-loss tests
  and the negative control. Flags: `-lin-runs`, `-lin-duration`, `-lin-seed`, `-lin-out`
  (Porcupine's HTML visualization of a failing history).
- **`test/chaos`** (runs on GitHub, see above): the same workload and checker against a
  Compose cluster, logging each scenario's growth in Raft terms (elections it forced); `deploy/compose/chaos.yml` (8 shards, faster failure handling) and
  `deploy/docker/nettools.Dockerfile` (iptables and tc, run in a node's network namespace).
- **Make and CI:** `make lin`, `make chaos`; a nightly workflow (`.github/workflows/nightly.yml`)
  with the failure matrix at 10 runs × 10 s, the Docker chaos scenarios, 100,000 simulation
  seeds, 1,000 kill -9 cycles and 5 minutes per fuzz target. Every push runs the failure matrix
  once under `-race` as part of `make check`.
- **Docs:** ADR 0025; ADR 0010 revised (`sync = never` skips both logs' fsyncs).

### Known limitations

- The Docker backend runs only on GitHub (no Docker here). Its first run predates the
  per-scenario Raft-term log line, so it does not yet show that its iptables partitions forced
  elections (netem visibly cut throughput: about 2,600 operations against 6,000–13,000).
- Simulator seed 51674 fails the liveness check (see "First nightly run").
- The in-process backend shares one Go runtime: `PauseTicks` stops only the Raft clock, not
  the node's other goroutines; real process pauses come with SIGSTOP in the Docker backend.
- Faults are injected where calls are sent; a response lost after the receiver acted is only
  covered by client-side resends and timeouts.
- Each scenario injects one kind of fault; there is no mixed nemesis yet.
- Detection is probabilistic: the vote bug above was caught in 2 of 15 runs. The simulator
  catches it through its Leader Completeness check (Phase 6 mutation table), and the nightly job
  runs each scenario 10 times.
- Slow disk is simulated only on fsync latency, and only for replica storage.
- Not done: an independent second review of the Phase 8 diff against the plan.

### First nightly run

Nightly run 44050943445 (dispatched by hand on 51cf806):

- `chaos`: passed, see the gate table. Per scenario (two runs each): `kill-leader` 8,378 and
  9,917 operations, 70 and 64 with unknown outcomes; `kill-majority` 8,986 and 13,493;
  `freeze-leader` (SIGSTOP for 3 s) 6,044 and 6,299; `wipe-node` 11,273 and 10,866;
  `slow-network` (netem 20 ms ± 15 ms) 2,657 and 2,658; `lossy-network` (10% loss, 5%
  duplication, 25% reordering) 2,644 and 2,573. All Ok.
- `crash`: 1,000 kill -9 cycles passed.
- `fuzz`: `FuzzDecodeBatch` (`internal/storage/lsm`, Phase 2 code) found a batch record whose
  decode-then-encode round trip changed it: a key length written as `0x80 0x00`, a padded
  varint for 0, which `binary.Uvarint` accepts. The encoder never writes one, so only
  corruption could, but accepting it gave one batch two encodings. Fixed in the commit after
  51cf806 ("Phase 8 follow-up"): the decoder rejects padded lengths. `TestDecodeBatchRejectsPaddedLengths`
  and the saved failing input (`testdata/fuzz/FuzzDecodeBatch/755a9cdbd06cc1b6`) both fail
  without the fix and pass with it; 3 more minutes of fuzzing found nothing else.
- `sim` (100,000 seeds, ten times CI's 10,000): seed 51674 fails the liveness check, "a
  proposal made during 300 calm steps was not applied by every member". Every other seed
  passes (1–51,673 on GitHub; 51,675–100,000 here, 6 m 40 s, zero violations). No safety
  violation. **Open.**
  - What happens: PreVote off, CheckQuorum on, two voters (n1, n3) after n2 was removed. n2
    never learned of its removal (it was added back as a learner at step 364, still behind);
    a partition at step 361 leaves the group without a leader. In the calm phase n2
    keeps starting elections it cannot win, at ever higher terms. n3's log is behind n1's,
    so only n1 can win, and the term bumps keep cutting its elections short; n1 wins only
    at step 646 of 700, too late for the final proposal to reach every member.
  - This is the disruptive removed server of the Raft thesis, §4.2.3. PreVote (§9.6) is the
    fix: n2's and n3's pre-votes fail without raising terms. dkv always runs with PreVote
    and CheckQuorum (`replica.go`). Rerun with PreVote on, the seed passes; with no
    membership changes, it also passes. Changing an option shifts the random schedule, so
    neither rerun is an exact counterfactual.
  - The simulator already exempts CheckQuorum-off schedules from membership changes, for
    the same reason. Options: (a) exempt PreVote-off schedules too, keeping them for safety
    without membership changes (recommended: it matches what dkv runs and keeps every
    safety check); (b) give PreVote-off schedules a longer calm phase, which makes a failure
    rarer but not impossible; (c) change Raft so voters ignore vote requests from servers
    that are not voters in their configuration, a core liveness change that would need its
    own analysis. Each of these changes the simulator's requirement or the Raft core, so it
    waits for a decision (see "Needs from you").
- `linearizability` (`make lin LIN_RUNS=10 LIN_DURATION=10s`): all 140 runs linearizable, in
  26 m 43 s, 1,275–14,692 operations per run; the power-loss test linearizable with `always`
  and `interval`, and the negative control Illegal.
