# Progress

Only what is implemented AND tested is listed here. See docs/PLAN.md for the full plan.

## Needs from you

- **AWS (the free plan deployment, below):** the IAM user `dkv-deploy` can use EC2 but
  not S3 or IAM, which `make aws-up` needs (the artifact bucket, the instances' role):
  attach `AdministratorAccess` to it in the IAM console. Then the word to apply.
- Docker Desktop's WSL integration is now on for this distro (`/usr/bin/docker` links to
  it since 21 November), but its engine is stopped: the benchmark matrix needed the machine
  to itself. Every Docker gate still runs on GitHub's runners: the Phase 4 Compose gate, the
  Phase 8 chaos scenarios, the Phase 9 dashboard gate and the Phase 10 five-node profile
  (`docker-gates.yml` and the nightly workflow). To run them here too, start Docker Desktop;
  it competes with everything else for this machine's 3.7 GiB of memory.

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

Status: **done.** Everything but the Compose half of gate item 1 passed locally on
2026-11-16 (CI run 43932653619 on main at 2e82939); the Compose half passed on GitHub's
runners on 2026-11-17, once the nightly workflow ran it (Phase 9).

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| Three nodes form a cluster from config, in-process | `test/integration` `TestThreeNodesFormACluster` (`make test-integration`): `nodetest.Start` runs three full nodes on ephemeral loopback ports with separate data directories and one bootstrap peer list; every node reports all three UP, with the node ID each actually has, itself marked as self. Formed in 29 ms. | Passed |
| Three nodes form a cluster from config, in Compose | `deploy/compose/docker-compose.yml` (three nodes configured only through `DKV_*` variables) and `deploy/compose/gate.sh` (`make compose-gate`) ran in GitHub's `docker-gates` workflow (run 44182276412 at acbc8fb, and 44181512051): three containers formed a cluster from their `DKV_*` variables, and after SIGKILL of n3 both survivors reported it DOWN 3.07 s (and 2.49 s) later, within the 3 s window plus the script's 2 s of polling slack. Docker is unavailable here. The exact Compose environment was smoke-tested with the real binary outside Docker (the node recognized itself by its advertise address and reported unresolvable peers DOWN after the window). As a stand-in, `test/e2e` `TestThreeProcessesFormACluster` runs three real `dkv-server` processes from one peer list and checks with `dkvctl status` that each sees all three UP. | Passed |
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
| A three-node RF=3 cluster keeps serving after its leader is killed, with a new leader within about two election timeouts and no acknowledged write lost | `test/integration` `TestLeaderFailover`: 3 in-process nodes, 4 shards, 20 ms ticks (election timeout 200–400 ms). Four writers run through the survivors' public APIs; the leader of shard 0 is stopped; every shard it led must get a new leader within two of the longest election timeouts, 4 × election ticks × tick (800 ms; ADR 0028, since the Phase 10 follow-up "Tests that fail at random"; it was 2 timeouts + 10 ticks, 600 ms) of the stop; writes continue; then every acknowledged write is read back (linearizable) through a survivor, and after the stopped node restarts, through it and from its own replicas. 40 runs under `-race`: slowest new leader 203–394 ms per run (median 309 ms); 316 ms in a full parallel `go test -race ./...`. `test/e2e` `TestSIGKILLedLeaderIsReplaced`: three real `dkv-server` processes; the shard-0 leader gets SIGKILL after 40 acknowledged writes; a new leader is seen within the limit (2 timeouts + 25 ticks, as it polls each survivor over gRPC from another process); 40 more writes succeed; all 80 read back from the survivors and, after restart on the same data directory, from the killed node. 5 runs: 214–359 ms. | Passed |
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
- The Docker Compose gate was blocked here; it passes on GitHub's runners since Phase 9.
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
- The Docker Compose gate was blocked here; it passes on GitHub's runners since Phase 9.
- Not done: an independent second review of the Phase 7 diff against the plan.


## Phase 8: Fault injection and distributed integration testing

Status: **done.** Both gate items passed locally on 2026-11-17 with the in-process backend, and
the Docker backend's scenarios passed on GitHub's runners (nightly run 44050943445). CI run
44050875076 on main at 51cf806 (`check`, `crash`, `storage-gate`, `sim`) is green. The first
nightly run found two problems, below; the Phase 8 follow-up commits resolve both.

The full suite also passes on the Phase 8 cluster layer: `go test -p 2 -count=1 ./...` ran all
31 packages (27 with tests) in 3 m 18 s with no failure. At the default parallelism the run was
killed for lack of memory (exit 137) on this 3 GB machine; run it here with `-p 2`
(`GOFLAGS=-p=2 make check`).

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| Every scenario in the failure matrix passes the linearizability check across repeated runs | `test/linearizability` `TestFailureMatrix` (`make lin LIN_RUNS=5`): 3 nodes, 4 shards plus the meta group, 20 ms ticks, `dead_after` 2 s. Per run, four clients (three pinned to one node each, one using all) issue puts, gets, deletes and compare-and-sets with unique values on 8 keys for 3 s of faults plus recovery; every operation is recorded with its invocation and response time, failed and timed-out ones as unknown outcomes; then every key is read once more and every such read must succeed. Porcupine checks the history against a per-key model that includes versions. One scenario per failure-matrix row (table below); 14 scenarios × 5 runs = 70 runs, all linearizable, in 5 m 33 s. Also run once each under `-race` inside `make check`. `TestPowerLossOfMajorityLosesNothing` (`storage.sync` always and interval): one node partitioned away while writes go on, the power of the other two cut (their replicas on `vfs.MemFS`, so only fsynced data survives), then everything restarted: linearizable, no acknowledged write lost. The Docker backend (`test/chaos`, `make chaos`: SIGKILL, SIGSTOP, iptables, tc netem against a three-node Compose cluster) cannot run here; on GitHub's runners (nightly run 44050943445, `CHAOS_RUNS=2`, 10 s of faults) all 12 of its scenarios × 2 runs = 24 histories were linearizable, 2,573–13,493 operations each. | Passed |
| A deliberately broken configuration fails the checker | `TestNegativeControlFailsTheChecker`: the same power loss with `storage.sync = never`, which now also skips the fsync of every Raft log write (ADR 0010). The two powered-off nodes come back without the writes they acknowledged; the partitioned node leads with older data; the checker must return Illegal, and does in 5 of 5 runs (seed 42, 1,129–1,788 operations each). | Passed |

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
- **`internal/replica`:** `Config.UnsafeNoSync` skips the fsync of every Raft log write
  (entries, term and vote); log compaction, snapshot installs and segment rotation still sync
  (`TestUnsafeNoSyncSkipsWriteSyncs`, added in the follow-up).
- **`internal/storage/vfs`:** `Slow`, a filesystem whose file and directory syncs take longer
  (`TestSlowDelaysEverySync`).
- **`internal/node/nodetest`:** `StartWith` and `Options` (faults, per-node `MemFS`, a filesystem
  wrapper), `PowerLoss` (crash a member's `MemFS` and keep only its durable image).
- **`test/linearizability`:** the history recorder, the Porcupine model (per-key partitions,
  versions, unknown outcomes), the shared `Workload`, the failure matrix, the power-loss tests
  and the negative control. Flags: `-lin-runs`, `-lin-duration`, `-lin-seed`, `-lin-out`
  (Porcupine's HTML visualization of a failing history).
- **`test/chaos`** (runs on GitHub, see above): the same workload and checker against a
  Compose cluster, logging each scenario's growth in Raft terms and, since the follow-up,
  asserting that the shards of a killed, frozen or cut-off leader elected a new one; `deploy/compose/chaos.yml` (8 shards, faster failure handling) and
  `deploy/docker/nettools.Dockerfile` (iptables and tc, run in a node's network namespace).
- **Make and CI:** `make lin`, `make chaos`; a nightly workflow (`.github/workflows/nightly.yml`)
  with the failure matrix at 10 runs × 10 s, the Docker chaos scenarios, 100,000 simulation
  seeds, 1,000 kill -9 cycles and 5 minutes per fuzz target. Every push runs the failure matrix
  once under `-race` as part of `make check`.
- **Docs:** ADR 0025; ADR 0010 revised (`sync = never` skips both logs' write fsyncs).

### Known limitations

- The Docker backend runs only on GitHub (no Docker here). Its iptables partitions do force
  elections (see "Nightly rerun"), and since the follow-up the kill, freeze, isolate and
  partition scenarios assert it; its wipe-node, rolling-restart and flapping scenarios assert
  only linearizability (the in-process backend asserts their effects).
- Simulated schedules with PreVote or CheckQuorum off make no membership changes (ADR 0026).
  dkv always runs with both on.
- The in-process backend shares one Go runtime: `PauseTicks` stops only the Raft clock, not
  the node's other goroutines; real process pauses come with SIGSTOP in the Docker backend.
- Faults are injected where calls are sent; a response lost after the receiver acted is only
  covered by client-side resends and timeouts.
- Each scenario injects one kind of fault; there is no mixed nemesis yet.
- Detection by the chaos scenarios is probabilistic: the vote bug above was caught in 2 of 15
  runs, and the nightly job runs each scenario 10 times. The simulator's targeted schedule
  catches it on every run (see "Vote-freshness bug in the simulator").
- Slow disk is simulated only on fsync latency, and only for replica storage. The scenario
  checks that syncs reach the slow filesystem, not that throughput drops.
- Raft messages are never reordered at the integrated level: a node sends one batch at a
  time to each peer, and netem's reordering sits under TCP, which restores the order. Only
  the simulator reorders them.
- A power loss in `nodetest` crashes replica storage only; the node ID, member record and
  bootstrap marker stay on the real filesystem.
- `client-retries` sends its duplicate after the first call has returned, never while it is
  still in flight.
- The negative control does not assert which node leads after the restart, and runs one
  seed (`go test -count=5 -run TestNegativeControlFailsTheChecker ./test/linearizability`
  repeats it; `make lin` runs it once).
- Reviewed: see "Review of phase-7..HEAD".
- Observed once, not reproduced: `test/integration` `TestAnyNodeAcceptsAnyKey` (Phase 5) failed
  in one full `make check` on this machine, a linearizable Get timing out after 10 s (RF 2,
  under `-race`, beside `test/e2e`'s three server processes). It then passed 6 of 6 runs on
  its own (13–33 s) and the next full `make check`; CI has never seen it fail. Possibly CPU
  starvation stalling a two-voter group, possibly a rare liveness bug: if it recurs, it
  needs a trace.

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
- `sim` (100,000 seeds, ten times CI's 10,000): seed 51674 failed the liveness check, "a
  proposal made during 300 calm steps was not applied by every member". Every other seed
  passed (1–51,673 on GitHub; 51,675–100,000 here, 6 m 40 s, zero violations), and no seed
  violated a safety check. Resolved: see "Seed 51674" below. The cause given here at first
  (the removed server n2 cutting elections short) was wrong.
- `linearizability` (`make lin LIN_RUNS=10 LIN_DURATION=10s`): all 140 runs linearizable, in
  26 m 43 s, 1,275–14,692 operations per run; the power-loss test linearizable with `always`
  and `interval`, and the negative control Illegal.

### Seed 51674

Decision (2026-11-17): fix the cause in the Raft core with the disruptive-server rule of
the Raft dissertation, §4.2.3, unless the core already has it; otherwise stop simulating
membership changes in PreVote-off schedules, keeping every safety check. Details and
alternatives: ADR 0026.

- **The core already has the rule**, since Phase 6: with CheckQuorum on, a node that heard
  from a leader within the minimum election timeout drops a higher-term `MsgVote` (no term
  change, no vote, no reply) and refuses a `MsgPreVote` (which never changes its term); a
  leader's `MsgTimeoutNow` starts a campaign whose `MsgVote`s carry `Transfer`, which skips
  the check. So the core is unchanged and the fallback applies.
- **What really happened**, from a replay recording every node's term, role and leader at
  every step: no vote ever changed the term of a node inside its lease (0 of the 12 term
  increases a vote caused). n2 deposed the leader once, at step 368, by answering its
  heartbeat with a higher term (Raft's stale-leader notification, not a vote). In the calm
  phase there was no leader until step 646, so no lease applied. The voters were n1 and n3;
  n3's log was behind, so it could never win, but without PreVote each of its timeouts
  raised n1's term (all six vote requests that raised n1's term in the calm phase came from
  n3), and the two split the vote six times when they timed out within one message delay of
  each other. n2 changed nothing in the calm phase. PreVote (§9.6), which dkv runs, is what
  stops a server that cannot win from raising terms; §4.2.3 protects a working leader and
  could not have helped.
- **New tests** (`internal/raft`): `TestLeaseIgnoresDisruptiveVotes` and
  `TestTransferCampaignBypassesLease`, each with PreVote on and off (ADR 0026 lists what
  they check). Mutation check, each change applied alone to the core:

  | Deliberate bug | Caught by |
  | --- | --- |
  | Higher-term votes never ignored (no lease for votes) | `TestLeaseIgnoresDisruptiveVotes`, `TestTransferCampaignBypassesLease` |
  | Every higher-term vote ignored unless a transfer, lease or not | `TestLeaseIgnoresDisruptiveVotes` and 19 other tests |
  | The lease applies with CheckQuorum off | `TestLeaseIgnoresDisruptiveVotes`, `TestFigure8`, `TestMustSync` and 3 more |
  | The lease lasts one tick longer than the minimum election timeout | `TestLeaseIgnoresDisruptiveVotes` only |
  | Votes marked `Transfer` still subject to the lease | `TestTransferCampaignBypassesLease`, `TestLeaderTransfer` |
  | A `MsgTimeoutNow` campaign does not mark its votes | `TestTransferCampaignBypassesLease`, `TestLeaderTransfer` |
  | A `MsgTimeoutNow` from any node, not only the leader, starts a campaign | `TestTransferCampaignBypassesLease` only |
  | Pre-votes granted inside the lease | `TestLeaseIgnoresDisruptiveVotes`, `TestPreCandidateAsksAgainAfterLease` |

- **Simulator** (`internal/sim`): PreVote-off schedules make no membership changes, like
  CheckQuorum-off ones before (`limitMembershipChanges`). `-prevote=on|off` (`make sim
  PREVOTE=…`) forces PreVote for a whole run; a seed's other options do not depend on it.
  Seed 51674 is a permanent regression seed (`TestRegressionSeeds`, in `make sim` and
  `make check`): as drawn and with PreVote forced on (membership changes kept) it passes
  every check; its original schedule still fails only the liveness check, with no safety
  violation. `make sim` now sets a 3-hour test timeout: 100,000 seeds take longer than
  the default 10 minutes on this machine (GitHub's runners did the first 51,673 in about
  3 minutes). The nightly `sim` job runs 100,000 seeds with PreVote on and
  100,000 with it off, which together cover every schedule of a run that draws PreVote per
  seed.
- **Runs** on this machine, all with zero violations:
  - `make sim` (seeds 1–1,000, PreVote drawn per seed): 6.6 s; 946 membership changes,
    1,842 leader transfers, 1,093 snapshots installed; `TestRegressionSeeds` and
    `TestReplayIsIdentical` pass.
  - PreVote on, seeds 1–100,000: 16 m 0 s; 345,716 leaders elected, 329,739 crashes,
    222,312 partitions, 125,680 membership changes, 184,983 leader transfers, 115,643
    snapshots installed, 3,119,203 reads confirmed, 334 million messages delivered and
    35.8 million dropped.
  - PreVote off, seeds 1–100,000: 14 m 58 s; 396,427 leaders elected, 330,010 crashes,
    222,421 partitions, no membership changes, 189,355 leader transfers, 112,215
    snapshots installed, 3,010,829 reads confirmed, 311 million messages delivered and
    33.7 million dropped.

  Together the two 100,000-seed runs cover every schedule of the nightly's former
  100,000-seed run with PreVote drawn per seed, seed 51674 included.

### Vote-freshness bug in the simulator

The chaos scenarios caught "votes granted without the log up-to-date check" in only 2 of 15
runs. Checked here whether the simulator's Leader Completeness check catches it every time.
The bug was planted again (`handleVote` grants votes and pre-votes without
`isUpToDate`), outside the repository.

- **Random schedules catch it in one schedule out of four.** Seeds 1–10,000: 2,509 report a
  violation, the first being Leader Completeness in 2,458, Election Safety in 50 and State
  Machine Safety in 1; the other 7,491 never put a lagging node in a position to win. The
  first catching seed is 4, and no 33 consecutive seeds all miss it, so every run of the
  suite catches it (`make check`: 1,000 seeds, CI: 10,000, nightly: 100,000), but not every
  schedule does.
- **A schedule used to run on after its first violation**, and with this bug that ran the
  test out of memory: seed 73 flooded the simulated network until the process was
  killed, reporting nothing (on this 3 GB machine after about 9
  minutes; a `make check` running alongside it failed two timing tests in
  `test/integration`, which pass when it runs alone). The suite never reached
  seed 73 because seed 4 failed first, but a replay with `-first-seed` would have. A
  schedule now stops at its first violation; the 10,000-seed tally above ran under a 1.5 GB
  address-space limit without trouble.
- **Targeted schedule** `TestDelayedTimeoutNowMeetsTheLogCheck` (`internal/sim`, PreVote on
  and off), a schedule an asynchronous network allows: a leader starts a transfer to a
  caught-up follower and the `MsgTimeoutNow` is delayed past the transfer's timeout; the
  leader then commits an entry the follower misses (its appends are lost, its heartbeats
  are not, so it keeps following). When the `MsgTimeoutNow` arrives, the follower campaigns
  at once with a stale log, no pre-vote and the lease bypassed (ADR 0026); everyone else
  would grant a transfer vote, so only the log check stops it. The test checks each step of
  the setup, then requires the calm phase to elect a leader holding the entry and apply a
  new proposal everywhere. With the bug planted it failed 6 of 6 runs (3 each with PreVote
  off and on), each time with "Leader Completeness: leader n1 of term 3 lacks committed
  entry 3 (term 2)"; without it, it passes.

### Nightly rerun

Nightly run 44150994040 (dispatched on 0cd55b6): all six jobs passed. `linearizability`: 140
runs linearizable, the power-loss test Ok with `always` and `interval`, the negative control
Illegal. `chaos`: 24 histories linearizable. `sim`: 100,000 seeds with PreVote on and 100,000
with it off, no violation. `crash`: 1,000 kill -9 cycles. `fuzz`: 5 minutes per target.

The chaos job's new per-scenario line shows that the iptables partitions forced elections.
Summed terms of the 8 data shards before and after each run:

| Scenario | Run 0 | Run 1 |
| --- | --- | --- |
| `isolate-leader` (iptables) | 17 → 31 | 17 → 30 |
| `random-partitions` (iptables) | 17 → 25 | 17 → 28 |
| `flapping-one-way` (iptables) | 17 → 23 | 17 → 24 |
| `kill-leader` | 17 → 33 | 16 → 29 |
| `freeze-leader` | 17 → 27 | 17 → 29 |
| `client-retries` (no fault) | 18 → 20 | 16 → 19 |

Terms also rise without faults (2 to 3 per run), from the rebalancer's leadership transfers,
so the Docker scenarios now check the faulted leader's own shards instead (see below).

### Review of phase-7..HEAD

A second agent reviewed the Phase 8 commits against docs/PLAN.md and listed 25 claims that
no test asserted, or asserted only in part. The most serious: the failure matrix could pass
vacuously. Every scenario asserted linearizability, a settled cluster and successful final
reads, and nothing else: with CheckQuorum turned off, `isolate-leader` stayed linearizable.
Closed in the follow-up commit (ADR 0025 revision):

| Gap | Now asserted by |
| --- | --- |
| Faults never shown to take effect; no availability floor | `TestFailureMatrix`: each scenario checks its evidence (table below): elections for kills, isolation, pauses and restarts; injector counters for partitions, delays, loss and duplication; at least 10 reads and 10 writes completed during the faults where the matrix promises service |
| Kill a majority: linearizable reads and writes fail fast | `kill-majority`: a Get and a Put on the survivor fail within their 1 s deadline |
| Isolated leader steps down; paused leader is replaced | `isolate-leader`: the node leads nothing after 1 s cut off; `paused-leader`: its shards have a newer leader after 1.5 s |
| PreVote keeps terms stable under one-way partitions | `flapping-one-way`: at most 1.5 elections per shard per second; `raft` `TestPreVoteKeepsTermStableUnderOneWayPartition` |
| A restarted node catches up | `rolling-restart`: every replica applies its leader's commit index |
| A wiped node rejoins as a new node through snapshots | `wipe-node`: new node ID, voter of every shard, snapshots installed. This exposed that the scenario never needed a snapshot: it wiped before any log was compacted. It now compacts every 20 entries and wipes once every log is compacted |
| Meta quorum loss | `meta-quorum-loss`: the meta group has no leader 2 s after its traffic is cut; data reads and writes continue |
| Client retries happen | `client-retries`: the workload counts duplicated writes |
| The slow disk is in node 0's path | `slow-disk`: syncs reach the slow filesystem (`vfs.CountSyncs`, tested by `TestCountSyncs`) |
| Docker term growth only logged | `test/chaos`: at the end of each kill, freeze, isolation and partition, every shard led from the faulted side has a newer term |
| "`sync = never` skips the Raft log's fsyncs" | Wording corrected (it skips the fsync of every Raft log write; compaction and snapshot installs still sync); `replica` `TestUnsafeNoSyncSkipsWriteSyncs` |
| Stream fault interceptor; a delay cut short by the caller | `fault` `TestTransportAppliesFaults` (streams blocked and delayed, never duplicated; a 1-minute delay ends at a 100 ms deadline) |
| Injector: per-link precedence and loss, nodes outside a partition, `Heal`, `Stats` | `fault` `TestLinkSettingsAndStats` |
| `PauseTicks` | `node` `TestPausedLeaderCannotNoticeLostQuorum` |
| No lease without a known leader | `raft` `TestNoLeaseWithoutALeader` |
| Regression seed 51674 could drift into a different schedule | `TestRegressionSeeds`: the original schedule must still fail the liveness check; the PreVote-on run must still change membership |
| Targeted schedule's precondition | `TestDelayedTimeoutNowMeetsTheLogCheck`: only transfer-marked votes, no pre-vote; both other voters answer with a refusal |
| A schedule stops at its first violation | `TestScheduleStopsAtFirstViolation` |
| PreVote-off exemption and `-prevote` held by construction only | `TestMembershipChangesNeedPreVoteAndCheckQuorum` (seeds 1–10,000), `TestForcedPreVoteChangesNothingElse` |
| The failure message's single-seed replay hint failed on "no meaningful work" | That sanity check applies from 100 seeds |
| An unknown read constrains nothing; unknown outcomes return last | Model case "unknown read constrains nothing"; `TestHistoryMovesUnknownOutcomesToTheEnd` |
| No fuzzing per commit; chaos not run at phase gates | CI `fuzz` job (10 s per target); the nightly workflow also runs on `phase-*` tags |

The items not closed are under "Known limitations" above: Raft messages not reordered at the
integrated level, power loss limited to replica storage, no retry while the first call is in
flight, slow-disk throughput not asserted, Docker wipe-node, rolling-restart and flapping
asserting only linearizability, and the negative control's single seed. The numbers quoted
in this file come from one-off runs; the replay analysis of seed 51674 used a recorder that is
not in the repository.

Evidence from the final run (`go test -run TestFailureMatrix ./test/linearizability
-lin-runs=3`, 42 runs in 197 s, all passing): reads and writes with known outcomes that
returned during the 3 s of faults, and the growth of the summed terms of the 4 data shards.

| Scenario | Reads | Writes | Elections |
| --- | --- | --- | --- |
| `kill-leader` | 382–646 | 609–990 | 6–8 |
| `kill-minority` | 448–560 | 657–792 | 4–5 |
| `kill-majority` | 1–3 | 0–5 | 5–6 |
| `isolate-leader` | 527–786 | 809–1,100 | 6–7 |
| `paused-leader` | 148–328 | 227–520 | 4–5 |
| `random-partitions` | 486–685 | 699–863 | 4–8 |
| `flapping-one-way` | 319–425 | 511–650 | 1–3 |
| `rolling-restart` | 320–571 | 500–853 | 6–7 |
| `wipe-node` | 554–890 | 903–1,297 | 3–5 |
| `slow-network` | 93–99 | 123–127 | 0–1 |
| `slow-disk` | 95–330 | 162–476 | 0 |
| `lossy-network` | 149–181 | 244–250 | 0 |
| `meta-quorum-loss` | 1,145–1,263 | 1,678–1,770 | 0–1 |
| `client-retries` | 765–1,027 | 1,224–1,523 | 0 |

Under the race detector, the scenarios with an availability floor completed at least 40
reads and 60 writes (3 runs each). The flapping bound (18 for 4 shards and 3 s) was set from
5 runs each way: growth 1 to 11 with PreVote (0 to 10 under the race detector), 21 to 34 with
PreVote turned off in `internal/replica`.

Mutation checks of the new assertions (each change alone, then reverted):

| Deliberate bug | Caught by |
| --- | --- |
| CheckQuorum off in replicas | `isolate-leader`: "node 0 still leads shards map[0:2] after 1 s cut off from its quorum" (the history stayed linearizable) |
| The injector ignores blocks | `random-partitions`, `flapping-one-way`: "blocked no call" |
| PreVote off in replicas | `flapping-one-way` term growth 21–34, above the bound of 18 |
| PreVote off in the one-way partition unit test | `TestPreVoteKeepsTermStableUnderOneWayPartition`: "s1 is at term 9 after a one-way partition; want 2" |

## Phase 9: CLI and developer dashboard

Status: **done.** The gate passed in process here and in Compose with Chromium on GitHub's
runners. All tests below pass locally (`make check`, `make web-test`, `go test -race
./test/dashboard -count=3`). CI run 44176239353 on main at 96513f9 is green in all 8 jobs,
including `console-postgres` (the PostgreSQL store tests ran there and passed) and `web`.
Nightly run 44176237922 at 96513f9 passed its 7 other jobs; its two Docker gates did not
start (the scripts had no executable bit; fixed in 826cd68). Docker gates run 44181512051
passed the Phase 4 gate and the kill and event checks of this one, then stopped at an
ambiguous link name in the browser test (fixed in acbc8fb); run 44182276412 at acbc8fb
passed both gates in full.

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| Killing a node in Compose appears on the dashboard within one poll interval, with election and re-replication events in the right order | In process, `test/dashboard` `TestDashboardGate`: 4 nodes (RF 3, 4 shards and the meta group), `dead_after` 2 s, a real `dkv-console` polling every 250 ms (timeout 200 ms), checked only through the console's API. The node leading shard 0 is stopped; the console's server-sent stream, which the dashboard follows, showed it down 220–232 ms later in 4 runs (1 plain, 3 under `-race`; the test allows interval + timeout + 250 ms = 700 ms), with no number left on it. The merged events then show, in time order, a new leader for one of its shards (191–226 ms after the stop), the meta leader's `node_dead` verdict (1.96–2.55 s) and the first `replica_added` (2.16–2.73 s). In Compose, `make dashboard-gate` (`deploy/compose/dashboard-gate.sh`, `web/e2e/dashboard.spec.ts`): 4 nodes, the console and PostgreSQL; Chromium on the real dashboard sees the SIGKILLed leader's card turn "down" within one poll interval (1 s) + timeout (800 ms) + 1 s, then the events page lists the same three events in order. Docker gates run 44182276412: n1 (the leader of shard 0) SIGKILLed, shown down on the dashboard 383 ms later (limit 2.8 s); new leader, `node_dead` 7 s later and `replica_added` 0.9 s after that, in order (run 44181512051: 372 ms, same order). Runs nightly (`docker-gates.yml`). | Passed |
| Every number on screen comes from a real node metric | The console derives every value from a node's own fields or from two snapshots of its counters (ADR 0015). `TestDashboardGate` ("numbers come from the nodes"): with the cluster idle and two direct reads of every node agreeing, the console's view of each node equals that node's `/v1/stats` field for field (each replica's shard, role, term, commit, applied, last index, tables, table and memtable bytes; the node's leaders, storage sums and cache capacity); a node that does not answer shows none. `internal/console` `TestDelta`, `TestPercentiles`, `TestShardViews` pin the derivations (rates per second from counter differences, percentiles interpolated in the histogram's buckets, lag from the leader's progress). The dashboard computes nothing but formatting, a cache hit rate and the sum of the nodes' samples per poll round: `web/src/pages.test.tsx` renders every page from JSON captured from a real console (`DKV_DASHBOARD_FIXTURE`) and checks each value shown against the fixture. | Passed |
| Stopping the console has no effect on the database | Nodes never contact the console; it pulls from their read-only admin API (ADR 0014). `TestDashboardGate`: writes through every node continue with no failure while the console stops: 489 and 476 writes in the second before and after (161–184 and 181–199 under `-race`). In Compose the gate stops the console, writes 50 keys through a live node, restarts it, and requires the history saved in PostgreSQL before the stop to be shown again: in run 44182276412, 50 writes through n2 succeeded with the console stopped, and the restarted console showed 13 samples of n2 saved before the stop. | Passed |

Mutation checks (each change applied alone, then reverted):

| Deliberate bug | Caught by |
| --- | --- |
| The console keeps showing a down node's last numbers | `TestDashboardGate` |
| Events identified by reporter and sequence number only (a restarted node's events taken for old ones) | `TestEventMerge` |
| Replicas do not report their elections | `TestReplicaEvents`, `TestAdminHTTPAPI` |
| Requests not counted | `TestStandaloneAdminAPI`, `TestAdminHTTPAPI` |
| The planner never moves leadership off drained nodes | `TestDrainMovesLeadershipAway` |
| Leader balancing counts drained nodes as transfer targets | `TestDrainMovesLeadershipAway` ("no convergence"), `TestDrainKeepsALoneLeader` |

### Implemented and tested

- **Admin API** (`proto/dkv/v1/admin.proto`): `Stats` (request counters and latency
  histograms per operation, each local replica's Raft status and engine metrics, the block
  cache), `Events`, `Drain`, `TransferLeader`; `MemberStatus.drained`. Read-only admin HTTP
  on every node (`server.admin_addr`): `/v1/status`, `/v1/shards`, `/v1/stats`,
  `/v1/events?after=&limit=`, Prometheus `/metrics`, `/healthz`. Bound after storage
  recovery, like the other ports. Tests: `node` `TestAdminHTTPAPI`, `server`
  `TestStandaloneAdminAPI`, `e2e` `TestConsoleWatchesRealProcesses` (real processes).
- **`internal/obs`**: an event log (a ring of 4,096 per node with gapless sequence numbers,
  `After` reports the oldest kept) and request metrics (fixed latency buckets from 0.25 ms
  to 4 s). Events: `leader_elected`, `leader_stepped_down`, `leader_transfer`,
  `snapshot_installed`, `replica_added`/`promoted`/`removed` (replicas); `peer_up`/`down`
  (the ping failure detector); `node_dead`/`alive` (the meta leader). Tests:
  `TestEventLogKeepsTheLatest`, `TestRequestsHistogram`, `TestRequestsConcurrent`,
  `replica` `TestReplicaEvents`, `cluster` `TestPeerEvents`.
- **Drain**: `NodeRecord.drained` in the meta group; the planner moves each drained node's
  leaderships to the least-loaded caught-up undrained voter and never makes it a transfer
  target. Tests: `meta` `TestDrainMovesLeadershipAway`,
  `TestDrainKeepsALoneLeader`; `node` `TestTransferLeaderAndDrain` (also NotFound for an
  unknown node, FailedPrecondition for a non-voter, InvalidArgument for a bad shard).
- **`dkvctl`**: `leaders` (merges every `--addr` node's view), `transfer-leader`, `drain`,
  `undrain`, `--consistency=linearizable|stale` (`--stale` kept as shorthand), `--json` for
  every command (values that are not UTF-8 as `value_base64`); `status` shows drained
  nodes. Tests: `TestJSONOutput`, `TestClusterCommands`, and the earlier ones unchanged.
- **Configuration**: the loader is generic over a settings table, so `dkv-console` loads
  like a node (defaults < YAML < `DKV_*` < flags, unknown keys rejected); `server.admin_addr`;
  `deploy/config/dkv-console.example.yaml`. Tests: `TestConsolePrecedence`,
  `TestConsoleRejectsMistakes`, `TestConsoleSettingsCoverConfig`,
  `TestExampleConsoleConfigLoads`, `TestAdminAddrValidation`.
- **`dkv-console`** (`internal/console`, `cmd/dkv-console`): polls every node concurrently,
  derives rates and percentiles, merges events (a membership change reported by every
  member is kept once), keeps a bounded history per node, serves `/api/overview`,
  `/api/history`, `/api/events`, `/api/health`, the server-sent `/api/stream` and the built
  dashboard with its routes. History in PostgreSQL when `console.postgres_dsn` is set (pgx,
  only in `cmd/dkv-console`), reloaded on restart; a failing database is reported and the
  polling goes on. Tests: `internal/console` (above), `test/dashboard`, `e2e`
  `TestConsoleWatchesRealProcesses`; `cmd/dkv-console` `TestPostgresStore` and
  `TestConsoleReloadsHistory` against a real PostgreSQL in CI's `console-postgres` job (they
  skip here, where there is no PostgreSQL).
- **Dashboard** (`web/`, Vite, React 18, TypeScript, Tailwind, TanStack Query, Recharts;
  exact versions in `package.json` and `package-lock.json`): Overview (nodes up, groups led,
  cluster rates and latency, a card per node, throughput over time, recent events), Shards
  (leader, term, commit, each member's lag and applied index), Nodes (storage, block cache,
  replicas, memtable and SSTable bytes over time), Events (newest first, filter by kind).
  REST on load, then the stream. Tests: `web/src/format.test.ts` and `web/src/pages.test.tsx`
  (7 tests, 6 of them on fixtures from a real console); `make web-test`, and CI's `web` job
  builds it.
- **Compose**: `deploy/docker/console.Dockerfile` (dashboard and console on distroless),
  `deploy/compose/dashboard.yml` (admin APIs, a fourth node, the console, PostgreSQL),
  `deploy/compose/dashboard-gate.sh`. The nightly workflow now also runs the Phase 4 Compose
  gate (`compose` job) and this one (`dashboard` job).
- **Docs**: ADRs 0014 and 0015; README sections on operating with `dkvctl` and on the
  dashboard.

### Fixed along the way

- `replica` `TestLearnerCatchesUpBySnapshot` (Phase 7) failed once in `make check`: the
  learner's snapshot ended just before the entry that added it, which followed by append,
  and the test checked its membership in between. It now waits until the learner has also
  applied that entry; every assertion is unchanged.
- The new drain tests at first sent `drain` before the meta group had recorded the nodes
  (right after every group elected a leader) and got NotFound, which is the right answer;
  they now wait for the records.

### Known limitations

- Drain is the rebalancer's doing, not Raft's: when a leader fails, a drained node can win
  the election, and leads until the next rebalance round moves it away. `transfer-leader`
  is a nudge: leader balancing may move leadership again if it leaves nodes unbalanced.
- The admin HTTP API and the console have no authentication; both are read-only and belong
  on an operator network.
- A node keeps 4,096 events; ones it drops before the console reads them are lost (the
  console logs it). Event order across nodes is their clocks' order.
- A member reports a membership change only if it applies the entry; one that receives the
  change inside a snapshot (a new learner, often) does not. The group's other members do,
  and the console keeps one event per change.
- Latency percentiles are interpolated within buckets from 0.25 ms to 4 s; slower requests
  show as 4 s.
- The dashboard's layout and charts are checked in jsdom and, in Compose, only through the
  gate's checks in Chromium; there are no visual regression tests.
- No Prometheus server or Grafana is shipped; `/metrics` is there for one.

## Phase 10: Docker, AWS deployment and benchmarking

Status: **gate passed except the AWS item**, which needs your AWS credentials and your
decision to spend (see "Needs from you"). The benchmark matrix ran from a clean clone of
a28db12; `docs/benchmarks.md` and `bench/results/` hold its results, and "Benchmark runs"
below records how they were screened.

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| `make bench-local` reproduces the local results from a clean clone | `bench/run-local.sh`: builds the binaries, and for each run starts a fresh cluster of `dkv-server` processes on this machine (data on a real disk; it refuses tmpfs, where fsync is free), preloads it, runs `dkv-bench`, stops it, and regenerates `docs/benchmarks.md` from the result files. It ran end to end from a clean clone of a28db12: 87 runs in 95 minutes, then the report; every result records a28db12 without uncommitted changes. 24 runs were then repeated with the same build and seeds because outside load overlapped them ("Benchmark runs" below). | Passed |
| The report lists hardware, configuration, git SHA, methodology and raw data | Each result file holds the run's configuration and labels, the machine (CPU model and count, memory, OS, kernel), the build's git revision and whether it had uncommitted changes, the Go version, the raw HDR latency histogram, per-operation percentiles, an optional per-second time series, and the nodes' own numbers over the measured window (Raft-log fsync and commit latencies, bytes written by users, to engine logs and to SSTables). The report gives the medians with their ranges over the runs, the machine and build of every run, and for each experiment what it measures and the settings its variants share and vary. `cmd/dkv-bench` `TestBenchAgainstAServer` runs a closed and an open loop against a real node and checks the counts, the read share, the time series, the histogram total, the server-side numbers, the machine and revision, and the report generated from the files. `docs/benchmarks.md`, generated from the 87 result files: the machine (i3-1115G4, 4 CPUs, 3.7 GiB, WSL2), the build (a28db12, go1.27.4), each experiment's settings, medians with ranges, the failover time series, and "Reruns", every replaced attempt with its screening numbers. Raw data in `bench/results/`, the replaced attempts under `bench/results/reruns/`. `TestReportListsReruns`. | Passed |
| Before-and-after numbers for each optimization | The "Optimizations" experiment runs the Phase 9 build (from the `phase-9` tag), this build with the engine's log kept, and this build's default, so each of the two cluster optimizations below is measured on its own; the "Standalone write path" experiment runs a standalone Phase 9 node against this build's. Cluster, writes only, median ops/s (range): Phase 9 build 453 (219–527); with the parallel leader write 534 (434–563); with the single log as well (the default) 775 (672–892), commit p50 48.1 → 26.6 ms, write amplification 2.29 → 1.17. The single log's gain is clear (the ranges are apart); the parallel leader write's is within the run-to-run spread on this one-disk machine (ADR 0003). Standalone: Phase 9 367 (309–383), group commit 4,645 (4,375–4,763). | Passed |
| The AWS cluster is created and destroyed with one command each | `make aws-up` (`deploy/aws/up.sh`: builds linux/amd64 binaries, `terraform apply`, prints the addresses) and `make aws-down` (`terraform destroy`). CI's `terraform` job checks formatting and runs `init` and `validate` against the AWS provider. Not applied: you asked me not to run `terraform apply`, and it needs AWS credentials. | Scripts written and validated; not run |

### Optimizations

Profiling a write-only run (workload W, 32 workers, three nodes, RF 3, on this machine's
ext4 disk) with the new opt-in pprof endpoint gave 516 ops/s with a commit latency of 46 ms
at the median and a Raft-log fsync of 11.5 ms: 29% of CPU time in system calls and no hot
spot in user code. The cluster was waiting on its disk, and each write waited on four fsyncs
in a row: the leader's Raft log, then its engine's log, then the followers' Raft logs and
engine logs. Two changes, each with an ADR revision, and a third that the first benchmark
matrix exposed:

- **The Raft log is a replica's only write-ahead log** (ADR 0010). Replica engines run
  without a log (`lsm.Options.DisableWAL`): applied entries stay in the memtable until a
  flush, and a restart replays the Raft log above the engine's last flushed index. Log
  compaction and snapshots already flush first. `storage.engine_wal` brings the second log
  back. Tests: `lsm` `TestDisableWALKeepsOnlyFlushedWrites`,
  `TestSwitchingOffTheWALKeepsLoggedWrites` (an engine that had a log replays it when it
  reopens without one); `replica` `TestPowerLossReplaysTheRaftLog` (a whole group loses
  power right after its writes, with log compaction off and every 20 entries); the Phase 8
  power-loss tests and failure matrix, now on this path.
- **The leader writes while it replicates** (ADR 0003, Raft thesis §10.2.1). A leader's
  appends and heartbeats now leave before it persists (`Ready.Appends`); the leader counts
  its own entries toward a commit only in `Advance`, once they are persisted. A Ready that
  changes the term or vote holds everything back, and votes and acknowledgments still wait
  for the state they promise. Tests: `raft` `TestLeaderCountsItselfOnlyOncePersisted`; the
  simulator sends a leader's appends before it persists and can crash it in between.
- **A standalone node's writes share fsyncs** (`internal/kvstore`). The first run of the
  matrix showed a standalone node slower than a three-node cluster on the same disk (586
  against 1,277 ops/s, workload A) and preloading at about 400 writes a second from 32
  clients. The store held its write lock through each `Apply`, fsync included, so the
  engine's group commit never had more than one write to commit. Put now holds the lock
  shared; Delete and CompareAndSet, which read before they write, still hold it exclusively.
  Tests: `kvstore` `TestConcurrentPutsShareFsyncs` (320 Puts from 32 clients on a disk whose
  fsyncs take 2 ms must take at most 80 fsyncs; 19–20 measured),
  `TestCompareAndSetIsAtomicWithPuts` (with Puts to the same key running, no write falls
  between the version a successful CompareAndSet expected and the one it wrote).

Mutation checks (each change applied alone, then reverted):

| Deliberate bug | Caught by |
| --- | --- |
| Log compaction without flushing the engine first | `replica` `TestPowerLossReplaysTheRaftLog` ("engine at 0 is behind the snapshot at 78") |
| The leader counts its entries as matched when it appends them, before persisting | `raft` `TestLeaderCountsItselfOnlyOncePersisted`; the simulator at seed 11 (Leader Completeness: a leader of a later term lacks a committed entry) |
| Put holds the store's lock exclusively again | `kvstore` `TestConcurrentPutsShareFsyncs` (320 fsyncs for 320 Puts; 3 of 3 runs) |
| Put takes no lock | `kvstore` `TestCompareAndSetIsAtomicWithPuts` (5 of 5 runs) |
| Join accepts a node before the meta group has recorded the cluster | `integration` `TestEarlyJoinerWaitsForTheMetaBootstrap` (the joiner never gets its share: timeout after 2 minutes) |

Verification of the Raft change beyond unit tests:

- Simulator, seeds 1–100,000, after both changes. PreVote on: 17 m 58 s, zero violations;
  385,344 leaders elected, 390,283 crashes (329,739 before the crash window between send and
  persist was added), 117,631 membership changes, 176,348 leader transfers, 125,324 snapshots
  installed. PreVote off: 17 m 14 s, zero violations; 432,050 leaders elected, 390,038
  crashes (330,010 before).
- Linearizability, in process (`test/linearizability`, `-lin-runs=3`): the failure matrix
  passed 42 of 42 scenario runs; the majority power loss is Ok with `storage.sync` always
  and interval, and Illegal with never, as it must be; the negative control still fails the
  checker.

### Benchmark runs

Decisions taken while running the matrix (21 November), in order:

- The first attempt, in a clone at `~/dkv-bench-clone`, was interrupted when the laptop
  slept; one run measured right after it woke gave 501 ops/s. All of its results are
  discarded. Only runs from one clean session count.
- Two attempts from a clean clone of a28db12 were stopped and discarded: the first
  overlapped the language server re-indexing after a source edit; the second ran while
  Docker Desktop's WSL distro was up (25 results). Docker Desktop was then stopped with
  `docker desktop stop`, and source files were left alone during runs.
- The matrix that counts ran from 12:53 to 14:29 UTC with a sampler reading /proc every
  5 s: every process's CPU time, the VM's CPU and other WSL distros' thread count. Docker
  Desktop was nonetheless started from a terminal from 12:55 to 12:58 UTC; three runs
  overlapped it.
- Screening rule. The rule set beforehand, rerun any run in which processes outside the
  benchmark used more than 1 CPU-s, could not be met: their idle floor (VS Code, its
  extension processes and language server, system services; the sampler excluded) was
  0.064 cores, 1.27 CPU-s per 20 s, and 86 of 87 runs exceeded it. It became: rerun a run
  if another WSL distro started during it, or if outside processes used more than 1 CPU-s
  above the floor during its 20 measured seconds. The floor stays the matrix's for every
  rerun session. At most three attempts per run; the first that passes is used, otherwise
  the one with the least outside CPU, so the choice never looks at throughput.
- 24 runs failed the screen and were repeated, 15 of them twice. 12 never passed. The
  4000-byte-value runs push the idle editor processes into swap (afterwards 829 of 1,024
  MiB of swap was in use and the extension host had 226 MiB swapped out); their page-ins
  show up as outside CPU, mostly system time, in those runs and, in the rerun sessions, in
  most runs after them. A fourth attempt could not avoid that, so none was made.
- The earlier fsync "always" run 1 at 126 ops/s came from the discarded clone. In the
  matrix that counts, run 1 measured 453 ops/s while the language server used 3.2 CPU-s of
  its measured window; its rerun measured 891 ops/s and is used. The report's "Reruns"
  table lists both, as it does every attempt.
- No run shows a heartbeat gap of 10 s or more (the longest, 9 s, was during the Docker
  Desktop start) or a wall time more than a second beyond its preload, warm-up and measured
  time. Every result records a28db12 without uncommitted changes.
- The per-attempt numbers are in `bench/results/reruns/index.json`. The sampler, its raw
  samples and the screening script are kept locally, outside the repository.

### Implemented and tested

- **`dkv-bench`** (`cmd/dkv-bench`): YCSB workloads A (50% reads, 50% updates), B (95/5), C
  (reads only) and W (writes only); uniform or scrambled Zipfian keys (YCSB's, θ 0.99);
  closed loop with N workers, or open loop at a fixed arrival rate with latency counted from
  each operation's intended start; preload; warm-up excluded from the measurement; HDR
  histograms; errors and timeouts counted apart; an optional per-second time series and a
  command run at a given time (the failover runs kill a leader with it); linearizable or
  stale reads; the nodes' own numbers read from their admin APIs before and after the
  measured window; one JSON file per run; `report` renders `docs/benchmarks.md` from the
  files, with every attempt of a repeated run from `bench/results/reruns/index.json` (it
  refuses an index whose attempt used is not the run's file in `bench/results/`); `leader`
  finds the node leading a shard. Tests: `TestKeyDistributions`, `TestBenchAgainstAServer`,
  `TestStat`, `TestReportListsReruns` (with goleak).
- **`make bench-local`** (`bench/run-local.sh`): the matrix of docs/PLAN.md scaled to one
  machine: cluster size (1, 3, 5 nodes), replication factor, fsync, durability path, read
  consistency, concurrency (1 to 256 workers), value size, open loop, key distribution,
  optimizations, the standalone write path and leader failover; three runs per variant. Ports, paths, durations and
  sizes come from the environment.
- **Node instrumentation**: Raft-log fsync and commit latency histograms; engine counters of
  bytes written by users, to the engine's log and to SSTables, and each replica's bytes
  written to its Raft log, so write amplification counts the log that carries the writes on
  the single-log path; `server.admin_pprof` serves `/debug/pprof/` on the admin port (off by
  default). Tests: `node` `TestAdminHTTPAPI` (commits at least the puts, fsyncs counted, user
  bytes, Raft-log bytes at least the user bytes, no engine-log bytes on the single-log
  path), `raftlog` `TestBytesWritten`, `dkv-bench` `TestServerDeltaCountsEveryLog`,
  `TestDescribeLeavesOutReadsForWrites`, `server` `TestAdminPprof`.
- **Compose profiles** (`deploy/compose/`): three nodes (`docker-compose.yml`), five nodes
  (`five.yml`: n4 and n5 join the three), chaos (`chaos.yml`, Phase 8) and the dashboard
  (`dashboard.yml`, Phase 9). `make compose-five` (`deploy/compose/five-check.sh`) checks
  that every node sees five nodes alive in the meta group, that both joiners are given
  replicas and that a write through n5 reads back through n1; it runs in the `five` job of
  `docker-gates.yml`, since Docker is not available here.
- **AWS** (`deploy/aws/`, Terraform ≥ 1.6, AWS provider 5.x pinned in `.terraform.lock.hcl`): a
  VPC with a subnet per availability zone, nodes spread over the zones at fixed private
  addresses (so the bootstrap peer list is known before they start), a load-generator
  instance, a security group (every port between the cluster's own instances; the public
  and admin APIs and SSH only from `allowed_cidr`), gp3 volumes by default, an S3 bucket the instances
  fetch the binaries from with an instance profile, systemd units from user data, and
  outputs for `dkv-bench` and the report. `terraform.tfvars.example`. Checked by CI's
  `terraform` job; never applied.
- **Docs**: `docs/architecture.md` (layers, the write path, the replica runner, durability,
  cluster management, observability, testing; Mermaid diagrams); README sections on
  benchmarks and deployment; ADRs 0003 and 0010 revised.

### Fixed along the way

- **A node that asked to join before the meta group had recorded the cluster was given an
  empty directory** (Phase 7 code). The Join handler accepted it as soon as the meta group
  had a leader, before that leader's bootstrap transaction (settings, the bootstrap nodes'
  records, every descriptor), so the answer held no node address and no meta voter. The
  joiner kept it, could never reach the meta group, and its replicas could not answer their
  leaders; the leaders gave up on them two minutes later. The five-node Compose profile found
  it on its second run in CI (Docker gates run 44411317033: n5 joined through n2 a second
  after starting, with one node record, its own). Its first run (44410365722) had failed on
  a mistake in the check itself, which read the ping detector's column; that detector
  watches only bootstrap peers. Join now answers Unavailable until the bootstrap transaction
  is in, and the joiner retries. Test: `integration` `TestEarlyJoinerWaitsForTheMetaBootstrap`
  (a long rebalance interval delays the meta bootstrap; the joiner must take its share and
  serve writes).
- The first full run of `make bench-local` (from a clean clone of 86bbbb7) stopped after 24
  of its runs: the script passed `--storage.engine_wal`, and flags are spelled with hyphens
  (`--storage.engine-wal`). Every experiment that starts nodes with extra flags or the Phase
  9 build has since been smoke-run (one short run each) before the matrix was run again.
- Write amplification counted only engine bytes, so on the single-log path it read 0.00 in
  every cluster run of that first matrix; it now counts the Raft logs too. Write-only
  experiments no longer list a read consistency among their settings.
- A `dkv-bench` binary built at the repository root by a bare `go build` had been committed
  on the work branch; it is removed and root-level binaries are ignored.
- `replica` test harness: its replicas ticked every 2 ms with an election timeout of 10
  ticks, so under `-race` and the load of `make check` a pause of 20 ms let CheckQuorum
  depose a healthy leader in the middle of a test that had just picked it
  (`TestReplicaEvents`: "not the leader of shard 7; try n2", twice in about 100 runs). The
  harness now uses 25 ticks (50 ms); no assertion changed, and the package passed 40 runs
  under two-way load, `TestReplicaEvents` and `TestLearnerCatchesUpBySnapshot` 100 runs each.
  This may also explain the replica test failure recorded below; that cannot be confirmed.
- `replica` `TestReplicaEvents` (Phase 9) required the first `snapshot_installed` event to be
  the learner's; a voter that falls behind the leader's compaction point also installs one,
  as it should. It now requires the learner's event among them.
- A data race in the `replica` test harness (`TestCrashDuringSnapshotInstall`, Phase 7): the
  test replaced a replica's filesystem in a map that a snapshot send read concurrently. The
  map is now guarded.
- `integration` `TestAddedNodeTakesItsShare` (a Phase 7 gate check) failed once in `make
  check`: one write took 5.2 s against its 2 s bound, with the median write at 187 ms
  (about 4 ms when the test runs alone), so the machine was saturated. It passed 3 of 3
  runs alone (slowest write 66–99 ms) and is unchanged.
- **`dkvctl leaders` printed nothing when the first node in `--addr` was dead** (Phase 9
  code), found in a live Compose demo. The client asked the nodes one after another under
  the single `--timeout` deadline. A stopped container behind Docker Desktop's port proxy
  accepts the connection and then says nothing, so it used the whole deadline and every
  node after it failed with DeadlineExceeded; listed first, it left no node to answer, and
  the command exited 2 with no table. `client.ShardMaps` now asks every node at once, each
  with its own timeout (dkvctl passes `--timeout`), and `leaders` reports each node it could
  not reach on stderr (`<addr> unreachable: …`); stdout, the table or the JSON array, is
  unchanged, so scripts that read it keep working. Tests: `client`
  `TestShardMapsAsksNodesAtOnce` (a live node between two silent listeners: the live node's
  map comes back, and the call takes one timeout, not two); `dkvctl`
  `TestLeadersWithDeadNodesFirst` (three nodes, one stopped; a silent listener and the
  stopped node first in `--addr`: every shard's leader shown, both reported unreachable,
  within `--timeout` plus 1 s; before the fix it exited 2 with no output). Against the
  Compose cluster, with a silent listener first: before, exit 2 and no table after 2.6 s;
  after, all 64 leaders and the listener reported unreachable in 2.0–2.2 s.
- **`dkvctl status`, `shards`, `transfer-leader`, `drain`, `undrain` and `decommission`
  asked only the first node in `--addr`** (Phase 9 code), although the flag's help says
  requests move to the next when one is unreachable; with that node dead they failed. The
  client's admin requests now try the nodes in turn, once each (ADR 0027): before sending,
  the client waits for the node's connection for at most its share of the time left (the
  time to the deadline divided by the nodes not yet tried), and moves on when the
  connection fails or does not come up in that time, or when the node answers
  `Unavailable`. Once connected, the request has all the time left, so a slow node is never
  passed over and a drain or transfer is never sent twice because a node was slow; every
  admin request is safe to repeat anyway. Tests: `client`
  `TestAdminRequestsMoveOnFromDeadNodes` (a silent listener and a listener that resets
  every connection before a live node: status and shard map answered, within the silent
  one's share plus 1 s; the live node's FailedPrecondition returned as it is; with no live
  node, an Unavailable error naming both); `dkvctl` `TestAdminCommandsMoveOnFromDeadAddresses`
  (three nodes behind the same two dead addresses: all six commands succeed, status within
  3 s). Both new tests failed on the previous commit (DeadlineExceeded, exit 2) and passed
  10 and 5 runs under `-race`. `make check` with one test binary at a time passed twice; a
  third run lost a port race (below). Two runs with two binaries at a time each failed
  timing checks in tests that make no admin request through the client (`dkv-bench`
  `TestBenchAgainstAServer`; then `TestConsoleWatchesRealProcesses`, `integration`
  `TestAnyNodeAcceptsAnyKey` and `paused-leader`'s 1.5 s bound); the previous commit passed
  one such run and two one-at-a-time runs.
- **A data race in `e2e` `TestConsoleWatchesRealProcesses`'s own code** (Phase 9). The
  console's stderr went into a `strings.Builder` that `exec` filled from its own goroutine
  while the console ran, and the test read it to report a failure (the "never saw three
  nodes up" check and the SIGTERM timeout) while the console was still writing, so every
  such failure also reported a data race, which hid the real cause. The output now goes into
  a buffer guarded by a mutex. With the test made to fail at that check after 3 s, in a
  scratch copy, the old code reported two data races in each of 3 runs and the new code
  none in 3, each failure showing the console's logs; the unchanged test passed 5 of 5
  runs under `-race`.

### Known limitations

- (Fixed in "Tests that fail at random", below.) The `e2e` tests picked "free" ports by
  listening on port 0 and closing the listener, and on this machine the ephemeral range is
  9130–13225 (4,096 ports), the range every outgoing connection also draws from, so a node
  now and then found its port taken: `bind: address already in use` in
  `TestThreeProcessesFormACluster` and `TestConsoleWatchesRealProcesses`, once each in the
  runs for the two `dkvctl` fixes. The other session saw the same in `TestLeaderFailover`
  ("the stopped node's port was taken").
- Key-value requests move to the next node only when one answers `Unavailable`, which
  includes a refused connection. A node that accepts the connection and never answers (a
  stopped container behind a port proxy) holds a key-value request until its deadline, so
  `dkvctl get` with such a node first in `--addr` fails with DeadlineExceeded. Admin
  requests and `leaders` move on from it (ADR 0027 describes how the same check could apply
  to key-value requests).
- The local benchmarks run every node and the load generator on one machine: they compare
  variants with each other, not with other databases or with a real cluster.
- The single-log path makes a restart replay up to a memtable's worth of the Raft log per
  replica.
- The benchmark numbers vary widely between runs on this machine (stale reads 7,148–12,767
  ops/s; Zipfian keys 1,825–2,615), and 12 of the 87 runs used are the least disturbed of
  three attempts rather than undisturbed ("Benchmark runs").
- (Fixed in "Tests that fail at random", below: the `WaitLeader` fix is committed, and
  each failure mode listed here has its own entry there.) **Tests that fail under
  sustained load; a fix is written but not committed.** CI's
  `check` job failed twice on the `phase-9` tag commit 608153e, which was not recorded then:
  run 44183205436 (`TestTransferLeaderAndDrain` panicked on index -1; `TestReplicaEvents`)
  and run 44183210645 (`TestReplicaEvents`; the `TestCrashDuringSnapshotInstall` data
  race; `TestDashboardGate` "stopping the console changes nothing": 80 writes in the second
  before, 0 in the second after). The `replica` failures are fixed above. The panic: a new
  leader publishes its role at the end of the Ready that made it leader, after its first
  appends have left, so its followers, a finished leadership transfer and `Settled` can
  know of it before `nodetest.Cluster.Leader` finds it. The fix, a `WaitLeader` helper used
  wherever a test reads a leader once, was run 50 times under `-race` with every test it
  touches, two test binaries at a time on 4 CPUs for 16 minutes. `TestRetriedPutAppliedOnce`
  and `TestPausedLeaderCannotNoticeLostQuorum` passed 50 of 50. The others failed in ways
  the fix does not address. `TestTransferLeaderAndDrain`, 3 of 50: leader balancing raced
  the test's transfer or moved the leadership again (transfer-leader is a nudge, Phase 9);
  the test now waits for a stable balance and makes a move that keeps it, and passed 3 of 3
  so far. `TestDashboardGate`, 12 of 50: the console showed a node whose 200 ms poll had
  timed out as down, while the test required its start time (8); writes stalled for a whole
  second (2); the election, dead verdict and re-replication after a kill were not all seen
  within 60 s (3, not diagnosed). `TestLeaderFailover`, 8 of 50: new leaders after 0.62–1.36
  s against the 600 ms bound (6); the stopped node's port was taken by another process when
  it restarted (3). The fix stays uncommitted until all of these pass 50 runs.
- `make check` for the results commit, after the matrix (879 of 1,024 MiB of swap in use;
  the editor's processes held about 1 GiB): two runs with two test binaries at a time each
  failed timing checks in tests the commit does not touch. `dkv-bench`
  `TestBenchAgainstAServer`: the first whole second held 860 of 1,844 operations, against
  its "at least half" (20 of 20 passed alone). `e2e` `TestConsoleWatchesRealProcesses`: the
  console's polls of the nodes timed out. `integration` `TestAnyNodeAcceptsAnyKey`: a read
  hit its deadline. The third run, one test binary at a time (`GOFLAGS=-p=1`), passed.
- `make check` for the `dkvctl leaders` fix ("Fixed along the way"), on a quiet machine
  (`GOFLAGS=-p=2`): one failure, `dkv-bench` `TestBenchAgainstAServer`, the timing check
  that also failed for the results commit above (the first whole second held 1,178 of 2,643
  operations). Its test binary built from the fix is identical to HEAD's in every section
  but the build ID (race build, `-trimpath`), so the fix cannot change its outcome; alone
  it failed 1 of 80 runs here (60 unloaded, 20 beside three CPU-bound loops) and 0 of 80 on
  HEAD. `e2e` `TestConsoleWatchesRealProcesses`, which had failed in earlier runs on a busy
  machine, passed 20 of 20 on HEAD and 20 of 20 with the fix, in 12.8–19.9 s each.
- Those earlier runs, while other work shared the machine (load averages of 15 to 48; the
  VM restarted once), each failed in different tests, none of which runs the changed code:
  the failure modes recorded above (`TestTransferLeaderAndDrain` and `e2e`
  `TestSIGKILLedLeaderIsReplaced` reading a leader before it was published,
  `TestDashboardGate`'s zero start time, `TestLeaderFailover` at 633 ms against 600 ms), a
  5 s deadline in `TestThreeProcessesFormACluster`, and two not recorded before.
  `linearizability` `wipe-node` failed twice on its own precondition, "the data shards'
  logs were not compacted before the faults ended" (86 and 135 writes during the faults,
  over 4 shards that compact every 20 entries); both histories were linearizable, and
  alone it passed. When `e2e` `TestConsoleWatchesRealProcesses` failed, it read the
  console's captured stderr while the console was still writing it, and the race detector
  reported a data race in the test itself (fixed: "Fixed along the way").
- One `replica` durability test failed once during this phase in a full `make check`
  run whose output was not kept; it did not fail again in 45 runs of the package's
  durability tests and 9 full runs of the package. The cause is unknown (the harness's
  election timeout, fixed above, is a candidate).

## Dashboard visual refresh

A visual redesign of the Phase 9 dashboard (`web/`). The console's API, the data and what
every figure means are unchanged: each number on the page is a field of the console's JSON,
or, as before, a sum of them (a node's and the cluster's ops are reads plus writes; the
throughput chart adds up the nodes' samples of each poll).

### Implemented and tested

- Layout: a left sidebar (a top bar on phones) for Overview, Shards, Nodes and Events; page
  titles; the stream's state beside them; a red banner while the live stream is
  disconnected, giving the time of the last update the page shows.
- Themes: dark by default, light behind a toggle kept in `localStorage` (`dkv-theme`) and
  applied before the first paint. Colors are CSS variables behind Tailwind tokens; green,
  amber and red mean state only (up/alive/leader, suspect/learner, down/dead) and always sit
  beside a text label. Every figure uses tabular numerals; IDs, terms and indexes are
  monospace.
- Overview: nodes up, groups with a leader, ops/s (reads, writes and errors beneath it),
  p50 and p99; the throughput chart (reads and writes, legend, gridlines, tooltip); a card
  per node; recent events with relative times.
- Shards: a table with a sticky header and row hover; replicas as chips per node, the
  leader's highlighted and labelled, learners dashed and labelled; terms and commits
  right-aligned; a filter by shard name or the start of a node ID or address.
- Nodes: a card per node with its status, short ID (a click copies the full ID; plain-HTTP
  consoles, which have no Clipboard API, fall back to `execCommand`), address, leaders,
  replicas, storage figures, the storage chart and the replica table behind a disclosure.
- Events: a timeline, newest first, a badge per kind, relative times with the UTC time
  beneath.
- Loading skeletons per page; empty states (no nodes, no groups, no events, no samples yet,
  no filter match); an error state when the console does not answer.
- Tests: the existing page tests pass unchanged; the selectors and test IDs they and the
  Phase 9 browser gate use are kept (`node-<addr>`, `nodes-up`, `groups-led`, `shard-<n>`,
  `node-detail-<addr>`, `memtable-bytes`, `table-bytes`, `events`, `tr[data-kind]`,
  `stream-state`, the Events link). New: `format` (relative times; split figures read as
  the whole ones did), `pages` (the shard filter and its empty state) and `layout` (the
  stream's state and the disconnected banner, through a fake `EventSource`; the theme saved
  and restored). `npm run typecheck`, `npm test` (13 tests) and `npm run build` pass.

### Decisions

- The throughput chart draws reads and writes only; errors per second moved to the Ops/s
  figure. Alternative: a third series, but its natural color, red, is reserved for state.
- Shards has no separate Leader column: the leader is the labelled chip, and a group
  without one shows a red "leader: none". With the column, the chips wrapped onto two lines
  in every row at 1440 px; without it they fit one line down to about 1400 px.
- The Events timeline is still a table (one row per event, a visually hidden header): the
  Phase 9 browser gate finds events as `tr[data-kind=…]`. A list would have meant changing
  the gate.
- Series colors: indigo `#6366f1` (the accent) and magenta `#d55181`, checked for
  colour-vision-deficiency separation, lightness and contrast on both card surfaces. Sky
  failed the contrast check on white and cyan the lightness band.
- The web package has no linter and CI does not lint it (it runs typecheck, tests and the
  build). A one-off run of typescript-eslint's recommended rules and the React hooks rules
  over `web/` reported no problems; neither was added as a dependency.

### Known limitations

- Below about 1400 px wide, the replica chips of a group wrap onto a second line.
- The Phase 9 browser gate (`make dashboard-gate`) was not run here: it brings up its own
  Compose cluster on port 8080, which the local demo cluster holds, and SIGKILLs a node.
  Its read-only locators were checked in Chromium against the rebuilt console instead.

## Tests that fail at random (Phase 10 follow-up)

Goal: `make check` and CI that pass or fail on the code alone. Each item was run 50 times
under `-race`, one test binary at a time (`GOFLAGS=-p=1`), with every test it touches,
before its commit.

### Implemented and tested

- **Ports released and bound again later** (test code since Phase 4). Tests found "free"
  ports by listening on port 0 and closing the listener, and bound them later. Such a port
  comes from the ephemeral range (9130–13225 on this machine), which every outgoing
  connection also draws from, so another socket could take it in between. New package
  `internal/porttest`. In-process servers listen on 127.0.0.1:0 and report the address they
  got, or are handed a listener that is already open (`nodetest` opens its listeners with
  `porttest.Listen`). The `e2e` servers and console bind their public, admin and dashboard
  ports themselves (port 0) and the tests read the addresses from their logs. Where an
  address has to be chosen before it is bound (an `e2e` cluster's peer list, the internal
  address in `dkv-server` `TestRunAsSingleNodeCluster`), `porttest.Pick` chooses a random
  port of at least 10000 outside `/proc/sys/net/ipv4/ip_local_port_range`, where only
  another deliberate listener can collide, and the caller starts again on new ports when a
  process reports `address already in use` (up to 5 times). A node restarted on its address
  retries that address while it is in use: `porttest.Relisten` for 5 s in process, 10 s
  for an `e2e` process. Tests that stand for a dead node use `porttest.Refusing`, a
  listener that closes every connection, instead of a closed listener's address, which
  something else could take; it replaces three copies of that helper. Tests: `porttest`
  `TestPortsStayOutOfTheEphemeralRange`, `TestRelistenAndInUse`, `TestRefusingAndSilent`.
  50 runs each under `-race`:

  | Package | Tests | Passed of 50 |
  | --- | --- | --- |
  | `porttest` | `TestPortsStayOutOfTheEphemeralRange`, `TestRelistenAndInUse`, `TestRefusingAndSilent` | 50 each |
  | `router` | `TestDeadReplicasFailWithinDeadline` | 50 |
  | `transport` | `TestUnreachablePeerFailsFast` | 50 |
  | `cluster` | `TestNeverReachedPeerGoesDown`, `TestPeerEvents` | 50 each |
  | `cmd/dkv-server` | `TestRunAsSingleNodeCluster` | 50 |
  | `pkg/client` | `TestAdminRequestsMoveOnFromDeadNodes` | 50 |
  | `cmd/dkvctl` | `TestAdminCommandsMoveOnFromDeadAddresses` | 50 |
  | `e2e` | `TestThreeProcessesFormACluster`, `TestSIGKILLedLeaderIsReplaced`, `TestConsoleWatchesRealProcesses` | 50 each |
  | `integration` | `TestRestartedPeerRejoinsWithItsID`, `TestAddedNodeTakesItsShare`, `TestEarlyJoinerWaitsForTheMetaBootstrap`, `TestWipedNodeRejoinsThroughSnapshots`, `TestShardDataSurvivesRestart` | 50 each |
  | `linearizability` | `TestPowerLossOfMajorityLosesNothing` (both sync modes); `TestFailureMatrix` `kill-leader`, `kill-minority`, `kill-majority`, `rolling-restart` | 50 each |
  | `linearizability` | `TestFailureMatrix` `wipe-node` | 48: both failures were its own precondition (below, "Known limitations"); both histories linearizable |

  No run reported a data race. `integration` `TestLeaderFailover`, which also restarts a node
  on its address, is run with its own fix (next item).

- **`integration` `TestLeaderFailover` asked for more than the plan** (Phase 6 test). The
  gate is "a new leader within about two election timeouts". Each follower draws its
  timeout from `[election_ticks, 2 × election_ticks)` ticks, 200–380 ms with the test's 20
  ms ticks and 10 election ticks, and the test allowed two of the shortest plus 10 ticks,
  600 ms. It failed now and then on a busy machine with nothing wrong (633 ms in one `make
  check`; 0.62–1.36 s in 6 of 50 runs with two test binaries at a time). The bound is now
  two of the longest timeouts, `4 × election_ticks × tick`, computed from the test's
  configuration: 800 ms (ADR 0028). The first covers the wait for the winning survivor's
  timer, which a refused pre-vote does not stretch (once both survivors campaign, the
  tie-break picks one); the second, the election rounds and the test's polling. `e2e`
  `TestSIGKILLedLeaderIsReplaced` keeps its looser 900 ms. 50 runs under `-race`, alone, on
  the ports fix plus this change: 50 passed, slowest new leader 203–398 ms (median 285 ms),
  no data race. An earlier batch of 50, run while the machine was slowed (below, "Known
  limitations"), passed 37: 7 new leaders after 0.92–3.76 s, 4 acknowledged writes that
  could not be read back within 5 s, 1 run with no write acknowledged to one shard in 8
  writes, and 1 "shard 0 has no leader" (the next item but one); stopping a node took up to
  2 s there, against at most 30 ms in the batch that passed.

- **A stopped test node's addresses dropped connections instead of refusing them** (the
  ports fix above, on this machine only). This WSL instance uses mirrored networking: a
  loopback port that nothing in Linux listens on is passed to the Windows host when it lies
  outside Linux's ephemeral range, and the host drops the attempt without an answer (a
  connect to an unused port in 9130–13225 is refused in about 10 ms; one to 13300 or 40000
  hangs). With the ports fix, a node a test stopped therefore went silent instead of
  refusing connections as a stopped process does on CI's runners: a run of
  `TestDashboardGate` under load failed goleak on the console's HTTP dials to the killed
  node, still waiting in `connect` after the test. Binding a refusing listener on the
  addresses after each stop made stops slower here (binding takes 5–110 ms under mirrored
  networking; stops took 5–326 ms against 1–5 ms), which moved the measured detection
  times of `TestKilledPeerIsReportedDownWithinWindow` toward its lower bound. Instead,
  `nodetest` now keeps every member's listening sockets for the whole test
  (`porttest.Keep`) and lends them to each start: the node's servers end the loan when
  they stop, and until the next start the socket closes every connection it accepts
  (within 2–29 ms of the loan ending). The port the kernel chose (127.0.0.1:0) is never
  released, so a restart cannot find it taken and nothing is bound again;
  `porttest.Relisten` is gone. `porttest.Pick`, for the `e2e` processes, also keeps its
  ports below 49152: the IANA dynamic range is where Windows (and macOS) take their
  connections' local ports, and under mirrored networking Linux shares the host's ports.
  Tests: `porttest` `TestKeep`, `TestKeepClosesWithItsLastLoan`, `TestInUse`,
  `TestPortsStayOutOfTheEphemeralRange` (now also below 49152). 50 runs under `-race` of
  every test that stops or restarts a `nodetest` member, after a reboot with Docker
  Desktop stopped:

  | Package | Tests | Passed |
  | --- | --- | --- |
  | `porttest` | `TestKeep`, `TestKeepClosesWithItsLastLoan`, `TestInUse`, `TestPortsStayOutOfTheEphemeralRange`, `TestRefusingAndSilent` | 50 of 50 each |
  | `cmd/dkvctl` | `TestLeadersWithDeadNodesFirst` | 50 of 50 |
  | `test/dashboard` | `TestDashboardGate` (all three subtests) | 50 of 50 |
  | `integration` | `TestKilledPeerIsReportedDownWithinWindow`, `TestRestartedPeerRejoinsWithItsID`, `TestDeadNodeReplicasAreRecreated`, `TestMetaQuorumLossStopsRebalancingNotData`, `TestWipedNodeRejoinsThroughSnapshots`, `TestClusterSettingsAreFixedAtBootstrap`, `TestMajorityDownFailsWithinDeadline`, `TestMinorityDownKeepsEveryShardWorking`, `TestShardDataSurvivesRestart`, `TestAddedNodeTakesItsShare`, `TestEarlyJoinerWaitsForTheMetaBootstrap` | 50 of 50 each (51 of 51 for the first two), except `TestAddedNodeTakesItsShare`, 49 of 50: one write took 2.6 s during the rebalance, against its 2 s bound (below) |
  | `integration` | `TestLeaderFailover` | 49 of 51: two new leaders after 1.20 s and 1.37 s (below) |
  | `integration` | `TestRetriedPutAppliedOnce` | 22 of 23: the 23rd panicked in "retry after leader change" reading a leader not yet published (index -1), which ended that batch; `WaitLeader` (below) fixes it |
  | `linearizability` | `TestPowerLossOfMajorityLosesNothing` (both sync modes); `TestFailureMatrix` `kill-leader`, `kill-minority`, `kill-majority`, `rolling-restart` | 50 of 50 each |
  | `linearizability` | `TestFailureMatrix` `wipe-node` | 49 of 50: its own precondition (below) |

  No run reported a data race.

  An earlier batch, while the machine was starved (below), ended early: `porttest` 50 of 50,
  `TestLeadersWithDeadNodesFirst` 46 of 50, `TestDashboardGate` 0 of 16, the integration
  tests 229 of 238 in 18 iterations; run alternately with and without the kept sockets
  then, `TestDashboardGate` passed 5 of 6 with them and 2 of 6 without.

- **The console showed a live node as down after one slow poll** (Phase 9 code). Any failed
  poll showed the node down at once, with no numbers, so a node that answered one 200 ms
  poll late flickered down for a round: `TestDashboardGate` "numbers come from the nodes"
  failed 8 of 50 runs on it (a zero start time). Now only a poll that fails other than by
  timing out (refused or reset connection, an unknown host, an error status) shows the node
  down at once, which keeps the Phase 9 gate's "within one poll interval" for a killed
  node; a node whose polls time out stays up, with its last numbers, until
  `console.down_after_misses` polls in a row have timed out (new setting, default 3, at
  least 1; ADR 0029, which revises ADR 0015). Tests: `console`
  `TestTimedOutPollsKeepANodeUp`, `TestFailedPollShowsNodeDownAtOnce`, `TestOnlyTimeouts`;
  `config` `TestConsoleRejectsMistakes` (0 misses), `TestConsolePrecedence` (the default).
- **`TestDashboardGate` "stopping the console changes nothing" timed its writes.** It
  required at least one write in the second after the console stopped. Under three
  CPU-bound loops, a run that logged every write and every attempt showed what fails it: a
  single write taking 802 ms (others 150–350 ms, against about 35 ms unloaded), with no
  failed attempt, no retry and no leader change; one such write over a second leaves the
  window empty. Nothing in it involves the console. The subtest now waits for 10
  successful writes before the console stops and 10 after, with no failure; each write
  still has its 2 s deadline, and a failure reports the slowest write.
- **The kill subtest could wait for an election that was not coming.** It took the victim
  from the nodes (`Leader(0)`) but the shards it led from the console's overview, which can
  be a poll behind; a shard the overview still showed on the victim, or a missing one, left
  it waiting 60 s (3 of 50 runs, the missing event not reported). The shards are now read
  from the nodes just before the kill, and a timeout reports which of the three events was
  seen. The test also logs how long the node took to stop.

  50 runs under `-race`, on the kept sockets above:

  | Package | Tests | Passed |
  | --- | --- | --- |
  | `console` | `TestTimedOutPollsKeepANodeUp`, `TestFailedPollShowsNodeDownAtOnce`, `TestOnlyTimeouts` | 50 of 50 each |
  | `config` | `TestConsoleRejectsMistakes`, `TestConsolePrecedence`, `TestConsoleSettingsCoverConfig`, `TestExampleConsoleConfigLoads` | 50 of 50 each |
  | `test/dashboard` | `TestDashboardGate` (all three subtests) | 50 of 50 |
  | `e2e` | `TestConsoleWatchesRealProcesses` | 50 of 50 |

  A first batch of the `console` tests, with the fake node behind a real HTTP server,
  failed 3 of 150 runs: an answer took longer than the test's 100 ms poll timeout. The fake
  is now the console's HTTP transport (the seam), so an answer takes no time and only a
  hanging node meets the deadline. No run reported a data race.

- **Tests read a shard's leader before it published itself** (test code since Phases 6–9;
  the fix held back in Phase 10, "Known limitations"). A new leader publishes its role at
  the end of the Ready that made it leader, after its first appends have left, so its
  followers, a finished leadership transfer and `Settled` can know of it before
  `nodetest.Cluster.Leader` finds it; a test that read the leader once got -1 and panicked
  on index -1 (CI on the `phase-9` tag; `TestRetriedPutAppliedOnce` "retry after leader
  change" in the runs above). New `nodetest.Cluster.WaitLeader` waits, up to a timeout,
  for a running member to report leading the shard; it is used wherever a test reads a
  leader once: `node` `TestTransferLeaderAndDrain` and
  `TestPausedLeaderCannotNoticeLostQuorum`, `TestDashboardGate`'s victim, and `integration`
  `TestLeaderFailover` (the old leader and the shards it led) and
  `TestRetriedPutAppliedOnce`. `TestTransferLeaderAndDrain` also raced leader balancing,
  which moves a leadership off a node leading two more groups than another every rebalance
  round, so it could undo the test's transfer or race it (transfer-leader is a nudge):
  the test now waits until the leaderships have stayed balanced for three rounds, then
  moves one from a node leading the most groups to the node leading the fewest, a move
  that keeps the balance. 50 runs under `-race`, on the changes above:

  | Package | Tests | Passed |
  | --- | --- | --- |
  | `node` | `TestTransferLeaderAndDrain`, `TestPausedLeaderCannotNoticeLostQuorum` | 50 of 50 each |
  | `integration` | `TestLeaderFailover`, `TestRetriedPutAppliedOnce` (both subtests) | 50 of 50 each |
  | `test/dashboard` | `TestDashboardGate` (all three subtests) | 50 of 50 |

  No run reported a data race. Item 3's commit also passed the Docker gates on GitHub
  (`docker-gates.yml` run 45028984044: the Compose gate, the five-node profile and the
  Phase 9 dashboard gate in Chromium).

### Known limitations

- `linearizability` `wipe-node` waits, during its 3 s of faults, until every data shard
  has compacted its log (every 20 entries) before it wipes a node, and fails when that does
  not happen in time. How many writes fit in those 3 s depends on the machine, and how they
  fall on the 4 shards on the keys: 53 to 808 in the 50 runs for the ports fix, and the runs
  with 53 and 137 failed this way (both histories linearizable). It failed the same way
  twice before (Phase 10, "Known limitations").
  Run alone 50 times it passed 50 of 50 with the ports fix (162–633 writes) and 50 of 50 on
  the commit before it (176–574): both failures came in runs after the matrix's other four
  scenarios in the same process.
- `integration` `TestAnyNodeAcceptsAnyKey` (4 nodes, 16 shards of 2 replicas, 20 ms ticks)
  needs most of this machine's 4 CPUs under `-race`, and every one of its groups needs both
  of its replicas to commit or to answer a read, so one starved node stalls a group. It
  failed `make check` for the ports fix (a read hit its 10 s deadline). Run alone after
  that, with Docker Desktop's engine, the editor and three other terminal sessions running
  (CPU pressure around 50%), it failed 3 of 3 runs on the commit before the ports fix and 3
  of 3 with it: a request hit its deadline, or not every shard elected a leader within
  20 s. It passed in CI (run 44984779518), and it had failed under load before (Phase 10,
  "Known limitations"). Between about 04:00 and 04:35 UTC on 22 November everything
  in-process ran slowly here (nodes took up to 2 s to stop; `TestLeaderFailover` failed 13
  of 50 runs, above), with CPU pressure (`/proc/pressure/cpu`, 10 s average) around 50%
  while only one test binary ran; the cause is outside this VM's view. Afterwards
  `TestLeaderFailover` passed 50 of 50 and this test 10 of 10 (13.7–24.2 s each, against
  22–300 s in that window).
- From about 06:40 UTC on 22 November until the VM restarted (between 08:05 and 11:10),
  this machine was starved again: Docker Desktop's distro was running with no containers
  and used up to about 2 CPUs outside this distro (the VM was 99% busy while this distro's
  processes used about 2 CPUs), swap was 950 of 1,024 MiB, and `docker system df` did not
  answer. Batches then failed broadly (above, the earlier batch for the kept sockets). The
  counts given for the kept sockets and later items come from after the restart, with
  Docker Desktop stopped.
- `integration` `TestLeaderFailover` still exceeds its 800 ms bound now and then: 2 of 51
  runs in the integration batches for the kept sockets (new leaders after 1.20 s and 1.37
  s, the old leader stopped in 14–17 ms, the machine quiet), against 0 of 50 alone for its
  own change and 0 of 30 with and 0 of 30 without the kept sockets when run alternately. The
  bound allows one contested round (ADR 0028); these took about three election timeouts.
  Not diagnosed: it needs the Raft logs of a failing run.
- `integration` `TestAddedNodeTakesItsShare` (a Phase 7 gate check) bounds every write
  during the rebalance at 2 s; one of 50 runs for the kept sockets had a write of 2.6 s
  (p50 46 ms, the machine quiet). It failed the same way once in Phase 10 (5.2 s).
- The console polls with Go's default HTTP transport, whose connection attempts run for up
  to 30 s after the poll that started them timed out. To a node whose address drops
  connection attempts (a host gone from the network; on this machine, a loopback port
  outside the ephemeral range that nothing listens on), each poll leaves one running, which
  goleak reported in `TestDashboardGate` before the kept sockets.
- `dkv-bench`'s server-side numbers include the warm-up's traffic, although its code and
  `docs/benchmarks.md` say they cover the measured time: `bench` reads the nodes' counters
  (`serverStats`) before the warm-up starts, not when the measured time does, so the bytes
  written, Raft-log syncs and their latency percentiles in the Phase 10 results also count
  the warm-up (ratios such as write amplification move little). Not fixed, by decision:
  fixing it changes what the published results mean. A sentence saying so in
  `docs/benchmarks.md` (and in the report generator) is drafted with item 5 (Paused, below).

## Paused: items 5 and 6 of the follow-up on tests that fail at random

Items 1–4 are committed above. Items 5 and 6 are drafted and partly tested but not
committed; the drafts are in `~/dkv-local/` (outside the repository). To resume one:
`git apply ~/dkv-local/<patch>`, run its tests 50 times under `-race` with
`GOFLAGS=-p=1` (one test binary at a time), commit, push and confirm CI is green. The
request's final steps, `make check` clean twice and the attribution check, also remain.

Draft files:

- `~/dkv-local/item5-bench-timing.patch`: `cmd/dkv-bench/run.go`, `bench_test.go`,
  `report.go`, the new `timing_test.go`, and `docs/benchmarks.md`.
- `~/dkv-local/item6-kv-failover.patch`: `pkg/client/client.go`, the new
  `pkg/client/failover_test.go` and `cmd/dkvctl/kv_failover_test.go`, and ADR 0027 with its
  row in `docs/adr/README.md`.
- `~/dkv-local/items5-6-drafts.tar.gz`: the same files whole, as they were in the tree.
- `~/dkv-local/flaky/`: `run50.sh` (runs a test regex 50 times under `-race` in `$WT` and
  prints pass counts), `stage_part.py` (stages part of a file, for a commit that should
  not take every change in it), `results-so-far.md` and `runs/` (the logs behind the
  counts above).

### Item 5: `TestBenchAgainstAServer` independent of wall-clock timing

The test checked, against a real server, things that depend on how fast the machine is:
the first whole second held at least half the operations, the series had exactly one
entry for a 1.5 s run, the open loop served 150–250 ops/s at 200, the read share of a
short run, and an event command racing the end of the run. Design:

- `run.go`: a `store` interface (`Get`, `GetStale`, `Put`), consumed where the workers
  send their operations. `bench` builds the real clients (one per 16 workers) and calls
  `benchWith(ctx, c, log, clients []store)`. The client is the transport seam, where
  test doubles are allowed. `run.go`'s `serverSide` comment and `report.go`'s text say
  the server-side numbers include the warm-up (above), and `docs/benchmarks.md` gets the
  same sentence; the counters are not moved.
- New `timing_test.go`: `TestClosedLoopCountsEveryOperation` and
  `TestOpenLoopKeepsItsRate` run `benchWith` inside `testing/synctest` against `memStore`,
  an in-memory store whose every operation takes a fixed latency. synctest's clock moves
  only when every goroutine waits on it, so the numbers are exact. Closed loop (workload
  B, 300 records, 4 workers, 200 ms warm-up, 1.5 s measured, event at 500 ms, 3 ms per
  operation): the preload takes 225 ms; the measured time runs from 425 ms to 1925 ms;
  each worker's operations 67–566 are measured (2000 in all), 333 each (1332) end in the
  first whole second; no operation ends at an edge of the measured time. Open loop (200
  ops/s, no preload, 1 ms per operation): arrivals 40–339 are measured, exactly 300, 200.0
  ops/s, every latency 1000 µs; with 33 ms per operation the 4 workers are always busy
  (worker w finishes at its first arrival, 5–20 ms, plus 33 ms × j): the expected count is
  computed that way, throughput is that count over 1.5 s, and latency counted from the
  intended start includes the queue (p50 over 330 ms, largest backlog over 100).
- `TestBenchAgainstAServer` keeps only checks that hold at any speed.

Every old check and where it goes (approved):

| # | Old check | New place |
| --- | --- | --- |
| 1 | `bench` returns no error | Server test, unchanged; also the closed-loop test |
| 2 | `OK > 0` | Server test, unchanged; the closed-loop test checks the exact count (2000) |
| 3 | `Errors + Timeouts == 0` | Server test, unchanged; also the closed-loop test |
| 4 | `Preload.Ops == 300` | Server test, unchanged; also the closed-loop test (and 225 ms) |
| 5 | `Latency.Count == OK` | Server test, unchanged; also the closed-loop test |
| 6 | `reads + updates == OK` | Server test, unchanged; also the closed-loop test, which also checks the counts against the reads and writes the store finished in the measured time |
| 7 | Read share 0.90–0.99 | Closed-loop test, same bounds, over a fixed set of operations (on a real server the share depends on how many operations each worker managed: with seed 1, worker 2's running share touches 0.900 after 50-plus operations, and very short runs read 1.000) |
| 8 | Whole seconds' total ≤ `OK` | Server test, unchanged; the closed-loop test exactly |
| 9 | Whole seconds' total ≥ `OK/2` | Closed-loop test, same expression, plus the exact count of the whole second (1332), equal to what the store finished in it |
| 10 | `len(Timeseries) == 1` | Closed-loop test, same expression (1.5 s of fake time is exactly 1.5 s); the server test checks no more entries than whole seconds in its own measured time, numbered from 0 |
| 11 | Histogram total == `OK` | Server test, unchanged; also the closed-loop test |
| 12–14 | `Server != nil`, `UserBytes > 0`, `SpaceAmplification > 0` | Server test, unchanged |
| 15–16 | `Machine.CPUs != 0`, `GitSHA != ""` | Server test, unchanged |
| 17 | The event command ran | Closed-loop test, same check, plus its timing: no operation that ended by 499 ms saw its file, every one from 502 ms on did |
| 18 | Open loop: no error | Server test, unchanged; it also requires `OK > 0` and `Errors + Timeouts == 0` |
| 19 | Open loop served 150–250 ops/s at 200 | Open-loop test, same bounds and exactly 200.0, plus the queued case. Not kept: that this machine's real server sustains 200 ops/s, which is the wall-clock dependence itself; the test's doc comment never claimed it |
| 20–23 | Results written, 2 loaded back, report written, its six strings | Server test, unchanged |

Tested so far: each new test and the changed server test passed once; seven mutations of
`run.go` were each caught: the series keeping its partial second, the measured start
taken before the warm-up, open-loop latency counted from the dequeue, the workload's read
share ignored, the event 10 ms late, a pacer that drifts instead of keeping its schedule
(166.7 ops/s, which the old 150–250 bound would have passed), and an operation counted
twice in the series. To do: 50 runs of `TestBenchAgainstAServer`,
`TestClosedLoopCountsEveryOperation` and `TestOpenLoopKeepsItsRate`; a PROGRESS entry with
the mapping above; commit; push; CI.

### Item 6: key-value commands move on from a silent or refusing first address

Key-value requests moved to the next node only when one answered `Unavailable`, so a node
that accepts connections and never answers (a stopped container behind a port proxy)
held them until their deadline (Phase 10, "Known limitations"). Design, the connection
check admin requests already use (ADR 0027):

- `pkg/client` `call`: before each try, `connect(ctx, conn, budget)` waits for the node's
  connection for at most its share of the time left (the time to ctx's deadline divided
  by the number of nodes; without a deadline, until gRPC gives up). A connection that
  fails or is not ready in time counts as `Unavailable`: the request moves to the next
  node after the usual backoff, within the attempt limit. A node never connected to never
  received the request, so this is safe for writes. Once connected, the try has all the
  time left, as before, so a slow node is not passed over. The package comment says so.
- ADR 0027 extended: title, status, a decision paragraph for key-value requests, and the
  consequence that replaces "left for when it is needed"; its row in the ADR index.
- Tests: `pkg/client` `TestKVRequestsMoveOnFromDeadNodes` (a silent listener and a
  resetting one before a live node, both orders; put, get, cas and delete, each through a
  fresh client with a 3 s deadline, which waiting on the silent node would exceed; with
  every node dead, `Unavailable` or `DeadlineExceeded`); `cmd/dkvctl`
  `TestKVCommandsMoveOnFromDeadAddresses` (the same through `dkvctl --timeout=3s`). Both
  failed on the commit before (DeadlineExceeded) and passed with the change, once each.

To do: 50 runs of both new tests and of the tests that exercise `call` through failures
(`pkg/client` `TestClientUnresponsiveServer`, `TestAdminRequestsMoveOnFromDeadNodes`,
`TestClientRoundTrip`; `integration` `TestLeaderFailover`; `TestDashboardGate`, whose
writer uses the client); replace the Phase 10 known limitation on key-value requests with
the fix; commit; push; CI.

## AWS deployment on the free plan (Phase 10 follow-up)

A one-time run of `deploy/aws` on an AWS free plan account in `ap-south-1`: create the
cluster, show failover on the dashboard, run a short benchmark from a load generator in the
region, destroy everything and prove nothing is left (ADR 0030).

Status: **planned, not applied.** The apply, the demo, the benchmark and the destroy wait
for the IAM user's permissions and the word to apply ("Needs from you").

### Before the apply (22 November)

- **Identity:** `aws sts get-caller-identity` shows the IAM user `dkv-deploy`, not the root
  user; the secret key was never printed or read.
- **Permissions:** dry runs succeed for `ec2:CreateVpc`, `CreateSecurityGroup`,
  `ImportKeyPair` and `RunInstances` of a `c7i-flex.large`; `iam:GetRole`,
  `iam:GetInstanceProfile`, `s3:ListAllMyBuckets`, the Price List API, Service Quotas and
  `freetier:GetAccountPlanState` (remaining credits) are denied. The apply would stop at
  the S3 bucket and the IAM role, so it waits for the user's permissions.
- **Free-tier-eligible types in `ap-south-1`** (`aws ec2 describe-instance-types --filters
  Name=free-tier-eligible,Values=true`): `c7i-flex.large`, `m7i-flex.large`, `t3.micro`,
  `t3.small`, `t4g.micro`, `t4g.small`, `t8i.micro`, `t8i.small`.
- **`terraform plan`** (binaries built from f8df8ea without uncommitted changes): 25
  resources to add, nothing else: a VPC, three subnets (`ap-south-1a`, `1b`, `1c`), an
  internet gateway, a route table and its three associations, a security group, a key pair,
  four `c7i-flex.large` instances with 8 GiB encrypted gp3 root volumes and IMDSv2, an IAM
  role with an inline policy and an instance profile, an S3 bucket with its public access
  block and five objects. No NAT gateway, Elastic IP, load balancer or separate volume.
  Every taggable resource is tagged `project=dkv`; the root volumes' tags are merged at
  apply time, so the run checks them then.
- **Cost estimate**, from AWS's public price files (the Price List API is denied to the
  user): `c7i-flex.large` $0.08479 an hour on demand (published 21 November), in-use public
  IPv4 $0.005 an hour (19 November), gp3 $0.0912 per GB-month (21 November), data between
  zones $0.01 per GB each way (19 November). Four instances $0.339, four public addresses
  $0.020, 32 GiB of gp3 $0.004: **$0.363 an hour, 0.36 credits**; the bucket's 86 MB and
  a few GB between zones add cents to the whole run. (A NAT gateway would have added $0.056
  an hour plus a charge per GB.)

### Implemented and tested

- **`deploy/aws` for the free plan** (ADR 0030): `ap-south-1`; `c7i-flex.large` for the three
  nodes and the load generator, with a precondition on each instance that refuses a type
  the account's `DescribeInstanceTypes` does not report as free-tier eligible
  (`free_tier_only`, default true); zones found from the instance types' offerings; public
  subnets, no NAT gateway, no Elastic IPs; 8 GiB encrypted gp3 root volumes at the
  baseline 3,000 IOPS and 125 MiB/s; IMDSv2 only; a key pair registered from a local public
  key (`ssh_public_key_file`) instead of one made by hand; default tags `project=dkv` and
  `cluster`, which root volumes inherit (AWS provider 5.100.0's `root_block_device.tags_all`);
  16 shards, as in `make bench-local`; the user data retries its downloads and calls the
  snap's `/snap/bin/aws`, which is not on cloud-init's PATH.
- **The console and dashboard on the load generator:** `up.sh` also builds `dkv-console`
  and the dashboard (`web.tar.gz`); the load generator runs the console under systemd,
  history in memory, on loopback only; output `console_tunnel` is the SSH command for it.
- **`make aws-up`** (`up.sh`) waits after the apply until the load generator's first boot
  finished, every node takes a write and the console answers, and prints the tunnel
  command. **`make aws-down`** (`down.sh`) destroys, then runs **`check-empty.sh`**: instances
  not terminated, volumes, Elastic IPs, NAT gateways, VPCs, subnets, internet gateways,
  security groups and key pairs tagged `project=dkv` in the region, and the S3 buckets, IAM
  roles and instance profiles named after the cluster; it exits 1 if any is left.
- **`deploy/aws/bench.sh`:** the AWS benchmarks with the local matrix's settings (workloads
  A, B, C and W with 32 workers; leader failover with 16 workers and a SIGKILL 10 s into
  the measured time, sent over an SSH connection the load generator keeps open, while a
  runtime systemd drop-in keeps the killed node down until the run ends); results to
  `bench/results/aws/`, the deployment's description to `bench/results/aws/meta/`, then
  `docs/benchmarks.md` regenerated.
- **`dkv-bench report` renders other environments as parts:** each subdirectory of the
  results directory that holds result files (`aws/`; not `reruns/`) becomes a section after
  the local experiments, with its own environment (the load generator's machine, the
  builds, its `environment` label) and its experiments one heading level down; its runs
  never enter the local tables. Test: `TestReportRendersOtherEnvironmentsAsParts` (it fails
  if `reruns/` is treated as a part). The committed `docs/benchmarks.md` regenerates byte
  for byte without parts.
- Checks: `terraform fmt -check` and `validate` (AWS provider 5.100.0); both user-data
  templates rendered with `terraform console` pass `bash -n`, and the YAML they write was
  loaded by the real binaries: `dkv-server` got through its configuration to the listen
  step (where binding the instance's private address fails on this machine), and
  `dkv-console` ran without error.

### Fixed along the way

- **AWS rejected a security group rule description** (Phase 10 code, never planned against
  AWS before): "everything between the cluster's own instances"; the apostrophe is outside
  the characters a rule description may use, which the provider checks only when it plans,
  not in `terraform validate`. It now reads "everything between the cluster instances".

### Decisions

- **The AWS results go in `docs/benchmarks.md` through the generator**, as a part rendered
  from `bench/results/aws/`, so regenerating the report keeps them. Alternatives: a
  hand-written section (the next `make bench-local` would drop it), or a second report
  file (the request named `docs/benchmarks.md`).
- **The AWS CLI is installed user-local** (v2.42.1 in `~/.local/aws-cli`, linked from
  `~/.local/bin`; GPG signature checked against AWS's key FB5D B77F D5C1 18B8 0511 ADA8
  A631 0ACC 4672 475C), since `sudo` needs a password. Terraform 1.9.8 was already in
  `~/.local/bin`.
- **The instances' SSH key** is a new ed25519 key, `~/.ssh/dkv-aws`, without a passphrase so
  that `bench.sh` can load it into its own agent; AWS forgets it on destroy.
  `deploy/aws/terraform.tfvars` (ignored by git) admits this machine's public address.
