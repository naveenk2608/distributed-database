# Progress

Only what is implemented AND tested is listed here. See docs/PLAN.md for the full plan.

## Needs from you

- **AWS (Phase 10, optional):** `make aws-up` creates the benchmark cluster in AWS and
  `make aws-down` destroys it. Neither has been run: it needs AWS credentials in the
  environment, a `deploy/aws/terraform.tfvars` (copy `terraform.tfvars.example`: your
  address as `allowed_cidr` and an EC2 key pair), and your decision to spend the money.
- Docker is still unavailable in this WSL distro (Docker Desktop's engine is not running
  and WSL integration is off), so every Docker gate runs on GitHub's runners instead: the
  Phase 4 Compose gate, the Phase 8 chaos scenarios, the Phase 9 dashboard gate and the
  Phase 10 five-node profile (`docker-gates.yml` and the nightly workflow). To run them here
  too, start Docker Desktop and enable Settings → Resources → WSL integration for this
  distro.

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

Status: **in progress.** Everything below is implemented and its tests pass locally (`make
check`). The benchmark matrix runs next, from a clean clone of this commit; its results and
the gate evidence that depends on them come in the following commit.

### Gate

| Gate item | Evidence | Status |
| --- | --- | --- |
| `make bench-local` reproduces the local results from a clean clone | `bench/run-local.sh`: builds the binaries, and for each run starts a fresh cluster of `dkv-server` processes on this machine (data on a real disk; it refuses tmpfs, where fsync is free), preloads it, runs `dkv-bench`, stops it, and regenerates `docs/benchmarks.md` from the result files. | Pending: the matrix |
| The report lists hardware, configuration, git SHA, methodology and raw data | Each result file holds the run's configuration and labels, the machine (CPU model and count, memory, OS, kernel), the build's git revision and whether it had uncommitted changes, the Go version, the raw HDR latency histogram, per-operation percentiles, an optional per-second time series, and the nodes' own numbers over the measured window (Raft-log fsync and commit latencies, bytes written by users, to engine logs and to SSTables). The report gives the medians with their ranges over the runs, the machine and build of every run, and for each experiment what it measures and the settings its variants share and vary. `cmd/dkv-bench` `TestBenchAgainstAServer` runs a closed and an open loop against a real node and checks the counts, the read share, the time series, the histogram total, the server-side numbers, the machine and revision, and the report generated from the files. | Pending: the matrix |
| Before-and-after numbers for each optimization | The "Optimizations" experiment runs the Phase 9 build (from the `phase-9` tag), this build with the engine's log kept, and this build's default, so each of the two cluster optimizations below is measured on its own; the "Standalone write path" experiment runs a standalone Phase 9 node against this build's. | Pending: the matrix |
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

### Implemented and tested

- **`dkv-bench`** (`cmd/dkv-bench`): YCSB workloads A (50% reads, 50% updates), B (95/5), C
  (reads only) and W (writes only); uniform or scrambled Zipfian keys (YCSB's, θ 0.99);
  closed loop with N workers, or open loop at a fixed arrival rate with latency counted from
  each operation's intended start; preload; warm-up excluded from the measurement; HDR
  histograms; errors and timeouts counted apart; an optional per-second time series and a
  command run at a given time (the failover runs kill a leader with it); linearizable or
  stale reads; the nodes' own numbers read from their admin APIs before and after the
  measured window; one JSON file per run; `report` renders `docs/benchmarks.md` from the
  files; `leader` finds the node leading a shard. Tests: `TestKeyDistributions`,
  `TestBenchAgainstAServer`, `TestStat` (with goleak).
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

### Known limitations

- The local benchmarks run every node and the load generator on one machine: they compare
  variants with each other, not with other databases or with a real cluster.
- The single-log path makes a restart replay up to a memtable's worth of the Raft log per
  replica.
- One `replica` durability test failed once during this phase in a full `make check`
  run whose output was not kept; it did not fail again in 45 runs of the package's
  durability tests and 9 full runs of the package. The cause is unknown (the harness's
  election timeout, fixed above, is a candidate).
