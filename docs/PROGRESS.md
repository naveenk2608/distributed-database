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
