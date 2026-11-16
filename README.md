# dkv

A sharded, Raft-replicated key-value store written in Go, with its own LSM-tree
storage engine. See [docs/PLAN.md](docs/PLAN.md) for the design and roadmap and
[docs/PROGRESS.md](docs/PROGRESS.md) for what is implemented and tested.

**Current state: Phase 1.** A single standalone node with an in-memory engine:
data is lost when the process exits. Persistence (WAL) arrives in Phase 2,
replication in Phase 6.

## Quick start

Requires Go 1.27.

```sh
make build

# Start a node (every option can also come from a YAML file or DKV_* env vars;
# see `bin/dkv-server -h` and deploy/config/dkv-server.example.yaml).
bin/dkv-server --server.grpc-addr=127.0.0.1:7070 --log.format=text &

export DKVCTL_ADDR=127.0.0.1:7070
bin/dkvctl put greeting "hello"     # OK (version 1)
bin/dkvctl get greeting             # hello
bin/dkvctl delete greeting          # deleted (version 2)
bin/dkvctl get greeting             # exit status 1: not found
```

## Development

```sh
make tools        # install pinned golangci-lint, buf and protoc plugins
make check        # gofmt, go vet, golangci-lint, go test -race ./...
make proto        # buf lint + regenerate gen/ from proto/
make proto-check  # fail if gen/ is stale
```

Randomized tests log their seed; rerun a failure with
`go test ./internal/storage/memtable -run TestName -seed=<seed>`.
