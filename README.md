# dkv

A sharded, Raft-replicated key-value store written in Go, with its own LSM-tree
storage engine. See [docs/PLAN.md](docs/PLAN.md) for the design and roadmap and
[docs/PROGRESS.md](docs/PROGRESS.md) for what is implemented and tested.

**Current state: Phase 5.** Keys are hashed into a fixed number of shards,
placed on nodes with a consistent hash ring; each node keeps one durable
LSM-tree engine per shard replica it holds, and any node accepts any key and
forwards it to the shard's owner (`dkvctl shards`, `dkvctl status`).
Replication with Raft arrives in Phase 6; until then only a shard's primary
serves it.

## Quick start

Requires Go 1.27.

```sh
make build

# Start a node (every option can also come from a YAML file or DKV_* env vars;
# see `bin/dkv-server -h` and deploy/config/dkv-server.example.yaml).
bin/dkv-server --server.grpc-addr=127.0.0.1:7070 --storage.dir=./data --log.format=text &

export DKVCTL_ADDR=127.0.0.1:7070
bin/dkvctl put greeting "hello"     # OK (version 1)
bin/dkvctl get greeting             # hello
bin/dkvctl delete greeting          # deleted (version 2)
bin/dkvctl get greeting             # exit status 1: not found
```

### Three local nodes

```sh
P=127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103
for i in 1 2 3; do
  bin/dkv-server --server.grpc-addr=127.0.0.1:707$i --cluster.internal-addr=127.0.0.1:710$i \
    --cluster.peers=$P --storage.dir=./data/n$i --log.format=text &
done
bin/dkvctl --addr=127.0.0.1:7071 status
bin/dkvctl --addr=127.0.0.1:7072 put greeting hello   # any node accepts any key
bin/dkvctl --addr=127.0.0.1:7073 get greeting
bin/dkvctl --addr=127.0.0.1:7071 shards               # where each shard lives
```

With Docker: `make compose-up`, then
`docker compose -f deploy/compose/docker-compose.yml exec n1 dkvctl --addr=localhost:7070 status`.

### Operating a cluster with dkvctl

```sh
A=127.0.0.1:7071,127.0.0.1:7072,127.0.0.1:7073
bin/dkvctl --addr=$A leaders                       # each shard's leader and term (ask every node)
bin/dkvctl --addr=$A transfer-leader 0 <node-id>   # hand shard 0 to one of its voters
bin/dkvctl --addr=$A drain <node-id>               # move every leadership off a node, keep its replicas
bin/dkvctl --addr=$A undrain <node-id>
bin/dkvctl --addr=$A decommission <node-id>        # move every replica off a node
bin/dkvctl --addr=$A --consistency=stale get greeting   # read any replica's copy
bin/dkvctl --addr=$A --json shards                 # every command can print JSON
```

### Dashboard

Each node serves a read-only admin HTTP API when `server.admin_addr` is set:
JSON status, shard map, statistics and events under `/v1/`, and Prometheus
metrics at `/metrics`. `dkv-console` polls those, keeps the history in
PostgreSQL (or in memory), and serves the dashboard:

```sh
make web    # build the dashboard into web/dist (Node 22)
bin/dkv-console --config=deploy/config/dkv-console.example.yaml
```

In Docker, with four nodes, the console and PostgreSQL
(open http://localhost:8080):

```sh
docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/dashboard.yml up -d --build
```

The cluster never depends on the console: stopping it, or its database,
changes nothing for clients (docs/adr/0014, docs/adr/0015).

## Development

```sh
make tools        # install pinned golangci-lint, buf and protoc plugins
make check        # gofmt, go vet, golangci-lint, go test -race ./...
make proto        # buf lint + regenerate gen/ from proto/
make proto-check  # fail if gen/ is stale
make crash        # kill -9 harness: CYCLES=150 writer crashes, every ack checked
make test-integration # in-process multi-node clusters
make compose-gate # Phase 4 gate in Docker Compose (needs Docker)
make web-test     # dashboard: typecheck and unit tests (Node 22)
make dashboard-gate # Phase 9 gate in Docker Compose with Chromium (needs Docker, jq, Playwright)
make storage-gate # Phase 3 gate: 1M-op model test, crash at every flush/compaction step, bloom rate
make bench        # storage microbenchmarks
make fuzz         # fuzz the WAL, batch and SSTable decoders (FUZZTIME=30s each)
```

Randomized tests log their seed; rerun a failure with
`go test ./internal/storage/memtable -run TestName -seed=<seed>`.
