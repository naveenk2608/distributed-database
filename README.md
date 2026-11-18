# dkv

A sharded, Raft-replicated key-value store written in Go, with its own LSM-tree
storage engine. See [docs/PLAN.md](docs/PLAN.md) for the design and roadmap and
[docs/PROGRESS.md](docs/PROGRESS.md) for what is implemented and tested.

Every shard is a Raft group of three replicas (PreVote, CheckQuorum, leader
transfer, joint-consensus membership changes, snapshots); a replica's Raft log
is its only write-ahead log. Any node accepts any key and forwards it to the
shard's leader; reads are linearizable by default. A meta group tracks the
nodes, declares dead ones, and moves replicas and leadership. Each node serves
a read-only admin HTTP API that `dkv-console` and its dashboard watch.
[docs/architecture.md](docs/architecture.md) is the map,
[docs/benchmarks.md](docs/benchmarks.md) the measurements, and `docs/adr/` the
decisions with their alternatives.

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

## Benchmarks

`make bench-local` builds the binaries, then for each run starts a fresh
cluster of `dkv-server` processes on this machine, preloads it, drives it with
`dkv-bench` and stops it; at the end it regenerates
[docs/benchmarks.md](docs/benchmarks.md) from the JSON result files in
`bench/results/` (configuration, git revision, machine, raw latency histogram,
the nodes' own fsync and commit latencies). Data goes under `bench/data`,
which must be on a real disk. A full matrix takes about 80 minutes;
`ONLY=<experiment>`, `RUNS`, `WARMUP`, `DURATION` and `RECORDS` narrow it
(see `bench/run-local.sh`).

```sh
make bench-local
ONLY=durability RUNS=1 make bench-local
bin/dkv-bench run --addr=$A --workload=A --concurrency=32 --duration=30s --out=bench/results/mine.json
bin/dkv-bench report --results=bench/results --out=docs/benchmarks.md
```

## Deployment

**Docker Compose.** `deploy/compose/` holds layered profiles on one image
(`deploy/docker/Dockerfile`, static binaries on distroless):

| Profile | Command | What |
| --- | --- | --- |
| three nodes | `make compose-up` | `docker-compose.yml` |
| five nodes | `docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/five.yml up -d --build` | two more nodes join the three; `make compose-five` checks it |
| chaos | `make chaos` | `chaos.yml`: fault injection on, for the failure matrix (`test/chaos`) |
| dashboard | see [Dashboard](#dashboard) | admin APIs, a fourth node, `dkv-console`, PostgreSQL |

**AWS.** `deploy/aws/` is Terraform for a benchmark cluster: a VPC with a
subnet per availability zone, nodes spread over the zones on fixed private
addresses, one load-generator instance, gp3 volumes by default, and an S3 bucket the
instances fetch the binaries from. It costs money from `aws-up` until
`aws-down`.

```sh
cp deploy/aws/terraform.tfvars.example deploy/aws/terraform.tfvars   # set allowed_cidr, ssh_key_name
make aws-up      # builds linux/amd64 binaries, terraform apply, prints the addresses
make aws-down    # terraform destroy
```

`terraform output bench_addrs` gives the `--addr` and `--admin` flags for
`dkv-bench` on the load generator, and `terraform output environment` a line
for the report.

## Development

```sh
make tools        # install pinned golangci-lint, buf and protoc plugins
make check        # gofmt, go vet, golangci-lint, go test -race ./...
make proto        # buf lint + regenerate gen/ from proto/
make proto-check  # fail if gen/ is stale
make crash        # kill -9 harness: CYCLES=150 writer crashes, every ack checked
make test-integration # in-process multi-node clusters
make compose-gate # Phase 4 gate in Docker Compose (needs Docker)
make compose-five # the five-node Compose profile (needs Docker)
make sim          # deterministic Raft simulation: SEEDS=1000, PREVOTE=random|on|off
make lin          # the failure matrix in process, histories checked for linearizability
make chaos        # the failure matrix against Docker Compose (needs Docker)
make bench-local  # the benchmark matrix; regenerates docs/benchmarks.md
make web-test     # dashboard: typecheck and unit tests (Node 22)
make dashboard-gate # Phase 9 gate in Docker Compose with Chromium (needs Docker, jq, Playwright)
make storage-gate # Phase 3 gate: 1M-op model test, crash at every flush/compaction step, bloom rate
make bench        # storage microbenchmarks
make fuzz         # fuzz the WAL, batch and SSTable decoders (FUZZTIME=30s each)
```

Randomized tests log their seed; rerun a failure with
`go test ./internal/storage/memtable -run TestName -seed=<seed>`.
