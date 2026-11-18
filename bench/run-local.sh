#!/usr/bin/env bash
# make bench-local: the benchmark matrix of docs/PLAN.md ("Benchmark
# methodology"), scaled to one machine. It builds the binaries, then for
# every run starts a fresh cluster of dkv-server processes on this machine
# (data under BENCH_DATA, which must be a real disk: on tmpfs fsync is
# free), preloads it, runs dkv-bench, and stops it; at the end it
# regenerates docs/benchmarks.md from the result files.
#
# Environment (defaults in parentheses): RUNS (3) runs per variant; WARMUP
# (10s) and DURATION (20s) per run; RECORDS (20000) records preloaded;
# SHARDS (16); BASE_PORT (17000): ports BASE_PORT+1.. (public), +101..
# (admin), +201.. (internal); BENCH_DATA (bench/data); RESULTS
# (bench/results); REPORT (docs/benchmarks.md); ONLY: run only the experiments
# whose name contains it.
# Results of the experiments run are replaced; the others are kept.
set -euo pipefail
cd "$(dirname "$0")/.."

RUNS=${RUNS:-3}
WARMUP=${WARMUP:-10s}
DURATION=${DURATION:-20s}
RECORDS=${RECORDS:-20000}
SHARDS=${SHARDS:-16}
BASE_PORT=${BASE_PORT:-17000}
DATA=${BENCH_DATA:-bench/data}
RESULTS=${RESULTS:-bench/results}
ONLY=${ONLY:-}
REPORT=${REPORT:-docs/benchmarks.md}
bin=bench/bin

mkdir -p "$bin" "$DATA" "$RESULTS"
go build -o "$bin/dkv-server" ./cmd/dkv-server
go build -o "$bin/dkvctl" ./cmd/dkvctl
go build -o "$bin/dkv-bench" ./cmd/dkv-bench
fstype=$(df -T "$DATA" | awk 'NR == 2 { print $2 }')
if [ "$fstype" = tmpfs ]; then
  echo "BENCH_DATA=$DATA is on tmpfs, where fsync costs nothing; use a real disk" >&2
  exit 1
fi
environment="Every node is a dkv-server process on the machine below, with its data on $fstype; the load generator runs on the same machine. $SHARDS shards (and the meta group)."

# The optimization experiment also runs the Phase 9 build (before either
# Phase 10 optimization), from the phase-9 tag.
before_bin=bench/bin/phase-9
build_before() {
  [ -x "$before_bin/dkv-server" ] && return
  local tree
  tree=$(mktemp -d)
  git worktree add --detach "$tree" phase-9 > /dev/null
  mkdir -p "$before_bin"
  (cd "$tree" && go build -o "$OLDPWD/$before_bin/dkv-server" ./cmd/dkv-server)
  git worktree remove --force "$tree"
}
server_bin="$bin/dkv-server"

pids=()
public=()
admin=()
nodes=0
rf=0

stop_cluster() {
  local p
  for p in "${pids[@]:-}"; do [ -n "$p" ] && kill -TERM "$p" 2>/dev/null || true; done
  for p in "${pids[@]:-}"; do [ -n "$p" ] && wait "$p" 2>/dev/null || true; done
  pids=()
}
trap stop_cluster EXIT

# start_cluster NODES RF [dkv-server flags...]: a fresh cluster (NODES 1 is
# a standalone node).
start_cluster() {
  nodes=$1 rf=$2
  shift 2
  stop_cluster
  rm -rf "$DATA"/node*
  public=() admin=()
  local i peers=""
  for i in $(seq 1 "$nodes"); do peers+="${peers:+,}127.0.0.1:$((BASE_PORT + 200 + i))"; done
  for i in $(seq 1 "$nodes"); do
    local args=(--server.grpc-addr="127.0.0.1:$((BASE_PORT + i))" --server.admin-addr="127.0.0.1:$((BASE_PORT + 100 + i))"
      --storage.dir="$DATA/node$i" --log.level=warn --log.format=json)
    if [ "$nodes" -gt 1 ]; then
      args+=(--cluster.internal-addr="127.0.0.1:$((BASE_PORT + 200 + i))" --cluster.peers="$peers"
        --cluster.shards="$SHARDS" --cluster.replication-factor="$rf" --cluster.meta-replicas=3)
    fi
    "$server_bin" "${args[@]}" "$@" > "$DATA/node$i.log" 2>&1 &
    pids+=($!)
    public+=("127.0.0.1:$((BASE_PORT + i))")
    admin+=("127.0.0.1:$((BASE_PORT + 100 + i))")
  done
  local p t
  for p in "${public[@]}"; do
    for t in $(seq 1 120); do
      "$bin/dkvctl" --addr="$p" --timeout=2s put bench-ready yes > /dev/null 2>&1 && break
      [ "$t" -lt 120 ] || { echo "the cluster did not become ready; logs in $DATA" >&2; exit 1; }
      sleep 0.5
    done
  done
  # A script that SIGKILLs the node leading shard 0, for the failover runs.
  {
    echo "a=\$($bin/dkv-bench leader --admin=$(IFS=,; echo "${admin[*]}") --shard=0)"
    for i in $(seq 1 "$nodes"); do echo "[ \"\$a\" = ${admin[$((i - 1))]} ] && kill -9 ${pids[$((i - 1))]} && echo killed node $i"; done
    echo true
  } > "$DATA/kill-leader.sh"
}

# variant EXPERIMENT ORDER TITLE WHAT NAME NAME_ORDER NODES RF SERVER_FLAGS -- BENCH_FLAGS...
variant() {
  local exp=$1 order=$2 title=$3 what=$4 name=$5 vorder=$6 n=$7 r=$8 server_flags=$9
  shift 9
  [ "${1:-}" = -- ] && shift
  local run
  for run in $(seq 1 "$RUNS"); do
    # shellcheck disable=SC2086 # server_flags is a list of flags
    start_cluster "$n" "$r" $server_flags
    echo "== $exp / $name, run $run of $RUNS"
    "$bin/dkv-bench" run --addr="$(IFS=,; echo "${public[*]}")" --admin="$(IFS=,; echo "${admin[*]}")" \
      --records="$RECORDS" --warmup="$WARMUP" --duration="$DURATION" --seed="$run" \
      --out="$RESULTS/$exp--$(echo "$name" | tr ' /' '__')--$run.json" \
      --label experiment="$exp" --label order="$order" --label title="$title" --label what="$what" \
      --label variant="$name" --label variant_order="$vorder" --label nodes="$n" --label rf="$r" \
      --label environment="$environment" "$@"
    stop_cluster
  done
}

wanted() { [ -z "$ONLY" ] || [[ "$1" == *"$ONLY"* ]]; }
begin() { # EXPERIMENT: replace its earlier results
  wanted "$1" || return 1
  rm -f "$RESULTS/$1--"*.json
}

if begin nodes; then
  t="Cluster size" w="One standalone node, then three and five nodes with every shard on three of them (RF 3)."
  n="Every node and the load generator share this machine's CPUs and its one disk: more nodes means more processes competing for them. These numbers compare configurations on one machine; they do not measure scaling out."
  variant nodes 1 "$t" "$w" "1 node (standalone)" 1 1 1 "" -- --workload=A --concurrency=32 --label note="$n"
  variant nodes 1 "$t" "$w" "3 nodes, RF 3" 2 3 3 "" -- --workload=A --concurrency=32 --label note="$n"
  variant nodes 1 "$t" "$w" "5 nodes, RF 3" 3 5 3 "" -- --workload=A --concurrency=32 --label note="$n"
fi
if begin replication; then
  t="Replication factor" w="Three nodes; every shard on one node (RF 1) or on all three (RF 3)."
  variant replication 2 "$t" "$w" "RF 1" 1 3 1 "" -- --workload=A --concurrency=32
  variant replication 2 "$t" "$w" "RF 3" 2 3 3 "" -- --workload=A --concurrency=32
fi
if begin sync; then
  t="fsync" w="Three nodes, RF 3, writes only. storage.sync never skips every fsync of the Raft log: it shows what fsync costs, and loses acknowledged writes on power loss."
  variant sync 3 "$t" "$w" "always" 1 3 3 "--storage.sync=always" -- --workload=W --concurrency=32
  variant sync 3 "$t" "$w" "never" 2 3 3 "--storage.sync=never" -- --workload=W --concurrency=32
fi
if begin durability; then
  t="Durability path" w="Three nodes, RF 3, writes only. The single-log path (the default: the Raft log is a replica's only write-ahead log) against the two-log path (storage.engine_wal: each replica's engine also logs and fsyncs every write), docs/adr/0010."
  variant durability 4 "$t" "$w" "single log" 1 3 3 "" -- --workload=W --concurrency=32
  variant durability 4 "$t" "$w" "two logs" 2 3 3 "--storage.engine-wal=true" -- --workload=W --concurrency=32
fi
if begin consistency; then
  t="Read consistency" w="Three nodes, RF 3, reads only. Linearizable reads confirm leadership with a heartbeat round (ReadIndex); stale reads answer from any replica. Lease reads are not implemented (docs/adr/0007)."
  variant consistency 5 "$t" "$w" "linearizable" 1 3 3 "" -- --workload=C --concurrency=32
  variant consistency 5 "$t" "$w" "stale" 2 3 3 "" -- --workload=C --concurrency=32 --consistency=stale
fi
if begin concurrency; then
  t="Concurrency" w="Three nodes, RF 3, workload A, closed loop: each worker waits for its answer, so throughput rises with workers until the cluster saturates, and latency then grows instead."
  o=1
  for c in 1 4 16 64 256; do
    variant concurrency 6 "$t" "$w" "$c workers" $o 3 3 "" -- --workload=A --concurrency="$c"
    o=$((o + 1))
  done
fi
if begin values; then
  t="Value size" w="Three nodes, RF 3, writes only."
  o=1
  for v in 100 1000 4000; do
    variant values 7 "$t" "$w" "$v bytes" $o 3 3 "" -- --workload=W --concurrency=32 --value-size="$v"
    o=$((o + 1))
  done
fi
if begin openloop; then
  t="Open loop" w="Three nodes, RF 3, workload A at fixed arrival rates; latency counts from each operation's intended start, so queueing in the client or the cluster is included."
  variant openloop 8 "$t" "$w" "250 ops/s" 1 3 3 "" -- --workload=A --concurrency=64 --rate=250
  variant openloop 8 "$t" "$w" "1000 ops/s" 2 3 3 "" -- --workload=A --concurrency=64 --rate=1000
fi
if begin keys; then
  t="Key distribution" w="Three nodes, RF 3, workload B. Zipfian keys (YCSB's, theta 0.99) send most operations to few keys, and so to few shards."
  variant keys 9 "$t" "$w" "uniform" 1 3 3 "" -- --workload=B --concurrency=32
  variant keys 9 "$t" "$w" "zipfian" 2 3 3 "" -- --workload=B --concurrency=32 --dist=zipfian
fi
if begin optimizations; then
  build_before
  t="Optimizations" w="Three nodes, RF 3, writes only: the Phase 9 build, in which a replica fsyncs its Raft log and then its engine's log for every write and a leader persists before replicating; then this build with the engine's log kept (storage.engine_wal), which isolates the leader replicating while it writes to its own disk (Raft thesis 10.2.1); then this build's default, the Raft log as a replica's only log (docs/adr/0010)."
  server_bin="$before_bin/dkv-server"
  variant optimizations 11 "$t" "$w" "phase-9: two logs, leader persists first" 1 3 3 "" -- --workload=W --concurrency=32
  server_bin="$bin/dkv-server"
  variant optimizations 11 "$t" "$w" "two logs, parallel leader write" 2 3 3 "--storage.engine-wal=true" -- --workload=W --concurrency=32
  variant optimizations 11 "$t" "$w" "single log, parallel leader write (default)" 3 3 3 "" -- --workload=W --concurrency=32
fi
if begin standalone; then
  build_before
  t="Standalone write path" w="One standalone node, writes only: the Phase 9 build, in which each write held the store's lock through its fsync, so writes from different clients never shared one; then this build, in which concurrent writes share the engine's group commit and its fsync (internal/kvstore)."
  server_bin="$before_bin/dkv-server"
  variant standalone 12 "$t" "$w" "phase-9: one fsync per write" 1 1 1 "" -- --workload=W --concurrency=32
  server_bin="$bin/dkv-server"
  variant standalone 12 "$t" "$w" "group commit (this build)" 2 1 1 "" -- --workload=W --concurrency=32
fi
if begin failover; then
  t="Leader failover" w="Three nodes, RF 3, workload A; the node leading shard 0 is SIGKILLed 10 s into the measured time. Election timeouts are 0.5-1 s (raft.tick_interval 50 ms, raft.election_ticks 10)."
  variant failover 10 "$t" "$w" "kill the leader of shard 0" 1 3 3 "" -- --workload=A --concurrency=16 --timeseries \
    --event-at=10s --event-cmd="sh $DATA/kill-leader.sh" --label event="SIGKILL of the node leading shard 0"
fi

"$bin/dkv-bench" report --results="$RESULTS" --out="$REPORT"
