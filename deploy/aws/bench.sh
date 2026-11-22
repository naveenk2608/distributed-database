#!/usr/bin/env bash
# The AWS benchmarks: runs dkv-bench on the load generator of the cluster
# deploy/aws/up.sh made, copies each result to bench/results/aws/, records
# the deployment in bench/results/aws/meta/, and regenerates
# docs/benchmarks.md, which renders those results as its AWS part.
#
# Experiments, with the local matrix's settings (bench/run-local.sh):
#   workloads  YCSB A, B, C and W, 32 workers
#   failover   workload A, 16 workers; 10 s into the measured time the node
#              leading shard 0 is SIGKILLed (over SSH from the load
#              generator) and stays down until the run ends
#
# Environment (defaults in parentheses): RUNS (3) runs per variant; WARMUP
# (10s) and DURATION (20s) per run; RECORDS (20000) records preloaded;
# ONLY: run only the experiments whose name contains it. The results of
# the experiments run are replaced; the others are kept.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
RUNS=${RUNS:-3}
WARMUP=${WARMUP:-10s}
DURATION=${DURATION:-20s}
RECORDS=${RECORDS:-20000}
ONLY=${ONLY:-}
results="$root/bench/results/aws"

tf() { terraform -chdir="$here" "$@"; }
key=$(tf output -raw ssh_key_file)
lg=$(tf output -raw loadgen_public_ip)
addr=$(tf output -raw bench_addr)
admin=$(tf output -raw bench_admin)
environment=$(tf output -raw environment)
region=$(tf output -raw region)
read -r -a private <<< "$(tf output -json node_private_ips | tr -d '[]"' | tr , ' ')"
read -r -a public <<< "$(tf output -json node_public_ips | tr -d '[]"' | tr , ' ')"
ssh_opts=(-i "$key" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$here/.known_hosts"
  -o ConnectTimeout=10 -o BatchMode=yes -o ServerAliveInterval=15)
# The load generator SSHes to the node a failover run kills with this key,
# through a forwarded agent: the key never leaves this machine.
eval "$(ssh-agent -s)" > /dev/null
trap 'ssh-agent -k > /dev/null' EXIT
ssh-add -q "$key"
on_lg() { ssh "${ssh_opts[@]}" -A "ubuntu@$lg" "$@"; }
on_node() { # INDEX COMMAND
  local i=$1
  shift
  ssh "${ssh_opts[@]}" "ubuntu@${public[$i]}" "$@"
}

mkdir -p "$results/meta"
on_lg 'mkdir -p results .ssh && chmod 700 .ssh && cat > .ssh/config' << EOF
# Kept-open connections, so a kill reaches its node within milliseconds.
Host ${private[*]}
  User ubuntu
  StrictHostKeyChecking accept-new
  BatchMode yes
  ControlMaster auto
  ControlPath ~/.ssh/cm-%h
  ControlPersist 2h
EOF
# ready.sh: every node takes a write, and the data shards' leaderships are
# as balanced as the rebalancer makes them, as in a fresh cluster (after a
# failover run they take a while to spread out again). It balances the
# data shards and the meta group together, within one of each other, and
# dkvctl leaders lists only the data shards: within two of each other.
on_lg 'cat > ready.sh' << EOF
set -e
for a in ${addr//,/ }; do dkvctl --addr=\$a --timeout=3s put bench-ready yes > /dev/null; done
dkvctl --addr=$addr --timeout=3s leaders | awk -v nodes=${#private[@]} '
  NR > 1 && \$2 == "unknown" { bad = 1 }
  NR > 1 && \$1 >= 0 { led[\$2]++ }
  END { n = 0; lo = 1e9; hi = 0
    for (l in led) { n++; if (led[l] < lo) lo = led[l]; if (led[l] > hi) hi = led[l] }
    exit !(!bad && n == nodes && hi - lo <= 2) }'
EOF
# kill-leader.sh: SIGKILLs the node leading shard 0.
{
  echo "a=\$(dkv-bench leader --admin=$admin --shard=0)"
  for ip in "${private[@]}"; do
    echo "[ \"\$a\" = $ip:7200 ] && ssh $ip sudo systemctl kill --signal=SIGKILL dkv-server && echo killed $ip"
  done
  echo true
} | on_lg 'cat > kill-leader.sh'

wait_ready() {
  local t
  for t in $(seq 1 90); do
    on_lg 'sh ready.sh' 2> /dev/null && return 0
    [ "$t" -lt 90 ] && sleep 2
  done
  echo "after 3 minutes some node takes no writes or leads no shard" >&2
  return 1
}

# stay_down on|off: whether a SIGKILLed dkv-server stays down (a runtime
# drop-in, gone at reboot) or systemd restarts it (Restart=on-failure).
stay_down() {
  local i d=/run/systemd/system/dkv-server.service.d
  for i in "${!public[@]}"; do
    if [ "$1" = on ]; then
      on_node "$i" "sudo mkdir -p $d && printf '[Service]\nRestart=no\n' | sudo tee $d/stay-down.conf > /dev/null && sudo systemctl daemon-reload"
    else
      on_node "$i" "sudo rm -f $d/stay-down.conf && sudo systemctl daemon-reload"
    fi
  done
}

start_nodes() { # systemctl start does nothing to a node that is running
  local i
  for i in "${!public[@]}"; do on_node "$i" 'sudo systemctl start dkv-server'; done
}

# variant EXPERIMENT ORDER TITLE WHAT NAME NAME_ORDER [-- BENCH_FLAGS...]
variant() {
  local exp=$1 order=$2 title=$3 what=$4 name=$5 vorder=$6 run file cmd
  shift 6
  [ "${1:-}" = -- ] && shift
  for run in $(seq 1 "$RUNS"); do
    wait_ready
    file="$exp--$(echo "$name" | tr ' /' '__')--$run.json"
    echo "== $exp / $name, run $run of $RUNS"
    # %q quotes every argument for the load generator's shell.
    printf -v cmd '%q ' dkv-bench run --addr="$addr" --admin="$admin" --records="$RECORDS" --warmup="$WARMUP" \
      --duration="$DURATION" --seed="$run" --out="results/$file" \
      --label part=AWS --label experiment="$exp" --label order="$order" --label title="$title" --label what="$what" \
      --label variant="$name" --label variant_order="$vorder" --label nodes="${#private[@]}" --label rf=3 \
      --label environment="$environment" --label region="$region" "$@"
    on_lg "$cmd"
    scp -q "${ssh_opts[@]}" "ubuntu@$lg:results/$file" "$results/$file"
    start_nodes # a node a run killed
  done
}

wanted() { [ -z "$ONLY" ] || [[ "$1" == *"$ONLY"* ]]; }
begin() { # EXPERIMENT: replace its earlier results
  wanted "$1" || return 1
  rm -f "$results/$1--"*.json
}

if begin workloads; then
  t="Workloads" w="Three nodes in three zones, RF 3: YCSB workloads A (50% reads, 50% updates), B (95% reads), C (reads only) and W (writes only), with the local matrix's 32 workers."
  variant workloads 1 "$t" "$w" A 1 -- --workload=A --concurrency=32
  variant workloads 1 "$t" "$w" B 2 -- --workload=B --concurrency=32
  variant workloads 1 "$t" "$w" C 3 -- --workload=C --concurrency=32
  variant workloads 1 "$t" "$w" W 4 -- --workload=W --concurrency=32
fi

if begin failover; then
  t="Leader failover" w="Three nodes in three zones, RF 3, workload A; the node leading shard 0 is SIGKILLed 10 s into the measured time, through an SSH connection the load generator keeps open to it, and stays down until the run ends. Election timeouts are 0.5-1 s (raft.tick_interval 50 ms, raft.election_ticks 10)."
  stay_down on
  on_lg "for ip in ${private[*]}; do ssh \$ip true; done" # open the kept connections
  variant failover 2 "$t" "$w" "kill the leader of shard 0" 1 -- --workload=A --concurrency=16 --timeseries \
    --event-at=10s --event-cmd="sh /home/ubuntu/kill-leader.sh" --label event="SIGKILL of the node leading shard 0"
  stay_down off
fi

# The deployment: Terraform's description, then each node's own view
# (kernel, CPU, memory, the data disk, its dkv-server binary against the
# one built here) and the round trip to it from the load generator.
tf output -json deployment > "$results/meta/deployment.json"
{
  echo "# Collected by deploy/aws/bench.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "local build: $(sha256sum "$root/bench/bin/linux-amd64/dkv-server" | cut -d' ' -f1)  dkv-server"
  for i in "${!public[@]}"; do
    echo
    echo "## node $((i + 1)) (${private[$i]})"
    on_node "$i" 'uname -sr; lscpu | grep -E "^(Model name|CPU\(s\)):" | tr -s " "; free -m | awk "/^Mem:/ { print \"memory: \" \$2 \" MiB\" }"
      findmnt -no SOURCE,FSTYPE,SIZE /; sha256sum /usr/local/bin/dkv-server | cut -d" " -f1 | sed "s/$/  dkv-server/"'
    on_lg "ping -c 20 -q ${private[$i]} | tail -1 | sed 's/^/from the load generator: /'"
  done
  echo
  echo "## load generator"
  on_lg 'uname -sr; lscpu | grep -E "^(Model name|CPU\(s\)):" | tr -s " "; free -m | awk "/^Mem:/ { print \"memory: \" \$2 \" MiB\" }"'
} > "$results/meta/nodes.txt"

(cd "$root" && go run ./cmd/dkv-bench report --results=bench/results --out=docs/benchmarks.md)
