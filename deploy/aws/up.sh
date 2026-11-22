#!/usr/bin/env bash
# Creates the AWS cluster: builds linux/amd64 binaries and the dashboard,
# runs terraform apply, then waits until every node takes a write and the
# console serves the dashboard on the load generator. It costs money (on a
# free plan account, credits) until deploy/aws/down.sh. Needs terraform,
# AWS credentials (aws configure), Node 22 for Linux on PATH, and
# terraform.tfvars (or -var flags) setting allowed_cidr and
# ssh_public_key_file; see variables.tf.
#
#   deploy/aws/up.sh [-var node_count=5 ...]
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
out="$root/bench/bin/linux-amd64"
if [ "$(node -p process.platform 2> /dev/null)" != linux ]; then
  echo "up.sh builds the dashboard (web/) and needs Node 22 for Linux on PATH" >&2
  exit 1
fi
mkdir -p "$out"
for b in dkv-server dkvctl dkv-bench dkv-console; do
  (cd "$root" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$out/$b" "./cmd/$b")
done
(cd "$root/web" && { [ -d node_modules ] || npm ci --no-audit --no-fund; } && npm run build)
tar -C "$root/web/dist" -czf "$out/web.tar.gz" .

tf() { terraform -chdir="$here" "$@"; }
tf init -input=false
tf apply -input=false -auto-approve -var "binaries_dir=$out" "$@"

# First boot installs the binaries from S3 and starts dkv-server on the
# nodes and dkv-console on the load generator; wait for both.
key=$(tf output -raw ssh_key_file)
lg=$(tf output -raw loadgen_public_ip)
addr=$(tf output -raw bench_addr)
port=$(tf output -raw console_port)
ssh_opts=(-i "$key" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$here/.known_hosts"
  -o ConnectTimeout=10 -o BatchMode=yes)
echo "waiting for the nodes and the console (first boot takes a few minutes)"
ready=""
for _ in $(seq 1 60); do
  rc=0
  # Exit 2: the load generator's first boot failed (cloud-init exits 1 on
  # an error, 2 when it only warned); 1: not ready yet.
  ssh "${ssh_opts[@]}" "ubuntu@$lg" "cloud-init status --wait > /dev/null 2>&1; [ \$? -ne 1 ] || exit 2
    for a in ${addr//,/ }; do dkvctl --addr=\$a --timeout=3s put aws-ready yes > /dev/null 2>&1 || exit 1; done
    curl -fsS -o /dev/null http://127.0.0.1:$port/" 2> /dev/null || rc=$?
  case $rc in
    0) ready=1 && break ;;
    2) echo "the load generator's first boot failed: see /var/log/cloud-init-output.log on it" >&2 && exit 1 ;;
  esac
  sleep 10
done
if [ -z "$ready" ]; then
  echo "the cluster did not take writes within 10 minutes; first boot on each node:" >&2
  for ip in $(tf output -json node_public_ips | tr -d '[]"' | tr , ' '); do
    echo "$ip: $(ssh "${ssh_opts[@]}" "ubuntu@$ip" 'cloud-init status; systemctl is-active dkv-server' 2>&1 | tr '\n' ' ')" >&2
  done
  exit 1
fi
tf output
echo
echo "Every node takes writes. Dashboard: run"
echo "  $(tf output -raw console_tunnel)"
echo "then open http://localhost:$port"
