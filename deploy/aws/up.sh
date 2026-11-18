#!/usr/bin/env bash
# Creates the AWS benchmark cluster: builds linux/amd64 binaries, then
# terraform apply. Costs money until deploy/aws/down.sh. Needs terraform,
# AWS credentials in the environment, and terraform.tfvars (or -var flags)
# setting allowed_cidr and ssh_key_name; see variables.tf.
#
#   deploy/aws/up.sh [-var node_count=5 ...]
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
out="$root/bench/bin/linux-amd64"
mkdir -p "$out"
for b in dkv-server dkvctl dkv-bench; do
  (cd "$root" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$out/$b" "./cmd/$b")
done
terraform -chdir="$here" init -input=false
terraform -chdir="$here" apply -input=false -auto-approve -var "binaries_dir=$out" "$@"
terraform -chdir="$here" output
