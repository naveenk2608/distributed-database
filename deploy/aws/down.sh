#!/usr/bin/env bash
# Destroys everything deploy/aws/up.sh created, then lists what is left in
# the region (check-empty.sh) and fails if anything tagged project=dkv is.
#
#   deploy/aws/down.sh [-var region=... ...]   (the same -var flags as up.sh)
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
out="$root/bench/bin/linux-amd64"
tf() { terraform -chdir="$here" "$@"; }
tf init -input=false > /dev/null
region=$(echo var.region | tf console "$@" | tr -d '"')
name=$(echo var.name | tf console "$@" | tr -d '"')
# Terraform reads the artifacts' checksums even to destroy them: if the
# builds were cleaned away since up.sh, empty stand-ins do.
mkdir -p "$out"
for f in dkv-server dkvctl dkv-bench dkv-console web.tar.gz; do
  [ -e "$out/$f" ] || : > "$out/$f"
done
tf destroy -input=false -auto-approve -var "binaries_dir=$out" "$@"
"$here/check-empty.sh" "$region" "$name"
