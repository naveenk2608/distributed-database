#!/usr/bin/env bash
# Destroys everything deploy/aws/up.sh created.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
terraform -chdir="$here" destroy -input=false -auto-approve -var "binaries_dir=$root/bench/bin/linux-amd64" "$@"
