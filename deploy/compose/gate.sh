#!/usr/bin/env bash
# Phase 4 gate in Docker Compose: three nodes form a cluster from config,
# and after SIGKILL of one node the others report it down within the
# configured window (DKV_CLUSTER_DOWN_AFTER=3s in docker-compose.yml).
set -euo pipefail
cd "$(dirname "$0")"

window=3   # seconds, must match DKV_CLUSTER_DOWN_AFTER
slack=2    # seconds of polling and docker exec latency
compose() { docker compose -f docker-compose.yml "$@"; }
trap 'compose down -v --remove-orphans >/dev/null 2>&1 || true' EXIT

state_of() { # observer target -> state column for target as seen by observer
  compose exec -T "$1" dkvctl --addr=localhost:7070 --timeout=2s status 2>/dev/null |
    awk -v t="$2:7100" '$1 == t { print $3 }'
}

compose up -d --build
echo "waiting for the cluster to form"
deadline=$((SECONDS + 90))
for obs in n1 n2 n3; do
  for target in n1 n2 n3; do
    until [ "$(state_of "$obs" "$target")" = "UP" ]; do
      [ "$SECONDS" -lt "$deadline" ] || { echo "FAIL: $obs never saw $target up"; compose logs; exit 1; }
      sleep 0.5
    done
  done
done
compose exec -T n1 dkvctl --addr=localhost:7070 status

echo "killing n3 with SIGKILL"
compose kill -s SIGKILL n3
start=$(date +%s.%N)
until [ "$(state_of n1 n3)" = "DOWN" ] && [ "$(state_of n2 n3)" = "DOWN" ]; do
  elapsed=$(echo "$(date +%s.%N) - $start" | bc)
  if (( $(echo "$elapsed > $window + $slack + 10" | bc) )); then
    echo "FAIL: n3 still not reported down after ${elapsed}s"; exit 1
  fi
  sleep 0.2
done
elapsed=$(echo "$(date +%s.%N) - $start" | bc)
compose exec -T n1 dkvctl --addr=localhost:7070 status
echo "n3 reported down ${elapsed}s after SIGKILL (window ${window}s)"
if (( $(echo "$elapsed > $window + $slack" | bc) )); then
  echo "FAIL: detection slower than the window plus ${slack}s"; exit 1
fi
[ "$(state_of n1 n2)" = "UP" ] || { echo "FAIL: survivor n2 not up"; exit 1; }
echo "PASS"
