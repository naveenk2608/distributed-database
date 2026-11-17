#!/usr/bin/env bash
# Phase 9 gate in Docker Compose (four nodes, dkv-console, PostgreSQL; see
# dashboard.yml). Needs Docker, curl, jq, and the dashboard's dependencies
# with Playwright's Chromium (cd web && npm ci && npx playwright install chromium).
#
#  1. In a browser: SIGKILL of the node leading shard 0 shows on the
#     dashboard within one poll interval, and its events page lists the
#     election, the dead verdict and the re-replication in that order
#     (web/e2e/dashboard.spec.ts).
#  2. With the console stopped, every write still succeeds.
#  3. Restarted, the console shows the history it saved in PostgreSQL
#     before it stopped.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
files=(-f "$root/deploy/compose/docker-compose.yml" -f "$root/deploy/compose/dashboard.yml")
compose() { docker compose "${files[@]}" "$@"; }
console=http://localhost:8080
cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    compose logs --no-color > "$root/dashboard-gate.log" 2>&1 || true
    echo "FAIL; container logs in dashboard-gate.log"
  fi
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

wait_for() { # seconds description command...
  local deadline=$((SECONDS + $1)) what=$2
  shift 2
  until "$@" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "FAIL: timed out waiting for $what"; return 1; }
    sleep 1
  done
}

compose build
compose up -d n1 n2 n3 postgres
wait_for 90 "the cluster to form" compose exec -T n1 dkvctl --addr=localhost:7070 put formed yes
compose up -d n4 console
wait_for 90 "the console" curl -fsS "$console/api/health"

echo "== 1. the dashboard, in Chromium"
(cd web && DKV_DASHBOARD_URL=$console DKV_COMPOSE="docker compose ${files[*]}" npx playwright test)

echo "== 2. writes with the console stopped"
compose stop console
node=$(compose ps --status running --services | grep '^n' | head -1)
for i in $(seq 1 50); do
  compose exec -T "$node" dkvctl --addr=localhost:7070 put "gate-$i" "value-$i" >/dev/null ||
    { echo "FAIL: write $i through $node failed with the console stopped"; exit 1; }
done
echo "50 writes through $node succeeded with the console stopped"

echo "== 3. the restarted console reloads its history from PostgreSQL"
restart=$(date -u +%Y-%m-%dT%H:%M:%S)
compose start console
wait_for 60 "the restarted console" curl -fsS "$console/api/health"
earlier=$(curl -fsS "$console/api/history?addr=$node:7200" | jq --arg t "$restart" '[.[] | select(.time < $t)] | length')
store=$(curl -fsS "$console/api/health" | jq -r .store)
if [ "$store" != postgres ] || [ "$earlier" -lt 1 ]; then
  echo "FAIL: store $store, $earlier samples from before the restart"; exit 1
fi
echo "the restarted console shows $earlier samples of $node saved before it stopped"
echo "PASS"
