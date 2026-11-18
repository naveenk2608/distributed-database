#!/usr/bin/env bash
# The five-node Compose profile (five.yml): the three nodes of
# docker-compose.yml form the cluster, n4 and n5 join it, every node sees
# five nodes alive in the meta group, the rebalancer gives both joiners
# replicas, and a write through a joiner is read back through a founder.
#
# Liveness is the meta group's NODE STATE column: the ping failure detector
# (STATE) watches only the configured bootstrap peers, so it shows joined
# nodes as UNKNOWN (docs/PROGRESS.md, Phase 7, known limitations).
set -euo pipefail
cd "$(dirname "$0")"

compose() { docker compose -f docker-compose.yml -f five.yml "$@"; }
trap 'compose down -v --remove-orphans >/dev/null 2>&1 || true' EXIT
ctl() { # node dkvctl-args...: dkvctl inside that node's container
  local n=$1
  shift
  compose exec -T "$n" dkvctl --addr=localhost:7070 --timeout=2s "$@"
}
nodes_alive() { ctl "$1" status 2>/dev/null | awk 'NR > 1 && $4 == "alive"' | wc -l; }
local_replicas() { ctl "$1" shards 2>/dev/null | awk '$3 == "yes"' | wc -l; }
fail() {
  echo "FAIL: $*"
  compose logs --tail=200 || true
  exit 1
}

compose up -d --build
deadline=$((SECONDS + 180))
for n in n1 n2 n3 n4 n5; do
  until [ "$(nodes_alive "$n")" -eq 5 ]; do
    [ "$SECONDS" -lt "$deadline" ] || { ctl "$n" status || true; fail "$n does not see five nodes alive"; }
    sleep 1
  done
done
echo "every node sees five nodes alive"
ctl n1 status

for n in n4 n5; do
  until [ "$(local_replicas "$n")" -gt 0 ]; do
    [ "$SECONDS" -lt "$deadline" ] || { ctl "$n" shards || true; fail "$n was given no replica"; }
    sleep 1
  done
  echo "$n holds $(local_replicas "$n") replicas"
done

until ctl n5 put five-check hello > /dev/null; do
  [ "$SECONDS" -lt "$deadline" ] || fail "no write through n5"
  sleep 1
done
got=$(ctl n1 get five-check)
[ "$got" = hello ] || fail "n1 read \"$got\" for the key written through n5"
echo "a write through n5 read back through n1"
echo "PASS"
