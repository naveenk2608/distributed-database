# ADR 0014: PostgreSQL holds dashboard history only, outside the data path

Status: Accepted (Phase 9)

## Context

The dashboard shows live state and how it changed: throughput and latency
over time, storage growth, and a log of elections, failures and
membership changes. Nodes keep only their current counters and a bounded
ring of recent events (`internal/obs`, 4,096 per node), which a restart
empties. The plan allows PostgreSQL in `cmd/dkv-console` only, and a
`depguard` rule enforces that.

## Decision

- **Only `dkv-console` talks to PostgreSQL**, and only to keep its own
  history: one sample per node per poll round (`dkv_console_samples`) and
  every event it has seen (`dkv_console_events`, one row per event: a
  membership change that every member reports is one row). Rows are JSON
  documents with the columns the console filters on; nothing else reads
  them. The console creates its tables on first use and prunes rows older
  than `console.retention` (default 24 h) every hour.
- **The cluster never depends on it.** Nodes do not know the console
  exists: the console pulls from their read-only admin HTTP API. The
  console does not depend on PostgreSQL either: without
  `console.postgres_dsn` it keeps a bounded history in memory
  (`console.memory_samples` per node); with it, a database that is down or
  fails a write is reported (`/api/health`) and the console keeps polling.
- **What PostgreSQL buys** is history across console restarts: a restarted
  console reloads the samples and events of its memory window from it.

## Alternatives considered

- **PostgreSQL as the cluster's metadata store** (rejected in ADR 0011):
  it would put a single-node database in the data path, and its failure
  would stop rebalancing or worse.
- **No database**: memory only. Simpler, and supported, but a console
  restart loses the history, and the brief asks for PostgreSQL.
- **A time-series database or a Prometheus server**: better for long
  metric histories, but another component to run; nodes do expose
  `/metrics`, so an operator can add Prometheus beside the console.
- **Nodes write their own history to PostgreSQL**: every node would need
  the database, and a slow database could slow them.

## Consequences

- `test/dashboard` stops the console under a write load (no write fails)
  and the Compose gate stops it, writes 50 keys, and checks that the
  restarted console reloaded its history from PostgreSQL.
- History is per console: two consoles on two databases see the same
  cluster but keep separate histories.
- Events a node dropped from its ring before the console read them are
  lost; the console logs when that happens (an event log's `oldest` moved
  past what it had read).
