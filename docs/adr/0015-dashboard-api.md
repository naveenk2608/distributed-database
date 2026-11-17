# ADR 0015: The dashboard API is REST and server-sent events from dkv-console

Status: Accepted (Phase 9)

## Context

The React dashboard shows the cluster's health, shards, nodes and events,
and must show a node that dies within one poll interval. The browser
cannot speak gRPC directly, and should not have to know the cluster's
topology or poll every node itself.

## Decision

- **Nodes serve a read-only admin HTTP API** (`server.admin_addr`): their
  status, shard map, statistics and events as the JSON form of the admin
  protobuf messages, and Prometheus metrics at `/metrics`. Changes (drain,
  transfer, decommission) are gRPC-only, through `dkvctl`.
- **The console polls every node** each `console.poll_interval` (default
  1 s), concurrently, each poll bounded by `console.poll_timeout`. A node
  that does not answer is shown down at once. Rates and latency
  percentiles are derived from the difference of two snapshots of a node's
  own counters and cumulative histogram buckets; the cluster's percentiles
  come from the sum of the nodes' bucket differences. The console shows no
  number it did not get from, or derive from, a node.
- **The console serves REST** (`/api/overview`, `/api/history`,
  `/api/events`, `/api/health`) **and one server-sent event stream**
  (`/api/stream`): an `overview` event after every poll round. The
  dashboard loads with REST and then keeps its TanStack Query cache current
  from the stream; `EventSource` reconnects by itself.
- **The console serves the built dashboard** from `console.web_dir`, with
  unknown paths answered by `index.html` so the dashboard's routes load.

## Alternatives considered

- **gRPC-Web with Envoy**: typed end to end, but another proxy to run and
  configure, and the browser still needs a streaming story.
- **Connect RPC**: gRPC-compatible and browser-friendly, but another code
  generator and runtime on both sides for a read-only API of four
  endpoints.
- **WebSockets**: bidirectional, which nothing here needs; server-sent
  events are plain HTTP, reconnect on their own and pass through proxies.
- **The browser polls the nodes**: every viewer would poll every node,
  needs the topology and cross-origin access, and could not keep history.

## Consequences

- One poller per console however many people watch; the stream fans each
  round out. A viewer that falls behind skips a round rather than slowing
  the others.
- A dead node shows within one poll interval plus the poll timeout:
  `test/dashboard` measured about 220 ms with a 250 ms interval in
  process, and the Compose gate checks it in Chromium.
- Elections, failures and re-replication are only as fresh as the poll;
  their timestamps are the nodes' own, so their order is the nodes' clocks'
  (one host in Compose; across machines, NTP).
- The admin HTTP API has no authentication; it is read-only and belongs on
  an operator network, like the internal port.
