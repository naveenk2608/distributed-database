# ADR 0022: Internal transport

Status: Accepted (Phase 4)

## Context

Nodes talk to each other for health pings now, and for Raft messages,
snapshot streams and request forwarding later. The plan names gRPC with
connection pooling, deadlines and keepalives.

## Decision

- **A separate internal port** (`cluster.internal_addr`) serves
  `InternalService`; the public port serves `KVService` and `AdminService`.
  Client traffic and peer traffic can then be firewalled, rate-limited and
  sized differently (internal messages may be up to 64 MiB for snapshot
  chunks; public ones follow the key and value limits).
- **One gRPC connection per peer**, created lazily and shared by every
  caller (`transport.Pool`). HTTP/2 multiplexes concurrent calls over it,
  so a second connection would add nothing until a single connection's
  throughput becomes a bottleneck, which Phase 10 can measure.
- **Fail fast while a peer is down.** Calls do not wait for a connection
  (no `WaitForReady`), so they return `Unavailable` at once; the connection
  reconnects in the background with jittered exponential backoff capped at
  1s. Callers retry at their own pace (membership pings, Raft ticks).
- **Deadlines on every call** come from the caller's context; there is no
  transport-wide default, because a ping, a Raft append and a snapshot
  chunk need very different bounds.
- **Keepalives** (`transport.keepalive_time`, `transport.keepalive_timeout`)
  detect a peer that vanished without closing its sockets. The server's
  enforcement policy accepts pings at that rate even on idle connections.
- No TLS or authentication: out of scope for v1 (ADR 0000).

## Alternatives considered

- **One port for everything.** Fewer ports to configure, but no way to
  separate client and peer traffic, and one set of message-size limits.
- **A connection per call or a pool of N connections per peer.** More
  sockets and handshakes for no gain at dkv's message rates.
- **Raw TCP with a custom framing protocol.** Less overhead per message,
  but reimplements deadlines, multiplexing, flow control and keepalives
  that gRPC already provides; the plan chooses gRPC.

## Consequences

- Transport tests cover a peer that never answers (deadline exceeded), a
  socket that accepts but never speaks HTTP/2, a closed port (immediate
  `Unavailable`), and a peer that restarts on the same address (the same
  pooled connection recovers, in about 70ms in the test).
- gRPC raises client keepalive intervals below 10s to 10s, so a peer that
  disappears silently with no calls in flight is noticed by keepalive only
  after ~13s; membership pings notice it within `cluster.down_after`.
- Phase 6 adds `InternalService.Raft`: each node batches the Raft messages
  of all its replicas per destination node into one unary call (ADR 0003).
