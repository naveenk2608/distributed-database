# ADR 0024: Routing with one-hop forwarding and NotOwner hints

Status: Accepted (Phase 5)

## Context

Clients may send any request to any node. The plan routes each request to
its shard's leader with at most one forwarding hop, retries with jittered
backoff inside the client's deadline, and uses ownership hints to correct
stale routing. Phase 5 has no replication: a shard's primary serves it.

## Decision

- The receiving node computes the shard. If it is the primary it serves the
  request from its local engine; otherwise it forwards it with
  `InternalService.Forward` to the primary.
- A node that receives a forwarded request for a shard it does not own
  never forwards it again: it answers `NotOwner` with the owner it knows of.
  The forwarding node then retries at the hinted owner.
- `Unavailable` (owner unreachable) and `NotOwner` are retried with
  exponential backoff (20 ms doubling to 500 ms) with jitter in
  [½, 1½) of the current step, until the client's deadline, and at most
  10 attempts so a request without a deadline still ends. Other errors from
  the owner are returned as they are.
- The router lives in `internal/router` and is the API server's backend in a
  cluster; per-engine operations live in `internal/kvstore`, used by the
  standalone backend and the router alike, so imports point downward
  (server → router → kvstore → lsm).

## Alternatives considered

- **Multi-hop forwarding.** A node with a stale view could bounce requests
  around; one hop with hints bounds the work per attempt and puts the retry
  decision in one place.
- **Client-side routing only.** Requires every client to know placement and
  to follow membership changes; the plan keeps "any node accepts any key"
  and makes leader-aware routing in `pkg/client` an optimization for later.
- **Proxying through a load balancer.** Adds a hop and a component outside
  dkv's control.

## Consequences

- A retried write is not idempotent in Phase 5: if a forward fails after the
  owner applied it (a connection that breaks mid-call), the retry applies it
  again with a new version. Phase 6 adds client sessions (ADR 0008) that make
  retries exactly-once.
- While a shard's primary is down, its keys fail with `Unavailable` or
  `DeadlineExceeded` until the primary returns; other shards are unaffected.
  Phase 6's Raft groups fail over to another replica.
