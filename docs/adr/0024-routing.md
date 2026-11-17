# ADR 0024: Routing with one-hop forwarding and NotOwner hints

Status: Accepted (Phase 5); revised in Phases 6 and 7

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

## Revision in Phase 6

With Raft, a shard is served by its current leader, which changes.

- The router caches the last known leader per shard. It sends a request to
  the cached leader, else to its own replica if the node holds one, else to
  the first replica in placement order.
- A replica that is not the leader answers `NotOwner` with the leader it
  knows of (its Raft leader ID, which is the leader's address). The router
  follows a fresh hint at once (up to twice per request), and moves to the
  next replica in the set when the answer has no hint or the target is
  unreachable, since an election may be running.
- Stale reads (ADR 0007) are answered by the node's own replica when it has
  one, and never update the leader cache.
- The attempt limit is 12 so a request can ride out an election; the same
  `client_id` and `request_id` travel with every attempt, so a forwarded
  write that was applied before its response was lost is answered from the
  session table (ADR 0008).
- `internal/kvstore` is now only the standalone backend; the router talks
  to `internal/replica`.

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

- Phase 5 only: a retried write was not idempotent, and a shard whose
  primary was down failed until it returned. Phase 6's sessions and Raft
  failover remove both: with a minority of a shard's replicas down every
  key keeps working once a new leader is elected, and with a majority down
  requests fail within the client's deadline.

## Revision in Phase 7

- Groups' members are node IDs, resolved to addresses through the
  directory (ADR 0011); a group's voters for routing come from its
  descriptor, refreshed every rebalance interval.
- The meta group is routed like a shard (number −1) for internal
  transactions and scans, so any node can read or change metadata through
  whichever node leads the meta group.
- Retries continue until the caller's deadline; the attempt limit (12)
  applies only to callers without one. A fixed limit made requests fail
  after about 3.5 s of leadership churn on a heavily loaded machine even
  when the caller allowed longer.
