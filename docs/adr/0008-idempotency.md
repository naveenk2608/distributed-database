# ADR 0008: Idempotent retries through client sessions

Status: Accepted (Phase 6)

## Context

A client that times out does not know whether its write happened: the
leader may have committed it and then lost the response, or died first.
Retrying must not apply a write twice. A put applied twice gets a second
version, a compare-and-set retried after it succeeded reports failure, and
a retried delete reports "not found".

## Decision

- **Sessions are replicated state.** Every write carries `client_id` and
  `request_id`. Applying an entry looks up the session record for
  `client_id` in the replica's engine:
  - the same `request_id` as recorded: return the stored result and change
    nothing;
  - an older `request_id`: refuse with `FAILED_PRECONDITION`;
  - otherwise apply, and store the new `request_id` and result in the same
    batch as the write.
  Because this happens at apply time from the log and the engine only,
  every replica reaches the same answer, and a retry that reaches a new
  leader after a failover is still recognized.
- **One write in flight per session.** The record keeps only the latest
  request, so the client library holds a pool of sessions with random
  128-bit IDs and gives each concurrent write its own; a write reuses its
  session's request ID on every retry and returns the session when it
  finishes.
- **Expiry uses leader-stamped time.** The leader stamps each command with
  its clock when proposing; a session whose last write is older than
  `raft.session_ttl` (default 1h) by the stamp of the new command is
  treated as absent. Expiry is therefore identical on every replica.
- `client_id` empty means no deduplication (tools and tests).
- The client retries only `UNAVAILABLE` (node unreachable, no leader,
  leadership changed with the write in flight), with jittered exponential
  backoff, moving through the configured nodes.

## Alternatives considered

- **A separate idempotency-key store** (key → result with a TTL). Needs
  its own replication or loses the guarantee on failover; in the replicated
  state machine it is the same thing with worse locality.
- **A window of request IDs per session** (RIFL-style, with the client
  reporting its lowest unacknowledged ID). Allows many writes per session
  in flight but needs more state per session; a pool of sessions gets the
  same concurrency with the simplest record.
- **No deduplication.** Puts are idempotent in value but not in version,
  and CAS and delete results become wrong after a retry.

## Consequences

- A retried write is applied exactly once as long as the retry arrives
  within the session TTL. The integration gate drops the first response of
  an applied put and checks the retry returns the original version; it also
  retries a put after killing the leader.
- Session records are never deleted physically in Phase 6; an expired one
  is overwritten on the session's next write. Snapshots (Phase 7) carry
  them with the rest of the state; physical cleanup of expired sessions is
  left for when snapshots compact the state.
- A standalone node does not deduplicate; retries there can apply twice.
- Session expiry depends on leaders' clocks being roughly right; a leader
  with a clock far ahead can expire sessions early, which only weakens
  deduplication for retries, never safety of the log.
