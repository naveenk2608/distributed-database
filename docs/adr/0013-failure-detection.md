# ADR 0013: Failure detection by heartbeats to the meta leader

Status: Accepted (Phase 7)

## Context

Replicas of a node that is gone for good must be recreated elsewhere; a
node that is merely slow, restarting or briefly partitioned must not
cause a burst of moves. Different nodes' views of a peer can disagree
(the Phase 4 ping membership is per node).

## Decision

- Every node sends a heartbeat to the meta leader every
  `liveness.heartbeat_interval` (default 1 s), following NotOwner hints
  to find it.
- **Liveness is soft state on the meta leader**: it keeps the last
  heartbeat time per node in memory. A node silent for
  `liveness.suspect_after` (3 s) is reported suspect in status; one silent
  for `liveness.dead_after` (30 s) is declared dead by a conditional write
  to its node record. A dead node that heartbeats again is written back to
  alive. Only these decisions go to the meta log.
- **A new meta leader waits a full `dead_after`** from when it took over
  before declaring anyone dead, so a leader change cannot cause false
  failures.
- Dead and decommissioning nodes are left out of the wanted placement
  (ADR 0012); a dead node's voters are removed first.

## Alternatives considered

- **SWIM-style gossip.** Scales to many nodes and needs no leader, but
  gives each node its own view; dkv needs one decision, which the meta
  leader is already positioned to make.
- **A phi-accrual detector.** Adapts to latency jitter; worth it with
  real network variance. A fixed timeout is easy to reason about and to
  test.
- **Writing every heartbeat to the meta log.** A log entry per node per
  second for information that matters only to the current leader.

## Consequences

- Detection takes `dead_after` after the last heartbeat, plus up to one
  rebalance interval; the default 30 s favours avoiding needless moves.
  The Phase 7 tests use 1–1.5 s and see every group back to RF live voters
  about 0.7–1.6 s after the dead timeout.
- Without a meta quorum no node is declared dead and nothing moves; data
  operations are unaffected.
- The ping-based membership of Phase 4 remains for status display.
