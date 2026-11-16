# ADR 0021: Persistent node IDs and a static bootstrap peer list

Status: Accepted (Phase 4)

## Context

Phase 4 forms a cluster before there is any replicated metadata: the meta
Raft group arrives in Phase 7. Nodes must still know who their peers are,
tell each other apart, and notice when one dies, and project rules forbid
hardcoding topology.

## Decision

**Identity.** On its first start a node generates a random 64-bit ID
(16 hex digits) and stores it in `<storage.dir>/NODE_ID`, written to a
temporary file, fsynced, renamed, and the directory fsynced. The ID survives
restarts and address changes; a node whose data directory is wiped comes
back as a new node, which is what Phase 7's wiped-node rejoin expects. A
damaged ID file stops startup rather than silently minting a new identity.

**Membership.** Every node reads the same bootstrap peer list
(`cluster.peers`, internal addresses; the node's own address may be in it)
and a cluster name. Each node pings every peer every `cluster.ping_interval`
over the internal transport, with `cluster.ping_timeout` per ping. A peer is
UP while its last answer is at most `cluster.down_after` old, DOWN after
that, and UNKNOWN until it first answers or the first window passes. State is
computed from the last answer's timestamp whenever status is read, so the
window is exact rather than rounded to ping boundaries. A peer that reports
another cluster name, or that turns out to be this node under another
address, is never counted as a member. Status is served by
`AdminService.ClusterStatus` on the public port and printed by
`dkvctl status`.

## Alternatives considered

- **Configured node IDs.** Simple, but an operator who copies a config
  file between machines creates duplicate identities, and IDs must be
  managed by hand.
- **Address as identity.** Breaks when a node moves (DHCP, container
  restarts) and conflates "same address" with "same data".
- **Gossip membership (SWIM).** Scales to large clusters and spreads
  failure knowledge without all-to-all pings. dkv clusters are small (3 to
  tens of nodes), so all-to-all pings cost little, and Phase 7 replaces
  this with decisions replicated by the meta group (ADR 0013 in the plan's
  backlog). Gossip would add a protocol only to throw it away.
- **Phi-accrual detection.** Adapts to network jitter; a fixed window is
  easier to reason about and to test against ("down within the configured
  window").

## Consequences

- Every node pings every other node: O(N^2) pings per interval, fine at
  dkv's scale.
- Each node has its own view; two nodes may briefly disagree about a third.
  No decision is taken from this view in Phase 4. Phase 7 makes liveness
  decisions in the meta group so the cluster agrees.
- Detection time is bounded by `down_after` after the last answer; the
  in-process gate test measures about 0.99s for a 1s window.
