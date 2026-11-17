# ADR 0023: Placement with a consistent hash ring keyed by member address

Status: Accepted (Phase 5); revised in Phase 7

## Context

Keys are hashed into a fixed number of shards S (ADR 0002). Each shard needs
a replica set of RF nodes that every node can compute on its own, without a
metadata service (which arrives in Phase 7), and adding a node should move as
little as possible. The Phase 5 gate asks for a balance test with a stated
tolerance and for adding a node to move close to 1/(N+1) of assignments and
no more.

## Decision

- `shard = xxhash64(key) mod S`.
- A consistent hash ring with 128 virtual nodes per member (point
  `xxhash64(member + "#" + i)`); each shard has a point of its own
  (`xxhash64("shard/" + s)`); its replica set is the first RF distinct members
  clockwise, and the first is the primary. Hash ties are broken by member so
  the result never depends on input order.
- In Phase 5 the members are the configured internal addresses
  (`cluster.peers`), which every node must list identically and which must
  include each node's own advertise address. Phase 7 switches the members to
  node IDs from the meta group.
- Nodes exchange a placement fingerprint (algorithm version, S, RF, hash of
  the member list) in pings, so a misconfigured node is visible in
  `dkvctl status` instead of silently routing keys elsewhere.

**Stated balance tolerance.** A node holds a replica of each shard with
probability p = RF/N, so its replica count behaves like Binomial(S, p). The
stated tolerance for the worst node's deviation from the mean is three
standard deviations plus 5% for the ring's own unevenness:
`3·sqrt((1−p)/(S·p)) + 0.05`, met by at least 95% of random member sets. For
the default S = 64, RF = 3 that is ±27% at N = 4, ±36% at N = 5, ±48% at
N = 7, ±62% at N = 10; measured p95 values are 21%, 28%, 39% and 56%.

## Alternatives considered

- **Consistent hashing with bounded loads** (capacity (1+ε)·mean). Measured
  with S = 64: it caps the most loaded node but not the least loaded one, so
  the worst-node deviation barely improves (p95 24% vs 30% at N = 5, ε = 0.1),
  while cascades raise movement above optimal (median 1.15×, p95 1.35× at
  9 → 10 nodes). The gate asks for movement close to optimal "and no more".
- **Rendezvous hashing.** Same movement and balance properties; the plan
  names a ring with virtual nodes.
- **A larger default S.** Balance improves as 1/sqrt(S) (S = 256 halves the
  deviation) but every shard replica is an engine with its own WAL and
  memtables; 64 keeps a 3-node cluster at 64 engines per node. Operators
  expecting more nodes should bootstrap with more shards.
- **Node IDs as members now.** IDs are only learned by pinging, so a node
  that is down at startup would leave placement undefined; addresses from
  configuration are known up front.

## Consequences

- Adding a member moves exactly the replica slots it takes over: every
  changed replica set gains only the new member and loses one old member,
  and primaries only ever move to the new member (checked on every trial).
  On average 25.1%, 20.0%, 16.6%, 12.4%, 10.0% of assignments move for
  N = 3, 4, 5, 7, 9 → N+1, against optimal 25.0%, 20.0%, 16.7%, 12.5%, 10.0%.
- Primary (write) balance is worse than replica balance: with S = 64 the
  busiest node can serve roughly twice its share of primaries. Phase 7's
  leader balancing addresses this.
- Changing the peer list changes placement; Phase 5 does not move data, so a
  node no longer holding a shard keeps its directory and logs a warning.

## Revision in Phase 7

- The ring is keyed by **node ID**, not address, and covers the
  **eligible** nodes: those the meta group records as alive and not being
  decommissioned. Dead, decommissioning and decommissioned nodes drop out
  of the wanted placement, and the rebalancer moves replicas to match it
  one step at a time (ADR 0012).
- The meta group is placed by the same ring at a point of its own, with
  `cluster.meta_replicas` voters.
- Placement no longer derives from the configured peer list after
  bootstrap; the peer list's fingerprint is only a configuration sanity
  check between bootstrap peers.
