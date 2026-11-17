# ADR 0011: Cluster metadata in a meta Raft group inside dkv

Status: Accepted (Phase 7)

## Context

Once replicas move between nodes, every node must learn where each
group's replicas are and which nodes exist, and something must decide
moves. That metadata needs the same durability and fault tolerance as the
data, and no external database may hold it (the project's hard rules).

## Decision

- **The meta group is an ordinary Raft group** (shard number −1) whose
  state machine is the same key-value replica as a data shard. Its records
  are node records (`n/<node-id>`: address, state alive / dead /
  decommissioning / decommissioned), shard descriptors (`d/<shard>`, and
  `d/meta`: voters and learners) and the cluster settings (`c/settings`:
  name, shard count, replication factor, meta group size). Records are
  protobuf messages.
- **Records change only by conditional transactions** (`OP_TXN`: every
  write applies, at one version, only if each condition's key is at its
  expected version). A new cluster's first meta leader writes the settings,
  node records and initial descriptors in one transaction conditioned on
  the settings not existing, so a repeat is a no-op. Clients cannot reach
  the meta group's keys; it is routed only for internal requests.
- **Placement at bootstrap**: every bootstrap node waits until it knows
  every bootstrap peer's node ID, then computes the same layout from the
  sorted IDs with the hash ring: RF voters per shard and
  `cluster.meta_replicas` voters for the meta group. Each creates its
  replicas with a bootstrap entry naming those voters.
- **Each data group's Raft membership is authoritative.** Descriptors
  cache it for routing; the rebalancer updates a descriptor after the
  group's change commits and repairs any descriptor that differs from what
  the group's leader reports.
- **Nodes refresh their directory by polling.** Every
  `rebalance.interval` each node reads the meta group's records with a
  linearizable scan through the router and caches node addresses and
  descriptors in memory and in `DIRECTORY` in its data directory, so a
  restarted node can reach the meta group even after it moved off the
  bootstrap nodes. A node that joins receives the descriptors in the join
  response.
- **The rebalancer and liveness tracking run on the meta leader**
  (ADR 0012, ADR 0013). If the meta group has no quorum, data reads and
  writes continue on cached routes, and nothing is rebalanced.

## Alternatives considered

- **PostgreSQL or etcd as the metadata store.** Not allowed for core
  functionality, and a second system to operate.
- **Gossip only.** No single decision maker; moves would need their own
  consensus anyway.
- **A watch stream from the meta leader** (the plan's first idea).
  Pushes changes at once, but needs reconnects, resumption and
  backpressure. Polling a few kilobytes once a second is simpler and fast
  enough for routing, which tolerates stale descriptors through NotOwner
  hints. A stream can replace it if refresh traffic matters at large node
  counts.
- **Descriptors as the authority** (moves decided by writing a
  descriptor, groups following it). Two sources of truth that can
  disagree after a crash mid-move; the group's own log is already the
  agreed history.

## Consequences

- The meta group's state is small (one record per node and per shard)
  and every node reads all of it each round.
- Routing can point at a stale leader or replica set for up to a refresh
  interval; NotOwner hints and retries cover the gap.
- A bootstrap needs every bootstrap peer reachable once; after that the
  cluster no longer depends on the bootstrap list.
