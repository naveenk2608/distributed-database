# ADR 0007: Read consistency

Status: Accepted (Phase 6)

## Context

A Raft leader can be deposed without knowing it, so serving reads from its
local state can return stale data. Reads are most of most workloads and
should not cost a log write.

## Decision

- **Linearizable reads by default, through ReadIndex.** The leader records
  its commit index, confirms with a heartbeat round that a majority still
  follows it, waits until it has applied up to that index, then reads
  locally. A new leader first waits until an entry of its own term (its
  no-op) commits, since before that its commit index may be behind.
- **Reads are batched.** All reads waiting at a replica when its
  goroutine picks them up share one ReadIndex round, so one heartbeat round
  confirms any number of concurrent reads.
- **Stale reads are opt-in** (`consistency = STALE`, `dkvctl --stale`,
  `client.GetStale`). Any replica answers from its applied state without
  contacting the leader; a node holding a replica of the shard answers
  locally without forwarding. `min_version` makes the replica wait until it
  has applied at least that index, so a client that passes the version of
  its own last write reads its own writes. Every answer carries the
  replica's applied index.
- A linearizable read sent to a follower is redirected to the leader
  (`NotOwner` with a hint), never answered by the follower.
- **Lease reads come later**, and only with the lease shorter than the
  minimum election timeout minus the assumed clock drift.

## Alternatives considered

- **Reads through the log.** Simple and obviously correct, but every read
  costs an fsync on a majority.
- **Lease reads only.** One round trip less than ReadIndex, but
  correctness then depends on bounded clock drift; wrong as a default.
- **Follower reads with ReadIndex from the follower** (the follower asks
  the leader for its commit index). Spreads read load; worth adding when
  benchmarks show the leader saturating on reads.

## Consequences

- A linearizable read costs a heartbeat round to a majority but no disk
  write, shared across concurrent reads.
- When a shard has lost its majority, linearizable reads fail fast while
  stale reads keep working on the surviving replicas.
- A stale read may miss writes acknowledged to other clients; the applied
  index in the answer says how fresh it is.
