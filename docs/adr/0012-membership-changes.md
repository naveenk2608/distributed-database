# ADR 0012: Single-server membership changes with learners

Status: Accepted (Phase 7)

## Context

Replicas must move when nodes die, join or are decommissioned, without
ever letting two majorities exist and without a window in which a group
depends on a replica that has no data yet.

## Decision

- **Single-server changes** (Raft dissertation §4.1): add a learner,
  promote a learner, remove a member. Each membership entry carries the
  full resulting membership. A change **takes effect when a node appends
  it**, not when it commits; a node whose log loses a membership entry to
  a new leader reverts to the previous one.
- **One uncommitted change at a time, and none before the leader has
  committed an entry of its own term** (its no-op): the fix to
  single-server changes from the dissertation's errata.
- **Learners** receive the log but neither vote nor count toward commit
  or ReadIndex quorums. New replicas always start as learners and are
  promoted only once their match index reaches the leader's commit index.
- **Bootstrap entry.** A new group's first entry (index 1, term 1, written
  identically by every bootstrap member) records the initial voters, so a
  replica that joins later and replays the log from index 1 learns the
  complete membership history. A replica created without bootstrap peers
  is uninitialized: it never campaigns and waits for the leader's log or a
  snapshot.
- **Member IDs are node IDs** (from `NODE_ID`), resolved to addresses
  through the directory (ADR 0011). A wiped disk gives a node a new ID, so
  a node that forgot its votes can never rejoin as the same member.
- **Who may campaign**: a voter; or a node that was a voter before an
  uncommitted change removed it, which may be needed to commit its own
  removal (dissertation §4.2.2); it does not count its own vote. A leader
  removed by a change keeps leading without counting itself until the
  change commits, then steps down. A non-voter keeps counting time since
  it last heard a leader, so its CheckQuorum lease expires and it can vote
  for a voter it does not yet know was promoted.
- **Leader transfer** (dissertation §3.10): the leader stops accepting
  proposals, brings the transferee up to date and sends `MsgTimeoutNow`;
  the transferee campaigns at once with a vote request that followers
  grant despite their lease. The attempt ends after an election timeout.
- **The rebalancer** (on the meta leader) changes each group one step at a
  time: a learner is promoted once caught up or removed if it no longer
  belongs; a voter on a dead node is removed first, so the group never
  depends on every remaining voter while it is repaired; a missing wanted
  node is added as a learner only while the group has no more voters than
  wanted, so a promotion leaves at most one extra; a surplus voter is
  removed, after leadership moves off it if it leads. Leaders are balanced
  by transfers between voters of the same group when two nodes' leader
  counts differ by two or more. A decommissioning node is left out of the
  wanted placement and marked decommissioned once no group holds it.
- **Replica clean-up**: a node deletes a replica it holds only when the
  group's leader reports a membership without it (removed replicas often
  never receive their own removal), after a grace of 25 rebalance rounds
  so a learner whose descriptor lags is not mistaken for a leftover.
  Replicas are created on demand when a group's leader first contacts the
  node.

## Alternatives considered

- **Joint consensus** (Raft paper §6). Changes several servers at once,
  at the cost of two configurations in effect during the change; every
  move dkv makes is single-server, so the extra mechanism buys nothing.
- **Changes taking effect on commit** (etcd/raft's model). Different nodes
  use different configurations for longer, and safety relies on subtler
  arguments; applying on append is the dissertation's proven rule.
- **Explicit replica creation by the rebalancer** before adding a
  learner. One more RPC and failure mode; on-demand creation needs none.
- **Addresses as member IDs** (Phase 6). A wiped node keeping its address
  would rejoin as the same member with no memory of its votes.

## Consequences

- Moving a replica costs four committed steps (add, promote, transfer if
  needed, remove) and a snapshot if the log was compacted. In the Phase 7
  measurements a new node took its share within about a second in tests
  with 200 ms rebalance rounds, with writes continuing throughout.
- The simulator exercises random add, promote and remove changes, leader
  transfers and snapshots in every schedule with CheckQuorum and PreVote
  on, as dkv runs. Without the CheckQuorum lease a removed server that
  never learns of its removal can keep forcing elections (dissertation
  §4.2.3); without PreVote, a voter that cannot win keeps raising terms,
  which can keep a two-voter group leaderless (ADR 0026). Schedules
  without either make no membership changes.
