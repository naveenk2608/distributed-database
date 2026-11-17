# ADR 0003: Raft as a deterministic core driven by a runner

Status: Accepted (Phase 6); extended in Phase 7

## Context

Every shard is its own Raft group (ADR 0001), so a node runs many groups at
once. The hardest Raft bugs are rare interleavings of elections, lost and
reordered messages and crashes; they have to be reproducible to be fixed.
The project may not use an existing Raft library.

## Decision

- **`internal/raft` is a pure state machine** in the style of etcd/raft:
  `Tick`, `Step(msg)`, `Propose(data)`, `ReadIndex(id)`, and `Ready()` /
  `Advance(rd)`. It has no goroutines, timers, network or disk calls and
  takes its randomness from a seeded source. `Ready` hands out the
  HardState and entries to persist, the messages to send, the committed
  entries to apply and the confirmed reads.
- **The runner** (`internal/replica`) owns one goroutine per replica and
  handles each `Ready` in order: persist (fsync if `Ready.MustSync`), send,
  apply, release reads, `Advance`. One node-wide ticker
  (`raft.tick_interval`) drives every replica; ticks coalesce if a replica
  is busy.
- **Protocol features**: randomized election timeouts in
  [`election_ticks`, 2·`election_ticks`); PreVote; CheckQuorum, with
  followers refusing pre-votes while they have heard from a leader within
  the minimum election timeout (the lease); a new leader appends a no-op
  and commits only entries of its own term by counting (Raft paper
  figure 8); conflict hints (term and index) so a follower's divergent
  suffix is skipped a term at a time; at most `MaxInflight` (8) append
  messages in flight per follower, with a single probe while the follower's
  position is unknown and stall detection through heartbeat responses;
  ReadIndex (ADR 0007).
- **Pre-vote tie-break.** Two followers whose timers fire together would
  grant each other's pre-votes, both start real elections, and split the
  vote, costing another election timeout. A pre-candidate therefore grants
  a rival in the same round only if the rival's log is more up to date, or
  as up to date with a lower ID; when it refuses a rival it also re-sends
  its own pre-vote to it, since the rival, now a pre-candidate, is out of
  its lease and will grant it. Pre-votes change no state, so this affects
  liveness only. In a three-replica group that lost its leader, the two
  survivors can no longer split.
- **Member IDs are advertise addresses**, the same strings placement uses
  (ADR 0023), so a NotLeader hint is directly a routable address.
- **Messages travel batched per node pair.** Every replica hands its
  messages to a node-level sender; one goroutine per peer node drains
  everything queued for that node, for all shards, into one
  `InternalService.Raft` call. The queue is bounded (8,192 messages) and
  drops beyond that, since Raft tolerates loss. A batch is abandoned after
  one election timeout. Plain heartbeats and their responses may wait up
  to 2 ms for company; anything else (appends, votes, ReadIndex
  heartbeats) wakes the sender at once and carries whatever is queued.
  Leaders heartbeat when their free-running tick count is a multiple of
  `heartbeat_ticks`, so all groups on a node heartbeat on the same ticks.
- **A deterministic simulator** (`internal/sim`) drives the core with
  seeded drops, duplicates, delays, reordering, partitions and crashes
  (losing anything not synced), checks Election Safety, Log Matching,
  Leader Completeness, State Machine Safety and ReadIndex safety on every
  step, and requires the group to elect a leader and commit once the
  network calms down. A failing seed replays to the identical trace hash.

## Alternatives considered

- **Goroutine-per-RPC (MIT 6.5840 style)**, with timers and RPC handlers
  mutating shared state under a lock. Natural to write, but schedules
  depend on the Go scheduler and wall-clock timers, so failures cannot be
  replayed, and many groups per node multiply goroutines and timers.
- **A ticker per replica.** Simpler wiring, but thousands of timers per
  node and no way to keep groups' clocks consistent in tests.
- **Heartbeats timed from each leader's election**, as in etcd/raft. Groups
  then heartbeat on different ticks and every tick costs an RPC per peer.
  An idle three-node cluster spends most of its CPU in those RPCs; aligning
  heartbeats and letting them linger lowered its CPU with 64 shards and
  the default 50 ms ticks from about 0.10 to 0.06 of a core per node
  (release build, three local processes).
- **A bidirectional gRPC stream per peer for Raft messages.** Saves a
  request header per batch but needs reconnect and back-pressure logic;
  a unary call per batch, sent only when something is queued, keeps the
  number of RPCs proportional to traffic rather than shard count. It can
  be revisited with Phase 10 profiles.
- **Resolving split votes only through random timeouts** (etcd's
  behavior). Correct, but in 30 failovers of a three-node group measured
  under the race detector, two took over 500 ms (200 ms election timeout),
  one of them with a split vote in the logs. With the tie-break, the
  re-ask, and the fsync reduction of ADR 0010, the slowest of 40 took
  394 ms.

## Consequences

- 10,000 seeded schedules run with zero violations (`make sim
  SEEDS=10000`). Thirteen deliberate bugs are each caught by a named test
  or a simulator violation: counting old-term entries for commit, voting
  more than once per term, ignoring log freshness in votes, skipping the
  append consistency check, committing without a quorum, ReadIndex without
  a heartbeat round, a leader ignoring a higher term, a vote not fsynced,
  followers ignoring their lease, no CheckQuorum, no tie-break, no re-ask,
  and applied entries not treated as committed on restart.
- The core must not grow hidden time or I/O; anything time-based enters as
  ticks, anything random through `Config.Rand`.
- Using addresses as member IDs ties a replica's identity to its address;
  Phase 7's membership changes decide whether node IDs replace them.
- Idle groups still cost CPU (the 0.06 of a core per node above).
  Quiescing idle groups or node-level heartbeats are options for Phase 10.

## Extension in Phase 7

The core gained log compaction and snapshots (`Compact`, `SnapshotAt`,
`MsgSnap`, `ReportSnapshot`, `Ready.Snapshot`), single-server membership
changes with learners and leader transfer (ADR 0012), all still without
I/O: a `MsgSnap` from the core asks the runner to stream a checkpoint,
and the receiving runner steps it back in with the snapshot's metadata
once the data is staged. The simulator now adds random membership
changes (including spare nodes that start uninitialized), leader
transfers, compactions and snapshot installs to its schedules, checks
Leader Completeness, State Machine Safety and Log Matching over
compacted logs and installed snapshots, and requires the members of the
final membership to apply a final proposal. Membership changes are made
only in schedules with CheckQuorum and PreVote on (ADRs 0012 and 0026).

## Phase 8 follow-up

A schedule now stops at its first violation: past it the state means
nothing, and a broken core could flood the simulated network until the
process ran out of memory without reporting (seed 73 with the vote-freshness
bug planted). Random schedules reach some situations only rarely (that bug
shows in one schedule in four), so targeted schedules build them directly
with the simulator's machinery: `TestDelayedTimeoutNowMeetsTheLogCheck`
delays a leader's `MsgTimeoutNow` until the transferee's log is stale,
leaving the vote's log check as the only guard.
