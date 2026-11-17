# ADR 0026: Disruptive servers: the CheckQuorum lease, PreVote, and the simulator's liveness requirement

Status: Accepted (Phase 8 follow-up)

## Context

The first nightly run (100,000 simulator schedules) failed at seed 51674:
"a proposal made during 300 calm steps was not applied by every member".
No safety check failed. The schedule had PreVote off, CheckQuorum on, and
random membership changes. dkv itself always runs with both PreVote and
CheckQuorum (`internal/replica`).

The proposed fix was the rule of the Raft dissertation, §4.2.3: with
CheckQuorum on, a server that has heard from a current leader within the
minimum election timeout ignores vote requests of a higher term, neither
raising its term nor granting its vote; a campaign started by a leader
transfer carries a flag that bypasses the check. If the core already had
the rule, the fallback was to stop changing membership in PreVote-off
schedules, keeping every safety check.

### The core already has the rule

It has had it since Phase 6 (ADR 0003, "CheckQuorum"):

- `inLease()` is true when CheckQuorum is on, the node knows a leader
  (itself, for a leader), and fewer than `ElectionTick` ticks (the minimum
  election timeout) have passed since it last heard from it.
- A `MsgVote` of a higher term that arrives inside the lease is dropped in
  `Step` before the term is touched: no term change, no vote, no reply.
- A `MsgPreVote` never changes the receiver's term; inside the lease it is
  refused. It is answered rather than dropped so the pre-candidate learns
  at once that it lost.
- A leader's `MsgTimeoutNow` makes the transferee campaign at once, without
  a pre-vote, and its `MsgVote`s carry `Transfer`, which skips the lease
  check. Only a `MsgTimeoutNow` from the node's current leader counts.

What was missing were tests aimed at the rule itself: it was covered only
through `TestPreCandidateAsksAgainAfterLease` (pre-votes) and
`TestLeaderTransfer` (the bypass, end to end).

### What happened in seed 51674

A replay that recorded every node's term, role and leader at every step
showed that the Phase 8 report had the cause wrong. It said the removed
server n2 kept cutting the only electable voter's elections short. It did
not:

- No vote ever changed the term of a node inside its lease (0 of 12 term
  increases caused by a vote in the whole schedule).
- Before the calm phase, leader n1 (term 7, voters n1 and n3) added n2
  back as a learner at step 364. n2, which never learned of its removal,
  had been campaigning alone up to term 23. At step 367 n1 dropped n2's
  term-23 vote request (the lease at work). At step 368 n1 stepped down
  anyway: n2 answered n1's heartbeat with an append response carrying
  term 23. That is Raft's notification to a stale leader, which a node
  whose term ran ahead needs in order to rejoin; it is not a vote.
- In the calm phase (steps 400–700) the group had no leader until step
  646, so no lease applied anywhere. The voters were n1 (last index 75)
  and n3 (last index 69). n3 could never win, because n1 refuses a
  candidate whose log is behind its own, but without PreVote each of
  n3's timeouts raised n1's term and restarted its timer: all six vote
  requests that raised n1's term in the calm phase came from n3. When n1 and n3 timed
  out within one message delay of each other (delays went up to 5 steps
  against timeouts of 10–19), they split the vote; this happened six
  times. n2's requests in the calm phase arrived at terms no higher than
  the receivers' and changed nothing.

So the disruptive server that mattered was a current voter with a stale
log, in a group with no leader. §4.2.3 protects a working leader and
cannot help there. The dissertation's remedy for a server that cannot win
raising everyone's term is PreVote (§9.6), which dkv runs.

## Decision

1. **The Raft core is unchanged.** Two unit tests pin the rule:
   `TestLeaseIgnoresDisruptiveVotes` (inside the lease, a follower and the
   leader leave term and vote alone, send nothing for a `MsgVote` and
   refuse a `MsgPreVote`; one tick before the lease ends the vote is still
   ignored, at the end of it the same request raises the term and wins the
   vote; with CheckQuorum off it always does), and
   `TestTransferCampaignBypassesLease` (only the leader's `MsgTimeoutNow`
   starts a campaign; it sends marked `MsgVote`s and no pre-votes; nodes
   inside their lease grant them; the same requests unmarked are ignored;
   a timeout's campaign is not marked). Both run with PreVote on and off.
2. **The simulator's fallback.** Schedules with PreVote off make no
   membership changes, as schedules with CheckQuorum off already did
   (`limitMembershipChanges` in `internal/sim`). They still run with
   crashes, partitions, message faults, transfers and compactions, and
   every safety check. Membership changes can leave two voters of which
   only one can win; without PreVote, liveness then depends on that one
   timing out first by more than a message delay, which no calm phase of
   fixed length guarantees.
3. **Seed 51674 is a permanent regression seed** (`TestRegressionSeeds`,
   run by `make sim` and `make check`): as drawn today and with PreVote
   forced on (membership changes kept) it must pass every check; as
   originally drawn it may fail only the liveness check.
4. **`-prevote=on|off`** (`make sim PREVOTE=…`) forces PreVote for a whole
   run. A seed's other options are drawn first and do not depend on it, so
   runs with `on` and `off` over the same seeds together cover every
   schedule of a run with PreVote drawn per seed. The nightly workflow runs
   100,000 seeds each way.

## Alternatives considered

- **A longer calm phase for PreVote-off schedules.** Makes a failure
  rarer, not impossible, and hides how long such a group can stay
  leaderless.
- **Voters ignore vote requests from servers outside their
  configuration** (option (c) of the Phase 8 report). It would not have
  helped: the server that kept the group leaderless was a voter.
- **Keep a voter that cannot win from raising terms when PreVote is off.**
  Deciding that it cannot win means asking the others first, which is
  PreVote; the two modes would become one.
- **Drop PreVote-off schedules.** The simulator would stop checking the
  vote path without pre-votes, a quarter of all schedules, for no gain:
  their safety checks pass.
- **Require liveness in PreVote-off schedules only while there are at
  least three voters.** Possible, but it is more machinery for a mode dkv
  does not run, and three voters with one stale and one crashed and
  restarted can still be slow.

## Consequences

- Membership changes are simulated only with PreVote and CheckQuorum both
  on, dkv's configuration: 37.5% of schedules (9/16 with both options, two
  thirds of those with a non-zero change rate), down from 50%.
- A node whose term ran ahead can still depose a working leader through
  the stale-leader notification when PreVote is off. That is standard
  Raft, and the way such a node rejoins; with PreVote a partitioned node
  does not raise its term in the first place.
- The transfer bypass means a delayed `MsgTimeoutNow` can make a node that
  has since fallen behind campaign with the lease bypassed. Safety then
  rests on the vote's log check alone, which no lease guards.
  `TestDelayedTimeoutNowMeetsTheLogCheck` (`internal/sim`) builds exactly
  this schedule on every run.
- Results: see docs/PROGRESS.md, Phase 8, "Seed 51674".
