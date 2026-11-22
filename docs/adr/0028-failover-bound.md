# ADR 0028: The failover gate's "two election timeouts" are two of the longest randomized timeouts

Status: Accepted (Phase 10 follow-up)

## Context

The Phase 6 gate in docs/PLAN.md asks for "a new leader within about two
election timeouts" after a shard's leader is killed. `raft.election_ticks`
does not fix one timeout: each follower draws its timeout from
`[election_ticks, 2 × election_ticks)` ticks after every reset (ADR 0003).
`integration` `TestLeaderFailover` runs with 20 ms ticks and 10 election
ticks, so one timeout lasts anything from 200 ms to 380 ms, and it asserted
`2 × election_ticks × tick + 10 ticks`, 600 ms: two of the *shortest*
timeouts plus 200 ms. It failed now and then on a busy machine (633 ms in
one `make check`; 0.62–1.36 s in 6 of 50 runs with two test binaries at a
time), never with a lost write.

What one failover takes here: the survivors' timers run from the dead
leader's last heartbeat. The first to fire starts a pre-vote. The other
survivor refuses it while its CheckQuorum lease lasts (it heard from the
leader less than `election_ticks` ago), which can happen when the first
draw is close to the minimum. The second survivor's own timer then fires,
at most `2 × election_ticks` ticks after its last heartbeat; both are now
pre-candidates, out of their leases, and the pre-vote tie-break (ADR 0003)
picks one, which the other grants. Either way a winner starts its campaign
within one of the longest timeouts, and the two survivors of a
three-replica group cannot split the vote. The pre-vote and vote rounds that follow are each a round trip plus
an fsync of the term and vote; under the race detector and on a loaded
machine they and the scheduling of the ticker take tens to hundreds of
milliseconds.

## Decision

An election timeout in this gate is the longest the randomized timeout can
last, `2 × election_ticks × tick`, and the bound is two of them,
`4 × election_ticks × tick`, computed from the test's configuration: 800 ms
for `TestLeaderFailover`. The first timeout covers the wait for the winning
survivor's timer, whatever it drew; the second covers the election rounds
and the test's polling of the shard maps.

`e2e` `TestSIGKILLedLeaderIsReplaced` keeps its own bound,
`2 × election_ticks × tick + 25 ticks` (900 ms for its settings): it
measures through separate processes and new gRPC clients, and it is already
looser than this definition.

## Alternatives

- **Keep 600 ms.** It asks for more than the plan: it leaves 220 ms for the
  rounds and polling after the longest draw, and fails at random on a busy
  machine although nothing is wrong.
- **Two of the shortest timeouts (400 ms).** Shorter than one longest draw
  plus a round trip: a failover that waits for the longest draw would fail
  the gate.
- **Measure from the survivors' last heartbeat instead of from the kill.**
  More exact, but the test cannot see that moment without instrumenting the
  Raft core, and the gate is about what a client sees.

## Consequences

- A failover that needs a second contested round (a refused pre-vote and
  then a lost vote) still exceeds the bound, and the test fails for a reason
  worth looking at.
- Other tests that bound failover time use this definition or say why they
  need more.
