# ADR 0029: The console shows a node down after several timed-out polls, or at once when its connection fails

Status: Accepted (Phase 10 follow-up; revises the "shown down at once" rule of ADR 0015)

## Context

ADR 0015 had the console show a node down as soon as one poll of it
failed, whatever the failure. A poll is bounded by `console.poll_timeout`,
so a node that is busy for a moment (a long fsync, a garbage collection,
a loaded machine) and answers one poll late was shown down for one round
with no numbers, and came back up on the next. In `test/dashboard`
`TestDashboardGate`, with 200 ms poll timeouts under the race detector,
that happened in 8 of 50 runs: a live node showed as down with no start
time while the test compared the console's numbers with the node's own.

The Phase 9 gate still has to hold: a node that is killed shows as down
within one poll interval (plus the poll timeout), in process and in the
Compose browser gate, where the console polls containers by name.

A poll can fail in two ways. Either the node is gone or broken: the
connection is refused or reset, or closed without an answer, the name does
not resolve, or the node answers with an error status. Or the poll timed
out, which may only mean the node is slow.

## Decision

- A poll that fails other than by timing out shows the node down at once,
  as before. A killed process closes its sockets, so its listener refuses
  connections and kept-alive ones are closed, and a stopped container's
  name stops resolving; all of those show it down within one interval.
- A poll that timed out (every failed request of the round passed its
  deadline, or its connection attempt timed out) counts as a miss. A node
  that was up stays up, showing its last answer, until
  `console.down_after_misses` polls in a row have timed out (default 3,
  at least 1); an answer resets the count. A node that has not answered yet
  is shown down after its first failed poll, as before.
- While a node misses polls it keeps its last numbers. Its rates come back
  right on the next answer, because they are computed from the two
  snapshots' own timestamps.

## Alternatives

- **Keep showing a node down after any failed poll.** The dashboard then
  flickers whenever a node is slow for one poll, and the gate test fails
  at random.
- **Count every failure as a miss.** A killed node would then take
  `down_after_misses` intervals to show, which breaks the Phase 9 gate's
  "within one poll interval".
- **A longer poll timeout.** It must stay below the poll interval, and it
  delays every round while a node hangs; it narrows the window rather than
  closing it.
- **Show a separate "not answering" state before "down".** Possibly more
  informative, but it changes the dashboard and its API, which this fix
  does not need.

## Consequences

- A node that hangs without closing its connections (a paused process, a
  container behind a port proxy that accepts and says nothing) shows as
  down after `down_after_misses` intervals instead of one: 3 s with the
  defaults.
- If, in some network, a killed node's polls time out instead of being
  refused (for example packets to a vanished address are dropped), it too
  shows as down after `down_after_misses` intervals.
- Samples recorded while a node misses polls repeat its last numbers.
