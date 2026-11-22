# ADR 0027: Admin requests move on from nodes that cannot be reached

Status: Accepted (Phase 10 follow-up)

## Context

`pkg/client` takes several node addresses, and `dkvctl --addr` says requests
move to the next when one is unreachable. Key-value requests do move on when
a node answers `Unavailable`, but the admin requests (cluster status, shard
map, drain, transfer leader, decommission) went only to the first address.
With that node dead they failed, and how they failed depended on how it was
dead: a stopped process refuses connections and the request failed at once
with `Unavailable`; a stopped container behind Docker Desktop's port proxy
accepts the TCP connection and then never answers, so the request waited for
the whole deadline and failed with `DeadlineExceeded`. The second case is the
one that matters for moving on: a node that says nothing can only be told
apart from a slow one by waiting.

Every admin request is safe to repeat. Status and shard map read local state.
Drain and decommission read the node record from the meta group and return
success when it is already in the requested state. Transfer leader finds the
shard's leader among its voters on the server and succeeds when the target
leads.

## Decision

An admin request tries the nodes in turn, starting with the one that last
answered (as key-value requests do), once each:

- Before sending, it waits for the node's connection to be ready, for at most
  the node's share of the time left: the time to the caller's deadline
  divided by the nodes not yet tried. A connection that fails (refused,
  reset) or is not ready within that share marks the node unreachable, and
  the request moves on. Without a deadline it waits until gRPC gives up
  connecting.
- Once the connection is ready, the request has all the time left, so a slow
  but live node is never skipped, and a drain or transfer is never sent to a
  second node because the first was slow.
- A node that answers `Unavailable` (for example, it cannot reach the meta
  group) is also passed over. Any other answer, success or error, is
  returned.
- If no node answers, the error lists each node's failure and carries the
  first one's code: `Unavailable`, or `DeadlineExceeded` when the caller's
  deadline ran out while the first node was being connected to.

`ShardMaps`, which needs every node's answer rather than any one, asks all
nodes at once, each with its own timeout (the `dkvctl leaders` fix).

## Alternatives

- **A fixed per-attempt timeout on the request itself** (the deadline split
  between the nodes). Simple, but it cannot tell a dead node from a slow one:
  a transfer that takes longer than its share would be abandoned and sent to
  another node, and a busy cluster would see spurious failovers. Waiting for
  the connection separates the two cases for the dead nodes that matter
  here: a stopped process, and a stopped container behind a port proxy.
- **Retry with backoff around all nodes until the deadline**, as key-value
  requests do. Admin requests have no leader to chase from the client (the
  server routes to the meta group or the shard's leader), so a second round
  would ask the same nodes the same question; one pass keeps the old
  single-node behaviour plus the move-on.
- **Ask every node at once and take the first answer.** Fastest, but it would
  send every drain or decommission to every node. They are idempotent, so it
  would be correct, but it multiplies meta-group transactions for no gain
  when the first node is alive, which is the common case.

## Consequences

- With a dead node first, an admin request costs at most that node's share of
  the deadline (with `dkvctl`'s default 5 s and three addresses, about 1.7 s)
  before the next node answers.
- A node that completes the connection and then stops answering (for
  example, frozen after the connection was made by an earlier request of the
  same client) still holds the request until the deadline. `dkvctl` makes new
  connections for every command, so this does not arise there.
- Key-value requests are unchanged: they move on only when a node answers
  `Unavailable`, so a node that accepts connections and never answers still
  holds a key-value request until its deadline. Using the same connection
  check there is possible (an unconnected node never received the request,
  so moving on is safe even for writes), and is left for when it is needed.
- The check uses gRPC's connectivity-state API (`Connect`, `GetState`,
  `WaitForStateChange`), which gRPC marks experimental.
