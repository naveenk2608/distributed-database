# ADR 0025: Fault injection at the transport seam; histories checked by Porcupine

Status: Accepted (Phase 8)

## Context

Phase 8 turns every row of the failure matrix (docs/PLAN.md) into an
automated scenario and checks what clients saw for linearizability. That
needs three things: a way to inject network, process, disk and clock
faults into a running cluster; a record of every client operation with
its invocation and response times; and a checker that decides whether
some sequential order of those operations, consistent with real time,
explains every answer. The plan asks for two fault backends: one
in-process, fast enough to run on every push, and one against Docker
containers.

## Decision

### In-process backend

- **Network faults live in the transport client** (`internal/transport`):
  every unary and streaming call from one node to another first asks a
  `transport.Faults` (`Decide(from, to, method)`) whether to drop it
  (fail with `Unavailable`, as a lost connection does), delay it, or send
  a unary call twice. `internal/fault.Injector` implements it: a
  partition table of blocked directed links (two-sided partitions,
  isolation of one node, one-way blocks) plus per-link and default
  loss, duplication, delay and jitter, driven by a seeded generator.
  Because every node-to-node call goes through the pool (Raft batches,
  snapshot streams, pings, heartbeats, joins, forwarded requests), one
  seam covers them all. Nodes are named by internal address, which a
  wiped node keeps when its ID changes.
- **Per-group partitions** use the Raft filter added in Phase 7
  (`Node.SetRaftFilter`): the meta group can lose quorum while data
  groups keep working.
- **Process faults**: `nodetest` stops a node (its peers see a crash),
  restarts it on the same addresses and data, wipes it, or cuts its power
  (below). `Node.PauseTicks` stops a node's Raft clock, which with an
  isolation reproduces a paused process: a leader that cannot notice it
  lost its quorum while a new one is elected.
- **Disk faults**: `vfs.Slow` adds latency to every file and directory
  fsync. `nodetest.Options.MemFS` keeps each node's replicas on a
  `vfs.MemFS`; `Cluster.PowerLoss` crashes it (ADR 0020's model: only
  what was fsynced survives) and restarts the node on the crash image.
- **Clients** (`test/linearizability`) run on several goroutines, most
  pinned to one node each so that an isolated old leader keeps serving
  someone, against a handful of keys so operations collide. Puts, gets,
  deletes and compare-and-sets carry unique values; a client interceptor
  can resend writes, as a client that lost a response does.

### Checking

- **Porcupine** (`github.com/anishathalye/porcupine`, allowed by the
  plan for tests only) with a nondeterministic per-key model. The model's
  state is presence, value and version; versions are part of the check
  (a write's version must exceed the key's earlier versions, a read must
  return the version of the value it returns, a failed compare-and-set
  must report the current version), so the checker also catches version
  bugs that values alone would hide.
- **An operation that failed or timed out has an unknown outcome**: it is
  recorded as returning at the end of the history, and the model lets it
  take effect at any point after its invocation, or not at all. A read
  with an unknown outcome constrains nothing.
- The history is partitioned by key, which keeps checking fast; a check
  that does not finish within 60 s counts as a failure (Porcupine's
  `Unknown`), never as a pass.
- **Negative control**: `storage.sync = never` now also skips the fsync
  of every Raft log write (ADR 0010, revised). Cutting the power of the two nodes
  that hold every acknowledged write while the third is partitioned away
  must then produce a history the checker rejects. If it ever passes, the
  checker (or the fault) has stopped working.

### Docker backend

- `test/chaos` (`make chaos`; skipped unless `-chaos` is given, so it
  still compiles, vets and lints on every push) runs the same workload
  and checker against the Compose cluster (`docker-compose.yml` plus the
  `chaos.yml` override with few shards and fast failure handling).
  Process faults use `docker kill -s KILL`, `docker stop` (SIGTERM, a
  graceful rolling restart), `docker kill -s STOP` and `-s CONT` (a frozen
  process that resumes unaware time passed) and `docker start`; a wipe
  empties the stopped container's volume. Network faults run `iptables`
  and `tc netem` in a short-lived helper container that joins the
  target container's network namespace (`--network container:<id>
  --cap-add NET_ADMIN`), so the node image stays distroless and
  unprivileged. Services and ports are read from the Compose files.

### Where each suite runs

- Every push: `make check` runs the model tests, the fault injector's
  tests and the failure matrix once (3 s of faults per scenario), plus
  the power-loss test and the negative control.
- Nightly: the failure matrix repeated with longer faults (`make lin
  LIN_RUNS=… LIN_DURATION=…`) and the long simulation; `make chaos` where
  Docker is available.

## Alternatives considered

- **A TCP proxy per link** (Toxiproxy style): faults at the byte level,
  which the transport cannot fake (half-open connections, slow reads),
  but one more process per link, and it cannot see which call or group a
  byte belongs to. The interceptor drops whole calls, which is what the
  Raft and forwarding code has to survive; byte-level faults are left to
  the Docker backend's `tc netem`.
- **Faults inside `raftnet` only**: simpler, but misses snapshots,
  heartbeats to the meta leader, joins and forwarding.
- **Network namespaces and iptables on the host**: realistic, but needs
  root and cannot run in `go test` on a developer machine or in CI
  without privileges.
- **Jepsen with Knossos or Elle**: the reference tools, but a JVM and
  Clojure stack and SSH-managed nodes; Elle targets transactional
  histories, which dkv does not have (ADR 0000). Porcupine checks the same
  property for single-key histories in Go.
- **A home-grown checker** (for example: no acknowledged write is lost,
  versions only grow): simple, but it misses stale reads, and a bug in a
  checker written alongside the code under test is likely to share its
  assumptions.
- **Value-only model** (ignore versions): fewer states, faster checks,
  but blind to a read returning the right value at the wrong version and
  to compare-and-set answers.
- **Installing iptables in the node image**: simpler commands, but a
  larger image running with `NET_ADMIN` in production too.

## Consequences

- Fault injection needs no privileges, so the gate runs in `go test` on
  every push and on this 3 GB development machine.
- The in-process backend shares one Go runtime: a garbage-collection
  pause stops every node at once, and `PauseTicks` stops only the Raft
  clock, not the node's other goroutines. Real process pauses are left to
  SIGSTOP in the Docker backend.
- Faults are injected on the sending side only, so a call can be lost
  before the receiver sees it or, by duplication, answered twice with
  the first answer lost; a response lost after the receiver acted is
  covered by client-side resends and timeouts, not by the injector.
- Unknown outcomes make the check weaker for operations that time out;
  the workload keeps timeouts well above an election so most operations
  have a known outcome.
- `storage.sync = never` in a cluster now trades Raft's guarantees for
  speed, not just the engine's; the example configuration says so.

## Revision in the Phase 8 follow-up

A review of the Phase 8 commits found that the gate could pass
vacuously: every scenario asserted only that the history was
linearizable, that the cluster settled and that the final reads
succeeded. A scenario whose faults did nothing, or that left the cluster
unable to serve anything, passed. With CheckQuorum turned off, for
example, `isolate-leader` stayed linearizable.

**Decision.** Every scenario also shows that its faults took effect, and
where the failure matrix promises availability, that the cluster served:

- From each in-process run the test keeps *evidence*: the injector's
  counters, the summed terms of the data shards before and after, and the
  reads and writes with a known outcome that returned while the faults
  lasted. Each scenario states what must hold: elections for kills,
  isolation, pauses and restarts; blocked, delayed, lost or duplicated
  calls for the network faults; at least 10 reads and 10 writes for the
  rows where service continues; at most 1.5 elections per shard per second
  under flapping one-way partitions (PreVote on: 0 to 11 in 3 s on 4
  shards; PreVote off: 21 to 34).
- Effects that can only be seen while the fault lasts are checked inside
  the nemesis: an isolated leader has stepped down after 1 s; a paused
  leader's shards have a leader at a newer term after 1.5 s; the meta
  group has no leader 2 s after its traffic is cut; with a majority down,
  linearizable reads and writes fail within their deadline.
- After recovery: every replica applies its leader's commit index
  (`rolling-restart`); the wiped node is a voter of every shard under a new
  ID and installed snapshots (`wipe-node`, which now waits until every log
  is compacted before wiping).
- The Docker backend checks, at the end of each kill, freeze, isolation
  or partition, that every shard led from the faulted side has a newer
  term.

**Alternatives.** A minimum on the summed term growth alone, as the
first Docker log line suggested: leadership transfers by the rebalancer
raise terms without faults (2 to 3 in 10 s), so a threshold either misses
weak faults or flakes; checking the faulted leader's own shards does not
depend on that baseline. Checking a fixed number of operations instead of
operations during the faults: a run whose faults stopped everything
still completes many operations before and after them.

**Consequences.** The floors and the flapping bound are measured, not
derived, and are set well inside the measured ranges, including under the
race detector; a much slower machine could need them revisited. Faults
that are only logged, not asserted, are listed under the Phase 8 known
limitations in docs/PROGRESS.md.
