# ADR 0010: Durability path for replicas

Status: Accepted for Phases 6–8 (two logs); to be revisited in Phase 10

## Context

In a cluster the Raft log already is a write-ahead log. Keeping the
engine's WAL as well means two fsyncs per write on every replica. The plan
starts with both because it is obviously correct, and measures a single-log
path in Phase 10.

## Decision (Phase 6)

- Each replica has its own Raft log (`raftlog`, on the WAL format of ADR
  0019) in `shards/NNNN/raft` and its own engine in `shards/NNNN/kv`.
- The Raft log is fsynced before any message leaves the node whenever a
  `Ready` carries new entries, a new term or a new vote
  (`Ready.MustSync`). A change of the commit index alone is written without
  fsync: forgetting it is harmless, because a restarted node learns it
  again from the leader.
- Applying an entry writes a batch to the engine at sequence number = the
  entry's index, including the key `m/applied`, so the engine's last
  sequence number is the replica's applied index. The engine's WAL follows
  `storage.sync`.
- On restart the runner passes the engine's last sequence number to Raft
  as the applied index; Raft treats everything up to it as committed (the
  persisted commit index may lag it, see above) and re-applies the rest of
  the committed log. The Raft log is never truncated in Phase 6, so any
  engine state the engine lost (with `storage.sync` interval or never) is
  rebuilt from it.

## Alternatives considered

- **Raft log as the only WAL** (engine WAL off for replicas): each flush
  records the applied index in the manifest, restart replays Raft entries
  above it, and the Raft log is truncated only below the flushed applied
  index. Half the fsyncs, but restart and log truncation must agree on the
  flushed index exactly. Planned for Phase 10 with a benchmark comparison.
- **Fsync the commit index too.** One more fsync on every follower for
  every commit advance (each heartbeat after a write) for no safety gain.
  Dropping it raised the writes four sequential writers got acknowledged
  in one run of the Phase 6 failover test, under the race detector, from
  58 to between 240 and 400.

## Consequences

- A write costs one Raft-log fsync on the leader and on each follower in
  the quorum, plus engine WAL fsyncs with `storage.sync = always`.
- With `storage.sync` interval or never, a power loss can lose engine state
  but, by this design, not acknowledged writes in a cluster, since the Raft
  log holds them and is replayed above the engine's last sequence number.
  No test yet combines a replica with a simulated power loss; Phase 8's
  fault injection covers it.
- The Raft log grows without bound until Phase 7 adds snapshots and
  truncation.

## Revision in Phase 7

- Logs are compacted: once a replica's Raft log holds `raft.compact_entries`
  applied entries (default 10,000), it flushes its engine, writes a
  checkpoint record (snapshot metadata, HardState and the remaining
  entries) as the only record of a new segment, fsyncs it and deletes the
  older segments, keeping the last quarter of the entries for followers
  that lag a little. A single checksummed record means a crash leaves the
  old log or the new one, never a mix; a test crashes at each of the seven
  filesystem operations of a compaction and of an install.
- A follower that needs compacted entries receives a snapshot: the
  leader checkpoints its engine (hard links, no copy) at its applied index
  and streams the files over `InternalService.InstallSnapshot`. The
  receiver stages them in `snap-N/`, marks them complete, records the
  snapshot in its Raft log and then repoints `ENGINE` at `snap-N/`; a
  restart finishes an install a crash interrupted. A test crashes the
  receiver's filesystem after each of the 61 operations of receiving and
  installing a snapshot and always recovers and catches up.

## Revision in Phase 8

- **`storage.sync = never` now also skips the Raft log's fsyncs** in a
  cluster (`replica.Config.UnsafeNoSync`). Before, it affected only the
  engine WAL, and the Raft log kept every acknowledged write safe, so the
  setting could not show what fsync costs a replica, which is what the
  benchmark matrix compares it for. It also gives the Phase 8 negative
  control a real bug to find: a power loss of a majority then destroys
  acknowledged writes and votes, and the linearizability checker must
  reject the history (`test/linearizability`
  `TestNegativeControlFailsTheChecker`). The example configuration and
  the setting's documentation say that `never` breaks Raft's guarantees.
- Alternatives: a separate `raft.unsafe_no_sync` key (two settings to
  keep consistent, and a benchmark of `never` that still pays for half
  the fsyncs), or keeping `never` engine-only and breaking durability for
  the negative control by a test-only hook (proves the checker works, but
  leaves `never` meaning something other than "no fsync").
- The Phase 6 consequence above is now tested:
  `TestPowerLossOfMajorityLosesNothing` cuts the power of the two nodes
  holding every acknowledged write (a `vfs.MemFS` crash image keeps only
  what was fsynced) while the third is partitioned away, with
  `storage.sync` `always` and `interval`, and every acknowledged write
  survives.
