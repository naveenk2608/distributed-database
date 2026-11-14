# Distributed Database from Scratch: Architecture & Development Plan

Plan as of Nov 15, 2026.

## Overview

We are building **dkv**, a sharded, Raft-replicated key-value store in Go. Every key belongs to one of a fixed number of shards, every shard is its own Raft group, and every replica stores data in a custom LSM-tree engine. (dkv is a working name; rename it freely.)

The design is closest to CockroachDB and TiKV, which run many Raft groups over a custom storage engine. It borrows SSTables and compaction from LevelDB and Cassandra, and the single-node simplicity of Redis. It deliberately does not copy Cassandra's leaderless quorum writes, because running those beside Raft would create two competing sources of truth.

| Property | Guarantee | Mechanism |
| --- | --- | --- |
| Writes | Linearizable per key | Every PUT and DELETE goes through its shard's Raft log |
| Reads (default) | Linearizable | ReadIndex on the shard leader |
| Reads (opt-in) | Stale, bounded by replication lag | Follower reads, chosen per request |
| Durability | An acknowledged write is fsynced on a majority of the shard's replicas | Raft commit rule plus fsync before ack |
| Fault tolerance | A shard survives f failed replicas when RF = 2f + 1 | Majority quorums |
| Retries | A retried write is applied at most once | Client sessions stored in replicated state |

Out of scope for v1, by design: multi-key transactions, SQL, range partitioning with automatic splits, cross-region replication and authentication. Each would double the project without teaching anything the core does not already cover, so they wait until after Phase 10. Hash partitioning also means there are no ordered scans across the whole keyspace; that is the price of even load.

## Overall system architecture

A dkv cluster is a set of identical `dkv-server` nodes. Each node hosts one replica of many shards, the replicas of each shard form a Raft group across three nodes, and clients may send any request to any node.

> Diagram: dkv cluster · 3 nodes, 3 data shards and the meta group at RF 3 (diagram not included in this Markdown version).

Every node runs the same layers: an API server and router on top, shard replicas in the middle, and per-replica storage underneath. With S shards, RF = 3 and N nodes, each node holds about 3S/N replicas and leads about S/N of them, so write load spreads evenly once leaders are balanced. The meta group is simply one more Raft group, placed on the first three nodes at bootstrap.

Two kinds of traffic share the internal gRPC transport: Raft messages between replicas of the same shard, and client requests forwarded to a shard's leader. The console, PostgreSQL and the React dashboard sit outside the cluster and only read node status, so dkv keeps serving if they go down.

The brief's stack maps cleanly onto this. Go, gRPC and Protocol Buffers run everything inside the nodes; PostgreSQL holds only dashboard history; React, TypeScript and Tailwind build the dashboard; Docker Compose runs the cluster locally and Terraform-provisioned EC2 instances run it in AWS.

## Component breakdown

The system has eighteen components in four layers: storage, consensus, cluster, and the interfaces on top. Storage knows nothing about Raft, the Raft core knows nothing about disks or networks, and only the replica layer joins them.

| Component | Package | Purpose | Phase |
| --- | --- | --- | --- |
| Config | `internal/config` | Loads flags, environment and YAML, validates them at startup; nothing about the cluster is hardcoded | 1 |
| Observability | `internal/obs` | Structured logs with `log/slog`, Prometheus-format metrics, the cluster event log | 1 |
| Memtable | `internal/storage/memtable` | Sorted in-memory skiplist holding the newest writes, so writes never wait on a disk seek | 1 |
| VFS | `internal/storage/vfs` | Thin filesystem interface plus a fault-injecting version that can drop unsynced data, so tests can simulate power loss | 2 |
| WAL | `internal/storage/wal` | Segmented, CRC-checked append-only log with group commit; makes the memtable survive crashes and is reused as the Raft log | 2 |
| SSTable | `internal/storage/sstable` | Immutable sorted file: data blocks, index, bloom filter, properties, footer | 3 |
| LSM engine | `internal/storage/lsm` | Owns memtables, SSTables, manifest, flush, compaction, reads, checkpoints and recovery; the only storage API the rest of dkv sees | 1 to 3 |
| Transport | `internal/transport` | Internal gRPC server and pooled clients: batched Raft messages, snapshot streaming, request forwarding | 4 |
| Placement | `internal/placement` | Consistent hash ring with virtual nodes; maps each shard to RF nodes deterministically from the membership list | 5 |
| Router | `internal/router` | Maps key to shard to leader, caches leader hints, forwards requests, applies the retry policy | 5 |
| Raft core | `internal/raft` | Deterministic Raft state machine: takes messages and ticks, emits messages, entries to persist and entries to apply | 6 |
| Raft log store | `internal/raft/raftlog` | Durable Raft entries, HardState (term, vote, commit) and snapshot metadata, built on the WAL package | 6 |
| Shard replica | `internal/replica` | One shard on one node: drives the Raft core, persists, sends messages, applies committed commands to its LSM engine, keeps the session table | 6 |
| Cluster manager | `internal/cluster` | Membership, node liveness, the meta group's state machine, and the rebalancer that moves replicas | 4, 7 |
| API server | `internal/server` | Public gRPC KV service and the admin HTTP API each node exposes | 1+ |
| Go client | `pkg/client` | Sessions, request IDs, retries and optional leader-aware routing; used by the CLI, benchmarks and tests | 1+ |
| CLI, bench, console | `cmd/dkvctl`, `cmd/dkv-bench`, `cmd/dkv-console`, `web/` | Operator CLI, YCSB-style load generator, and the dashboard backend plus React UI | 1, 9, 10 |
| Simulator | `internal/sim` (tests only) | Deterministic network, disk and clock simulation for Raft and cluster tests | 6 to 8 |

Imports only point downward: server, then router, then replica, then the Raft core and the LSM engine. The replica receives the transport as a small interface it defines itself, which is the Go habit of declaring interfaces where they are consumed. A dependency check in CI (golangci-lint's `depguard`) keeps those boundaries from eroding as the code grows.

## Data flow for PUT, GET and DELETE

All three operations share one route: any node accepts the request, hashes the key to a shard, and hands it to that shard's leader. Writes then go through the Raft log; reads skip the log but still confirm leadership.

### Routing (shared)

A client sends the request to any node, or the Go client sends it straight to the leader it has cached. The receiving node computes `shard = xxhash64(key) mod S`, where S is fixed when the cluster is bootstrapped (default 64). The router looks up that shard's leader in its cache and forwards the request over internal gRPC if it is not the leader itself.

Forwarding is limited to one hop. A replica that is no longer leader answers `NotLeader` with a hint about who is, and the gateway retries there with jittered backoff inside the client's deadline.

### PUT

Every write carries `client_id` and `request_id`. The leader first checks its session table; if that pair was already applied, it returns the stored result without touching Raft. Otherwise it encodes a `Command{PUT, key, value, client_id, request_id, expected_version?}` and proposes it to its Raft group.

The Raft core appends the entry to the leader's log, and the replica fsyncs it through the Raft log store using group commit. In parallel it sends `AppendEntries` to the followers, which persist the entry and acknowledge. Once a majority, the leader included, has the entry on disk, the leader advances its commit index.

Committed entries are applied strictly in log order. Applying a PUT writes `key → (value, version)` into the shard's LSM engine, where `version` is the entry's Raft log index, and records the result in the session table. The leader then replies with the new version; followers apply the same entry once the next heartbeat tells them it is committed.

The cost of one write is one round trip to the nearest majority plus one fsync on each of those replicas. If `expected_version` is set and the stored version differs, applying the entry produces a `VersionMismatch` result instead of a write; that outcome is still deterministic on every replica.

### GET

A default GET is linearizable. The leader notes its current commit index as the read index, confirms it is still leader with one heartbeat round acknowledged by a majority (shared by all reads in flight), waits until its applied index reaches the read index, and then reads locally.

The local read checks the active memtable, then immutable memtables, then SSTables from newest to oldest, using each SSTable's bloom filter to skip files that cannot hold the key. The first entry found wins: a value returns `(value, version)`, a tombstone returns `NotFound`. No log entry is written, so a read costs a network round trip but no fsync.

With `consistency=STALE`, any replica answers immediately from its applied state and includes its applied index, so the caller can see how stale the answer may be. Passing `min_version` makes a stale read wait until the replica has applied at least that version, which gives a client read-your-writes without going to the leader.

### DELETE

DELETE follows the PUT path exactly, except that applying it writes a tombstone whose version is the log index. Deleting a missing key still commits and reports `found=false`, which keeps retries harmless.

The tombstone is physically removed only when compaction writes the oldest data for that key and nothing older can exist below it. Because every replica applies the same log, dkv needs no Cassandra-style `gc_grace_seconds`: there is no anti-entropy process that could resurrect a deleted value.

## Raft integration architecture

Each shard is an independent Raft group ("multi-Raft"), and one extra group, the meta group, stores membership and shard placement. Raft owns ordering, replication and leadership; the LSM engine is only the state machine each replica applies committed commands to.

### The Raft core is a deterministic state machine

The core follows the etcd/raft design rather than the goroutine-per-RPC style of the MIT 6.5840 labs. It exposes `Step(msg)`, `Tick()` and `Propose(data)`, and a `Ready()` call that returns what the outside world must do: HardState to persist, entries to append, messages to send, committed entries to apply, and any snapshot. After the runner has done those things it calls `Advance()`.

The core contains no goroutines, timers, network or disk calls, so a simulator can drive it with seeded randomness and replay any failing schedule exactly. That costs some structure up front, but the hardest Raft bugs are rare interleavings, and this is the only practical way to reproduce them.

| Raft feature | Why dkv needs it | Phase |
| --- | --- | --- |
| Leader election with randomized timeouts | One leader per shard per term | 6 |
| Heartbeats | Keep followers from starting elections; carry the commit index | 6 |
| Log replication with conflict hints | Followers converge fast after divergence | 6 |
| Commit only current-term entries by counting | Avoids the overwrite bug in Figure 8 of the Raft paper | 6 |
| PreVote | A rejoining partitioned node cannot force needless elections | 6 |
| CheckQuorum | A leader cut off from its majority steps down | 6 |
| ReadIndex | Linearizable reads without writing to the log | 6 |
| Snapshots and InstallSnapshot | Bounded log size; catch-up for far-behind or new replicas | 7 |
| Learners and single-server membership changes | Move replicas without ever having two majorities | 7 |
| Leader transfer | Graceful shutdown and spreading leaders evenly | 7 |

### How replicas run on a node

A node hosts many replicas, one per shard it holds. One ticker drives every replica (for example a 50 ms tick, heartbeats every 2 ticks, election timeouts randomized between 10 and 20 ticks). Each replica runs its own Ready loop goroutine; the transport groups outgoing messages by destination node into one `RaftMessageBatch` per gRPC stream send.

Applying an entry calls the LSM engine with the entry's log index as the sequence number, so every key's version is the index that last wrote it. The same apply step updates the session table, which maps `client_id` to its latest `request_id` and result. That table is part of replicated state and is included in snapshots. Sessions expire using timestamps the leader stamps into entries, never each replica's own clock, so expiry is identical everywhere.

### Durability: one log or two

In standalone mode the engine's own WAL makes writes durable. In clustered mode the Raft log already is a write-ahead log, so keeping the engine WAL too means two fsyncs per write.

Phase 6 starts with both enabled because it is simple and obviously correct. Phase 10 then turns the engine WAL off for replicas: each flush records the applied index in the manifest, a restart replays Raft entries above it, and the Raft log is truncated only below the flushed applied index. The benchmark difference between the two goes into the report and an ADR.

### Snapshots

When a replica's Raft log passes a threshold (for example 10,000 entries or 64 MB), it takes an engine checkpoint at its applied index and truncates the log. A follower that is too far behind receives InstallSnapshot: the checkpoint's SSTables, manifest and session table streamed in chunks over gRPC. The follower ingests them into a fresh directory and swaps it in atomically.

### The meta group

The meta group runs on three (or five) nodes chosen at bootstrap. It stores node records (ID, address, state), shard descriptors (replica set and a configuration epoch) and cluster settings such as S and RF. Every node watches it through a stream from the meta leader to keep its routing cache fresh.

Node liveness is soft state: each node heartbeats the meta leader every few seconds, and the leader keeps the timestamps in memory. Only decisions are written to the meta log, such as marking a node dead. A newly elected meta leader waits one full liveness timeout before declaring anyone dead, so a leader change cannot cause false failures.

The rebalancer runs on the meta leader. It compares the placement the hash ring wants for the live nodes with the replica sets that exist, and fixes one difference at a time per shard: add a learner, let it catch up by snapshot, promote it, then remove the old replica. Each step is a configuration change proposed in that shard's own Raft group, which stays authoritative; the meta descriptor is updated afterwards. If the coordinator crashes mid-move, the next leader recomputes the difference and carries on.

If the meta group loses quorum, data reads and writes continue on cached routes, but no rebalancing happens until it recovers.

### How split-brain is prevented

Any two majorities of the same group overlap, so two leaders can never both commit in the same term, and terms fence off stale leaders. CheckQuorum makes an isolated leader step down, PreVote stops term inflation from a node that rejoins, and ReadIndex makes a leader prove it still leads before it serves a read. Lease-based reads come later as an optimization, and only with the rule that the lease is shorter than the election timeout minus the assumed maximum clock drift.

The fixed shard count is what keeps this achievable for one developer. Keys never move between shards; only whole shard replicas move, and Raft snapshots already know how to ship those. The trade-off is that S is chosen once at bootstrap and a single hot key cannot be split.

## Storage engine architecture

Each shard replica owns one LSM-tree engine: writes go to a WAL and a skiplist memtable, full memtables flush to immutable SSTables, and background compaction merges SSTables. One instance per replica keeps snapshots and replica moves simple, because a shard's data is just a directory.

> Diagram: LSM engine · write path, flush, compaction, checkpoint and read order (diagram not included in this Markdown version).

A write is durable once its WAL record is fsynced; everything to the right and below happens in the background, and a Get checks the numbered stores in order.

### Keys, versions and the engine API

Inside the engine every entry is keyed by `userKey | seq | kind`, sorted by user key ascending and sequence descending, so the newest version of a key is found first. In clustered mode the sequence number is the Raft log index; in standalone mode the engine assigns it.

```go
db, err := lsm.Open(dir, lsm.Options{MemtableBytes: 4 << 20, Sync: lsm.SyncAlways})
err = db.Apply(batch, seq)                   // seq = Raft index, or 0 to let the engine assign
val, ver, err := db.Get(key, snap)           // snap = nil for latest, or a read snapshot
it := db.NewIterator(lsm.IterOptions{Snapshot: snap})
applied, err := db.Checkpoint(dir)           // hard-link checkpoint for Raft snapshots
err = db.Close()
```

### File formats

The WAL and SSTables both checksum every record or block with CRC32C, so torn writes and bit rot are detected rather than silently read.

```text
WAL segment 000042.wal
  record  := crc32c(4) | length(4) | type(1) | payload
  payload := seq(8) | count(4) | { kind(1) | klen(uvarint) | key | vlen(uvarint) | value }*

SSTable 000317.sst
  [data block 0] ... [data block N]   ~4 KB each, prefix-compressed keys + restart points
  [bloom filter block]                ~10 bits per key, about 1% false positives
  [index block]                       last key of each data block -> (offset, size)
  [properties block]                  min/max key, min/max seq, counts, applied index
  [footer]                            handles to index/filter/props, format version, magic
  every block := payload | compression(1) | crc32c(4)
```

### Write path

Concurrent writers queue up and a single leader writer combines their batches into one WAL record and one fsync, which is LevelDB-style group commit. The batch then goes into the active memtable, and only after that do the waiting writers return.

When the memtable passes its size limit it becomes immutable, a new memtable and WAL segment start, and a background goroutine flushes the old one to a new SSTable. Once the SSTable and the updated manifest are durable, the old WAL segment is deleted. If too many SSTables pile up before compaction catches up, writes are slowed and then stalled, which is better than letting read cost grow without limit.

### Read path

A read takes a reference to the current version, meaning the memtables plus the set of live SSTables, without holding a lock during the lookup. It checks memtables first, then SSTables from newest to oldest; for each file it consults the bloom filter, binary-searches the index block, and reads one data block through a node-wide LRU block cache. Reference counting ensures compaction never deletes a file a reader is still using.

### Manifest

The manifest lists the live SSTables, the next file number and the last flushed sequence number (the applied index in clustered mode). It is rewritten in full to a temp file, fsynced, renamed over the old one, and the directory is fsynced. An append-only version-edit log like LevelDB's is the alternative; with small per-shard engines the full rewrite is simpler and cheap enough.

### Compaction

Size-tiered compaction comes first: when four or more SSTables of similar size exist, they are merged into one with a heap-based k-way merge. The merge keeps only the newest version of each key unless an open read snapshot still needs an older one, and it drops tombstones only when its output is the oldest data in the engine. A rate limiter in MB/s keeps compaction from starving foreground I/O, and a shared worker pool caps how many engines compact at once.

Leveled compaction is a stretch goal. Implementing both and measuring read, write and space amplification on the same workload makes a strong section of the final report.

### Snapshots and checkpoints

The word "snapshot" means two things here. A read snapshot is just a sequence number: reads ignore newer entries, and compaction preserves versions a live snapshot can see. A checkpoint flushes the memtable, then hard-links every live SSTable and copies the manifest into a new directory, which copies no data and is what a Raft snapshot ships.

### Recovery

On startup the engine reads the manifest, opens and verifies each SSTable footer, and deletes any SSTable not listed, since those are leftovers from a flush or compaction that crashed. It then replays WAL segments newer than the last flushed sequence into a fresh memtable.

A bad checksum or short record at the very end of the newest segment is a torn write from the crash, so it is truncated. The same problem anywhere else means real corruption, and the engine refuses to start with a clear error rather than guess.

### Sync policy

`SyncAlways` fsyncs before acknowledging, amortized by group commit, and is the default. `SyncInterval` fsyncs every few milliseconds and can lose recent acknowledged writes on power loss, so it is allowed only in standalone mode and is documented as such. `SyncNever` exists for benchmarks that measure fsync cost. The Raft log always uses `SyncAlways`, because a vote or an acknowledgment that is forgotten after a crash breaks Raft's safety.

## Consistency model, versioning and CAP trade-offs

dkv is strongly consistent by default and offers eventual consistency only as an explicit read option. There is exactly one write path, the shard's Raft log, so replicas never hold conflicting histories that need merging.

### Strong consistency

Every operation on a key appears to take effect at one instant between its call and its reply, and all clients agree on the order. Once a write is acknowledged, every later read from any client returns that value or a newer one. Raft's log order provides this for writes, and ReadIndex provides it for reads.

### Eventual consistency, and why it is narrower here

A `STALE` read from a follower may lag the leader, but it never shows a value that was not committed, and it catches up as the follower applies the log. That is bounded staleness rather than Dynamo-style eventual consistency, where replicas accept conflicting writes and reconcile later.

`docs/consistency.md` should contrast this with Cassandra's model: tunable ONE, QUORUM and ALL levels, last-write-wins timestamps, read repair, hinted handoff and `gc_grace_seconds`. Those mechanisms exist because leaderless replicas can disagree, and explaining why dkv does not need them shows you understand both designs.

### Versioning and concurrent updates

A key's version is the Raft log index of the write that produced it, which is unique and increasing within a shard. GET returns it and PUT returns the new one.

Concurrent blind writes resolve by log order: whichever commits later wins, and every replica agrees which one that was. For read-modify-write, clients use compare-and-set by passing `expected_version`; if another write got there first, the client receives `VersionMismatch` with the current version and decides whether to retry. dkv never merges values, never returns siblings and needs no vector clocks, because a single log already orders everything.

### Behavior under partition (CAP and PACELC)

| Situation | What dkv does | CAP reading |
| --- | --- | --- |
| Shard leader and a majority on the same side | Reads and writes continue normally | Consistent and available on that side |
| A shard's replicas on the minority side | Writes and linearizable reads fail with `Unavailable` or `NotLeader` | Chooses consistency over availability |
| Client asks for `STALE` on the minority side | Served from local applied state, with its applied index | Availability over consistency, by explicit choice |
| Different shards' majorities on different sides | Each shard keeps working wherever its majority is | Availability is decided per shard, not per cluster |
| No partition | One majority round trip plus fsync per write; reads pay a heartbeat round | PACELC: latency is paid for consistency |

The durability promise is precise: an acknowledged write survives the failure of any f replicas of its shard when RF = 2f + 1, including power loss, provided the disks honor fsync. It does not survive losing the disks of a majority of that shard's replicas at once.

## Repository folder structure

One Go module holds the server, CLI, benchmark tool and console; the React dashboard lives in `web/`. Generated protobuf code is committed so a fresh clone builds without extra tools.

```text
dkv/
├── README.md
├── Makefile                  # build, check, sim, test-integration, chaos, bench, proto
├── go.mod
├── buf.yaml, buf.gen.yaml    # protobuf lint + codegen
├── .github/workflows/        # CI: check on every push, long sim + chaos nightly
├── proto/dkv/v1/
│   ├── kv.proto              # public API: Put, Get, Delete, CompareAndSet
│   ├── raft.proto            # internal: RaftMessageBatch, snapshot chunks
│   ├── cluster.proto         # internal: join, liveness, shard-map watch
│   └── admin.proto           # status, metrics, events, drain
├── gen/dkv/v1/               # generated Go code (committed)
├── cmd/
│   ├── dkv-server/           # node binary
│   ├── dkvctl/               # CLI
│   ├── dkv-bench/            # load generator
│   └── dkv-console/          # dashboard backend; the only code that talks to PostgreSQL
├── internal/
│   ├── config/               # flags, env, YAML, validation
│   ├── obs/                  # slog setup, metrics, event log
│   ├── storage/
│   │   ├── vfs/              # FS interface + fault-injecting FS
│   │   ├── wal/              # segmented, checksummed log
│   │   ├── memtable/         # skiplist
│   │   ├── sstable/          # blocks, bloom, index, writer, reader
│   │   └── lsm/              # DB: write/read path, flush, compaction, manifest, recovery
│   ├── raft/                 # deterministic Raft core, no I/O
│   │   └── raftlog/          # durable log + HardState on top of wal
│   ├── replica/              # one shard replica: Raft runner, state machine, sessions
│   ├── transport/            # internal gRPC, batching, snapshot streams
│   ├── placement/            # consistent hash ring, shard -> replica set
│   ├── cluster/              # membership, liveness, meta state machine, rebalancer
│   ├── router/               # key -> shard -> leader, forwarding, retries
│   ├── server/               # public gRPC service + admin HTTP API
│   └── sim/                  # deterministic network/disk/clock simulator (tests)
├── pkg/client/               # public Go client: sessions, request IDs, retries
├── test/
│   ├── crash/                # kill -9 harness for a single node
│   ├── integration/          # in-process multi-node clusters
│   ├── chaos/                # Docker fault scenarios
│   └── linearizability/      # history recorder + Porcupine models
├── web/                      # React + TypeScript + Tailwind (Vite)
├── deploy/
│   ├── docker/               # Dockerfiles
│   ├── compose/              # 3- and 5-node clusters, console, PostgreSQL
│   └── aws/                  # Terraform for EC2 + bootstrap scripts
├── bench/
│   ├── workloads/            # YCSB-style workload definitions
│   └── results/              # raw JSON results + environment metadata
└── docs/
    ├── PLAN.md               # this plan
    ├── PROGRESS.md           # what is implemented AND tested, per phase
    ├── architecture.md, consistency.md, storage-format.md
    ├── protocol.md, benchmarks.md
    └── adr/                  # one file per decision: 0001-multi-raft.md, ...
```

The dependency list is short on purpose, and it bans anything that would do the core work for you.

| Allowed | Used for | Where |
| --- | --- | --- |
| `google.golang.org/grpc`, `google.golang.org/protobuf`, buf | RPC and serialization | everywhere |
| `github.com/cespare/xxhash/v2` | key and ring hashing | placement, router |
| `github.com/prometheus/client_golang` | metrics endpoint | obs |
| `github.com/HdrHistogram/hdrhistogram-go` | latency percentiles | bench, obs |
| `github.com/jackc/pgx/v5` | PostgreSQL access | console only |
| `github.com/anishathalye/porcupine`, `go.uber.org/goleak`, `github.com/google/go-cmp` | linearizability checks, leak checks, test diffs | tests only |
| Vite, React, TypeScript, Tailwind CSS, TanStack Query, a chart library such as Recharts | dashboard | web |

Banned outright: any Raft library (hashicorp/raft, etcd-io/raft), any embedded storage engine (Pebble, Badger, bbolt, RocksDB bindings), and any external database in the data path.

## Phase-by-phase roadmap

The plan keeps your ten phases, and each one ends at a gate: named tests that must pass before the next phase starts. The total is roughly 20 to 24 focused part-time weeks; tooling speeds up typing, but understanding and verifying Raft still takes the time it takes.

> Diagram: Roadmap · 10 phases, 4 milestone gates, about 23 planned weeks (diagram not included in this Markdown version).

Phases 6 to 8 are where the project earns its credibility and where schedules slip, so protect that stretch first if time gets tight.

Three boundaries move compared with the original brief. Leader election moves into Phase 6, because Raft log replication cannot run without it; Phase 7 takes snapshots, dynamic membership and node-level failure detection instead. Phase 5 runs sharding with one live replica per shard, which works without any metadata service because consistent-hash placement is deterministic. A minimal CLI ships in Phase 1, since every later phase needs it for manual testing, and Phase 9 polishes it.

### Phase 1: Skeleton and single-node in-memory store (1 to 1.5 weeks)

Build the Go module, Makefile, buf setup and `kv.proto`, config loading and slog logging. Then the skiplist memtable with internal keys, `lsm.DB` in memory-only mode, the gRPC KV server, `pkg/client` and a minimal `dkvctl`. Add CI (vet, golangci-lint, `go test -race`), `docs/PROGRESS.md`, and ADRs 0000 to 0002.

**Gate:** a randomized skiplist test against a Go map passes under `-race` with concurrent readers, `dkvctl put/get/delete` works against a running server, and CI is green.

### Phase 2: WAL, persistence and crash recovery (1.5 to 2 weeks)

Build the VFS with its fault-injecting version, then the WAL: segments, CRC32C records, group commit and sync policies. The engine recovers by replaying the WAL, truncates torn tails, and rotates segments with the memtable. A `test/crash` harness writes from a child process, kills it with SIGKILL at random moments, restarts it and checks every acknowledged key. Until Phase 3 adds flushes, the WAL grows without bound; record that one-phase limitation in PROGRESS.md.

**Gate:** under simulated power loss, every write acknowledged with `SyncAlways` survives; 100 or more kill -9 cycles lose no acknowledged write; a corrupted record in the middle of the log stops startup with a clear error.

### Phase 3: SSTables, LSM tree, indexing and compaction (2.5 to 3 weeks)

Build the SSTable writer and reader, memtable flush, the manifest, the full read path with a merging iterator, the block cache, size-tiered compaction with tombstone removal, write stalls, read snapshots, hard-link checkpoints and orphan-file cleanup. Add `go test -bench` microbenchmarks and write `docs/storage-format.md`.

**Gate:** a model test of one million random operations, with flushes, compactions and restarts interleaved, matches a Go map exactly; the measured bloom filter false-positive rate is close to the configured one; a crash injected at every step of flush and compaction recovers to a consistent state.

### Phase 4: Multi-node communication and cluster formation (1 to 1.5 weeks)

Build the internal gRPC transport with connection pooling, deadlines and keepalives, plus a persistent node ID. Cluster membership comes from a bootstrap peer list in config (never from code), with health pings and a cluster status RPC. Add an in-process test cluster helper (N servers on ephemeral ports with separate data directories) and a three-node Docker Compose setup.

**Gate:** three nodes form a cluster from config both in-process and in Compose, status reports a killed peer as down within the configured window, and transport tests cover timeouts and reconnects.

### Phase 5: Consistent hashing, sharding and data placement (1.5 weeks)

Build the hash ring with virtual nodes, the fixed shard count, deterministic shard-to-replica-set placement with a configurable RF, one LSM engine per shard on each node, and the router with forwarding, `NotOwner` handling and jittered retries. In this phase only the first replica in each set serves traffic; replication arrives in Phase 6.

**Gate:** a balance test shows shard counts per node within a stated tolerance of the mean; adding a node to N nodes moves close to 1/(N+1) of shard assignments and no more; any node accepts any key and the request reaches the owner.

### Phase 6: Raft consensus and replication (3.5 to 4.5 weeks)

This is the core of the project. Build the Raft core (elections, heartbeats, replication with conflict hints, the commit rule, PreVote, CheckQuorum), `raftlog` with HardState fsynced before any vote or acknowledgment, and the deterministic simulator with drops, delays, reordering, partitions and crashes. Then the replica runner, one Raft group per shard on the Phase 5 replica sets, ReadIndex and stale reads, client sessions, compare-and-set, `NotLeader` hints and per-node message batching.

**Gate:** at least 10,000 seeded simulator schedules run with zero violations of Election Safety, Log Matching, Leader Completeness and State Machine Safety, and any failing seed replays identically. A three-node RF=3 cluster keeps serving after its leader is killed, with a new leader within about two election timeouts and no acknowledged write lost. A retried PUT is applied exactly once.

### Phase 7: Snapshots, failure detection, membership changes and recovery (2.5 to 3 weeks)

Build log compaction and InstallSnapshot using engine checkpoints streamed over gRPC. Add the meta group, node liveness with Alive, Suspect and Dead states, join and decommission, learners with single-server configuration changes, and the rebalancer: re-replication away from dead nodes, moves onto new nodes, and leader balancing through leader transfer.

**Gate:** a node down past the dead timeout has its replicas recreated elsewhere and every shard returns to RF voters; a wiped node rejoins and catches up through snapshots; adding a node rebalances with write unavailability limited to brief leader-transfer pauses, which are measured and reported; losing meta quorum stops rebalancing but not reads or writes.

### Phase 8: Fault injection and distributed integration testing (2 weeks)

Build a fault injector with two backends: in-process (a partition table and message drop or delay inside the transport) and Docker (SIGKILL, SIGSTOP, iptables partitions, `tc netem` latency and loss). Record client histories, check them with a Porcupine key-value model, script nemesis schedules (random partitions, leader isolation, rolling restarts), and run a short suite in CI with a longer nightly one.

**Gate:** every scenario in the failure matrix passes the linearizability check across repeated runs. As a negative control, a deliberately broken configuration (for example `SyncNever` plus killing a majority) must fail the checker, which proves the checker can catch real bugs.

### Phase 9: CLI and developer dashboard (2 weeks)

Finish `dkvctl`: put, get, delete, CAS, a consistency flag, cluster status, shard map, leaders, drain, decommission, transfer-leader and JSON output. Add the admin HTTP API on every node, and `dkv-console`, which polls nodes, stores samples and events in PostgreSQL and streams updates to the browser over server-sent events. The React dashboard gets four pages: Overview (health, nodes, throughput and latency), Shards (placement, leaders, replication lag), Nodes (storage, SSTables, compaction) and Events (elections, failures, rebalances).

**Gate:** killing a node in Compose appears on the dashboard within one poll interval, with election and re-replication events in the right order; every number on screen comes from a real node metric; stopping the console has no effect on the database.

### Phase 10: Docker, AWS deployment and benchmarking (2 to 2.5 weeks)

Build multi-stage Dockerfiles with small static images, Compose profiles for three nodes, five nodes and chaos, and Terraform for EC2 (VPC, security groups, nodes across availability zones, one load-generator instance). Finish `dkv-bench`, run the experiment matrix in the testing section, profile with pprof, fix at least one real bottleneck, and switch replicas to the single-log durability path. Then write the benchmark report and finalize the README, diagrams and ADRs.

**Gate:** `make bench-local` reproduces the local results from a clean clone; the report lists hardware, configuration, git SHA, methodology and raw data, with before-and-after numbers for each optimization; the AWS cluster is created and destroyed with one command each.

Before Phase 6, read the Raft paper and chapters 3 to 6 of Diego Ongaro's dissertation, plus section 9.6 on PreVote; they answer most questions that come up during implementation.

## Major technical challenges and their solutions

Most of the risk sits in three places: fsync correctness, Raft's edge cases, and testing things that fail only under rare timing. The table names fifteen specific traps and how the design handles each.

| Challenge | Why it is hard | Solution in dkv |
| --- | --- | --- |
| Durability that is real | The page cache hides unsynced data; a new or renamed file is not durable until its directory is fsynced; crashes leave torn records | CRC on every record and block, fsync of file and parent directory, torn-tail truncation, and a fault FS that discards unsynced data in tests |
| Raft safety edge cases | Committing old-term entries by count (Figure 8), granting a vote before persisting it, mishandling log conflicts | Commit only current-term entries by counting; persist HardState before any reply; the simulator checks the four safety invariants after every step |
| Paying for two logs | The Raft log and the engine WAL both fsync every write | Start with both, then make the Raft log the only WAL for replicas, store the applied index in the manifest, and benchmark the change |
| Cheap linearizable reads | A deposed leader that does not know it yet can serve stale data | ReadIndex with one batched heartbeat confirmation; lease reads only with a documented clock-drift bound |
| Duplicate writes on retry | A write that timed out may have committed, and a blind retry can overwrite a newer value | Client ID plus request ID, a dedup table inside replicated state, included in snapshots, expired by leader-stamped log time |
| Snapshotting a live LSM | The snapshot must match an exact applied index while writes keep arriving | Flush plus hard-link checkpoint at the applied index; truncate the Raft log only below the flushed index |
| Safe membership changes | Changing voters carelessly can create two majorities | Single-server changes one at a time, learners catch up before promotion, never remove the old replica before the new one votes |
| Multi-Raft overhead | Many groups each send heartbeats and fsync their own logs | A modest fixed shard count, one ticker, message batching per node pair, group commit across replicas; one shared Raft log per node as a stretch goal |
| Bootstrapping the meta group | Nodes need the meta group to join, but the meta group needs nodes | An explicit `--bootstrap` flag on the first nodes with an initial peer list; every other node joins through any seed address |
| Non-reproducible failures | Distributed bugs appear once in thousands of runs and vanish on rerun | Deterministic Raft core, seeded simulator, Porcupine checks, and every failing seed saved as a regression test |
| Compaction hurting latency | Background merges compete with foreground I/O, and too many SSTables slow reads | Compaction rate limiter, write backpressure past an SSTable-count threshold, shared compaction worker pool |
| Stale routing | Gateways and clients keep sending to old leaders after an election | `NotLeader` replies carry hints, caches are invalidated, retries are bounded with jittered backoff inside the caller's deadline |
| Goroutine leaks and deadlocks | Long-lived loops, streams and timers outlive their owners | Context cancellation everywhere, `goleak` in tests, `-race` in CI, documented lock ordering in `replica` and `lsm` |
| Honest benchmarks | Closed-loop load generators hide queueing delay (coordinated omission), and cloud disks vary | Open-loop mode with a fixed arrival rate, HDR histograms, warm-up, at least three runs, full environment metadata |
| Scope creep | Transactions, range splits or SQL would each double the project | Non-goals recorded in an ADR; stretch goals only after Phase 10 |

## Testing and benchmarking strategy

Testing runs in six layers, from fast unit tests on every commit to Jepsen-style chaos runs checked for linearizability every night. Benchmarks follow a written methodology so every number in the report can be reproduced.

### Test layers

| Layer | What it covers | Tools | When it runs |
| --- | --- | --- | --- |
| Unit | Skiplist, WAL codec, SSTable round trip, bloom filter rate, hash ring, Raft step functions | `go test`, table-driven tests, go-cmp | Every commit |
| Model and fuzz | Random operation sequences against a Go map, with flush, compaction and restart mixed in; decoders fed corrupt bytes must return errors, never panic | Seeded `math/rand`, `go test -fuzz` | Every commit (short), nightly (long) |
| Crash | Simulated power loss through the fault FS; real SIGKILL of a child process | `vfs` fault FS, `os/exec` harness | Every commit (fault FS), nightly (SIGKILL) |
| Simulation | Raft and cluster logic under drops, delays, reordering, partitions and crashes, with invariant checks | `internal/sim`, seeded schedules | Every commit (1,000 seeds), nightly (100,000) |
| Integration | Multi-node clusters over real gRPC on localhost | `test/integration` | Every commit |
| Chaos and linearizability | Docker clusters under scripted faults, client histories checked by Porcupine | Compose, iptables, `tc netem`, Porcupine | Nightly and at each phase gate |

### Failure matrix

Phase 8 turns each row into an automated scenario; Phases 6 and 7 already cover the basic ones.

| Fault | Expected behavior |
| --- | --- |
| Kill a shard leader | New leader within about two election timeouts; no acknowledged write lost |
| Kill a minority of a shard's replicas | Reads and writes continue |
| Kill a majority of a shard's replicas | Writes and linearizable reads fail fast; `STALE` reads still work; full recovery when nodes return |
| Isolate the leader in a minority partition | It steps down through CheckQuorum; the majority elects a new leader; nothing commits twice |
| Flapping or one-way partitions | PreVote prevents runaway terms; progress resumes when the network heals |
| Restart a node | It replays its Raft log or snapshot and catches up |
| Wipe a node's disk | It rejoins as a new node and receives snapshots |
| Slow disk or high network latency | Throughput drops; correctness holds |
| Lost, duplicated or reordered messages | Raft tolerates them; the session table prevents double application |
| Meta group loses quorum | Data operations continue; rebalancing pauses |
| Client retries after a timeout | The write is applied exactly once |

### Benchmark methodology

`dkv-bench` runs YCSB-style workloads: A (50% reads, 50% updates), B (95/5), C (read-only) and a write-only workload, with uniform or Zipfian key choice, one to ten million preloaded keys, and 100-byte or 1 KB values. It supports closed-loop mode for peak throughput and open-loop mode at a fixed arrival rate for honest latency. Latencies go into HDR histograms and the report shows average, p50, p95, p99 and p99.9, plus the error and timeout rate.

The server side reports its own numbers too: fsync latency, Raft commit latency, apply lag, compaction bytes, and read, write and space amplification. Those make it possible to explain a latency curve, not just show it.

The experiment matrix compares one, three and five nodes; RF 1 and RF 3; `SyncAlways` against `SyncNever` (only to show fsync's cost); the two-log and single-log durability paths; ReadIndex, lease and stale reads; a concurrency sweep from 1 to 256 clients to find the saturation point; a value-size sweep; and a time series of throughput across a forced leader failover.

Each configuration runs at least three times after a 30 to 60 second warm-up, and the report shows the median with the spread. Every run writes a JSON file with the configuration, git SHA, machine details and raw histogram to `bench/results/`, and the report is generated from those files. Bottlenecks are found with pprof CPU, mutex and block profiles plus `go tool trace`; the usual suspects are fsync, memtable write contention, protobuf allocations and per-message gRPC overhead.

On AWS, every node uses the same instance type and the load generator runs on its own instance in the same region. Avoid burstable instance types for benchmark runs, since CPU credits distort results. Record whether volumes are EBS or local NVMe instance storage and their provisioned settings, because fsync latency dominates write performance and differs sharply between them.

## ADR backlog

Sixteen decisions in this plan deserve their own Architecture Decision Record, each with context, the choice, the alternatives and the consequences. Write each one in the phase where the decision first bites, and revise it if benchmarks later change your mind.

| ADR | Decision | Chosen | Alternatives considered | Phase |
| --- | --- | --- | --- | --- |
| 0000 | Scope | Single-key operations with CAS; no transactions, SQL or range splits | Full transactional KV | 1 |
| 0001 | Replication model | One Raft group per shard | Leaderless quorums (Dynamo, Cassandra); one Raft group for everything; primary-backup | 1 |
| 0002 | Partitioning | Fixed S hash shards, placed by consistent hashing | Range partitioning with splits (CockroachDB); per-key token ring (Cassandra); Redis Cluster's 16,384 slots | 1 |
| 0003 | Raft structure | Deterministic core driven by an external runner | Goroutine-per-RPC (MIT 6.5840 style) | 6 |
| 0004 | Engine per shard | One LSM instance per shard replica | One shared LSM per node with shard-prefixed keys | 5 |
| 0005 | Compaction | Size-tiered first | Leveled; hybrid | 3 |
| 0006 | Manifest format | Full rewrite with atomic rename | Append-only version-edit log (LevelDB) | 3 |
| 0007 | Read consistency | ReadIndex by default, stale reads opt-in, lease reads later | Reads through the log; lease-only | 6 |
| 0008 | Idempotency | Client sessions in replicated state | Separate idempotency-key store; none | 6 |
| 0009 | Conflict resolution | Log order plus compare-and-set | Last-write-wins timestamps; vector clocks with siblings; CRDTs | 6 |
| 0010 | Durability path | Raft log as the only WAL for replicas | Raft log plus engine WAL | 6, revisited in 10 |
| 0011 | Cluster metadata | Meta Raft group inside dkv | PostgreSQL or etcd as metadata store; gossip only | 7 |
| 0012 | Membership changes | Single-server changes with learners | Joint consensus | 7 |
| 0013 | Failure detection | Heartbeats to the meta leader as soft state; decisions replicated | SWIM gossip; phi accrual detector | 7 |
| 0014 | Role of PostgreSQL | Dashboard history only, outside the data path | Cluster metadata store; not used | 9 |
| 0015 | Dashboard API | REST and server-sent events from the console | gRPC-Web with Envoy; Connect RPC | 9 |
