# ADR 0000: Scope of v1

Status: Accepted (Phase 1)

## Context

dkv is a learning and portfolio project built by one developer over roughly
20–24 part-time weeks. Its value comes from getting the core right: a
storage engine that survives crashes, Raft that is safe under faults, and
sharding that spreads load. Every feature outside that core competes for the
same time.

## Decision

v1 supports single-key operations only: PUT, GET, DELETE, and compare-and-set
on one key (from Phase 6). Out of scope until after Phase 10: multi-key
transactions, SQL or any query language, range partitioning with automatic
splits, ordered scans across the keyspace, cross-region replication, and
authentication/TLS.

## Alternatives considered

- **Full transactional KV** (multi-key ACID, as in CockroachDB or TiKV with
  Percolator): needs MVCC timestamps across shards, a transaction record,
  intent resolution and a timestamp oracle or hybrid clocks. It roughly
  doubles the project and would not be credible without Jepsen-level testing
  of its own.
- **Range partitioning with splits**: enables ordered scans, but adds split
  and merge protocols and range-lookup metadata; see ADR 0002.

## Consequences

- The client API stays small, and the linearizability checker (Phase 8) only
  needs a single-register-per-key model.
- Applications needing atomic multi-key updates must design around
  single-key CAS.
- Hash partitioning (ADR 0002) means there are no ordered scans across the
  whole keyspace.
