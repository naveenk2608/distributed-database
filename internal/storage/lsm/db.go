// Package lsm is dkv's storage engine and the only storage API the rest of
// dkv uses.
//
// Phase 1 provides the in-memory engine only: a single memtable with no
// write-ahead log, flushes or SSTables, so data does not survive a restart.
// The WAL arrives in Phase 2 and SSTables, flush and compaction in Phase 3
// (see docs/PLAN.md).
//
// Every write gets a sequence number, and a key's version is the sequence
// number of the write that produced it. In standalone mode the engine
// assigns sequence numbers; in clustered mode the caller passes the Raft log
// index.
package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
	"github.com/naveenk2608/distributed-database/internal/storage/memtable"
)

// Errors returned by DB methods. Check them with errors.Is.
var (
	ErrNotFound        = errors.New("lsm: key not found")
	ErrClosed          = errors.New("lsm: database closed")
	ErrEmptyBatch      = errors.New("lsm: empty batch")
	ErrSeqNotIncreased = errors.New("lsm: sequence number must exceed the last applied one")
	ErrSeqTooLarge     = errors.New("lsm: sequence number exceeds the maximum")
)

// DB is a storage engine instance. It is safe for concurrent use.
type DB struct {
	mu      sync.Mutex // serializes Apply and Close
	lastSeq uint64     // last sequence number used; guarded by mu
	failed  error      // sticky internal error; guarded by mu

	mem *memtable.Memtable

	// visibleSeq is the highest sequence number whose whole batch has been
	// applied. Reads without a snapshot read at visibleSeq, so they never
	// observe part of a batch.
	visibleSeq atomic.Uint64
	closed     atomic.Bool
}

// OpenInMemory returns an empty in-memory database.
func OpenInMemory() *DB {
	return &DB{mem: memtable.New()}
}

// Apply applies every operation in b atomically at one sequence number and
// returns it; that number becomes the version of every key the batch
// writes. If seq is 0 the engine uses the next sequence number; otherwise seq
// is used and must be greater than every sequence number applied before
// (gaps are allowed, so a caller can pass Raft log indexes). See
// docs/adr/0018-batch-sequence-numbers.md.
func (d *DB) Apply(b *Batch, seq uint64) (uint64, error) {
	if b.Count() == 0 {
		return 0, ErrEmptyBatch
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed.Load() {
		return 0, ErrClosed
	}
	if d.failed != nil {
		return 0, d.failed
	}
	switch {
	case seq == 0:
		seq = d.lastSeq + 1
	case seq <= d.lastSeq:
		return 0, fmt.Errorf("%w: got %d, last applied %d", ErrSeqNotIncreased, seq, d.lastSeq)
	}
	if seq > ikey.MaxSeq {
		return 0, fmt.Errorf("%w: %d", ErrSeqTooLarge, seq)
	}

	for _, op := range b.effectiveOps() {
		if err := d.mem.Add(seq, op.kind, op.key, op.value); err != nil {
			// Unreachable while the checks above hold: seq is fresh, each
			// key appears once, and kinds come from Batch. If it happens
			// anyway, part of the batch is in the memtable but invisible;
			// refuse further writes so a later batch can never expose it.
			d.failed = fmt.Errorf("lsm: internal error applying batch at seq %d: %w", seq, err)
			return 0, d.failed
		}
	}
	d.lastSeq = seq
	d.visibleSeq.Store(seq)
	return seq, nil
}

// Snapshot is a read snapshot: reads through it see exactly the batches
// applied before it was taken.
type Snapshot struct {
	seq uint64
}

// Seq returns the snapshot's sequence number.
func (s *Snapshot) Seq() uint64 { return s.seq }

// NewSnapshot returns a snapshot of the current state.
func (d *DB) NewSnapshot() *Snapshot {
	return &Snapshot{seq: d.visibleSeq.Load()}
}

func (d *DB) readSeq(snap *Snapshot) uint64 {
	if snap != nil {
		return snap.seq
	}
	return d.visibleSeq.Load()
}

// Get returns the value of key and its version (the sequence number of the
// write that produced it), reading at snap, or at the latest state if snap
// is nil. It returns ErrNotFound if the key has no value there. The returned
// value is a copy owned by the caller.
func (d *DB) Get(key []byte, snap *Snapshot) (value []byte, version uint64, err error) {
	if d.closed.Load() {
		return nil, 0, ErrClosed
	}
	v, tr, ok := d.mem.Get(key, d.readSeq(snap))
	if !ok || tr.Kind() == ikey.KindDelete {
		return nil, 0, ErrNotFound
	}
	return bytes.Clone(v), tr.Seq(), nil
}

// LastSeq returns the sequence number of the last applied write, or 0 if
// nothing has been written.
func (d *DB) LastSeq() uint64 { return d.visibleSeq.Load() }

// Close closes the database. Later calls return ErrClosed; iterators that
// are already open keep working.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed.Swap(true) {
		return ErrClosed
	}
	return nil
}
