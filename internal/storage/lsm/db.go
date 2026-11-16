// Package lsm is dkv's storage engine and the only storage API the rest of
// dkv uses.
//
// Phase 2 makes the engine durable: every batch is written to a segmented,
// checksummed write-ahead log before it is acknowledged, and Open replays
// the log after a crash. Memtables rotate together with log segments, but
// nothing is flushed to SSTables until Phase 3, so the log and the memtables
// grow without bound for now (see docs/PROGRESS.md).
//
// Every write gets a sequence number, and a key's version is the sequence
// number of the write that produced it. In standalone mode the engine
// assigns sequence numbers; in clustered mode the caller passes the Raft log
// index.
//
// Lock order: qmu (commit queue) is never held while taking mu; mu is held
// by the group leader for the whole commit and by Close.
package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
	"github.com/naveenk2608/distributed-database/internal/storage/memtable"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
	"github.com/naveenk2608/distributed-database/internal/storage/wal"
)

// Errors returned by DB methods. Check them with errors.Is.
var (
	ErrNotFound        = errors.New("lsm: key not found")
	ErrClosed          = errors.New("lsm: database closed")
	ErrEmptyBatch      = errors.New("lsm: empty batch")
	ErrSeqNotIncreased = errors.New("lsm: sequence number must exceed the last applied one")
	ErrSeqTooLarge     = errors.New("lsm: sequence number exceeds the maximum")
	// ErrCorruption means the on-disk state is damaged in a way a crash
	// cannot explain. Open refuses to start rather than guess.
	ErrCorruption = errors.New("lsm: data corruption")
)

// SyncPolicy says when the engine fsyncs the log. See docs/PLAN.md, "Sync
// policy".
type SyncPolicy int

const (
	// SyncAlways fsyncs before acknowledging a write. Concurrent writes
	// share one fsync through group commit. This is the default and the
	// only policy that never loses an acknowledged write.
	SyncAlways SyncPolicy = iota
	// SyncInterval fsyncs every Options.SyncInterval. A power loss can lose
	// writes acknowledged in the last interval; standalone mode only.
	SyncInterval
	// SyncNever leaves fsync to the operating system (and to Close). It
	// exists to measure what fsync costs.
	SyncNever
)

func (p SyncPolicy) String() string {
	switch p {
	case SyncAlways:
		return "always"
	case SyncInterval:
		return "interval"
	case SyncNever:
		return "never"
	default:
		return fmt.Sprintf("SyncPolicy(%d)", int(p))
	}
}

// Options configures Open. The zero value is valid.
type Options struct {
	// FS is the filesystem; nil means the operating system's.
	FS vfs.FS
	// MemtableBytes is the size at which the active memtable becomes
	// immutable and a new log segment starts. 0 means 4 MiB.
	MemtableBytes int64
	// Sync is the sync policy.
	Sync SyncPolicy
	// SyncInterval is the fsync period for SyncInterval. 0 means 10ms.
	SyncInterval time.Duration
	// Logger receives recovery and background-error messages; nil discards.
	Logger *slog.Logger
}

func (o Options) withDefaults() Options {
	if o.FS == nil {
		o.FS = vfs.Default
	}
	if o.MemtableBytes <= 0 {
		o.MemtableBytes = 4 << 20
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = 10 * time.Millisecond
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	return o
}

const (
	lockFileName = "LOCK"
	// recordBatch is the WAL record type of an encoded Batch.
	recordBatch uint8 = 1
	// maxGroupBytes caps how much one group commit carries, so a writer
	// that becomes leader does not wait behind an unbounded queue.
	maxGroupBytes = 1 << 20
)

// memView is an immutable snapshot of the engine's memtables. A new view is
// published when the active memtable rotates.
type memView struct {
	mem *memtable.Memtable   // active
	imm []*memtable.Memtable // immutable, oldest first
}

// DB is a storage engine instance. It is safe for concurrent use.
type DB struct {
	opts Options
	dir  string    // "" for an in-memory DB
	lock io.Closer // nil for an in-memory DB

	qmu   sync.Mutex // guards queue
	queue []*pendingWrite

	mu      sync.Mutex  // held by the group leader during a commit, and by Close
	lastSeq uint64      // guarded by mu
	failed  error       // sticky; guarded by mu
	wal     *wal.Writer // nil for an in-memory DB

	view atomic.Pointer[memView]
	// visibleSeq is the sequence number of the newest batch that is fully
	// in a memtable. Reads without a snapshot read at visibleSeq, so they
	// never observe part of a batch.
	visibleSeq atomic.Uint64
	closed     atomic.Bool

	syncStop chan struct{} // closed by Close to stop the SyncInterval goroutine
	syncDone chan struct{}
}

// pendingWrite is one Apply call waiting in the commit queue.
type pendingWrite struct {
	batch *Batch
	seq   uint64 // requested sequence number; 0 = engine assigns
	cond  sync.Cond
	done  bool
	out   uint64 // assigned sequence number
	err   error
}

// OpenInMemory returns an empty database without a log: nothing survives
// Close. It is for tests and tools that do not need durability.
func OpenInMemory() *DB {
	d := &DB{opts: Options{}.withDefaults()}
	d.view.Store(&memView{mem: memtable.New()})
	return d
}

// Open opens the database in dir, creating it if needed, and recovers every
// write the log holds. If the log is damaged in a way a crash cannot
// explain, Open returns an error matching ErrCorruption and changes nothing.
func Open(dir string, opts Options) (*DB, error) {
	opts = opts.withDefaults()
	fs := opts.FS
	if err := fs.MkdirAll(dir); err != nil {
		return nil, fmt.Errorf("lsm: create %s: %w", dir, err)
	}
	// Fsync the parent so the engine directory's own entry survives a power
	// loss; otherwise everything in it could vanish with it.
	if err := fs.SyncDir(filepath.Dir(filepath.Clean(dir))); err != nil {
		return nil, fmt.Errorf("lsm: sync parent of %s: %w", dir, err)
	}
	lock, err := fs.Lock(filepath.Join(dir, lockFileName))
	if err != nil {
		return nil, fmt.Errorf("lsm: lock %s: %w", dir, err)
	}
	d := &DB{opts: opts, dir: dir, lock: lock}
	if err := d.recover(); err != nil {
		_ = lock.Close() // the recovery error is the one worth reporting
		return nil, err
	}
	if opts.Sync == SyncInterval {
		d.syncStop = make(chan struct{})
		d.syncDone = make(chan struct{})
		go d.syncLoop()
	}
	return d, nil
}

// recover replays the log into a fresh memtable and starts a new segment.
func (d *DB) recover() error {
	mem := memtable.New()
	var last uint64
	res, err := wal.Recover(d.opts.FS, d.dir, func(_ uint64, r wal.Record) error {
		if r.Type != recordBatch {
			return fmt.Errorf("%w: unknown record type %d", ErrCorruption, r.Type)
		}
		seq, ops, err := decodeBatch(r.Payload)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrCorruption, err)
		}
		if seq <= last || seq > ikey.MaxSeq {
			return fmt.Errorf("%w: sequence number %d after %d", ErrCorruption, seq, last)
		}
		for _, op := range ops {
			if err := mem.Add(seq, op.kind, op.key, op.value); err != nil {
				return fmt.Errorf("%w: %w", ErrCorruption, err)
			}
		}
		last = seq
		return nil
	})
	if errors.Is(err, wal.ErrCorrupt) {
		err = fmt.Errorf("%w: %w", ErrCorruption, err)
	}
	if err != nil {
		if errors.Is(err, ErrCorruption) {
			return fmt.Errorf("lsm: open %s: refusing to start: %w", d.dir, err)
		}
		return fmt.Errorf("lsm: open %s: %w", d.dir, err)
	}
	if res.TornBytes > 0 {
		d.opts.Logger.Warn("truncated a torn write at the end of the log",
			"segment", wal.SegmentPath(d.dir, res.Segments[len(res.Segments)-1]),
			"offset", res.TornOffset, "bytes", res.TornBytes, "reason", res.TornReason)
	}
	next := uint64(1)
	if n := len(res.Segments); n > 0 {
		next = res.Segments[n-1] + 1
	}
	w, err := wal.NewWriter(d.opts.FS, d.dir, next)
	if err != nil {
		return fmt.Errorf("lsm: open %s: %w", d.dir, err)
	}
	d.wal = w
	d.lastSeq = last
	d.visibleSeq.Store(last)
	d.view.Store(&memView{mem: mem})
	d.opts.Logger.Debug("recovered", "dir", d.dir, "records", res.Records,
		"segments", len(res.Segments), "last_seq", last)
	return nil
}

func (d *DB) syncLoop() {
	defer close(d.syncDone)
	t := time.NewTicker(d.opts.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-d.syncStop:
			return
		case <-t.C:
			// A failed sync makes the WAL writer unusable, so the next
			// commit reports the error to its writers.
			if err := d.wal.Sync(); err != nil && !errors.Is(err, wal.ErrClosed) {
				d.opts.Logger.Error("background log sync failed", "error", err)
			}
		}
	}
}

// Apply applies every operation in b atomically at one sequence number and
// returns it; that number becomes the version of every key the batch
// writes. If seq is 0 the engine uses the next sequence number; otherwise seq
// is used and must be greater than every sequence number applied before
// (gaps are allowed, so a caller can pass Raft log indexes). See
// docs/adr/0018-batch-sequence-numbers.md.
//
// With SyncAlways, Apply returns only after the batch is durable.
// Concurrent calls are committed in groups: each batch is its own log
// record, and one fsync covers the whole group.
func (d *DB) Apply(b *Batch, seq uint64) (uint64, error) {
	if b.Count() == 0 {
		return 0, ErrEmptyBatch
	}
	if seq > ikey.MaxSeq {
		return 0, fmt.Errorf("%w: %d", ErrSeqTooLarge, seq)
	}
	if d.closed.Load() {
		return 0, ErrClosed
	}

	w := &pendingWrite{batch: b, seq: seq}
	w.cond.L = &d.qmu
	d.qmu.Lock()
	d.queue = append(d.queue, w)
	for !w.done && d.queue[0] != w {
		w.cond.Wait()
	}
	if w.done {
		d.qmu.Unlock()
		return w.out, w.err
	}
	// w leads the next group: every queued write up to the size cap.
	n, size := 1, b.bytes
	for n < len(d.queue) && size+d.queue[n].batch.bytes <= maxGroupBytes {
		size += d.queue[n].batch.bytes
		n++
	}
	group := slices.Clone(d.queue[:n])
	d.qmu.Unlock()

	d.commit(group)

	d.qmu.Lock()
	for _, g := range group[1:] {
		g.done = true
		g.cond.Signal()
	}
	w.done = true
	d.queue = slices.Delete(d.queue, 0, n)
	if len(d.queue) > 0 {
		d.queue[0].cond.Signal() // the next leader
	}
	d.qmu.Unlock()
	return w.out, w.err
}

// commit writes a group of batches to the log, each as its own record, syncs
// once if the policy says so, and then applies them to the memtable in
// order. Only one group commits at a time.
func (d *DB) commit(group []*pendingWrite) {
	d.mu.Lock()
	defer d.mu.Unlock()
	fail := func(err error) {
		for _, g := range group {
			if g.err == nil {
				g.err = err
			}
		}
	}
	if d.closed.Load() {
		fail(ErrClosed)
		return
	}
	if d.failed != nil {
		fail(d.failed)
		return
	}

	// Assign sequence numbers in queue order. A write with a bad requested
	// number fails alone; the rest of the group proceeds.
	last := d.lastSeq
	ok := group[:0:0]
	var recs []wal.Record
	for _, g := range group {
		s := g.seq
		if s == 0 {
			s = last + 1
		} else if s <= last {
			g.err = fmt.Errorf("%w: got %d, last applied %d", ErrSeqNotIncreased, s, last)
			continue
		}
		if s > ikey.MaxSeq {
			g.err = fmt.Errorf("%w: %d", ErrSeqTooLarge, s)
			continue
		}
		g.out, last = s, s
		ok = append(ok, g)
		if d.wal != nil {
			recs = append(recs, wal.Record{Type: recordBatch, Payload: g.batch.encode(nil, s)})
		}
	}
	if len(ok) == 0 {
		return
	}

	v := d.view.Load()
	if v.mem.ApproximateSize() >= d.opts.MemtableBytes {
		if err := d.rotateLocked(v); err != nil {
			d.failed = fmt.Errorf("lsm: rotate memtable: %w", err)
			fail(d.failed)
			return
		}
		v = d.view.Load()
	}

	if d.wal != nil {
		if err := d.wal.Append(recs...); err != nil {
			d.failed = err
			fail(err)
			return
		}
		if d.opts.Sync == SyncAlways {
			if err := d.wal.Sync(); err != nil {
				d.failed = err
				fail(err)
				return
			}
		}
	}
	for _, g := range ok {
		for _, op := range g.batch.ops {
			if err := v.mem.Add(g.out, op.kind, op.key, op.value); err != nil {
				// Unreachable while sequence numbers are fresh and a batch
				// holds each key once. If it happens anyway, part of a batch
				// is in the memtable but invisible; refuse further writes
				// so a later batch can never expose it.
				d.failed = fmt.Errorf("lsm: internal error applying batch at seq %d: %w", g.out, err)
				fail(d.failed)
				return
			}
		}
	}
	d.lastSeq = last
	d.visibleSeq.Store(last)
}

// rotateLocked makes the active memtable immutable and starts a new
// memtable and log segment. Caller holds d.mu.
func (d *DB) rotateLocked(v *memView) error {
	if d.wal != nil {
		if _, err := d.wal.Rotate(); err != nil {
			return err
		}
	}
	imm := append(slices.Clone(v.imm), v.mem)
	d.view.Store(&memView{mem: memtable.New(), imm: imm})
	return nil
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

// readState returns the sequence number to read at and the memtables to
// read. The sequence number is loaded first: every batch at or below it was
// in a memtable of a view published no later than the sequence number, and
// views only ever gain memtables, so the view loaded next contains it.
func (d *DB) readState(snap *Snapshot) (uint64, *memView) {
	seq := d.visibleSeq.Load()
	if snap != nil {
		seq = snap.seq
	}
	return seq, d.view.Load()
}

// Get returns the value of key and its version (the sequence number of the
// write that produced it), reading at snap, or at the latest state if snap
// is nil. It returns ErrNotFound if the key has no value there. The returned
// value is a copy owned by the caller.
func (d *DB) Get(key []byte, snap *Snapshot) (value []byte, version uint64, err error) {
	if d.closed.Load() {
		return nil, 0, ErrClosed
	}
	seq, v := d.readState(snap)
	// The active memtable holds the newest batches, then each immutable one
	// from newest to oldest; the first version found is the newest.
	v2, tr, ok := v.mem.Get(key, seq)
	for i := len(v.imm) - 1; !ok && i >= 0; i-- {
		v2, tr, ok = v.imm[i].Get(key, seq)
	}
	if !ok || tr.Kind() == ikey.KindDelete {
		return nil, 0, ErrNotFound
	}
	return bytes.Clone(v2), tr.Seq(), nil
}

// LastSeq returns the sequence number of the last applied write, or 0 if
// nothing has been written.
func (d *DB) LastSeq() uint64 { return d.visibleSeq.Load() }

// Close waits for an in-flight commit, syncs and closes the log, and
// releases the directory lock. Later calls return ErrClosed; iterators that
// are already open keep working.
func (d *DB) Close() error {
	if d.closed.Swap(true) {
		return ErrClosed
	}
	if d.syncStop != nil {
		close(d.syncStop)
		<-d.syncDone
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	var err error
	if d.wal != nil {
		err = d.wal.Close()
		if d.failed != nil {
			err = nil // already reported to the writers that hit it
		}
	}
	if d.lock != nil {
		if lerr := d.lock.Close(); lerr != nil && err == nil {
			err = fmt.Errorf("lsm: release lock: %w", lerr)
		}
	}
	return err
}
