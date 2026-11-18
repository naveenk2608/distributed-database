// Package lsm is dkv's storage engine and the only storage API the rest of
// dkv uses.
//
// Writes go to a checksummed write-ahead log and a skiplist memtable. A full
// memtable becomes immutable, a new log segment starts, and a background
// goroutine flushes it to an SSTable; once the manifest names the table, the
// old segment is deleted. A second goroutine merges runs of similar-sized
// adjacent tables (size-tiered compaction, ADR 0005). Reads check the
// memtables and then the tables from newest to oldest, using each table's
// bloom filter, through a shared block cache.
//
// Every write gets a sequence number, and a key's version is the sequence
// number of the write that produced it. In standalone mode the engine
// assigns sequence numbers; in clustered mode the caller passes the Raft log
// index.
//
// Concurrency and lock order: qmu (commit queue) is never held while taking
// another lock. flushMu and compactMu serialize flushes and compactions and
// are taken before mu. mu guards the write path and every change to the
// manifest or the current read state; stateMu only guards the state
// pointer and is taken inside mu. snapMu guards the snapshot list. Readers
// take only stateMu, briefly, to reference the current state.
package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
	"github.com/naveenk2608/distributed-database/internal/storage/memtable"
	"github.com/naveenk2608/distributed-database/internal/storage/sstable"
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

	errStopping = errors.New("lsm: engine closing")
)

const (
	lockFileName = "LOCK"
	// recordBatch is the WAL record type of an encoded Batch.
	recordBatch uint8 = 1
	// maxGroupBytes caps how much one group commit carries, so a writer
	// that becomes leader does not wait behind an unbounded queue.
	maxGroupBytes = 1 << 20
)

// DB is a storage engine instance. It is safe for concurrent use.
type DB struct {
	opts Options
	dir  string
	lock io.Closer

	qmu   sync.Mutex // guards queue
	queue []*pendingWrite

	mu          sync.Mutex
	bgCond      *sync.Cond // on mu; signalled when a flush or compaction finishes
	lastSeq     uint64     // guarded by mu
	failed      error      // sticky; guarded by mu
	wal         *wal.Writer
	nextFileNum uint64 // guarded by mu
	flushedSeq  uint64 // guarded by mu
	logNum      uint64 // guarded by mu

	stateMu sync.RWMutex
	state   *readState // nil after Close

	// visibleSeq is the sequence number of the newest batch that is fully
	// in a memtable. Reads without a snapshot read at visibleSeq, so they
	// never observe part of a batch.
	visibleSeq atomic.Uint64
	closed     atomic.Bool

	snapMu    sync.Mutex
	snapshots map[*Snapshot]struct{}

	flushMu   sync.Mutex
	compactMu sync.Mutex

	stopCh    chan struct{} // closed by Close
	bgWG      sync.WaitGroup
	flushCh   chan struct{}
	compactCh chan struct{}

	stats engineStats
}

type engineStats struct {
	flushes, compactions               atomic.Int64
	flushBytes                         atomic.Int64
	compactReadBytes, compactWriteByte atomic.Int64
	stalls, slowdowns                  atomic.Int64
	// userBytes: keys and values applied; walBytes: records appended to
	// the log. With table bytes they give write amplification.
	userBytes, walBytes atomic.Int64
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

// OpenInMemory returns an empty database on a private in-memory filesystem:
// nothing survives Close. It is for tests and tools that need no
// durability; it runs the same code as a database on disk.
func OpenInMemory() *DB {
	d, err := Open("/db", Options{FS: vfs.NewMemFS(), Sync: SyncNever})
	if err != nil {
		panic(fmt.Sprintf("lsm: opening a fresh in-memory database failed: %v", err))
	}
	return d
}

// Open opens the database in dir, creating it if needed, and recovers every
// write the manifest, tables and log hold. If they are damaged in a way a
// crash cannot explain, Open returns an error matching ErrCorruption and
// changes nothing.
func Open(dir string, opts Options) (*DB, error) {
	opts = opts.withDefaults()
	fsys := opts.FS
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, fmt.Errorf("lsm: create %s: %w", dir, err)
	}
	// Fsync the parent so the engine directory's own entry survives a power
	// loss; otherwise everything in it could vanish with it.
	if err := fsys.SyncDir(filepath.Dir(filepath.Clean(dir))); err != nil {
		return nil, fmt.Errorf("lsm: sync parent of %s: %w", dir, err)
	}
	lock, err := fsys.Lock(filepath.Join(dir, lockFileName))
	if err != nil {
		return nil, fmt.Errorf("lsm: lock %s: %w", dir, err)
	}
	d := &DB{
		opts:      opts,
		dir:       dir,
		lock:      lock,
		snapshots: map[*Snapshot]struct{}{},
		stopCh:    make(chan struct{}),
		flushCh:   make(chan struct{}, 1),
		compactCh: make(chan struct{}, 1),
	}
	d.bgCond = sync.NewCond(&d.mu)
	if err := d.recover(); err != nil {
		_ = lock.Close() // the recovery error is the one worth reporting
		return nil, err
	}
	if !opts.manual {
		d.bgWG.Add(2)
		go d.flushLoop()
		go d.compactLoop()
		signal(d.compactCh) // the recovered tables may need compacting
	}
	if opts.Sync == SyncInterval && d.wal != nil {
		d.bgWG.Add(1)
		go d.syncLoop()
	}
	return d, nil
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (d *DB) syncLoop() {
	defer d.bgWG.Done()
	t := time.NewTicker(d.opts.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-d.stopCh:
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

// setFailed records a background error. Writes fail from then on; reads
// continue.
func (d *DB) setFailed(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failed == nil {
		d.failed = err
		d.opts.Logger.Error("storage engine failed; refusing further writes", "error", err)
	}
	d.bgCond.Broadcast()
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
	// makeRoom may release mu while it waits, but only the leader changes
	// lastSeq, so the numbers assigned above stay valid.
	if err := d.makeRoomLocked(); err != nil {
		fail(err)
		return
	}
	// Without a log (Options.DisableWAL) the memtable is the only copy
	// until a flush.
	if d.wal != nil {
		var n int64
		for _, r := range recs {
			n += int64(len(r.Payload))
		}
		d.stats.walBytes.Add(n)
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
	mem := d.state.mem
	var user int64
	for _, g := range ok {
		for _, op := range g.batch.ops {
			user += int64(len(op.key) + len(op.value))
			if err := mem.Add(g.out, op.kind, op.key, op.value); err != nil {
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
	d.stats.userBytes.Add(user)
	d.lastSeq = last
	d.visibleSeq.Store(last)
}

// makeRoomLocked makes sure the active memtable can take more writes:
// it rotates a full memtable, delays a write group once when there are many
// tables, and waits while too many memtables await a flush or too many
// tables await compaction. Caller holds d.mu, which is released while
// waiting.
func (d *DB) makeRoomLocked() error {
	delayed := false
	for {
		if d.closed.Load() {
			return ErrClosed
		}
		if d.failed != nil {
			return d.failed
		}
		s := d.state
		switch {
		case !d.opts.manual && !delayed && len(s.tables) >= d.opts.SlowdownTables:
			// Give compaction a millisecond rather than stopping writes
			// abruptly later.
			d.stats.slowdowns.Add(1)
			delayed = true
			d.mu.Unlock()
			time.Sleep(time.Millisecond)
			d.mu.Lock()
		case s.mem.ApproximateSize() < d.opts.MemtableBytes:
			return nil
		case !d.opts.manual && (len(s.imm) >= d.opts.MaxImmutableMemtables || len(s.tables) >= d.opts.StopTables):
			d.stats.stalls.Add(1)
			signal(d.flushCh)
			signal(d.compactCh)
			d.bgCond.Wait()
		default:
			return d.rotateLocked()
		}
	}
}

// rotateLocked makes the active memtable immutable and starts a new
// memtable and log segment. Caller holds d.mu.
func (d *DB) rotateLocked() error {
	s := d.state
	seg := s.memLog + 1 // without a log, memtables keep their numbering
	if d.wal != nil {
		var err error
		if seg, err = d.wal.Rotate(); err != nil {
			d.failed = fmt.Errorf("lsm: rotate log: %w", err)
			return d.failed
		}
	}
	imm := append(slices.Clone(s.imm), &immMem{mt: s.mem, logNum: s.memLog, maxSeq: d.lastSeq})
	d.installState(newReadState(memtable.New(), seg, imm, s.tables))
	signal(d.flushCh)
	return nil
}

// Snapshot is a read snapshot: reads through it see exactly the batches
// applied before it was taken. A live snapshot keeps compaction from
// discarding the versions it can see, so release it when done.
type Snapshot struct {
	seq uint64
	db  *DB
}

// Seq returns the snapshot's sequence number.
func (s *Snapshot) Seq() uint64 { return s.seq }

// Release lets compaction discard versions only this snapshot could see.
// Reads through a released snapshot may miss data. Release is idempotent.
func (s *Snapshot) Release() {
	if s.db == nil {
		return
	}
	s.db.snapMu.Lock()
	delete(s.db.snapshots, s)
	s.db.snapMu.Unlock()
}

// NewSnapshot returns a snapshot of the current state.
func (d *DB) NewSnapshot() *Snapshot {
	d.snapMu.Lock()
	defer d.snapMu.Unlock()
	s := &Snapshot{seq: d.visibleSeq.Load(), db: d}
	d.snapshots[s] = struct{}{}
	return s
}

// smallestSnapshot returns the oldest sequence number any reader may still
// need: the oldest live snapshot, or the latest state if there is none.
func (d *DB) smallestSnapshot() uint64 {
	d.snapMu.Lock()
	defer d.snapMu.Unlock()
	seq := d.visibleSeq.Load()
	for s := range d.snapshots {
		seq = min(seq, s.seq)
	}
	return seq
}

// readSeq returns the sequence number a read uses. It must be loaded
// before the state: every batch at or below it is in a memtable or table of
// any state published since.
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
	seq := d.readSeq(snap)
	s := d.loadState()
	if s == nil {
		return nil, 0, ErrClosed
	}
	defer s.unref()
	result := func(v []byte, tr ikey.Trailer) ([]byte, uint64, error) {
		if tr.Kind() == ikey.KindDelete {
			return nil, 0, ErrNotFound
		}
		return bytes.Clone(v), tr.Seq(), nil
	}
	// Memtables and tables hold disjoint, newest-first sequence ranges, so
	// the first version found is the newest one visible at seq.
	if v, tr, ok := s.mem.Get(key, seq); ok {
		return result(v, tr)
	}
	for i := len(s.imm) - 1; i >= 0; i-- {
		if v, tr, ok := s.imm[i].mt.Get(key, seq); ok {
			return result(v, tr)
		}
	}
	for _, t := range s.tables {
		if t.meta.minSeq > seq {
			continue // every entry is newer than the read
		}
		v, tr, ok, err := t.reader.Get(key, seq)
		if err != nil {
			return nil, 0, readError(err)
		}
		if ok {
			return result(v, tr)
		}
	}
	return nil, 0, ErrNotFound
}

func readError(err error) error {
	if errors.Is(err, sstable.ErrCorrupt) {
		return fmt.Errorf("%w: %w", ErrCorruption, err)
	}
	return err
}

// LastSeq returns the sequence number of the last applied write, or 0 if
// nothing has been written.
func (d *DB) LastSeq() uint64 { return d.visibleSeq.Load() }

// Close stops background work, syncs and closes the log, and releases the
// directory lock. Later calls return ErrClosed; iterators that are already
// open keep working until they are closed.
func (d *DB) Close() error {
	if d.closed.Swap(true) {
		return ErrClosed
	}
	close(d.stopCh)
	d.mu.Lock()
	d.bgCond.Broadcast() // stalled writers return ErrClosed
	d.mu.Unlock()
	d.bgWG.Wait()

	d.mu.Lock()
	defer d.mu.Unlock()
	var err error
	if d.wal != nil {
		err = d.wal.Close()
	}
	if d.failed != nil {
		err = nil // already reported to the writers that hit it
	}
	d.stateMu.Lock()
	s := d.state
	d.state = nil
	d.stateMu.Unlock()
	s.unref()
	if lerr := d.lock.Close(); lerr != nil && err == nil {
		err = fmt.Errorf("lsm: release lock: %w", lerr)
	}
	return err
}

// Metrics describes the engine's current state and activity.
type Metrics struct {
	LastSeq, FlushedSeq  uint64
	Memtables            int // active plus immutable
	MemtableBytes        int64
	Tables               int
	TableBytes           uint64
	TableEntries         uint64
	Flushes, Compactions int64
	FlushBytes           int64
	CompactionBytesRead  int64
	CompactionBytesWrite int64
	WriteStalls          int64
	WriteSlowdowns       int64
	// UserBytes counts the keys and values applied, WALBytes the bytes
	// appended to the write-ahead log (0 with DisableWAL).
	UserBytes, WALBytes int64
	LiveSnapshots       int
	BlockCache          sstable.CacheStats
}

// Metrics returns a snapshot of engine metrics.
func (d *DB) Metrics() Metrics {
	m := Metrics{
		LastSeq:              d.visibleSeq.Load(),
		Flushes:              d.stats.flushes.Load(),
		Compactions:          d.stats.compactions.Load(),
		FlushBytes:           d.stats.flushBytes.Load(),
		CompactionBytesRead:  d.stats.compactReadBytes.Load(),
		CompactionBytesWrite: d.stats.compactWriteByte.Load(),
		WriteStalls:          d.stats.stalls.Load(),
		WriteSlowdowns:       d.stats.slowdowns.Load(),
		UserBytes:            d.stats.userBytes.Load(),
		WALBytes:             d.stats.walBytes.Load(),
		BlockCache:           d.opts.BlockCache.Stats(),
	}
	d.mu.Lock()
	m.FlushedSeq = d.flushedSeq
	d.mu.Unlock()
	d.snapMu.Lock()
	m.LiveSnapshots = len(d.snapshots)
	d.snapMu.Unlock()
	if s := d.loadState(); s != nil {
		m.Memtables = 1 + len(s.imm)
		m.MemtableBytes = s.mem.ApproximateSize()
		for _, im := range s.imm {
			m.MemtableBytes += im.mt.ApproximateSize()
		}
		m.Tables = len(s.tables)
		for _, t := range s.tables {
			m.TableBytes += t.meta.size
			m.TableEntries += t.meta.entries
		}
		s.unref()
	}
	return m
}
