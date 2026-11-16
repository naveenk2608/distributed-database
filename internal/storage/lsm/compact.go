package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
	"github.com/naveenk2608/distributed-database/internal/storage/sstable"
)

// Size-tiered compaction over a time-ordered list of tables (ADR 0005).
//
// Tables are kept newest first, and their sequence ranges never overlap, so
// a read can stop at the first table holding a visible version of a key.
// Merging only adjacent tables keeps that property: the output covers the
// union of the inputs' ranges and takes their place in the list.

// compaction is a run of adjacent tables to merge into one.
type compaction struct {
	inputs []*table // contiguous in the state's table list, newest first
	// bottom is true when the run includes the oldest table, so nothing
	// older exists below the output and tombstones can be dropped.
	bottom bool
	reason string
}

// pickCompaction chooses the next compaction for tables (newest first), or
// nil. Caller holds d.mu.
func (d *DB) pickCompaction(tables []*table) *compaction {
	n := len(tables)
	minT, maxT := d.opts.CompactionMinTables, d.opts.CompactionMaxTables
	if n < minT {
		return nil
	}
	// Tables smaller than a quarter memtable (a manual flush, a startup
	// flush) count as that size, so they merge with their neighbours
	// instead of splitting runs forever.
	floor := uint64(max(d.opts.MemtableBytes/4, 1))
	size := func(i int) uint64 { return max(tables[i].meta.size, floor) }
	ratio := d.opts.CompactionSizeRatio

	// 1. The newest run of at least minT adjacent tables of similar size.
	for i := 0; i+minT <= n; i++ {
		lo, hi := size(i), size(i)
		j := i + 1
		for ; j < n && j-i < maxT; j++ {
			s := size(j)
			nlo, nhi := min(lo, s), max(hi, s)
			if float64(nhi) > ratio*float64(nlo) {
				break
			}
			lo, hi = nlo, nhi
		}
		if j-i >= minT {
			return &compaction{inputs: tables[i:j], bottom: j == n, reason: "similar sizes"}
		}
	}
	// 2. Too many tables for reads to stay cheap: merge the newest ones
	// whatever their sizes.
	if n >= d.opts.SlowdownTables {
		k := min(n, maxT)
		return &compaction{inputs: tables[:k], bottom: k == n, reason: "too many tables"}
	}
	// 3. Space amplification: more data above the oldest table than in it.
	// Merging everything bounds the space taken by overwritten versions and
	// tombstones, which only a bottom compaction can drop.
	var above uint64
	for i := range n - 1 {
		above += tables[i].meta.size
	}
	if above > tables[n-1].meta.size {
		return &compaction{inputs: tables, bottom: true, reason: "space amplification"}
	}
	return nil
}

func (d *DB) compactLoop() {
	defer d.bgWG.Done()
	for {
		select {
		case <-d.stopCh:
			return
		case <-d.compactCh:
		}
		for {
			more, err := d.compactOnce()
			if errors.Is(err, errStopping) {
				return
			}
			if err != nil {
				d.setFailed(err)
				break
			}
			if !more {
				break
			}
		}
	}
}

// compactOnce runs one compaction if there is one to run. It reports
// whether it ran one.
func (d *DB) compactOnce() (bool, error) {
	return d.compact(func(tables []*table) *compaction { return d.pickCompaction(tables) })
}

// compactAll merges every table into one bottom table, dropping every
// version and tombstone no live snapshot can see. Tests and maintenance use
// it.
func (d *DB) compactAll() (bool, error) {
	return d.compact(func(tables []*table) *compaction {
		if len(tables) == 0 {
			return nil
		}
		return &compaction{inputs: tables, bottom: true, reason: "manual"}
	})
}

func (d *DB) compact(pick func([]*table) *compaction) (bool, error) {
	d.compactMu.Lock()
	defer d.compactMu.Unlock()

	d.mu.Lock()
	s := d.state
	if d.failed != nil || s == nil {
		d.mu.Unlock()
		return false, nil
	}
	c := pick(s.tables)
	if c == nil {
		d.mu.Unlock()
		return false, nil
	}
	s.refs.Add(1) // keeps the inputs open while they are read
	fileNum := d.nextFileNum
	d.nextFileNum++
	d.mu.Unlock()
	defer s.unref()

	if pool := d.opts.CompactionPool; pool != nil {
		select {
		case pool.slots <- struct{}{}:
			defer func() { <-pool.slots }()
		case <-d.stopCh:
			return false, errStopping
		}
	}

	smallestSnap := d.smallestSnapshot()
	var read int64
	for _, t := range c.inputs {
		read += int64(t.meta.size)
	}
	out, err := d.buildTable(fileNum, func(w *sstable.Writer) error {
		return d.mergeInto(w, c, smallestSnap)
	})
	if err != nil {
		if errors.Is(err, errStopping) {
			return false, errStopping
		}
		return false, fmt.Errorf("lsm: compaction (%s): %w", c.reason, err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	cur := d.state
	if cur == nil {
		closeTable(out)
		return false, errStopping
	}
	// Flushes only add tables at the newest end and only this function
	// replaces tables, so the inputs are still a contiguous run.
	at := -1
	for i, t := range cur.tables {
		if t == c.inputs[0] {
			at = i
			break
		}
	}
	if at < 0 || at+len(c.inputs) > len(cur.tables) {
		closeTable(out)
		return false, errors.New("lsm: internal error: compaction inputs left the table list")
	}
	for k, t := range c.inputs {
		if cur.tables[at+k] != t {
			closeTable(out)
			return false, errors.New("lsm: internal error: compaction inputs are no longer adjacent")
		}
	}
	tables := make([]*table, 0, len(cur.tables)-len(c.inputs)+1)
	tables = append(tables, cur.tables[:at]...)
	if out != nil {
		tables = append(tables, out)
	}
	tables = append(tables, cur.tables[at+len(c.inputs):]...)
	mf := manifest{nextFileNum: d.nextFileNum, logNum: d.logNum, flushedSeq: d.flushedSeq, tables: tableMetas(tables)}
	if err := writeManifest(d.opts.FS, d.dir, &mf); err != nil {
		// Delete nothing: if the rename reached the disk, the inputs are
		// obsolete and the output is live, and the next Open sorts it out
		// from whichever manifest it finds.
		closeTable(out)
		return false, fmt.Errorf("lsm: compaction: %w", err)
	}
	for _, t := range c.inputs {
		t.obsolete.Store(true) // deleted when the last reader lets go
	}
	d.installState(newReadState(cur.mem, cur.memLog, cur.imm, tables))
	d.stats.compactions.Add(1)
	d.stats.compactReadBytes.Add(read)
	if out != nil {
		d.stats.compactWriteByte.Add(int64(out.meta.size))
	}
	d.bgCond.Broadcast()
	signal(d.compactCh) // there may be more to do
	return true, nil
}

// mergeInto writes the merged contents of c's inputs to w, dropping what no
// reader can see:
//
//   - a version is dropped if a newer version of the same key is visible to
//     every live snapshot (its sequence number is <= the oldest snapshot);
//   - a tombstone at or below the oldest snapshot is dropped if the output
//     is the bottom of the engine, because then nothing older can be hidden
//     by it.
//
// This is LevelDB's rule. It is conservative: it keeps every version newer
// than the oldest snapshot, even ones no snapshot needs.
func (d *DB) mergeInto(w *sstable.Writer, c *compaction, smallestSnap uint64) error {
	iters := make([]internalIterator, len(c.inputs))
	for i, t := range c.inputs {
		iters[i] = t.reader.NewIter()
	}
	m := newMergingIter(iters...)
	var (
		lastKey        []byte
		hasLast        bool
		lastSeqForKey  uint64
		written, count int64
		rate           = d.opts.CompactionBytesPerSec
		start          = time.Now()
	)
	for m.First(); m.Valid(); m.Next() {
		k := m.Key()
		if !hasLast || !bytes.Equal(k.UserKey, lastKey) {
			lastKey = append(lastKey[:0], k.UserKey...)
			hasLast = true
			lastSeqForKey = math.MaxUint64 // no newer version of this key yet
		}
		drop := lastSeqForKey <= smallestSnap ||
			(c.bottom && k.Kind() == ikey.KindDelete && k.Seq() <= smallestSnap)
		lastSeqForKey = k.Seq()
		if !drop {
			if err := w.Add(k, m.Value()); err != nil {
				return err
			}
			written += int64(len(k.UserKey) + len(m.Value()) + 8)
		}
		if count++; count%256 == 0 {
			select {
			case <-d.stopCh:
				return errStopping
			default:
			}
			if rate > 0 {
				// Stay at or below rate bytes per second.
				ahead := time.Duration(float64(written)/float64(rate)*float64(time.Second)) - time.Since(start)
				if ahead > 0 {
					select {
					case <-d.stopCh:
						return errStopping
					case <-time.After(ahead):
					}
				}
			}
		}
	}
	return m.Err()
}
