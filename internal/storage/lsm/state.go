package lsm

import (
	"errors"
	"io/fs"
	"sync/atomic"

	"github.com/naveenk2608/distributed-database/internal/storage/memtable"
	"github.com/naveenk2608/distributed-database/internal/storage/sstable"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

// table is one open SSTable. It is reference counted: every readState that
// lists it holds one reference. When the count drops to zero the file is
// closed, and deleted if a compaction has made the table obsolete. So a
// reader that still holds an old state can keep reading tables that newer
// states no longer contain.
type table struct {
	db       *DB
	meta     tableMeta
	file     vfs.File
	reader   *sstable.Reader
	refs     atomic.Int32
	obsolete atomic.Bool
}

func (t *table) ref() { t.refs.Add(1) }

func (t *table) unref() {
	if t.refs.Add(-1) != 0 {
		return
	}
	t.reader.EvictFromCache()
	_ = t.file.Close() // read-only use at this point; nothing to lose
	if t.obsolete.Load() {
		path := tablePath(t.db.dir, t.meta.fileNum)
		// No directory fsync: if the removal is lost in a crash, the file is
		// an orphan that the next Open deletes.
		if err := t.db.opts.FS.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.db.opts.Logger.Warn("removing obsolete table", "path", path, "error", err)
		}
	}
}

// immMem is a memtable waiting to be flushed.
type immMem struct {
	mt     *memtable.Memtable
	logNum uint64 // the log segment its writes went to
	maxSeq uint64 // newest sequence number it holds
}

// readState is an immutable view of everything a read consults. Readers
// take a reference for as long as they read; the DB holds one on the
// current state.
type readState struct {
	refs   atomic.Int32
	mem    *memtable.Memtable // active
	memLog uint64             // log segment of mem
	imm    []*immMem          // oldest first
	tables []*table           // newest first; sequence ranges do not overlap
}

func newReadState(mem *memtable.Memtable, memLog uint64, imm []*immMem, tables []*table) *readState {
	s := &readState{mem: mem, memLog: memLog, imm: imm, tables: tables}
	s.refs.Store(1)
	for _, t := range tables {
		t.ref()
	}
	return s
}

func (s *readState) unref() {
	if s.refs.Add(-1) == 0 {
		for _, t := range s.tables {
			t.unref()
		}
	}
}

// loadState returns the current state with a reference the caller must
// drop with unref, or nil after Close.
func (d *DB) loadState() *readState {
	d.stateMu.RLock()
	defer d.stateMu.RUnlock()
	s := d.state
	if s != nil {
		s.refs.Add(1)
	}
	return s
}

// installState publishes s as the current state and drops the DB's
// reference to the previous one. Caller holds d.mu.
func (d *DB) installState(s *readState) {
	d.stateMu.Lock()
	old := d.state
	d.state = s
	d.stateMu.Unlock()
	if old != nil {
		old.unref()
	}
}

func tableMetas(tables []*table) []tableMeta {
	out := make([]tableMeta, len(tables))
	for i, t := range tables {
		out[i] = t.meta
	}
	return out
}
