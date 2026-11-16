package lsm

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/naveenk2608/distributed-database/internal/storage/memtable"
	"github.com/naveenk2608/distributed-database/internal/storage/sstable"
)

func (d *DB) flushLoop() {
	defer d.bgWG.Done()
	for {
		select {
		case <-d.stopCh:
			return
		case <-d.flushCh:
		}
		for {
			more, err := d.flushOne()
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

// Flush writes every memtable that holds data to a table: the active one is
// made immutable first. When Flush returns, every batch applied before the
// call is in a table named by a durable manifest.
func (d *DB) Flush() error {
	if d.closed.Load() {
		return ErrClosed
	}
	d.mu.Lock()
	if d.failed != nil {
		err := d.failed
		d.mu.Unlock()
		return err
	}
	if d.state == nil {
		d.mu.Unlock()
		return ErrClosed
	}
	if d.state.mem.Len() > 0 {
		if err := d.rotateLocked(); err != nil {
			d.mu.Unlock()
			return err
		}
	}
	d.mu.Unlock()
	for {
		more, err := d.flushOne()
		if err != nil {
			d.setFailed(err)
			return err
		}
		if !more {
			return nil
		}
	}
}

// flushOne flushes the oldest immutable memtable. It reports whether it
// flushed one.
func (d *DB) flushOne() (bool, error) {
	d.flushMu.Lock()
	defer d.flushMu.Unlock()

	d.mu.Lock()
	s := d.state
	if d.failed != nil || s == nil || len(s.imm) == 0 {
		d.mu.Unlock()
		return false, nil
	}
	m := s.imm[0]
	fileNum := d.nextFileNum
	d.nextFileNum++
	d.mu.Unlock()

	// The slow part runs without mu, so writes continue into the active
	// memtable meanwhile.
	t, err := d.buildTable(fileNum, memtableFill(m.mt))
	if err != nil {
		return false, fmt.Errorf("lsm: flush: %w", err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	s = d.state
	if s == nil || len(s.imm) == 0 || s.imm[0] != m {
		// Only flushOne removes immutable memtables, and flushMu is held,
		// so this means the engine closed underneath us.
		closeTable(t)
		return false, ErrClosed
	}
	tables := s.tables
	if t != nil {
		tables = append([]*table{t}, s.tables...)
	}
	logNum := s.memLog
	if len(s.imm) > 1 {
		logNum = s.imm[1].logNum
	}
	mf := manifest{nextFileNum: d.nextFileNum, logNum: logNum, flushedSeq: m.maxSeq, tables: tableMetas(tables)}
	if err := writeManifest(d.opts.FS, d.dir, &mf); err != nil {
		// Do not delete the new table: if the rename reached the disk, the
		// manifest there names it. A table no manifest names is removed at
		// the next Open.
		closeTable(t)
		return false, fmt.Errorf("lsm: flush: %w", err)
	}
	d.flushedSeq, d.logNum = m.maxSeq, logNum
	d.installState(newReadState(s.mem, s.memLog, slices.Clone(s.imm[1:]), tables))
	d.stats.flushes.Add(1)
	if t != nil {
		d.stats.flushBytes.Add(int64(t.meta.size))
	}
	d.bgCond.Broadcast()
	d.removeObsoleteLogs(logNum)
	signal(d.compactCh)
	return true, nil
}

// closeTable closes a table that never made it into a state, without
// deleting its file.
func closeTable(t *table) {
	if t != nil {
		t.ref()
		t.unref()
	}
}

func memtableFill(mt *memtable.Memtable) func(*sstable.Writer) error {
	return func(w *sstable.Writer) error {
		it := mt.NewIter()
		for it.First(); it.Valid(); it.Next() {
			if err := w.Add(it.Key(), it.Value()); err != nil {
				return err
			}
		}
		return nil
	}
}

// buildTable writes table fileNum with fill, makes it durable and opens it.
// It returns nil if fill added nothing.
func (d *DB) buildTable(fileNum uint64, fill func(*sstable.Writer) error) (*table, error) {
	fsys := d.opts.FS
	path := tablePath(d.dir, fileNum)
	f, err := fsys.Create(path)
	if err != nil {
		return nil, fmt.Errorf("lsm: create %s: %w", path, err)
	}
	discard := func() {
		_ = f.Close()
		_ = fsys.Remove(path) // best effort: the next Open removes leftovers
	}
	w := sstable.NewWriter(f, sstable.WriterOptions{BlockSize: d.opts.BlockSize, BloomBitsPerKey: d.opts.BloomBitsPerKey})
	if err := fill(w); err != nil {
		discard()
		return nil, err
	}
	if w.NumEntries() == 0 {
		discard()
		return nil, nil
	}
	props, err := w.Finish()
	if err != nil {
		discard()
		return nil, err
	}
	// Fsync the table before any manifest can name it.
	if err := f.Sync(); err != nil {
		discard()
		return nil, fmt.Errorf("lsm: sync %s: %w", path, err)
	}
	// Fsync the directory so the table's own entry is durable, instead of
	// depending on the manifest write that follows to do it.
	if err := fsys.SyncDir(d.dir); err != nil {
		discard()
		return nil, fmt.Errorf("lsm: sync directory %s: %w", d.dir, err)
	}
	r, err := sstable.Open(f, int64(w.Size()), sstable.ReaderOptions{Cache: d.opts.BlockCache, Name: path})
	if err != nil {
		discard()
		return nil, err
	}
	return &table{db: d, file: f, reader: r, meta: tableMeta{
		fileNum:   fileNum,
		size:      w.Size(),
		minSeq:    props.MinSeq,
		maxSeq:    props.MaxSeq,
		entries:   props.NumEntries,
		deletions: props.NumDeletions,
		smallest:  bytes.Clone(props.SmallestKey),
		largest:   bytes.Clone(props.LargestKey),
	}}, nil
}
