package lsm

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
	"github.com/naveenk2608/distributed-database/internal/storage/memtable"
	"github.com/naveenk2608/distributed-database/internal/storage/sstable"
	"github.com/naveenk2608/distributed-database/internal/storage/wal"
)

// recover loads the manifest, removes files a crash left behind, opens the
// live tables, replays the log above the flushed sequence number, flushes
// what it replayed to a table, and starts a new log segment. Flushing at
// startup keeps every later memtable matched to exactly one segment, and
// keeps replay bounded by one memtable's worth of log.
func (d *DB) recover() error {
	fsys, dir := d.opts.FS, d.dir
	mf, _, err := readManifest(fsys, dir)
	if err != nil {
		return d.openError(err)
	}
	if err := d.removeLeftovers(&mf); err != nil {
		return d.openError(err)
	}
	var tables []*table
	closeAll := func() {
		for _, t := range tables {
			_ = t.file.Close() // read-only so far
		}
	}
	for _, meta := range mf.tables {
		t, err := d.openTable(meta)
		if err != nil {
			closeAll()
			return d.openError(err)
		}
		tables = append(tables, t)
	}

	mem := memtable.New()
	last := mf.flushedSeq
	res, err := wal.Recover(fsys, dir, func(_ uint64, r wal.Record) error {
		if r.Type != recordBatch {
			return fmt.Errorf("%w: unknown record type %d", ErrCorruption, r.Type)
		}
		seq, ops, err := decodeBatch(r.Payload)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrCorruption, err)
		}
		if seq <= mf.flushedSeq {
			return nil // already in a table
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
	if err != nil {
		closeAll()
		return d.openError(err)
	}
	if res.TornBytes > 0 {
		d.opts.Logger.Warn("truncated a torn write at the end of the log",
			"segment", wal.SegmentPath(dir, res.Segments[len(res.Segments)-1]),
			"offset", res.TornOffset, "bytes", res.TornBytes, "reason", res.TornReason)
	}

	d.nextFileNum = max(mf.nextFileNum, 1)
	for _, t := range tables {
		d.nextFileNum = max(d.nextFileNum, t.meta.fileNum+1)
	}
	if mem.Len() > 0 {
		fileNum := d.nextFileNum
		d.nextFileNum++
		t, err := d.buildTable(fileNum, memtableFill(mem))
		if err != nil {
			closeAll()
			return fmt.Errorf("lsm: open %s: flush the recovered log: %w", dir, err)
		}
		tables = append([]*table{t}, tables...)
	}
	newSeg := max(mf.logNum, 1)
	if n := len(res.Segments); n > 0 {
		newSeg = max(newSeg, res.Segments[n-1]+1)
	}
	m := manifest{nextFileNum: d.nextFileNum, logNum: newSeg, flushedSeq: last, tables: tableMetas(tables)}
	if err := writeManifest(fsys, dir, &m); err != nil {
		closeAll()
		return fmt.Errorf("lsm: open %s: %w", dir, err)
	}
	if !d.opts.DisableWAL {
		w, err := wal.NewWriter(fsys, dir, newSeg)
		if err != nil {
			closeAll()
			return fmt.Errorf("lsm: open %s: %w", dir, err)
		}
		d.wal = w
	}
	// Every older segment is flushed (into the table built above, if any):
	// the manifest just written says so.
	d.removeObsoleteLogs(newSeg)
	d.lastSeq, d.flushedSeq, d.logNum = last, last, newSeg
	d.visibleSeq.Store(last)
	d.state = newReadState(memtable.New(), newSeg, nil, tables)
	d.opts.Logger.Debug("recovered", "dir", dir, "tables", len(tables), "log_records", res.Records,
		"last_seq", last)
	return nil
}

// openError labels an error from Open; damage is reported as corruption
// with an explicit refusal.
func (d *DB) openError(err error) error {
	if (errors.Is(err, wal.ErrCorrupt) || errors.Is(err, sstable.ErrCorrupt)) && !errors.Is(err, ErrCorruption) {
		err = fmt.Errorf("%w: %w", ErrCorruption, err)
	}
	if errors.Is(err, ErrCorruption) {
		return fmt.Errorf("lsm: open %s: refusing to start: %w", d.dir, err)
	}
	return fmt.Errorf("lsm: open %s: %w", d.dir, err)
}

// removeLeftovers deletes files a crash left behind: a manifest that was
// never installed, tables the manifest does not name (from a flush or
// compaction that crashed before its manifest was written), and log
// segments the manifest says are flushed. No directory fsync: if a removal
// is lost in a crash, the next Open repeats it.
func (d *DB) removeLeftovers(mf *manifest) error {
	fsys := d.opts.FS
	live := map[uint64]bool{}
	for _, t := range mf.tables {
		live[t.fileNum] = true
	}
	names, err := fsys.List(d.dir)
	if err != nil {
		return fmt.Errorf("lsm: list %s: %w", d.dir, err)
	}
	for _, name := range names {
		n, isTable := parseTableName(name)
		if name != manifestTmpName && (!isTable || live[n]) {
			continue
		}
		path := filepath.Join(d.dir, name)
		if err := fsys.Remove(path); err != nil {
			return fmt.Errorf("lsm: remove leftover %s: %w", path, err)
		}
		d.opts.Logger.Info("removed a file left by an interrupted flush or compaction", "path", path)
	}
	segs, err := wal.ListSegments(fsys, d.dir)
	if err != nil {
		return err
	}
	for _, seg := range segs {
		if seg >= mf.logNum {
			break
		}
		if err := fsys.Remove(wal.SegmentPath(d.dir, seg)); err != nil {
			return fmt.Errorf("lsm: remove flushed log segment: %w", err)
		}
	}
	return nil
}

// removeObsoleteLogs deletes log segments below logNum, whose contents are
// all in tables named by a durable manifest. Errors are logged: a segment
// that survives is removed at the next Open.
func (d *DB) removeObsoleteLogs(logNum uint64) {
	segs, err := wal.ListSegments(d.opts.FS, d.dir)
	if err != nil {
		d.opts.Logger.Warn("listing log segments", "error", err)
		return
	}
	for _, seg := range segs {
		if seg >= logNum {
			return
		}
		if err := d.opts.FS.Remove(wal.SegmentPath(d.dir, seg)); err != nil {
			d.opts.Logger.Warn("removing flushed log segment", "segment", seg, "error", err)
		}
	}
}

// openTable opens a table the manifest names and checks it against the
// manifest.
func (d *DB) openTable(meta tableMeta) (*table, error) {
	path := tablePath(d.dir, meta.fileNum)
	f, err := d.opts.FS.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: the manifest names %s, which does not exist", ErrCorruption, path)
	}
	if err != nil {
		return nil, fmt.Errorf("lsm: open %s: %w", path, err)
	}
	size, err := f.Size()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("lsm: size of %s: %w", path, err)
	}
	if uint64(size) != meta.size {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s is %d bytes but the manifest says %d", ErrCorruption, path, size, meta.size)
	}
	r, err := sstable.Open(f, size, sstable.ReaderOptions{Cache: d.opts.BlockCache, Name: path})
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &table{db: d, meta: meta, file: f, reader: r}, nil
}
