package lsm

import (
	"fmt"
	"path/filepath"
)

// Checkpoint writes a consistent copy of the database to dir, which must be
// empty or not exist, and returns the sequence number it holds: every batch
// at or below it and nothing above. It flushes the memtables first, then
// hard-links the live tables into dir (no data is copied) and writes a
// manifest there; the copy has no log. Open on dir gives a working
// database. Phase 7 streams checkpoints as Raft snapshots.
func (d *DB) Checkpoint(dir string) (uint64, error) {
	if err := d.Flush(); err != nil {
		return 0, err
	}
	fsys := d.opts.FS
	if names, err := fsys.List(dir); err == nil && len(names) > 0 {
		return 0, fmt.Errorf("lsm: checkpoint directory %s is not empty", dir)
	}

	d.mu.Lock()
	s := d.state
	if s == nil {
		d.mu.Unlock()
		return 0, ErrClosed
	}
	s.refs.Add(1) // keeps the tables from being deleted while they are linked
	mf := manifest{nextFileNum: d.nextFileNum, flushedSeq: d.flushedSeq, tables: tableMetas(s.tables)}
	d.mu.Unlock()
	defer s.unref()

	if err := fsys.MkdirAll(dir); err != nil {
		return 0, fmt.Errorf("lsm: checkpoint: %w", err)
	}
	for _, t := range s.tables {
		src, dst := tablePath(d.dir, t.meta.fileNum), tablePath(dir, t.meta.fileNum)
		if err := fsys.Link(src, dst); err != nil {
			return 0, fmt.Errorf("lsm: checkpoint: link %s: %w", src, err)
		}
	}
	// writeManifest fsyncs the manifest and then the directory, which also
	// makes the links above durable.
	if err := writeManifest(fsys, dir, &mf); err != nil {
		return 0, fmt.Errorf("lsm: checkpoint: %w", err)
	}
	// Fsync the parent so the checkpoint directory's own entry is durable.
	if err := fsys.SyncDir(filepath.Dir(filepath.Clean(dir))); err != nil {
		return 0, fmt.Errorf("lsm: checkpoint: sync parent of %s: %w", dir, err)
	}
	return mf.flushedSeq, nil
}
