package vfs

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync/atomic"
	"time"
)

// RemoveAll deletes path and everything under it. A missing path is not
// an error. The removals are not durable until SyncDir on the parent.
func RemoveAll(fsys FS, path string) error {
	names, err := fsys.List(path)
	if err != nil {
		// Not a directory (or gone): remove it as a file.
		if rerr := fsys.Remove(path); rerr != nil && !errors.Is(rerr, fs.ErrNotExist) {
			return fmt.Errorf("vfs: remove %s: %w", path, rerr)
		}
		return nil
	}
	for _, n := range names {
		if err := RemoveAll(fsys, filepath.Join(path, n)); err != nil {
			return err
		}
	}
	if err := fsys.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("vfs: remove %s: %w", path, err)
	}
	return nil
}

// WriteFileDurably replaces name with data atomically: it writes a
// temporary file, fsyncs it, renames it over name and fsyncs the
// directory, so after a crash name holds either the old or the new data.
func WriteFileDurably(fsys FS, name string, data []byte) error {
	tmp := name + ".tmp"
	f, err := fsys.Create(tmp)
	if err != nil {
		return fmt.Errorf("vfs: create %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("vfs: write %s: %w", tmp, err)
	}
	// Fsync the contents before the rename makes them visible as name.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("vfs: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("vfs: close %s: %w", tmp, err)
	}
	if err := fsys.Rename(tmp, name); err != nil {
		return fmt.Errorf("vfs: rename %s: %w", tmp, err)
	}
	// Fsync the directory so the rename itself survives a power loss.
	if err := fsys.SyncDir(filepath.Dir(name)); err != nil {
		return fmt.Errorf("vfs: sync directory of %s: %w", name, err)
	}
	return nil
}

// ReadFile returns the contents of name.
func ReadFile(fsys FS, name string) ([]byte, error) {
	f, err := fsys.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	b := make([]byte, size)
	if _, err := f.ReadAt(b, 0); err != nil && size > 0 {
		return nil, err
	}
	return b, nil
}

// Slow wraps fsys so that every file and directory sync takes at least
// delay longer: a slow disk, for fault-injection tests.
func Slow(fsys FS, delay time.Duration) FS { return slowFS{FS: fsys, delay: delay} }

type slowFS struct {
	FS
	delay time.Duration
}

type slowFile struct {
	File
	delay time.Duration
}

func (s slowFS) Create(name string) (File, error) {
	f, err := s.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return slowFile{File: f, delay: s.delay}, nil
}

func (s slowFS) OpenReadWrite(name string) (File, error) {
	f, err := s.FS.OpenReadWrite(name)
	if err != nil {
		return nil, err
	}
	return slowFile{File: f, delay: s.delay}, nil
}

func (s slowFS) SyncDir(dir string) error {
	time.Sleep(s.delay)
	return s.FS.SyncDir(dir)
}

func (f slowFile) Sync() error {
	time.Sleep(f.delay)
	return f.File.Sync()
}

// CountSyncs wraps fsys so that every file and directory sync adds one to
// n, for tests that check what reaches the disk.
func CountSyncs(fsys FS, n *atomic.Int64) FS { return countingFS{FS: fsys, n: n} }

type countingFS struct {
	FS
	n *atomic.Int64
}

type countingFile struct {
	File
	n *atomic.Int64
}

func (c countingFS) Create(name string) (File, error) {
	f, err := c.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return countingFile{File: f, n: c.n}, nil
}

func (c countingFS) OpenReadWrite(name string) (File, error) {
	f, err := c.FS.OpenReadWrite(name)
	if err != nil {
		return nil, err
	}
	return countingFile{File: f, n: c.n}, nil
}

func (c countingFS) SyncDir(dir string) error {
	c.n.Add(1)
	return c.FS.SyncDir(dir)
}

func (f countingFile) Sync() error {
	f.n.Add(1)
	return f.File.Sync()
}
