// Package vfs is the filesystem interface the storage engine uses, so tests
// can substitute a filesystem that simulates power loss (see MemFS).
//
// The interface is deliberately small: exactly the operations the WAL,
// SSTables and manifest need, with durability made explicit. A write is not
// durable until File.Sync returns, and a change to a directory (creating,
// renaming, linking or removing a file) is not durable until SyncDir on that
// directory returns. Code that relies on durability must call both; see
// docs/adr/0020-vfs-fault-model.md.
package vfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
)

// FS is a filesystem. Implementations are safe for concurrent use.
type FS interface {
	// Create creates the named file, truncating it if it exists, and opens
	// it for reading and writing. Writes append at the current offset,
	// which starts at 0.
	Create(name string) (File, error)
	// Open opens an existing file for reading.
	Open(name string) (File, error)
	// OpenReadWrite opens an existing file for reading and writing at
	// offset 0, without truncating it.
	OpenReadWrite(name string) (File, error)
	// Remove deletes a file or an empty directory.
	Remove(name string) error
	// Rename atomically replaces newname with oldname.
	Rename(oldname, newname string) error
	// Link creates newname as a hard link to oldname.
	Link(oldname, newname string) error
	// MkdirAll creates dir and any missing parents.
	MkdirAll(dir string) error
	// List returns the sorted names of the entries in dir.
	List(dir string) ([]string, error)
	// SyncDir makes every completed change to dir's entries durable.
	SyncDir(dir string) error
	// Lock takes an exclusive lock on the named file, creating it if
	// needed. It fails if another process (or, for MemFS, another caller)
	// holds the lock. Closing the returned Closer releases it; a process
	// that dies releases its locks.
	Lock(name string) (io.Closer, error)
}

// File is an open file.
type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Closer
	// Sync makes the file's contents durable.
	Sync() error
	// Truncate changes the file's size.
	Truncate(size int64) error
	// Size returns the file's current size.
	Size() (int64, error)
}

// ErrLocked is returned by Lock when the lock is already held.
var ErrLocked = errors.New("vfs: lock is held by another process")

// Default is the operating system's filesystem.
var Default FS = osFS{}

type osFS struct{}

func (osFS) Create(name string) (File, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) Open(name string) (File, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) OpenReadWrite(name string) (File, error) {
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) Remove(name string) error             { return os.Remove(name) }
func (osFS) Rename(oldname, newname string) error { return os.Rename(oldname, newname) }
func (osFS) Link(oldname, newname string) error   { return os.Link(oldname, newname) }
func (osFS) MkdirAll(dir string) error            { return os.MkdirAll(dir, 0o755) }

func (osFS) List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	slices.Sort(names)
	return names, nil
}

func (osFS) SyncDir(dir string) error {
	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	// Fsync on a directory flushes its entries (names -> inodes), which is
	// what makes a create, rename or unlink in it survive a power loss.
	if err := d.Sync(); err != nil {
		_ = d.Close() // the sync error is the one worth reporting
		return err
	}
	return d.Close()
}

func (osFS) Lock(name string) (io.Closer, error) { return lockFile(name) }

type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
