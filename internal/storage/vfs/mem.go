package vfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync"
)

// ErrCrashed is returned by every operation on a MemFS after it has crashed.
var ErrCrashed = errors.New("vfs: simulated crash")

// MemFS is an in-memory filesystem that records which data is durable, so
// tests can simulate a power loss at any moment. It is a test double for the
// one I/O seam the storage engine has.
//
// The model (docs/adr/0020-vfs-fault-model.md):
//   - File contents are durable as of the file's last Sync. Unsynced bytes
//     may be lost on a crash. If the unsynced change only appended to the
//     synced contents, CrashImage may keep a prefix of the appended bytes,
//     and the last bytes of that prefix may be garbage, like a sector that
//     was only partly written.
//   - Directory entries (create, rename, link, remove) are durable as of the
//     directory's last SyncDir. A file whose entry was never made durable
//     disappears on a crash, even if its contents were synced.
//   - Directories made by MkdirAll are durable immediately. This is a
//     simplification; real code still syncs the parent directory.
//
// After Crash, or once a limit set by CrashAfter is reached, every
// operation fails with ErrCrashed, as if the process had died; CrashImage
// returns what a restarted process would find on disk.
type MemFS struct {
	mu         sync.Mutex
	dirs       map[string]*memDir // cleaned path -> directory
	locks      map[string]bool
	ops        int64 // mutating operations performed
	crashAfter int64 // crash when ops would exceed this; -1 = never
	crashed    bool
}

type memDir struct {
	live    map[string]*memNode // base name -> file
	durable map[string]*memNode // entries as of the last SyncDir
}

type memNode struct {
	data   []byte
	synced []byte // contents as of the last Sync
}

// NewMemFS returns an empty MemFS whose root directories "/" and "." exist.
func NewMemFS() *MemFS {
	m := &MemFS{dirs: map[string]*memDir{}, locks: map[string]bool{}, crashAfter: -1}
	m.dirs["/"] = newMemDir()
	m.dirs["."] = newMemDir()
	return m
}

func newMemDir() *memDir {
	return &memDir{live: map[string]*memNode{}, durable: map[string]*memNode{}}
}

// Crash makes the filesystem fail every later operation with ErrCrashed.
func (m *MemFS) Crash() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.crashed = true
}

// CrashAfter lets n more mutating operations succeed and then crashes:
// operation n+1 fails with ErrCrashed and so does everything after it.
// Mutating operations are Create, Write, Sync, Truncate, Remove, Rename,
// Link, MkdirAll and SyncDir.
func (m *MemFS) CrashAfter(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.crashAfter = m.ops + n
}

// OpCount returns the number of mutating operations performed so far.
func (m *MemFS) OpCount() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ops
}

// Crashed reports whether the filesystem has crashed.
func (m *MemFS) Crashed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.crashed
}

// CrashImage crashes the filesystem if it has not crashed yet and returns a
// new MemFS holding only what survived. If rng is nil, exactly the durable
// state survives. Otherwise some unsynced appended bytes may survive too, as
// described on MemFS; the same rng seed gives the same image.
func (m *MemFS) CrashImage(rng *rand.Rand) *MemFS {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.crashed = true
	img := NewMemFS()
	converted := map[*memNode]*memNode{} // hard links stay shared
	for _, path := range slices.Sorted(maps.Keys(m.dirs)) {
		d := m.dirs[path]
		nd := newMemDir()
		for _, name := range slices.Sorted(maps.Keys(d.durable)) {
			n := d.durable[name]
			nn, ok := converted[n]
			if !ok {
				data := survivingContents(n, rng)
				nn = &memNode{data: data, synced: bytes.Clone(data)}
				converted[n] = nn
			}
			nd.live[name] = nn
			nd.durable[name] = nn
		}
		img.dirs[path] = nd
	}
	return img
}

func survivingContents(n *memNode, rng *rand.Rand) []byte {
	out := bytes.Clone(n.synced)
	if out == nil {
		out = []byte{}
	}
	if rng == nil || len(n.data) <= len(n.synced) || !bytes.HasPrefix(n.data, n.synced) {
		return out
	}
	extra := n.data[len(n.synced):]
	var keep int
	switch rng.IntN(4) {
	case 0: // nothing unsynced reached the disk
	case 1:
		keep = len(extra) // everything did
	default:
		keep = rng.IntN(len(extra) + 1)
	}
	tail := bytes.Clone(extra[:keep])
	if keep > 0 && rng.IntN(3) == 0 {
		// A partly written sector: the last surviving bytes are garbage.
		g := 1 + rng.IntN(min(keep, 16))
		for i := keep - g; i < keep; i++ {
			tail[i] = byte(rng.Uint32())
		}
	}
	return append(out, tail...)
}

// mutate counts a mutating operation and applies the crash rules. Caller
// holds m.mu.
func (m *MemFS) mutate() error {
	if m.crashed {
		return ErrCrashed
	}
	if m.crashAfter >= 0 && m.ops >= m.crashAfter {
		m.crashed = true
		return ErrCrashed
	}
	m.ops++
	return nil
}

// lookup returns the directory holding name and name's base. Caller holds
// m.mu.
func (m *MemFS) lookup(op, name string) (*memDir, string, error) {
	name = filepath.Clean(name)
	d, ok := m.dirs[filepath.Dir(name)]
	if !ok {
		return nil, "", &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
	}
	return d, filepath.Base(name), nil
}

func (m *MemFS) Create(name string) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.mutate(); err != nil {
		return nil, err
	}
	d, base, err := m.lookup("create", name)
	if err != nil {
		return nil, err
	}
	if _, isDir := m.dirs[filepath.Clean(name)]; isDir {
		return nil, &fs.PathError{Op: "create", Path: name, Err: errors.New("is a directory")}
	}
	n, ok := d.live[base]
	if ok {
		n.data = nil // O_TRUNC truncates the existing inode in place
	} else {
		n = &memNode{}
		d.live[base] = n
	}
	return &memFile{fs: m, n: n, name: name, writable: true}, nil
}

func (m *MemFS) open(op, name string, writable bool) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.crashed {
		return nil, ErrCrashed
	}
	d, base, err := m.lookup(op, name)
	if err != nil {
		return nil, err
	}
	n, ok := d.live[base]
	if !ok {
		return nil, &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
	}
	return &memFile{fs: m, n: n, name: name, writable: writable}, nil
}

func (m *MemFS) Open(name string) (File, error) { return m.open("open", name, false) }

func (m *MemFS) OpenReadWrite(name string) (File, error) { return m.open("open", name, true) }

func (m *MemFS) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.mutate(); err != nil {
		return err
	}
	clean := filepath.Clean(name)
	if sub, isDir := m.dirs[clean]; isDir {
		if len(sub.live) > 0 || m.hasSubdir(clean) {
			return &fs.PathError{Op: "remove", Path: name, Err: errors.New("directory not empty")}
		}
		delete(m.dirs, clean)
		return nil
	}
	d, base, err := m.lookup("remove", name)
	if err != nil {
		return err
	}
	if _, ok := d.live[base]; !ok {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	delete(d.live, base)
	return nil
}

func (m *MemFS) hasSubdir(dir string) bool {
	for p := range m.dirs {
		if p != dir && filepath.Dir(p) == dir {
			return true
		}
	}
	return false
}

func (m *MemFS) Rename(oldname, newname string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.mutate(); err != nil {
		return err
	}
	od, obase, err := m.lookup("rename", oldname)
	if err != nil {
		return err
	}
	nd, nbase, err := m.lookup("rename", newname)
	if err != nil {
		return err
	}
	n, ok := od.live[obase]
	if !ok {
		return &fs.PathError{Op: "rename", Path: oldname, Err: fs.ErrNotExist}
	}
	delete(od.live, obase)
	nd.live[nbase] = n
	return nil
}

func (m *MemFS) Link(oldname, newname string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.mutate(); err != nil {
		return err
	}
	od, obase, err := m.lookup("link", oldname)
	if err != nil {
		return err
	}
	nd, nbase, err := m.lookup("link", newname)
	if err != nil {
		return err
	}
	n, ok := od.live[obase]
	if !ok {
		return &fs.PathError{Op: "link", Path: oldname, Err: fs.ErrNotExist}
	}
	if _, exists := nd.live[nbase]; exists {
		return &fs.PathError{Op: "link", Path: newname, Err: fs.ErrExist}
	}
	nd.live[nbase] = n
	return nil
}

func (m *MemFS) MkdirAll(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.mutate(); err != nil {
		return err
	}
	for p := filepath.Clean(dir); ; p = filepath.Dir(p) {
		if _, ok := m.dirs[p]; ok {
			break
		}
		if d, ok := m.dirs[filepath.Dir(p)]; ok {
			if _, isFile := d.live[filepath.Base(p)]; isFile {
				return &fs.PathError{Op: "mkdir", Path: p, Err: errors.New("not a directory")}
			}
		}
		m.dirs[p] = newMemDir()
		if filepath.Dir(p) == p {
			break
		}
	}
	return nil
}

func (m *MemFS) List(dir string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.crashed {
		return nil, ErrCrashed
	}
	clean := filepath.Clean(dir)
	d, ok := m.dirs[clean]
	if !ok {
		return nil, &fs.PathError{Op: "list", Path: dir, Err: fs.ErrNotExist}
	}
	names := slices.Collect(maps.Keys(d.live))
	for p := range m.dirs {
		if p != clean && filepath.Dir(p) == clean {
			names = append(names, filepath.Base(p))
		}
	}
	slices.Sort(names)
	return names, nil
}

func (m *MemFS) SyncDir(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.mutate(); err != nil {
		return err
	}
	d, ok := m.dirs[filepath.Clean(dir)]
	if !ok {
		return &fs.PathError{Op: "syncdir", Path: dir, Err: fs.ErrNotExist}
	}
	d.durable = maps.Clone(d.live)
	return nil
}

func (m *MemFS) Lock(name string) (io.Closer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.crashed {
		return nil, ErrCrashed
	}
	d, base, err := m.lookup("lock", name)
	if err != nil {
		return nil, err
	}
	clean := filepath.Clean(name)
	if m.locks[clean] {
		return nil, fmt.Errorf("%w: %s", ErrLocked, name)
	}
	if _, ok := d.live[base]; !ok {
		d.live[base] = &memNode{} // like the OS version, the lock file is created
	}
	m.locks[clean] = true
	return &memLock{fs: m, name: clean}, nil
}

type memLock struct {
	fs   *MemFS
	name string
	once sync.Once
}

func (l *memLock) Close() error {
	l.once.Do(func() {
		l.fs.mu.Lock()
		defer l.fs.mu.Unlock()
		delete(l.fs.locks, l.name)
	})
	return nil
}

// ReadFile returns a copy of the current contents of name. It is a test
// helper and ignores crashes.
func (m *MemFS) ReadFile(name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, base, err := m.lookup("read", name)
	if err != nil {
		return nil, err
	}
	n, ok := d.live[base]
	if !ok {
		return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrNotExist}
	}
	return bytes.Clone(n.data), nil
}

// WriteFile replaces the contents of an existing file and marks them
// durable. It is a test helper for corrupting files on purpose and ignores
// crashes.
func (m *MemFS) WriteFile(name string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, base, err := m.lookup("write", name)
	if err != nil {
		return err
	}
	n, ok := d.live[base]
	if !ok {
		return &fs.PathError{Op: "write", Path: name, Err: fs.ErrNotExist}
	}
	n.data = bytes.Clone(data)
	n.synced = bytes.Clone(data)
	return nil
}

type memFile struct {
	fs       *MemFS
	n        *memNode
	name     string
	pos      int64
	writable bool
}

func (f *memFile) Read(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.fs.crashed {
		return 0, ErrCrashed
	}
	if f.pos >= int64(len(f.n.data)) {
		return 0, io.EOF
	}
	k := copy(p, f.n.data[f.pos:])
	f.pos += int64(k)
	return k, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.fs.crashed {
		return 0, ErrCrashed
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "readat", Path: f.name, Err: fs.ErrInvalid}
	}
	if off >= int64(len(f.n.data)) {
		return 0, io.EOF
	}
	k := copy(p, f.n.data[off:])
	if k < len(p) {
		return k, io.EOF
	}
	return k, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if !f.writable {
		return 0, &fs.PathError{Op: "write", Path: f.name, Err: fs.ErrPermission}
	}
	if err := f.fs.mutate(); err != nil {
		return 0, err
	}
	end := f.pos + int64(len(p))
	if end > int64(len(f.n.data)) {
		f.n.data = append(f.n.data, make([]byte, end-int64(len(f.n.data)))...)
	}
	copy(f.n.data[f.pos:], p)
	f.pos = end
	return len(p), nil
}

func (f *memFile) Sync() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.fs.mutate(); err != nil {
		return err
	}
	f.n.synced = bytes.Clone(f.n.data)
	if f.n.synced == nil {
		f.n.synced = []byte{}
	}
	return nil
}

func (f *memFile) Truncate(size int64) error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if !f.writable {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrPermission}
	}
	if err := f.fs.mutate(); err != nil {
		return err
	}
	switch {
	case size < 0:
		return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrInvalid}
	case size <= int64(len(f.n.data)):
		f.n.data = f.n.data[:size:size]
	default:
		f.n.data = append(f.n.data, make([]byte, size-int64(len(f.n.data)))...)
	}
	return nil
}

func (f *memFile) Size() (int64, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.fs.crashed {
		return 0, ErrCrashed
	}
	return int64(len(f.n.data)), nil
}

func (f *memFile) Close() error { return nil }
