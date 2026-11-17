package vfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"
)

func readAll(t *testing.T, fsys FS, name string) []byte {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return b
}

func writeFile(t *testing.T, fsys FS, name string, data []byte, sync bool) {
	t.Helper()
	f, err := fsys.Create(name)
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	if sync {
		if err := f.Sync(); err != nil {
			t.Fatalf("sync %s: %v", name, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestConformance runs the same operations against the OS filesystem and
// MemFS, so MemFS behaves like the real thing apart from crashes.
func TestConformance(t *testing.T) {
	for _, tc := range []struct {
		name string
		fs   FS
		root func(t *testing.T) string
	}{
		{"os", Default, func(t *testing.T) string { return t.TempDir() }},
		{"mem", NewMemFS(), func(*testing.T) string { return "/data" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fsys, root := tc.fs, tc.root(t)
			dir := filepath.Join(root, "a", "b")
			if err := fsys.MkdirAll(dir); err != nil {
				t.Fatal(err)
			}
			p := filepath.Join(dir, "f")
			writeFile(t, fsys, p, []byte("hello world"), true)
			if got := readAll(t, fsys, p); string(got) != "hello world" {
				t.Fatalf("read back %q", got)
			}

			f, err := fsys.Open(p)
			if err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 5)
			if n, err := f.ReadAt(buf, 6); n != 5 || err != nil || string(buf) != "world" {
				t.Errorf("ReadAt = %d %v %q", n, err, buf)
			}
			if n, err := f.ReadAt(buf, 8); n != 3 || !errors.Is(err, io.EOF) {
				t.Errorf("short ReadAt = %d, %v; want 3, EOF", n, err)
			}
			if sz, err := f.Size(); sz != 11 || err != nil {
				t.Errorf("Size = %d, %v", sz, err)
			}
			if _, err := f.Write([]byte("x")); err == nil {
				t.Error("Write on a read-only file succeeded")
			}
			_ = f.Close()

			rw, err := fsys.OpenReadWrite(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := rw.Truncate(5); err != nil {
				t.Fatal(err)
			}
			if err := rw.Sync(); err != nil {
				t.Fatal(err)
			}
			_ = rw.Close()
			if got := readAll(t, fsys, p); string(got) != "hello" {
				t.Errorf("after Truncate(5): %q", got)
			}

			q := filepath.Join(dir, "g")
			if err := fsys.Rename(p, q); err != nil {
				t.Fatal(err)
			}
			if _, err := fsys.Open(p); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("old name after rename: %v", err)
			}
			if err := fsys.Link(q, p); err != nil {
				t.Fatal(err)
			}
			if err := fsys.Link(q, p); !errors.Is(err, fs.ErrExist) {
				t.Errorf("link onto existing name: %v, want ErrExist", err)
			}
			if got := readAll(t, fsys, p); string(got) != "hello" {
				t.Errorf("hard link reads %q", got)
			}
			if err := fsys.SyncDir(dir); err != nil {
				t.Fatal(err)
			}
			names, err := fsys.List(dir)
			if err != nil || !slices.Equal(names, []string{"f", "g"}) {
				t.Errorf("List = %v, %v", names, err)
			}
			if err := fsys.Remove(q); err != nil {
				t.Fatal(err)
			}
			if err := fsys.Remove(q); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("second Remove: %v", err)
			}
			if _, err := fsys.Open(filepath.Join(root, "missing", "x")); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("open in missing dir: %v", err)
			}

			l1, err := fsys.Lock(filepath.Join(dir, "LOCK"))
			if err != nil {
				t.Fatal(err)
			}
			// flock locks belong to open file descriptions, so a second
			// Lock conflicts even from the same process: that is what stops
			// two engines from opening one directory.
			if _, err := fsys.Lock(filepath.Join(dir, "LOCK")); !errors.Is(err, ErrLocked) {
				t.Errorf("second Lock: %v, want ErrLocked", err)
			}
			if err := l1.Close(); err != nil {
				t.Fatal(err)
			}
			l2, err := fsys.Lock(filepath.Join(dir, "LOCK"))
			if err != nil {
				t.Fatalf("Lock after release: %v", err)
			}
			_ = l2.Close()
		})
	}
}

func TestMemCrashKeepsOnlyDurableState(t *testing.T) {
	m := NewMemFS()
	if err := m.MkdirAll("/d"); err != nil {
		t.Fatal(err)
	}
	writeFile(t, m, "/d/synced-file-and-dir", []byte("A"), true)
	writeFile(t, m, "/d/synced-file-only", []byte("B"), true)
	if err := m.SyncDir("/d"); err != nil {
		t.Fatal(err)
	}
	// After the directory sync: a new synced file whose entry is not durable,
	// unsynced appends, and a rename that is not durable.
	writeFile(t, m, "/d/new-entry", []byte("C"), true)
	f, _ := m.OpenReadWrite("/d/synced-file-and-dir")
	_, _ = f.Write([]byte("lost"))
	if err := m.Rename("/d/synced-file-only", "/d/renamed"); err != nil {
		t.Fatal(err)
	}

	img := m.CrashImage(nil)
	if _, err := m.List("/d"); !errors.Is(err, ErrCrashed) {
		t.Errorf("List after crash: %v, want ErrCrashed", err)
	}
	names, _ := img.List("/d")
	if want := []string{"synced-file-and-dir", "synced-file-only"}; !slices.Equal(names, want) {
		t.Errorf("surviving names %v, want %v", names, want)
	}
	if got := readAll(t, img, "/d/synced-file-and-dir"); string(got) != "A" {
		t.Errorf("unsynced overwrite survived: %q", got)
	}
}

func TestMemCrashImageTornAppends(t *testing.T) {
	synced := []byte("durable|")
	extra := bytes.Repeat([]byte("0123456789"), 10)
	outcomes := map[string]int{}
	for seed := range uint64(300) {
		m := NewMemFS()
		_ = m.MkdirAll("/d")
		writeFile(t, m, "/d/log", synced, true)
		_ = m.SyncDir("/d")
		f, _ := m.OpenReadWrite("/d/log")
		_, _ = f.Read(make([]byte, len(synced))) // move to the end
		_, _ = f.Write(extra)
		img := m.CrashImage(rand.New(rand.NewPCG(seed, 0)))
		got := readAll(t, img, "/d/log")
		if !bytes.HasPrefix(got, synced) {
			t.Fatalf("seed %d: synced prefix lost: %q", seed, got)
		}
		tail := got[len(synced):]
		if len(tail) > len(extra) {
			t.Fatalf("seed %d: image longer than what was written", seed)
		}
		switch {
		case len(tail) == 0:
			outcomes["none"]++
		case bytes.Equal(tail, extra):
			outcomes["all"]++
		case bytes.Equal(tail, extra[:len(tail)]):
			outcomes["clean prefix"]++
		default:
			outcomes["garbled prefix"]++
		}
		again := m.CrashImage(rand.New(rand.NewPCG(seed, 0)))
		if !bytes.Equal(readAll(t, again, "/d/log"), got) {
			t.Fatalf("seed %d: CrashImage is not deterministic", seed)
		}
	}
	for _, k := range []string{"none", "all", "clean prefix", "garbled prefix"} {
		if outcomes[k] == 0 {
			t.Errorf("outcome %q never happened in 300 seeds: %v", k, outcomes)
		}
	}
	t.Logf("outcomes: %v", outcomes)
}

func TestMemCrashAfter(t *testing.T) {
	m := NewMemFS()
	_ = m.MkdirAll("/d")
	m.CrashAfter(2)
	f, err := m.Create("/d/f") // op 1
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("x")); err != nil { // op 2
		t.Fatal(err)
	}
	if err := f.Sync(); !errors.Is(err, ErrCrashed) { // op 3 crashes
		t.Fatalf("third op: %v, want ErrCrashed", err)
	}
	if !m.Crashed() {
		t.Fatal("not crashed")
	}
	if _, err := m.Open("/d/f"); !errors.Is(err, ErrCrashed) {
		t.Errorf("Open after crash: %v", err)
	}
	if names, _ := m.CrashImage(nil).List("/d"); len(names) != 0 {
		t.Errorf("file with unsynced directory entry survived: %v", names)
	}
}

func TestMemHardLinksShareContents(t *testing.T) {
	m := NewMemFS()
	_ = m.MkdirAll("/d")
	writeFile(t, m, "/d/a", []byte("v1"), true)
	if err := m.Link("/d/a", "/d/b"); err != nil {
		t.Fatal(err)
	}
	_ = m.SyncDir("/d")
	img := m.CrashImage(nil)
	if err := img.WriteFile("/d/a", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, img, "/d/b"); string(got) != "v2" {
		t.Errorf("link did not survive the crash as a link: %q", got)
	}
}

// TestMemCrashImageKeepsAPrefixOfDirectoryChanges checks the journaling
// model: unsynced directory changes survive a crash only as an in-order
// prefix, and a rename is atomic.
func TestMemCrashImageKeepsAPrefixOfDirectoryChanges(t *testing.T) {
	setup := func() *MemFS {
		m := NewMemFS()
		_ = m.MkdirAll("/d")
		writeFile(t, m, "/d/old", []byte("o"), true)
		writeFile(t, m, "/d/tmp", []byte("t"), true)
		_ = m.SyncDir("/d")
		writeFile(t, m, "/d/a", []byte("a"), true) // 1: add a
		_ = m.Remove("/d/old")                     // 2: remove old
		_ = m.Rename("/d/tmp", "/d/final")         // 3: tmp -> final, atomically
		writeFile(t, m, "/d/b", []byte("b"), true) // 4: add b
		return m
	}
	prefixes := []string{
		"[old tmp]",
		"[a old tmp]",
		"[a tmp]",
		"[a final]",
		"[a b final]",
	}
	seen := map[string]int{}
	for seed := range uint64(200) {
		img := setup().CrashImage(rand.New(rand.NewPCG(seed, 3)))
		names, _ := img.List("/d")
		got := fmt.Sprint(names)
		if !slices.Contains(prefixes, got) {
			t.Fatalf("seed %d: entries %s are not an in-order prefix of the unsynced changes", seed, got)
		}
		seen[got]++
	}
	if seen[prefixes[0]] == 0 || seen[prefixes[len(prefixes)-1]] == 0 || len(seen) < 4 {
		t.Errorf("prefix outcomes %v: expected none, all and partial prefixes", seen)
	}
	if names, _ := setup().CrashImage(nil).List("/d"); fmt.Sprint(names) != prefixes[0] {
		t.Errorf("exact image %v, want only the synced entries", names)
	}
}

func TestRemoveAllAndWriteFileDurably(t *testing.T) {
	m := NewMemFS()
	for _, d := range []string{"/a/b/c", "/a/d"} {
		if err := m.MkdirAll(d); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"/a/x", "/a/b/y", "/a/b/c/z"} {
		if err := WriteFileDurably(m, f, []byte(f)); err != nil {
			t.Fatal(err)
		}
	}
	if b, err := ReadFile(m, "/a/b/y"); err != nil || string(b) != "/a/b/y" {
		t.Fatalf("read back: %q %v", b, err)
	}
	if err := RemoveAll(m, "/a/b"); err != nil {
		t.Fatal(err)
	}
	if names, _ := m.List("/a"); len(names) != 2 {
		t.Fatalf("left in /a: %v", names)
	}
	if err := RemoveAll(m, "/missing"); err != nil {
		t.Fatalf("missing path: %v", err)
	}
	// A crash image keeps either the old or the new contents.
	if err := WriteFileDurably(m, "/a/x", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if b, err := ReadFile(m.CrashImage(nil), "/a/x"); err != nil || string(b) != "new" {
		t.Fatalf("after crash: %q %v", b, err)
	}
}
