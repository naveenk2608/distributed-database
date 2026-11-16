package lsm

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
	"github.com/naveenk2608/distributed-database/internal/storage/wal"
)

var modelOps = flag.Int("model-ops", 0, "operations in TestModelMillionOps (0 = 1,000,000; 200,000 under -race; 50,000 with -short)")

// TestModelMillionOps is the Phase 3 gate's model test: random puts,
// deletes, multi-key batches and reads against a Go map, with background
// flushes and compactions running, clean restarts, and long-lived snapshots
// that must keep seeing their versions across compactions.
func TestModelMillionOps(t *testing.T) {
	n := *modelOps
	switch {
	case n > 0:
	case testing.Short():
		n = 50_000
	case raceEnabled:
		n = 200_000
	default:
		n = 1_000_000
	}
	r := rand.New(rand.NewPCG(testSeed(t), 31))
	fs := vfs.NewMemFS()
	// SyncNever: durability is the crash tests' job, and MemFS copies the
	// whole file on every fsync. Small memtables and tables make flushes
	// and compactions frequent.
	opts := Options{FS: fs, Sync: SyncNever, MemtableBytes: 32 << 10, BlockSize: 1024,
		SlowdownTables: 12, StopTables: 20}
	d, err := Open(testDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	const numKeys = 20_000
	key := func() []byte { return []byte(fmt.Sprintf("k%06d", r.IntN(numKeys))) }
	model := state{}

	type held struct {
		snap  *Snapshot
		model state
	}
	var snaps []held
	var flushes, compactions, restarts, reads, snapChecks int64
	tally := func() {
		m := d.Metrics()
		flushes += m.Flushes
		compactions += m.Compactions
	}
	checkSnapshot := func(h held) {
		for range 100 {
			k := key()
			v, ver, err := d.Get(k, h.snap)
			w, ok := h.model[string(k)]
			if ok != (err == nil) || (ok && (!bytes.Equal(v, w.value) || ver != w.version)) {
				t.Fatalf("snapshot %d: Get(%s) = %q@%d, %v; want %q@%d present=%v", h.snap.Seq(), k, v, ver, err, w.value, w.version, ok)
			}
		}
		snapChecks++
	}

	for i := range n {
		switch op := r.IntN(100); {
		case op < 55:
			var b Batch
			k := key()
			v := []byte(fmt.Sprintf("v%d-%s", i, strings.Repeat("x", r.IntN(80))))
			b.Set(k, v)
			model[string(k)] = entry{v, mustApply(t, d, &b, 0)}
		case op < 70:
			var b Batch
			k := key()
			b.Delete(k)
			mustApply(t, d, &b, 0)
			delete(model, string(k))
		case op < 75:
			var b Batch
			pending := map[string]*[]byte{}
			for range 2 + r.IntN(4) {
				k := key()
				if r.IntN(3) == 0 {
					b.Delete(k)
					pending[string(k)] = nil
				} else {
					v := []byte(fmt.Sprintf("m%d", i))
					b.Set(k, v)
					pending[string(k)] = &v
				}
			}
			seq := mustApply(t, d, &b, 0)
			for k, v := range pending {
				if v == nil {
					delete(model, k)
				} else {
					model[k] = entry{*v, seq}
				}
			}
		default:
			k := key()
			v, ver, err := d.Get(k, nil)
			w, ok := model[string(k)]
			if ok != (err == nil) || (ok && (!bytes.Equal(v, w.value) || ver != w.version)) {
				t.Fatalf("op %d: Get(%s) = %q@%d, %v; want %q@%d present=%v", i, k, v, ver, err, w.value, w.version, ok)
			}
			reads++
		}

		if i%25_000 == 12_500 {
			snaps = append(snaps, held{d.NewSnapshot(), maps.Clone(model)})
			if len(snaps) > 3 {
				snaps[0].snap.Release()
				snaps = snaps[1:]
			}
		}
		if i%5_000 == 0 && len(snaps) > 0 {
			checkSnapshot(snaps[r.IntN(len(snaps))])
		}
		if i%100_000 == 50_000 {
			checkScan(t, d, model)
		}
		if i%(n/10) == n/20 { // ten clean restarts spread over the run
			tally()
			for _, h := range snaps {
				h.snap.Release()
			}
			snaps = nil
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			if d, err = Open(testDir, opts); err != nil {
				t.Fatalf("reopen after %d ops: %v", i, err)
			}
			restarts++
			checkScan(t, d, model)
		}
	}
	checkScan(t, d, model)
	for _, h := range snaps {
		checkSnapshot(h)
	}
	tally()
	m := d.Metrics()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d ops (%d point reads checked, %d snapshot checks): %d flushes, %d compactions, %d restarts; final %d tables, %d keys",
		n, reads, snapChecks, flushes, compactions, restarts, m.Tables, len(model))
	if flushes < 20 || compactions < 5 || restarts < 10 {
		t.Errorf("too little background activity to count: %d flushes, %d compactions, %d restarts", flushes, compactions, restarts)
	}
}

// TestCrashDuringFlushAndCompaction is the Phase 3 gate's crash test. A
// sequential workload flushes and compacts explicitly (manual mode), so
// every run performs the same filesystem operations; the test crashes at
// each one, recovers from the exact and from a torn image, and checks the
// recovered engine against every acknowledged batch. For a sample of crash
// points it also crashes at every step of that recovery.
func TestCrashDuringFlushAndCompaction(t *testing.T) {
	opts := Options{MemtableBytes: 1 << 10, CompactionMinTables: 2, manual: true}
	run := func(fs *vfs.MemFS, w *workload) {
		o := opts
		o.FS = fs
		d, err := Open(testDir, o)
		if err != nil {
			return
		}
		defer func() { _ = d.Close() }()
		r := rand.New(rand.NewPCG(5, 6))
		for i := range 48 {
			id, b := w.newBatch(r, 12)
			seq, err := d.Apply(b, 0)
			if err != nil {
				return
			}
			w.ack(id, seq)
			switch {
			case i%8 == 7:
				if d.Flush() != nil {
					return
				}
			case i%16 == 11:
				if _, err := d.compactOnce(); err != nil {
					return
				}
			}
		}
		_, _ = d.compactAll() // a bottom compaction drops tombstones
	}
	dry := vfs.NewMemFS()
	run(dry, newWorkload())
	total := dry.OpCount()
	check := Open
	{
		d, err := check(testDir, Options{FS: dry.CrashImage(nil), manual: true})
		if err != nil {
			t.Fatal(err)
		}
		if m := d.Metrics(); m.Tables != 1 {
			t.Fatalf("workload ends with %d tables; it should end with one compacted table", m.Tables)
		}
		_ = d.Close()
	}

	recoveries := 0
	for crashAt := range total + 1 {
		for torn := range 2 {
			fs := vfs.NewMemFS()
			fs.CrashAfter(crashAt)
			w := newWorkload()
			run(fs, w)
			var rng *rand.Rand
			if torn == 1 {
				rng = rand.New(rand.NewPCG(uint64(crashAt), 9))
			}
			img := fs.CrashImage(rng)
			nested := torn == 0 && crashAt%7 == 0
			for recoverCrash := int64(0); ; recoverCrash++ {
				attempt := img.CrashImage(nil)
				if nested {
					attempt.CrashAfter(recoverCrash)
				}
				d, err := Open(testDir, Options{FS: attempt, manual: true})
				if err == nil {
					_ = d.Close()
				}
				finished := !attempt.Crashed()
				if !nested && err != nil {
					t.Fatalf("crash at op %d (torn=%d): reopen: %v", crashAt, torn, err)
				}
				d, err = Open(testDir, Options{FS: attempt.CrashImage(nil), manual: true})
				if err != nil {
					t.Fatalf("crash at op %d (torn=%d), recovery crashed at op %d: reopen: %v", crashAt, torn, recoverCrash, err)
				}
				w.verify(t, d)
				_ = d.Close()
				recoveries++
				if finished || !nested {
					break
				}
			}
		}
	}
	t.Logf("%d crash points x 2 images, %d recoveries checked", total+1, recoveries)
}

func manualDB(t *testing.T, fs vfs.FS) *DB {
	t.Helper()
	d, err := Open(testDir, Options{FS: fs, manual: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func put(t *testing.T, d *DB, k, v string) uint64 {
	t.Helper()
	var b Batch
	b.Set([]byte(k), []byte(v))
	return mustApply(t, d, &b, 0)
}

func del(t *testing.T, d *DB, k string) uint64 {
	t.Helper()
	var b Batch
	b.Delete([]byte(k))
	return mustApply(t, d, &b, 0)
}

func mustFlush(t *testing.T, d *DB) {
	t.Helper()
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotsKeepVersionsThroughCompaction(t *testing.T) {
	d := manualDB(t, vfs.NewMemFS())
	v1 := put(t, d, "k", "v1")
	put(t, d, "other", "x")
	s1 := d.NewSnapshot()
	put(t, d, "k", "v2")
	mustFlush(t, d)
	put(t, d, "k", "v3")
	s3 := d.NewSnapshot()
	del(t, d, "k")
	mustFlush(t, d)
	if _, err := d.compactAll(); err != nil {
		t.Fatal(err)
	}
	if v, ver, err := d.Get([]byte("k"), s1); err != nil || string(v) != "v1" || ver != v1 {
		t.Fatalf("snapshot read after compaction = %q@%d, %v; want v1@%d", v, ver, err, v1)
	}
	if v, _, err := d.Get([]byte("k"), s3); err != nil || string(v) != "v3" {
		t.Fatalf("second snapshot = %q, %v; want v3", v, err)
	}
	if _, _, err := d.Get([]byte("k"), nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("latest read of a deleted key: %v", err)
	}
	it, _ := d.NewIterator(IterOptions{Snapshot: s1})
	it.SeekGE([]byte("k"))
	if !it.Valid() || string(it.Key()) != "k" || string(it.Value()) != "v1" {
		t.Error("snapshot scan lost k=v1")
	}
	_ = it.Close()

	s1.Release()
	s3.Release()
	if _, err := d.compactAll(); err != nil {
		t.Fatal(err)
	}
	if m := d.Metrics(); m.Tables != 1 || m.TableEntries != 1 {
		t.Errorf("after releasing snapshots: %d tables, %d entries; want 1 table holding only \"other\"", m.Tables, m.TableEntries)
	}
}

// TestTombstoneOutlivesNonBottomCompaction: a delete merged with newer
// tables, but not with the table holding the old value, must keep its
// tombstone, or the old value would come back.
func TestTombstoneOutlivesNonBottomCompaction(t *testing.T) {
	d := manualDB(t, vfs.NewMemFS())
	put(t, d, "k", "old")
	mustFlush(t, d) // oldest table: k=old
	del(t, d, "k")
	mustFlush(t, d) // tombstone
	put(t, d, "z", "1")
	mustFlush(t, d)
	ok, err := d.compact(func(tables []*table) *compaction {
		return &compaction{inputs: tables[:2], bottom: false, reason: "test"}
	})
	if err != nil || !ok {
		t.Fatalf("compaction ran=%v: %v", ok, err)
	}
	if _, _, err := d.Get([]byte("k"), nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key resurrected after a non-bottom compaction: %v", err)
	}
	if _, err := d.compactAll(); err != nil {
		t.Fatal(err)
	}
	if m := d.Metrics(); m.TableEntries != 1 {
		t.Errorf("bottom compaction kept %d entries; want only z", m.TableEntries)
	}
	if _, _, err := d.Get([]byte("k"), nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key after a bottom compaction: %v", err)
	}
}

func TestPickCompaction(t *testing.T) {
	d := &DB{opts: Options{MemtableBytes: 4 << 20}.withDefaults()}
	mk := func(sizes ...uint64) []*table {
		out := make([]*table, len(sizes))
		for i, s := range sizes {
			out[i] = &table{meta: tableMeta{fileNum: uint64(100 - i), size: s}}
		}
		return out
	}
	const M = 4 << 20
	for _, tc := range []struct {
		name   string
		sizes  []uint64 // newest first
		inputs int      // number of newest tables merged after skipping `from`
		from   int
		bottom bool
		reason string
	}{
		{"too few", []uint64{M, M, M}, 0, 0, false, ""},
		{"four similar newest", []uint64{M, M, M, M, 16 * M}, 4, 0, false, "similar sizes"},
		{"whole list similar", []uint64{M, M, M, M}, 4, 0, true, "similar sizes"},
		{"tiny tables count as a quarter memtable", []uint64{10, 20, M / 4, M / 3, 64 * M}, 4, 0, false, "similar sizes"},
		{"run broken by a big table", []uint64{M, M, M, 16 * M, 16 * M, 16 * M, 16 * M, 256 * M}, 4, 3, false, "similar sizes"},
		{"capped at max tables", []uint64{M, M, M, M, M, M, M, M, M, M, M, M, 999 * M}, 10, 0, false, "similar sizes"},
		{"space amplification", []uint64{M, 3 * M, 9 * M, 8 * M}, 4, 0, true, "space amplification"},
		{"tiered and healthy", []uint64{M, 4 * M, 16 * M, 64 * M}, 0, 0, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables := mk(tc.sizes...)
			c := d.pickCompaction(tables)
			if tc.inputs == 0 {
				if c != nil {
					t.Fatalf("picked %d tables (%s), want none", len(c.inputs), c.reason)
				}
				return
			}
			if c == nil {
				t.Fatal("picked nothing")
			}
			if len(c.inputs) != tc.inputs || c.inputs[0] != tables[tc.from] || c.bottom != tc.bottom || c.reason != tc.reason {
				t.Fatalf("picked %d tables from %d (bottom=%v, %s); want %d from %d (bottom=%v, %s)",
					len(c.inputs), 100-c.inputs[0].meta.fileNum, c.bottom, c.reason, tc.inputs, tc.from, tc.bottom, tc.reason)
			}
		})
	}
	d.opts.SlowdownTables = 6
	many := mk(64*M, M, 32*M, 2*M, 16*M, 4*M, 8*M)
	if c := d.pickCompaction(many); c == nil || c.reason != "too many tables" || len(c.inputs) != len(many) {
		t.Errorf("past SlowdownTables: %+v", c)
	}
}

func TestCheckpointHardLinksAndReopens(t *testing.T) {
	root := t.TempDir()
	src, dst := filepath.Join(root, "db"), filepath.Join(root, "ckpt")
	d, err := Open(src, Options{manual: true, MemtableBytes: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	model := state{}
	for i := range 3000 {
		k := fmt.Sprintf("key-%04d", i%700)
		v := fmt.Sprintf("v%d", i)
		model[k] = entry{[]byte(v), put(t, d, k, v)}
		if i%997 == 0 {
			mustFlush(t, d)
		}
	}
	seq, err := d.Checkpoint(dst)
	if err != nil {
		t.Fatal(err)
	}
	if seq != d.LastSeq() {
		t.Errorf("checkpoint seq %d, last seq %d", seq, d.LastSeq())
	}
	tables, _ := filepath.Glob(filepath.Join(dst, "*.sst"))
	if len(tables) == 0 {
		t.Fatal("checkpoint holds no tables")
	}
	for _, p := range tables {
		a, _ := os.Stat(p)
		b, err := os.Stat(filepath.Join(src, filepath.Base(p)))
		if err != nil || !os.SameFile(a, b) {
			t.Errorf("%s is not a hard link of the live table: %v", p, err)
		}
	}
	if logs, _ := filepath.Glob(filepath.Join(dst, "*.wal")); len(logs) != 0 {
		t.Errorf("checkpoint contains log segments %v", logs)
	}
	// Keep writing and compact, so the checkpointed tables become obsolete
	// in the source and are deleted there.
	for i := range 500 {
		put(t, d, fmt.Sprintf("key-%04d", i), "after")
	}
	mustFlush(t, d)
	if _, err := d.compactAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Checkpoint(dst); err == nil {
		t.Error("checkpoint into a non-empty directory succeeded")
	}

	c, err := Open(dst, Options{})
	if err != nil {
		t.Fatalf("open checkpoint: %v", err)
	}
	defer func() { _ = c.Close() }()
	if c.LastSeq() != seq {
		t.Errorf("checkpoint opens at seq %d, want %d", c.LastSeq(), seq)
	}
	checkScan(t, c, model)
}

// blockingFS holds the creation of table files until released.
type blockingFS struct {
	vfs.FS
	gate    chan struct{}
	entered atomic.Bool
}

func (b *blockingFS) Create(name string) (vfs.File, error) {
	if strings.HasSuffix(name, tableSuffix) {
		b.entered.Store(true)
		<-b.gate
	}
	return b.FS.Create(name)
}

func TestWritesStallWhileFlushesFallBehind(t *testing.T) {
	fs := &blockingFS{FS: vfs.NewMemFS(), gate: make(chan struct{})}
	d, err := Open(testDir, Options{FS: fs, MemtableBytes: 2 << 10, MaxImmutableMemtables: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	done := make(chan error, 1)
	go func() {
		for i := range 400 {
			var b Batch
			b.Set([]byte(fmt.Sprintf("k%04d", i)), bytes.Repeat([]byte("v"), 100))
			if _, err := d.Apply(&b, 0); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	deadline := time.Now().Add(10 * time.Second)
	for d.Metrics().WriteStalls == 0 {
		select {
		case err := <-done:
			t.Fatalf("writer finished (%v) without stalling while the flush was blocked", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("writer never stalled")
		}
		time.Sleep(time.Millisecond)
	}
	if !fs.entered.Load() {
		t.Error("stalled before any flush started")
	}
	select {
	case err := <-done:
		t.Fatalf("writer finished (%v) while stalled", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(fs.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	t.Logf("metrics after release: %+v", d.Metrics())
}

func TestCompactionRateLimit(t *testing.T) {
	d, err := Open(testDir, Options{FS: vfs.NewMemFS(), manual: true, CompactionBytesPerSec: 256 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	for round := range 4 {
		for i := range 300 {
			put(t, d, fmt.Sprintf("r%d-%04d", round, i), strings.Repeat("v", 200))
		}
		mustFlush(t, d)
	}
	start := time.Now()
	if _, err := d.compactAll(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	written := d.Metrics().CompactionBytesWrite
	if minimum := time.Duration(float64(written) / float64(256<<10) * 0.8 * float64(time.Second)); elapsed < minimum {
		t.Errorf("compacted %d bytes in %v at 256 KiB/s; expected at least %v", written, elapsed, minimum)
	}
}

func TestLeftoverFilesRemovedAtOpen(t *testing.T) {
	fs := vfs.NewMemFS()
	d := manualDB(t, fs)
	put(t, d, "a", "1")
	mustFlush(t, d)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"000099.sst", manifestTmpName} {
		f, _ := fs.Create(filepath.Join(testDir, name))
		_, _ = f.Write([]byte("leftover"))
	}
	_ = fs.SyncDir(testDir)
	d = manualDB(t, fs)
	names, _ := fs.List(testDir)
	for _, n := range names {
		if n == "000099.sst" || n == manifestTmpName {
			t.Errorf("leftover %s survived Open", n)
		}
	}
	if v, _, err := d.Get([]byte("a"), nil); err != nil || string(v) != "1" {
		t.Errorf("data lost: %q, %v", v, err)
	}
}

func TestDamagedTablesAndManifest(t *testing.T) {
	setup := func(t *testing.T) (*vfs.MemFS, string) {
		fs := vfs.NewMemFS()
		d := manualDB(t, fs)
		for i := range 500 {
			put(t, d, fmt.Sprintf("key-%04d", i), strings.Repeat("v", 50))
		}
		mustFlush(t, d)
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		names, _ := fs.List(testDir)
		for _, n := range names {
			if strings.HasSuffix(n, tableSuffix) {
				return fs, filepath.Join(testDir, n)
			}
		}
		t.Fatal("no table written")
		return nil, ""
	}
	refuses := func(t *testing.T, fs vfs.FS, want string) {
		t.Helper()
		_, err := Open(testDir, Options{FS: fs, manual: true})
		if !errors.Is(err, ErrCorruption) || !strings.Contains(err.Error(), want) {
			t.Fatalf("Open = %v; want ErrCorruption mentioning %q", err, want)
		}
		t.Logf("error: %v", err)
	}

	t.Run("data block damage fails the read", func(t *testing.T) {
		fs, path := setup(t)
		data, _ := fs.ReadFile(path)
		data[200] ^= 0x01
		_ = fs.WriteFile(path, data)
		d := manualDB(t, fs) // footer, index and filter are intact
		failed := 0
		for i := range 500 {
			if _, _, err := d.Get([]byte(fmt.Sprintf("key-%04d", i)), nil); err != nil {
				if !errors.Is(err, ErrCorruption) {
					t.Fatalf("Get error %v is not ErrCorruption", err)
				}
				failed++
			}
		}
		if failed == 0 {
			t.Error("no read hit the damaged block")
		}
		it, _ := d.NewIterator(IterOptions{})
		for it.First(); it.Valid(); it.Next() {
		}
		if !errors.Is(it.Err(), ErrCorruption) {
			t.Errorf("scan over a damaged block: %v", it.Err())
		}
		_ = it.Close()
	})
	t.Run("footer damage refuses to open", func(t *testing.T) {
		fs, path := setup(t)
		data, _ := fs.ReadFile(path)
		data[len(data)-3] ^= 0x01
		_ = fs.WriteFile(path, data)
		refuses(t, fs, filepath.Base(path))
	})
	t.Run("missing table refuses to open", func(t *testing.T) {
		fs, path := setup(t)
		_ = fs.Remove(path)
		_ = fs.SyncDir(testDir)
		refuses(t, fs, "does not exist")
	})
	t.Run("damaged manifest refuses to open", func(t *testing.T) {
		fs, _ := setup(t)
		mpath := filepath.Join(testDir, manifestName)
		data, _ := fs.ReadFile(mpath)
		data[len(data)/2] ^= 0x01
		_ = fs.WriteFile(mpath, data)
		refuses(t, fs, "MANIFEST")
	})
}

func TestFlushDeletesFlushedLogSegments(t *testing.T) {
	fs := vfs.NewMemFS()
	d, err := Open(testDir, Options{FS: fs, manual: true, MemtableBytes: 8 << 10}) // several segments
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	for i := range 2000 {
		put(t, d, fmt.Sprintf("k%d", i), strings.Repeat("v", 100))
	}
	before, _ := wal.ListSegments(fs, testDir)
	mustFlush(t, d)
	after, _ := wal.ListSegments(fs, testDir)
	if len(after) != 1 || len(before) < 2 {
		t.Errorf("segments before flush %v, after %v; want only the active one left", before, after)
	}
}

func TestConcurrentReadersDuringBackgroundWork(t *testing.T) {
	d, err := Open(testDir, Options{FS: vfs.NewMemFS(), Sync: SyncNever, MemtableBytes: 8 << 10,
		CompactionMinTables: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	// One writer sets every key of a group to the same value per batch;
	// readers check, through snapshots, that groups are never split, while
	// flushes and compactions replace tables underneath them.
	const keys = 32
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for n := range 3000 {
			var b Batch
			for k := range keys {
				b.Set([]byte(fmt.Sprintf("g%02d", k)), []byte(fmt.Sprintf("%06d-%s", n, strings.Repeat("p", 40))))
			}
			if _, err := d.Apply(&b, 0); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s := d.NewSnapshot()
				it, err := d.NewIterator(IterOptions{Snapshot: s})
				if err != nil {
					t.Error(err)
					return
				}
				var first []byte
				count := 0
				for it.First(); it.Valid(); it.Next() {
					if first == nil {
						first = bytes.Clone(it.Value())
					} else if !bytes.Equal(it.Value(), first) {
						t.Errorf("snapshot %d split a batch: %q vs %q", s.Seq(), first, it.Value())
					}
					count++
				}
				if it.Err() != nil || (count != 0 && count != keys) {
					t.Errorf("scan of %d keys: %v", count, it.Err())
				}
				_ = it.Close()
				s.Release()
			}
		}()
	}
	wg.Wait()
	if m := d.Metrics(); m.Flushes == 0 || m.Compactions == 0 {
		t.Errorf("no background work happened: %+v", m)
	}
}

func benchDB(b *testing.B, sync SyncPolicy) *DB {
	b.Helper()
	d, err := Open(b.TempDir(), Options{Sync: sync})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = d.Close() })
	return d
}

func BenchmarkApplySyncNever(b *testing.B) {
	d := benchDB(b, SyncNever)
	v := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := range b.N {
		var batch Batch
		batch.Set([]byte(fmt.Sprintf("key-%010d", i)), v)
		if _, err := d.Apply(&batch, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkApplySyncAlwaysParallel measures group commit. On a filesystem
// where fsync is free (tmpfs) it shows only the engine's own overhead.
func BenchmarkApplySyncAlwaysParallel(b *testing.B) {
	d := benchDB(b, SyncAlways)
	v := bytes.Repeat([]byte("v"), 100)
	var n atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var batch Batch
			batch.Set([]byte(fmt.Sprintf("key-%010d", n.Add(1))), v)
			if _, err := d.Apply(&batch, 0); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func benchLoaded(b *testing.B, keys int, flush bool) *DB {
	b.Helper()
	d := benchDB(b, SyncNever)
	v := bytes.Repeat([]byte("v"), 100)
	for i := range keys {
		var batch Batch
		batch.Set([]byte(fmt.Sprintf("key-%08d", i)), v)
		if _, err := d.Apply(&batch, 0); err != nil {
			b.Fatal(err)
		}
	}
	if flush {
		if err := d.Flush(); err != nil {
			b.Fatal(err)
		}
		if _, err := d.compactAll(); err != nil {
			b.Fatal(err)
		}
	}
	return d
}

func BenchmarkGetMemtable(b *testing.B) {
	const keys = 20_000
	d := benchLoaded(b, keys, false)
	b.ResetTimer()
	for i := range b.N {
		if _, _, err := d.Get([]byte(fmt.Sprintf("key-%08d", i%keys)), nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetTable(b *testing.B) {
	const keys = 100_000
	d := benchLoaded(b, keys, true)
	b.ResetTimer()
	for i := range b.N {
		if _, _, err := d.Get([]byte(fmt.Sprintf("key-%08d", i%keys)), nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetAbsentTable(b *testing.B) {
	d := benchLoaded(b, 100_000, true)
	b.ResetTimer()
	for i := range b.N {
		if _, _, err := d.Get([]byte(fmt.Sprintf("nope-%08d", i)), nil); !errors.Is(err, ErrNotFound) {
			b.Fatal(err)
		}
	}
}

func BenchmarkScanTable(b *testing.B) {
	const keys = 100_000
	d := benchLoaded(b, keys, true)
	b.ResetTimer()
	for range b.N {
		it, _ := d.NewIterator(IterOptions{})
		n := 0
		for it.First(); it.Valid(); it.Next() {
			n++
		}
		_ = it.Close()
		if n != keys {
			b.Fatalf("scanned %d keys", n)
		}
	}
	b.ReportMetric(keys, "keys/op")
}
