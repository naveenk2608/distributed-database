package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
	"github.com/naveenk2608/distributed-database/internal/storage/wal"
)

const testDir = "/db"

func openMem(t *testing.T, fs vfs.FS, opts Options) *DB {
	t.Helper()
	opts.FS = fs
	d, err := Open(testDir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return d
}

// TestBatchLastOpWins covers the ADR 0018 edge case: a batch has one
// sequence number, so two operations on one key would get identical internal
// keys. The later operation replaces the earlier one, in memory and after
// the batch is replayed from the log.
func TestBatchLastOpWins(t *testing.T) {
	type op struct {
		del bool
		val string
	}
	for _, tc := range []struct {
		name    string
		ops     []op
		present bool // whether k exists before the batch
		want    *string
	}{
		{"put+put", []op{{val: "a"}, {val: "b"}}, false, ptr("b")},
		{"put+put over existing", []op{{val: "a"}, {val: "b"}}, true, ptr("b")},
		{"put+delete", []op{{val: "a"}, {del: true}}, false, nil},
		{"put+delete over existing", []op{{val: "a"}, {del: true}}, true, nil},
		{"delete+put over existing", []op{{del: true}, {val: "a"}}, true, ptr("a")},
		{"put+delete+put", []op{{val: "a"}, {del: true}, {val: "c"}}, true, ptr("c")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := vfs.NewMemFS()
			d := openMem(t, fs, Options{})
			if tc.present {
				var b Batch
				b.Set([]byte("k"), []byte("old"))
				mustApply(t, d, &b, 0)
			}
			var b Batch
			b.Set([]byte("other"), []byte("x"))
			for _, o := range tc.ops {
				if o.del {
					b.Delete([]byte("k"))
				} else {
					b.Set([]byte("k"), []byte(o.val))
				}
			}
			if b.Count() != 2 {
				t.Fatalf("Count = %d, want 2: the batch must hold one operation per key", b.Count())
			}
			seq := mustApply(t, d, &b, 0)
			want := state{"other": {[]byte("x"), seq}}
			if tc.want != nil {
				want["k"] = entry{[]byte(*tc.want), seq}
			}
			probe := [][]byte{[]byte("k"), []byte("other")}
			checkState(t, d, nil, want, probe)

			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d = openMem(t, fs, Options{})
			defer func() { _ = d.Close() }()
			checkState(t, d, nil, want, probe)
		})
	}
}

func ptr(s string) *string { return &s }

func TestBatchIndexForLargeBatches(t *testing.T) {
	var b Batch
	for round := range 3 {
		for i := range 100 {
			b.Set([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprint(round)))
		}
	}
	b.Delete([]byte("k050"))
	if b.Count() != 100 {
		t.Fatalf("Count = %d, want 100", b.Count())
	}
	_, ops, err := decodeBatch(b.encode(nil, 7))
	if err != nil {
		t.Fatal(err)
	}
	for i, op := range ops {
		wantKind := ikey.KindSet
		if i == 50 {
			wantKind = ikey.KindDelete
		}
		if op.kind != wantKind || (wantKind == ikey.KindSet && string(op.value) != "2") {
			t.Fatalf("op %d = %v %q", i, op.kind, op.value)
		}
	}
}

// TestDecodeBatchRejectsPaddedLengths: a length written in more varint
// bytes than needed (0x80 0x00 for 0) is refused, so every batch has
// exactly one encoding. Found by FuzzDecodeBatch.
func TestDecodeBatchRejectsPaddedLengths(t *testing.T) {
	var b Batch
	b.Set([]byte("k"), []byte("v"))
	good := b.encode(nil, 9)
	if _, _, err := decodeBatch(good); err != nil {
		t.Fatalf("minimal encoding: %v", err)
	}
	// header | kind | klen | "k" | vlen | "v": pad each length in turn.
	for _, at := range []int{batchHeaderSize + 1, batchHeaderSize + 3} {
		padded := slices.Concat(good[:at], []byte{good[at] | 0x80, 0x00}, good[at+1:])
		if _, _, err := decodeBatch(padded); !errors.Is(err, errBadBatch) {
			t.Errorf("length at byte %d padded: err %v, want errBadBatch", at, err)
		}
	}
}

// workload drives random batches and remembers every batch it attempted,
// so a test can check what a recovered database must contain. Each batch
// also writes a unique marker key; the marker's version in the recovered
// database is the sequence number the batch got, which identifies exactly
// which batches survived, acknowledged or not.
type workload struct {
	mu      sync.Mutex
	nextID  int
	batches map[int][]batchOp // id -> operations, including the marker
	acked   map[int]uint64    // id -> sequence number Apply returned
}

func newWorkload() *workload {
	return &workload{batches: map[int][]batchOp{}, acked: map[int]uint64{}}
}

func markerKey(id int) []byte { return []byte(fmt.Sprintf("batch/%06d", id)) }

func (w *workload) newBatch(r *rand.Rand, numKeys int) (int, *Batch) {
	w.mu.Lock()
	id := w.nextID
	w.nextID++
	w.mu.Unlock()
	var b Batch
	for range 1 + r.IntN(4) {
		k := []byte(fmt.Sprintf("key/%03d", r.IntN(numKeys)))
		if r.IntN(4) == 0 {
			b.Delete(k)
		} else {
			b.Set(k, []byte(fmt.Sprintf("v%d-%s", id, strings.Repeat("x", r.IntN(64)))))
		}
	}
	b.Set(markerKey(id), []byte(fmt.Sprint(id)))
	w.mu.Lock()
	w.batches[id] = slices.Clone(b.ops)
	w.mu.Unlock()
	return id, &b
}

func (w *workload) ack(id int, seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.acked[id] = seq
}

// verify checks a recovered database against the workload.
func (w *workload) verify(t *testing.T, d *DB) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	survived := map[uint64]int{} // seq -> batch id
	for id := range w.batches {
		_, ver, err := d.Get(markerKey(id), nil)
		if errors.Is(err, ErrNotFound) {
			if seq, ok := w.acked[id]; ok {
				t.Fatalf("acknowledged batch %d (seq %d) was lost", id, seq)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if seq, ok := w.acked[id]; ok && seq != ver {
			t.Fatalf("batch %d acknowledged at seq %d recovered at %d", id, seq, ver)
		}
		if other, dup := survived[ver]; dup {
			t.Fatalf("batches %d and %d both recovered at seq %d", other, id, ver)
		}
		survived[ver] = id
	}
	last := d.LastSeq()
	if uint64(len(survived)) != last {
		t.Fatalf("recovered %d batches but LastSeq is %d: the log is not a prefix", len(survived), last)
	}
	model := state{}
	for seq := uint64(1); seq <= last; seq++ {
		id, ok := survived[seq]
		if !ok {
			t.Fatalf("seq %d is missing while seq %d survived: the log is not a prefix", seq, last)
		}
		for _, op := range w.batches[id] {
			if op.kind == ikey.KindDelete {
				delete(model, string(op.key))
			} else {
				model[string(op.key)] = entry{op.value, seq}
			}
		}
	}
	checkScan(t, d, model)
}

// checkScan compares a full scan and a point read of every key with want.
func checkScan(t *testing.T, d *DB, want state) {
	t.Helper()
	probe := make([][]byte, 0, len(want))
	for k := range want {
		probe = append(probe, []byte(k))
	}
	checkState(t, d, nil, want, probe)
	if t.Failed() {
		t.FailNow()
	}
}

// TestPowerLossKeepsAcknowledgedWrites is the Phase 2 gate check for
// simulated power loss: concurrent writers with SyncAlways, a crash at a
// random point, torn unsynced data, recovery, more writes, and another crash.
// Every acknowledged write must survive and the recovered state must be
// exactly a prefix of the log.
func TestPowerLossKeepsAcknowledgedWrites(t *testing.T) {
	seeds := 150
	if testing.Short() {
		seeds = 20
	}
	base := testSeed(t)
	var acked, lost atomic.Int64
	for s := range seeds {
		seed := base + uint64(s)
		r := rand.New(rand.NewPCG(seed, 7))
		fs := vfs.NewMemFS()
		w := newWorkload()
		for round := range 3 {
			d, err := Open(testDir, Options{FS: fs, MemtableBytes: 2 << 10})
			if err != nil {
				t.Fatalf("seed %d round %d: Open after crash: %v", seed, round, err)
			}
			w.verify(t, d)
			fs.CrashAfter(int64(r.IntN(300)))
			var wg sync.WaitGroup
			for g := range 4 {
				wr := rand.New(rand.NewPCG(seed, uint64(round*10+g)))
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						id, b := w.newBatch(wr, 40)
						seq, err := d.Apply(b, 0)
						if err != nil {
							return // the filesystem crashed
						}
						w.ack(id, seq)
						acked.Add(1)
					}
				}()
			}
			wg.Wait()
			_ = d.Close() // fails on the crashed filesystem; releases the lock
			fs = fs.CrashImage(rand.New(rand.NewPCG(seed, uint64(round))))
		}
		d, err := Open(testDir, Options{FS: fs})
		if err != nil {
			t.Fatalf("seed %d: final Open: %v", seed, err)
		}
		w.verify(t, d)
		lost.Add(int64(len(w.batches)) - int64(d.LastSeq()))
		_ = d.Close()
	}
	t.Logf("%d seeds: %d acknowledged batches all survived; %d unacknowledged batches were lost", seeds, acked.Load(), lost.Load())
}

// runSequential applies n batches from one writer until one fails.
func runSequential(d *DB, w *workload, r *rand.Rand, n int) {
	for range n {
		id, b := w.newBatch(r, 20)
		seq, err := d.Apply(b, 0)
		if err != nil {
			return
		}
		w.ack(id, seq)
	}
}

// TestCrashAtEveryStep crashes the filesystem at every mutating operation of
// a sequential workload that includes Open, segment rotations and Close, and
// recovers from both the exact durable state and a torn one. It then crashes
// again at every step of that recovery, which must also be safe to repeat.
func TestCrashAtEveryStep(t *testing.T) {
	const batches = 40
	// manual: no background goroutines, so every run performs the same
	// filesystem operations in the same order. Flush and compaction crash
	// points are covered by TestCrashDuringFlushAndCompaction.
	opts := Options{MemtableBytes: 1 << 10, manual: true}
	run := func(fs *vfs.MemFS, w *workload) {
		o := opts
		o.FS = fs
		d, err := Open(testDir, o)
		if err != nil {
			return
		}
		runSequential(d, w, rand.New(rand.NewPCG(1, 2)), batches)
		_ = d.Close()
	}
	dry := vfs.NewMemFS()
	run(dry, newWorkload())
	total := dry.OpCount()
	if total < 50 {
		t.Fatalf("workload performs only %d filesystem operations", total)
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
				rng = rand.New(rand.NewPCG(uint64(crashAt), 3))
			}
			img := fs.CrashImage(rng)

			// Crash at every step of recovery itself, then recover again.
			for recoverCrash := int64(0); ; recoverCrash++ {
				attempt := img.CrashImage(nil) // a fresh copy of the crashed state
				attempt.CrashAfter(recoverCrash)
				d, err := Open(testDir, Options{FS: attempt, manual: true})
				if err == nil {
					_ = d.Close()
				}
				finished := !attempt.Crashed()
				d, err = Open(testDir, Options{FS: attempt.CrashImage(nil), manual: true})
				if err != nil {
					t.Fatalf("crash at op %d (torn=%d), recovery crashed at op %d: reopen: %v", crashAt, torn, recoverCrash, err)
				}
				w.verify(t, d)
				_ = d.Close()
				recoveries++
				if finished {
					break
				}
			}
		}
	}
	t.Logf("%d crash points x 2 images, %d recoveries checked", total+1, recoveries)
}

func writeBatches(t *testing.T, d *DB, n int) {
	t.Helper()
	for i := range n {
		var b Batch
		b.Set([]byte(fmt.Sprintf("key-%04d", i)), bytes.Repeat([]byte{byte('a' + i%26)}, 100))
		mustApply(t, d, &b, 0)
	}
}

// TestCorruptRecordMidLogStopsOpen is the Phase 2 gate check for real
// corruption: a damaged record in the middle of the log makes Open fail
// with a clear error that names the file and offset, and changes nothing.
func TestCorruptRecordMidLogStopsOpen(t *testing.T) {
	for _, where := range []string{"older segment", "middle of newest segment"} {
		t.Run(where, func(t *testing.T) {
			fs := vfs.NewMemFS()
			// manual: nothing is flushed, so the records stay in log
			// segments for the test to damage.
			d := openMem(t, fs, Options{MemtableBytes: 4 << 10, manual: true})
			writeBatches(t, d, 200)
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			segs, err := wal.ListSegments(fs, testDir)
			if err != nil || len(segs) < 3 {
				t.Fatalf("segments %v, %v; want at least 3", segs, err)
			}
			seg := segs[0]
			if where == "middle of newest segment" {
				// Reopen and write more, so the newest segment holds
				// records rather than being the empty one Close left.
				d = openMem(t, fs, Options{manual: true})
				writeBatches(t, d, 20)
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				segs, _ = wal.ListSegments(fs, testDir)
				seg = segs[len(segs)-1] // freshly written, 20 records
			}
			path := wal.SegmentPath(testDir, seg)
			data, _ := fs.ReadFile(path)
			off := len(data) / 2
			data[off] ^= 0xff
			if err := fs.WriteFile(path, data); err != nil {
				t.Fatal(err)
			}

			for attempt := range 2 { // the lock must be released after a failed Open
				_, err := Open(testDir, Options{FS: fs})
				if !errors.Is(err, ErrCorruption) || !errors.Is(err, wal.ErrCorrupt) {
					t.Fatalf("attempt %d: Open = %v; want ErrCorruption", attempt, err)
				}
				msg := err.Error()
				for _, want := range []string{path, "offset", "refusing to start"} {
					if !strings.Contains(msg, want) {
						t.Errorf("error %q does not mention %q", msg, want)
					}
				}
				if attempt == 0 {
					t.Logf("error: %v", err)
				}
			}
			after, _ := fs.ReadFile(path)
			if !bytes.Equal(after, data) {
				t.Error("a failed Open modified the corrupt segment")
			}
		})
	}
}

func TestReopenKeepsVersionsAndRaftIndexes(t *testing.T) {
	fs := vfs.NewMemFS()
	d := openMem(t, fs, Options{})
	for _, seq := range []uint64{10, 20, 35} { // Raft indexes with gaps
		var b Batch
		b.Set([]byte("k"), []byte(fmt.Sprint(seq)))
		b.Set([]byte(fmt.Sprintf("session/%d", seq)), []byte("r"))
		mustApply(t, d, &b, seq)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openMem(t, fs, Options{})
	defer func() { _ = d.Close() }()
	if d.LastSeq() != 35 {
		t.Fatalf("LastSeq after reopen = %d, want 35", d.LastSeq())
	}
	if v, ver, err := d.Get([]byte("k"), nil); err != nil || string(v) != "35" || ver != 35 {
		t.Fatalf("Get = %q@%d, %v", v, ver, err)
	}
	var b Batch
	b.Set([]byte("k"), nil)
	if _, err := d.Apply(&b, 35); !errors.Is(err, ErrSeqNotIncreased) {
		t.Errorf("reapplying index 35: %v", err)
	}
	mustApply(t, d, &b, 36)
}

func TestSecondOpenIsLocked(t *testing.T) {
	for _, tc := range []struct {
		name string
		fs   vfs.FS
		dir  string
	}{{"mem", vfs.NewMemFS(), testDir}, {"os", vfs.Default, t.TempDir()}} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := Open(tc.dir, Options{FS: tc.fs})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Open(tc.dir, Options{FS: tc.fs}); !errors.Is(err, vfs.ErrLocked) {
				t.Errorf("second Open: %v, want ErrLocked", err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d, err = Open(tc.dir, Options{FS: tc.fs})
			if err != nil {
				t.Fatalf("Open after Close: %v", err)
			}
			_ = d.Close()
		})
	}
}

// syncCountingFS counts fsyncs of WAL segments and can hold the first one.
type syncCountingFS struct {
	vfs.FS
	syncs   atomic.Int64
	hold    chan struct{} // if non-nil, the first Sync blocks until it is closed
	entered chan struct{}
	once    sync.Once
}

func (f *syncCountingFS) Create(name string) (vfs.File, error) {
	file, err := f.FS.Create(name)
	if err != nil || !strings.HasSuffix(name, ".wal") {
		return file, err
	}
	return &countedFile{File: file, fs: f}, nil
}

type countedFile struct {
	vfs.File
	fs *syncCountingFS
}

func (c *countedFile) Sync() error {
	if c.fs.hold != nil {
		c.fs.once.Do(func() {
			close(c.fs.entered)
			<-c.fs.hold
		})
	}
	c.fs.syncs.Add(1)
	return c.File.Sync()
}

// TestGroupCommitOneRecordPerBatchOneSync checks how group commit uses the
// log: while the first writer's fsync is in progress the others queue up,
// and the next leader writes each of their batches as its own record, with
// its own sequence number, and covers them all with a single fsync.
func TestGroupCommitOneRecordPerBatchOneSync(t *testing.T) {
	const writers = 16
	fs := &syncCountingFS{FS: vfs.NewMemFS(), hold: make(chan struct{}), entered: make(chan struct{})}
	d := openMem(t, fs, Options{})
	base := fs.syncs.Load()

	results := make([]uint64, writers)
	var wg sync.WaitGroup
	apply := func(i int) {
		defer wg.Done()
		var b Batch
		b.Set([]byte(fmt.Sprintf("w%02d", i)), []byte(fmt.Sprint(i)))
		results[i] = mustApply(t, d, &b, 0)
	}
	wg.Add(1)
	go apply(0)
	<-fs.entered // writer 0 leads a group of one and is inside fsync
	for i := 1; i < writers; i++ {
		wg.Add(1)
		go apply(i)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		d.qmu.Lock()
		queued := len(d.queue)
		d.qmu.Unlock()
		if queued == writers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d writers queued", queued)
		}
		time.Sleep(time.Millisecond)
	}
	close(fs.hold)
	wg.Wait()

	if got := fs.syncs.Load() - base; got != 2 {
		t.Errorf("%d fsyncs for %d batches; want 2 (one per group)", got, writers)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	seqs := map[uint64]bool{}
	for _, s := range results {
		seqs[s] = true
	}
	if len(seqs) != writers {
		t.Errorf("writers got %d distinct sequence numbers, want %d", len(seqs), writers)
	}
	var recSeqs []uint64
	_, err := wal.Recover(fs, testDir, func(_ uint64, r wal.Record) error {
		seq, ops, err := decodeBatch(r.Payload)
		if err != nil || len(ops) != 1 {
			return fmt.Errorf("record %d: %d ops: %w", seq, len(ops), err)
		}
		recSeqs = append(recSeqs, seq)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(recSeqs, slices.Sorted(maps.Keys(seqs))) {
		t.Errorf("log records have sequence numbers %v; want one record per batch, in order", recSeqs)
	}
}

func TestSyncPolicies(t *testing.T) {
	put := func(d *DB, k string) {
		var b Batch
		b.Set([]byte(k), []byte("v"))
		mustApply(t, d, &b, 0)
	}
	has := func(fs vfs.FS, k string) bool {
		d, err := Open(testDir, Options{FS: fs})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = d.Close() }()
		_, _, err = d.Get([]byte(k), nil)
		return err == nil
	}

	t.Run("never loses unsynced writes on power loss but syncs on Close", func(t *testing.T) {
		mem := vfs.NewMemFS()
		d := openMem(t, mem, Options{Sync: SyncNever})
		put(d, "a")
		if has(mem.CrashImage(nil), "a") {
			t.Error("SyncNever write survived a power loss; the test cannot tell the policies apart")
		}
		_ = d.Close() // fails on the crashed filesystem but stops background work
		mem = vfs.NewMemFS()
		d = openMem(t, mem, Options{Sync: SyncNever})
		put(d, "a")
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		if !has(mem.CrashImage(nil), "a") {
			t.Error("write lost after a clean Close")
		}
	})

	t.Run("interval syncs in the background", func(t *testing.T) {
		mem := vfs.NewMemFS()
		fs := &syncCountingFS{FS: mem}
		d := openMem(t, fs, Options{Sync: SyncInterval, SyncInterval: 5 * time.Millisecond})
		before := fs.syncs.Load()
		put(d, "a")
		deadline := time.Now().Add(10 * time.Second)
		for fs.syncs.Load() == before {
			if time.Now().After(deadline) {
				t.Fatal("no background fsync within 10s")
			}
			time.Sleep(time.Millisecond)
		}
		img := mem.CrashImage(nil)
		_ = d.Close() // fails: the original filesystem has crashed
		if !has(img, "a") {
			t.Error("write not durable after a background fsync")
		}
	})
}

// TestReopenModel runs random batches with tiny memtables, so reads and
// scans span many memtables, and reopens at random points.
func TestReopenModel(t *testing.T) {
	r := rand.New(rand.NewPCG(testSeed(t), 11))
	fs := vfs.NewMemFS()
	opts := Options{MemtableBytes: 512, manual: true} // memtables pile up unflushed
	d := openMem(t, fs, opts)
	w := newWorkload()
	maxImm := 0
	for round := range 2000 {
		id, b := w.newBatch(r, 60)
		w.ack(id, mustApply(t, d, b, 0))
		if s := d.loadState(); s != nil {
			maxImm = max(maxImm, len(s.imm))
			s.unref()
		}
		if r.IntN(200) == 0 {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d = openMem(t, fs, opts)
		}
		if round%400 == 0 {
			w.verify(t, d)
		}
	}
	if maxImm < 5 {
		t.Errorf("at most %d immutable memtables; the test does not exercise the merge", maxImm)
	}
	w.verify(t, d)
	_ = d.Close()
}

func TestClosedDBRefusesWrites(t *testing.T) {
	fs := vfs.NewMemFS()
	d := openMem(t, fs, Options{})
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	var b Batch
	b.Set([]byte("k"), nil)
	if _, err := d.Apply(&b, 0); !errors.Is(err, ErrClosed) {
		t.Errorf("Apply after Close: %v", err)
	}
	if err := d.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("second Close: %v", err)
	}
}

func TestFailedLogMakesWritesFail(t *testing.T) {
	fs := vfs.NewMemFS()
	d := openMem(t, fs, Options{})
	fs.Crash()
	var b Batch
	b.Set([]byte("k"), []byte("v"))
	if _, err := d.Apply(&b, 0); !errors.Is(err, vfs.ErrCrashed) {
		t.Fatalf("Apply on a crashed filesystem: %v", err)
	}
	if _, err := d.Apply(&b, 0); !errors.Is(err, vfs.ErrCrashed) {
		t.Errorf("second Apply: %v, want the sticky error", err)
	}
	if _, _, err := d.Get([]byte("k"), nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("a failed write became visible: %v", err)
	}
	_ = d.Close()
}

func FuzzDecodeBatch(f *testing.F) {
	var b Batch
	b.Set([]byte("key"), []byte("value"))
	b.Delete([]byte("gone"))
	b.Set(nil, nil)
	f.Add(b.encode(nil, 42))
	f.Add([]byte{})
	f.Add(make([]byte, 12))
	f.Fuzz(func(t *testing.T, p []byte) {
		seq, ops, err := decodeBatch(p)
		if err != nil {
			return
		}
		var re Batch
		for _, op := range ops {
			if op.kind == ikey.KindDelete {
				re.Delete(op.key)
			} else {
				re.Set(op.key, op.value)
			}
		}
		if !bytes.Equal(re.encode(nil, seq), p) {
			t.Fatalf("decode then encode changed the payload")
		}
	})
}
