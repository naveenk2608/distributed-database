package lsm

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

var seedFlag = flag.Uint64("seed", 0, "seed for randomized tests (0 = derive from the clock)")

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// memDB opens an in-memory database that is closed when the test ends;
// its background goroutines would otherwise outlive the test.
func memDB(t *testing.T) *DB {
	t.Helper()
	d := OpenInMemory()
	t.Cleanup(func() { _ = d.Close() }) // ErrClosed if the test closed it already
	return d
}

func testSeed(t *testing.T) uint64 {
	t.Helper()
	seed := *seedFlag
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	t.Logf("seed %d (replay with -seed=%d)", seed, seed)
	return seed
}

type entry struct {
	value   []byte
	version uint64
}

// state is the model: the live keys of the database at some point.
type state map[string]entry

func mustApply(t *testing.T, d *DB, b *Batch, seq uint64) uint64 {
	t.Helper()
	got, err := d.Apply(b, seq)
	if err != nil {
		t.Fatalf("Apply(seq=%d): %v", seq, err)
	}
	return got
}

// checkState compares every point read and a full scan at snap with want.
func checkState(t *testing.T, d *DB, snap *Snapshot, want state, probeKeys [][]byte) {
	t.Helper()
	for _, k := range probeKeys {
		v, ver, err := d.Get(k, snap)
		w, ok := want[string(k)]
		switch {
		case !ok && !errors.Is(err, ErrNotFound):
			t.Errorf("Get(%q) = %q@%d, %v; want ErrNotFound", k, v, ver, err)
		case ok && (err != nil || !bytes.Equal(v, w.value) || ver != w.version):
			t.Errorf("Get(%q) = %q@%d, %v; want %q@%d", k, v, ver, err, w.value, w.version)
		}
	}

	it, err := d.NewIterator(IterOptions{Snapshot: snap})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	wantKeys := slices.Sorted(maps.Keys(want))
	var gotKeys []string
	for it.First(); it.Valid(); it.Next() {
		k := string(it.Key())
		gotKeys = append(gotKeys, k)
		if w := want[k]; !bytes.Equal(it.Value(), w.value) || it.Version() != w.version {
			t.Errorf("scan: %q = %q@%d; want %q@%d", k, it.Value(), it.Version(), w.value, w.version)
		}
	}
	if !slices.Equal(gotKeys, wantKeys) {
		t.Errorf("scan keys = %q\nwant %q", gotKeys, wantKeys)
	}
}

func TestBasicOperations(t *testing.T) {
	d := memDB(t)
	defer func() { _ = d.Close() }()

	if _, _, err := d.Get([]byte("a"), nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on empty db: %v, want ErrNotFound", err)
	}

	var b Batch
	b.Set([]byte("a"), []byte("1"))
	if seq := mustApply(t, d, &b, 0); seq != 1 {
		t.Errorf("first engine-assigned seq = %d, want 1", seq)
	}
	b.Reset()
	b.Set([]byte("b"), []byte("2"))
	b.Set([]byte("a"), []byte("x"))
	b.Set([]byte("a"), []byte("3")) // same key later in the batch wins
	if seq := mustApply(t, d, &b, 0); seq != 2 {
		t.Errorf("second seq = %d, want 2", seq)
	}
	snap := d.NewSnapshot()

	b.Reset()
	b.Delete([]byte("a"))
	mustApply(t, d, &b, 0)

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	// Every key a batch writes gets the batch's sequence number as version.
	checkState(t, d, nil, state{"b": {[]byte("2"), 2}}, keys)
	checkState(t, d, snap, state{"a": {[]byte("3"), 2}, "b": {[]byte("2"), 2}}, keys)
	if d.LastSeq() != 3 {
		t.Errorf("LastSeq = %d, want 3", d.LastSeq())
	}
}

// TestBatchSharesOneSequenceNumber checks the property Raft needs: a batch
// applied at log index i makes every key it writes have version i, and the
// next log index is still accepted.
func TestBatchSharesOneSequenceNumber(t *testing.T) {
	d := memDB(t)
	var b Batch
	b.Set([]byte("value-key"), []byte("v"))
	b.Set([]byte("session-row"), []byte("client-1:req-7"))
	b.Delete([]byte("other"))
	mustApply(t, d, &b, 10)
	b.Reset()
	b.Set([]byte("value-key"), []byte("w"))
	mustApply(t, d, &b, 11)
	checkState(t, d, nil, state{
		"value-key":   {[]byte("w"), 11},
		"session-row": {[]byte("client-1:req-7"), 10},
	}, [][]byte{[]byte("value-key"), []byte("session-row"), []byte("other")})
}

func TestBatchDeleteThenSetSameKey(t *testing.T) {
	d := memDB(t)
	var b Batch
	b.Set([]byte("k"), []byte("old"))
	mustApply(t, d, &b, 0)
	b.Reset()
	b.Delete([]byte("k"))
	b.Set([]byte("k"), []byte("new"))
	b.Delete([]byte("j"))
	b.Set([]byte("j"), []byte("j"))
	b.Delete([]byte("j"))
	mustApply(t, d, &b, 0)
	checkState(t, d, nil, state{"k": {[]byte("new"), 2}}, [][]byte{[]byte("k"), []byte("j")})
}

func TestEmptyKeyAndValue(t *testing.T) {
	d := memDB(t)
	var b Batch
	b.Set(nil, nil)
	mustApply(t, d, &b, 0)
	v, ver, err := d.Get([]byte{}, nil)
	if err != nil || len(v) != 0 || ver != 1 {
		t.Fatalf("Get(empty) = %q, %d, %v; want empty value at version 1", v, ver, err)
	}
}

func TestGetReturnsCopy(t *testing.T) {
	d := memDB(t)
	var b Batch
	b.Set([]byte("k"), []byte("value"))
	mustApply(t, d, &b, 0)
	v, _, _ := d.Get([]byte("k"), nil)
	v[0] = 'X'
	if v2, _, _ := d.Get([]byte("k"), nil); string(v2) != "value" {
		t.Fatalf("mutating a returned value changed the db: %q", v2)
	}
}

func TestExplicitSequenceNumbers(t *testing.T) {
	d := memDB(t)
	var b Batch
	b.Set([]byte("k"), []byte("v"))
	if got := mustApply(t, d, &b, 10); got != 10 {
		t.Fatalf("Apply at 10 returned %d", got)
	}
	// Gaps are fine; going backwards or repeating is not.
	for _, seq := range []uint64{10, 5} {
		if _, err := d.Apply(&b, seq); !errors.Is(err, ErrSeqNotIncreased) {
			t.Errorf("Apply at %d: %v, want ErrSeqNotIncreased", seq, err)
		}
	}
	if got := mustApply(t, d, &b, 0); got != 11 {
		t.Errorf("engine-assigned seq after 10 = %d, want 11", got)
	}
	if got := mustApply(t, d, &b, 100); got != 100 {
		t.Errorf("Apply at 100 returned %d", got)
	}
	if _, v, _ := d.Get([]byte("k"), nil); v != 100 {
		t.Errorf("version = %d, want 100", v)
	}

	if _, err := d.Apply(&b, 1<<56); !errors.Is(err, ErrSeqTooLarge) {
		t.Errorf("Apply past MaxSeq: %v, want ErrSeqTooLarge", err)
	}
	if _, err := d.Apply(&Batch{}, 0); !errors.Is(err, ErrEmptyBatch) {
		t.Errorf("empty batch: %v, want ErrEmptyBatch", err)
	}
	if d.LastSeq() != 100 {
		t.Errorf("rejected batches moved LastSeq to %d", d.LastSeq())
	}

	// The last usable sequence number works; the engine then refuses to
	// assign one past it instead of wrapping around.
	if got := mustApply(t, d, &b, 1<<56-1); got != 1<<56-1 {
		t.Errorf("Apply at MaxSeq returned %d", got)
	}
	if _, err := d.Apply(&b, 0); !errors.Is(err, ErrSeqTooLarge) {
		t.Errorf("engine-assigned seq after MaxSeq: %v, want ErrSeqTooLarge", err)
	}
}

func TestClose(t *testing.T) {
	d := memDB(t)
	var b Batch
	b.Set([]byte("k"), []byte("v"))
	mustApply(t, d, &b, 0)
	it, err := d.NewIterator(IterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("second Close: %v, want ErrClosed", err)
	}
	if _, err := d.Apply(&b, 0); !errors.Is(err, ErrClosed) {
		t.Errorf("Apply after Close: %v", err)
	}
	if _, _, err := d.Get([]byte("k"), nil); !errors.Is(err, ErrClosed) {
		t.Errorf("Get after Close: %v", err)
	}
	if _, err := d.NewIterator(IterOptions{}); !errors.Is(err, ErrClosed) {
		t.Errorf("NewIterator after Close: %v", err)
	}
	it.First()
	if !it.Valid() || string(it.Key()) != "k" {
		t.Error("an iterator opened before Close stopped working")
	}
}

func TestIteratorSeekGE(t *testing.T) {
	d := memDB(t)
	var b Batch
	for _, k := range []string{"a", "c", "e", "g"} {
		b.Set([]byte(k), []byte(k))
	}
	b.Delete([]byte("e"))
	mustApply(t, d, &b, 0)
	it, _ := d.NewIterator(IterOptions{})
	for _, tc := range []struct{ seek, want string }{
		{"", "a"}, {"b", "c"}, {"c", "c"}, {"d", "g"}, {"e", "g"}, {"h", ""},
	} {
		it.SeekGE([]byte(tc.seek))
		got := ""
		if it.Valid() {
			got = string(it.Key())
		}
		if got != tc.want {
			t.Errorf("SeekGE(%q) = %q, want %q", tc.seek, got, tc.want)
		}
	}
}

// TestModelRandomized runs random batches of sets and deletes against both
// the DB and a Go map, taking snapshots along the way, and checks the latest
// state and every snapshot against the model.
func TestModelRandomized(t *testing.T) {
	r := rand.New(rand.NewPCG(testSeed(t), 1))
	const rounds, numKeys = 3000, 200
	d := memDB(t)
	model := state{}
	probe := make([][]byte, numKeys+5)
	for i := range probe {
		probe[i] = []byte(fmt.Sprintf("k%03d", i))
	}
	type saved struct {
		snap  *Snapshot
		state state
	}
	var snaps []saved
	nextSeq := uint64(1)

	for round := range rounds {
		var b Batch
		pending := maps.Clone(model)
		explicit := r.IntN(3) == 0
		first := uint64(0)
		if explicit {
			first = nextSeq + r.Uint64N(5) // leave gaps, like Raft no-op entries
		}
		ops := 1 + r.IntN(4)
		base := first
		if base == 0 {
			base = d.LastSeq() + 1
		}
		for i := range ops {
			k := probe[r.IntN(numKeys)]
			if r.IntN(4) == 0 {
				b.Delete(k)
				delete(pending, string(k))
			} else {
				v := []byte(fmt.Sprintf("v%d.%d", round, i))
				b.Set(k, v)
				pending[string(k)] = entry{v, base}
			}
		}
		got := mustApply(t, d, &b, first)
		if got != base {
			t.Fatalf("round %d: Apply returned seq %d, want %d", round, got, base)
		}
		nextSeq = base + 1
		model = pending

		if r.IntN(50) == 0 {
			snaps = append(snaps, saved{d.NewSnapshot(), maps.Clone(model)})
		}
		if round%250 == 0 {
			checkState(t, d, nil, model, probe)
		}
		if t.Failed() {
			t.FailNow()
		}
	}
	checkState(t, d, nil, model, probe)
	for _, s := range snaps {
		checkState(t, d, s.snap, s.state, probe)
	}
	t.Logf("checked %d snapshots", len(snaps))
}

// TestConcurrentReadsSeeWholeBatches checks batch atomicity under
// concurrency: the writer applies batches that set every key to the same
// value, and readers using snapshots must always see all keys equal.
func TestConcurrentReadsSeeWholeBatches(t *testing.T) {
	const numKeys, numBatches, numReaders = 16, 2000, 6
	d := memDB(t)
	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%02d", i))
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		var b Batch
		for n := range numBatches {
			b.Reset()
			for _, k := range keys {
				b.Set(k, []byte(fmt.Sprint(n)))
			}
			if _, err := d.Apply(&b, 0); err != nil {
				t.Errorf("Apply: %v", err)
				return
			}
		}
	}()
	var checks atomic.Int64
	for range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for finished := false; !finished && !t.Failed(); {
				select {
				case <-done:
					finished = true
				default:
				}
				snap := d.NewSnapshot()
				var first []byte
				for i, k := range keys {
					v, _, err := d.Get(k, snap)
					if errors.Is(err, ErrNotFound) && snap.Seq() == 0 {
						continue // before the first batch
					}
					if err != nil {
						t.Errorf("Get(%q) at %d: %v", k, snap.Seq(), err)
						return
					}
					if i == 0 {
						first = v
					} else if !bytes.Equal(v, first) {
						t.Errorf("snapshot %d saw a partial batch: %q=%q but %q=%q", snap.Seq(), keys[0], first, k, v)
						return
					}
				}
				// An iterator at the same snapshot must agree, even while the
				// writer keeps adding newer versions underneath it.
				it, err := d.NewIterator(IterOptions{Snapshot: snap})
				if err != nil {
					t.Errorf("NewIterator: %v", err)
					return
				}
				n := 0
				for it.First(); it.Valid(); it.Next() {
					if !bytes.Equal(it.Value(), first) || it.Version() != snap.Seq() {
						t.Errorf("iterator at %d: %q=%q@%d, want %q@%d", snap.Seq(), it.Key(), it.Value(), it.Version(), first, snap.Seq())
					}
					n++
				}
				_ = it.Close()
				if snap.Seq() > 0 && n != numKeys {
					t.Errorf("iterator at %d saw %d keys, want %d", snap.Seq(), n, numKeys)
				}
				checks.Add(1)
			}
		}()
	}
	wg.Wait()
	t.Logf("%d consistent snapshot checks", checks.Load())
}
