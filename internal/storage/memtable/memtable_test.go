package memtable

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
)

var seedFlag = flag.Uint64("seed", 0, "seed for randomized tests (0 = derive from the clock)")

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// testSeed returns the seed for a randomized test and logs it so that any
// failure can be replayed with -seed.
func testSeed(t *testing.T) uint64 {
	t.Helper()
	seed := *seedFlag
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	t.Logf("seed %d (replay with -seed=%d)", seed, seed)
	return seed
}

// version is one entry for a user key in the model.
type version struct {
	seq   uint64
	kind  ikey.Kind
	value []byte
}

// model is the reference implementation: a Go map from user key to its
// versions in ascending sequence order.
type model map[string][]version

// at returns the newest version of key with seq <= s.
func (md model) at(key string, s uint64) (version, bool) {
	vs := md[key]
	i := sort.Search(len(vs), func(i int) bool { return vs[i].seq > s })
	if i == 0 {
		return version{}, false
	}
	return vs[i-1], true
}

func keyName(i int) []byte { return []byte(fmt.Sprintf("key-%04d", i)) }

func randValue(r *rand.Rand) []byte {
	v := make([]byte, r.IntN(24))
	for i := range v {
		v[i] = byte('a' + r.IntN(26))
	}
	return v
}

// checkGet compares Memtable.Get with the model for one key and snapshot.
func checkGet(t *testing.T, m *Memtable, md model, key []byte, s uint64) {
	t.Helper()
	want, wantOK := md.at(string(key), s)
	val, tr, ok := m.Get(key, s)
	if ok != wantOK {
		t.Errorf("Get(%q, %d): ok=%v, want %v", key, s, ok, wantOK)
		return
	}
	if !ok {
		return
	}
	if tr.Seq() != want.seq || tr.Kind() != want.kind || !bytes.Equal(val, want.value) {
		t.Errorf("Get(%q, %d) = %q#%d,%s; want %q#%d,%s",
			key, s, val, tr.Seq(), tr.Kind(), want.value, want.seq, want.kind)
	}
}

// checkScan iterates the whole memtable, checks that internal keys are
// strictly increasing, and checks that the newest entry visible at s for
// each user key matches the model. Entries newer than s are ignored, which
// lets readers run this while a writer is adding entries.
func checkScan(t *testing.T, m *Memtable, md model, s uint64) {
	t.Helper()
	it := m.NewIter()
	var prev ikey.Key
	first := true
	visible := 0
	var lastVisibleUser []byte
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if !first && ikey.Compare(prev, k) >= 0 {
			t.Errorf("scan order: %s then %s", prev, k)
			return
		}
		prev, first = k, false
		if k.Seq() > s {
			continue
		}
		if lastVisibleUser != nil && bytes.Equal(lastVisibleUser, k.UserKey) {
			continue // an older version of a key already checked
		}
		lastVisibleUser = k.UserKey
		visible++
		want, ok := md.at(string(k.UserKey), s)
		if !ok || want.seq != k.Seq() || want.kind != k.Kind() || !bytes.Equal(want.value, it.Value()) {
			t.Errorf("scan at %d: found %s=%q, model has %+v (ok=%v)", s, k, it.Value(), want, ok)
		}
	}
	wantVisible := 0
	for key := range md {
		if _, ok := md.at(key, s); ok {
			wantVisible++
		}
	}
	if visible != wantVisible {
		t.Errorf("scan at %d: %d user keys visible, model has %d", s, visible, wantVisible)
	}
}

func TestBasic(t *testing.T) {
	m := New()
	mustAdd := func(seq uint64, kind ikey.Kind, k, v string) {
		t.Helper()
		if err := m.Add(seq, kind, []byte(k), []byte(v)); err != nil {
			t.Fatalf("Add(%d, %s, %q): %v", seq, kind, k, err)
		}
	}
	mustAdd(1, ikey.KindSet, "a", "a1")
	mustAdd(2, ikey.KindSet, "b", "b2")
	mustAdd(3, ikey.KindSet, "a", "a3")
	mustAdd(4, ikey.KindDelete, "a", "ignored")

	for _, tc := range []struct {
		key      string
		seq      uint64
		ok       bool
		wantSeq  uint64
		wantKind ikey.Kind
		wantVal  string
	}{
		{"a", 0, false, 0, 0, ""},
		{"a", 1, true, 1, ikey.KindSet, "a1"},
		{"a", 2, true, 1, ikey.KindSet, "a1"},
		{"a", 3, true, 3, ikey.KindSet, "a3"},
		{"a", 4, true, 4, ikey.KindDelete, ""},
		{"a", ikey.MaxSeq, true, 4, ikey.KindDelete, ""},
		{"b", 1, false, 0, 0, ""},
		{"b", 9, true, 2, ikey.KindSet, "b2"},
		{"c", 9, false, 0, 0, ""},
		{"", 9, false, 0, 0, ""},
	} {
		v, tr, ok := m.Get([]byte(tc.key), tc.seq)
		if ok != tc.ok || (ok && (tr.Seq() != tc.wantSeq || tr.Kind() != tc.wantKind || string(v) != tc.wantVal)) {
			t.Errorf("Get(%q, %d) = %q, %d, %s, %v; want %q, %d, %s, %v",
				tc.key, tc.seq, v, tr.Seq(), tr.Kind(), ok, tc.wantVal, tc.wantSeq, tc.wantKind, tc.ok)
		}
	}
	if m.Len() != 4 {
		t.Errorf("Len = %d, want 4", m.Len())
	}
	if m.ApproximateSize() <= 0 {
		t.Errorf("ApproximateSize = %d, want > 0", m.ApproximateSize())
	}
}

func TestAddCopiesInputs(t *testing.T) {
	m := New()
	k, v := []byte("key"), []byte("value")
	if err := m.Add(1, ikey.KindSet, k, v); err != nil {
		t.Fatal(err)
	}
	k[0], v[0] = 'X', 'X'
	got, _, ok := m.Get([]byte("key"), 1)
	if !ok || string(got) != "value" {
		t.Fatalf("Get after mutating caller buffers = %q, %v; want \"value\", true", got, ok)
	}
}

func TestAddRejectsBadInput(t *testing.T) {
	m := New()
	if err := m.Add(5, ikey.KindSet, []byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := m.Add(5, ikey.KindSet, []byte("k"), []byte("v2")); !errors.Is(err, ErrDuplicate) {
		t.Errorf("duplicate Add: err = %v, want ErrDuplicate", err)
	}
	// Same key and seq with a different kind is also a duplicate, whether
	// it would sort after (DEL) or before (SET) the existing entry.
	if err := m.Add(5, ikey.KindDelete, []byte("k"), nil); !errors.Is(err, ErrDuplicate) {
		t.Errorf("DEL at an existing SET's seq: err = %v, want ErrDuplicate", err)
	}
	if err := m.Add(7, ikey.KindDelete, []byte("k"), nil); err != nil {
		t.Fatal(err)
	}
	if err := m.Add(7, ikey.KindSet, []byte("k"), []byte("v")); !errors.Is(err, ErrDuplicate) {
		t.Errorf("SET at an existing DEL's seq: err = %v, want ErrDuplicate", err)
	}
	if err := m.Add(ikey.MaxSeq+1, ikey.KindSet, []byte("k"), nil); !errors.Is(err, ErrSeqTooLarge) {
		t.Errorf("huge seq: err = %v, want ErrSeqTooLarge", err)
	}
	if err := m.Add(6, ikey.Kind(9), []byte("k"), nil); !errors.Is(err, ErrInvalidKind) {
		t.Errorf("bad kind: err = %v, want ErrInvalidKind", err)
	}
	if m.Len() != 2 {
		t.Errorf("rejected Adds changed Len to %d, want 2", m.Len())
	}
	if v, _, _ := m.Get([]byte("k"), 5); string(v) != "v1" {
		t.Errorf("duplicate Add overwrote the value: got %q", v)
	}
}

func TestSeekGE(t *testing.T) {
	m := New()
	for i, k := range []string{"b", "d", "f"} {
		if err := m.Add(uint64(i+1), ikey.KindSet, []byte(k), nil); err != nil {
			t.Fatal(err)
		}
	}
	it := m.NewIter()
	for _, tc := range []struct{ seek, want string }{
		{"a", "b"}, {"b", "b"}, {"c", "d"}, {"f", "f"}, {"g", ""},
	} {
		it.SeekGE(ikey.SeekKey([]byte(tc.seek), ikey.MaxSeq))
		got := ""
		if it.Valid() {
			got = string(it.Key().UserKey)
		}
		if got != tc.want {
			t.Errorf("SeekGE(%q) at %q, want %q", tc.seek, got, tc.want)
		}
	}
}

// TestModelRandomized applies a random sequence of sets and deletes over a
// small key space, mirroring them in a Go map, and checks point reads at
// random snapshots and full scans against the map.
func TestModelRandomized(t *testing.T) {
	r := rand.New(rand.NewPCG(testSeed(t), 1))
	const numOps, numKeys = 20000, 300
	m := New()
	md := model{}
	for seq := uint64(1); seq <= numOps; seq++ {
		key := keyName(r.IntN(numKeys))
		kind, val := ikey.KindSet, randValue(r)
		if r.IntN(4) == 0 {
			kind, val = ikey.KindDelete, nil
		}
		if err := m.Add(seq, kind, key, val); err != nil {
			t.Fatalf("Add(%d): %v", seq, err)
		}
		md[string(key)] = append(md[string(key)], version{seq, kind, val})

		if seq%97 == 0 {
			for range 20 {
				checkGet(t, m, md, keyName(r.IntN(numKeys+10)), r.Uint64N(seq+2))
			}
		}
		if seq%5000 == 0 {
			checkScan(t, m, md, seq)
			checkScan(t, m, md, r.Uint64N(seq))
		}
		if t.Failed() {
			t.FailNow()
		}
	}
	if m.Len() != numOps {
		t.Errorf("Len = %d, want %d", m.Len(), numOps)
	}
}

// TestConcurrentReadersMatchModel is the Phase 1 gate test: one writer adds
// a random, pre-generated stream of entries while several readers run point
// reads and full scans at the latest published sequence number, comparing
// every result with a Go map. Run it under -race.
func TestConcurrentReadersMatchModel(t *testing.T) {
	seed := testSeed(t)
	r := rand.New(rand.NewPCG(seed, 2))
	const numOps, numKeys, numReaders = 30000, 500, 8

	// Generate the whole stream up front so the model is immutable and
	// readers can consult it without synchronization.
	type op struct {
		key   []byte
		kind  ikey.Kind
		value []byte
	}
	ops := make([]op, numOps)
	md := model{}
	for i := range ops {
		key := keyName(r.IntN(numKeys))
		kind, val := ikey.KindSet, randValue(r)
		if r.IntN(5) == 0 {
			kind, val = ikey.KindDelete, nil
		}
		ops[i] = op{key, kind, val}
		md[string(key)] = append(md[string(key)], version{uint64(i + 1), kind, val})
	}

	m := New()
	var published atomic.Uint64 // highest seq fully added
	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for i, o := range ops {
			seq := uint64(i + 1)
			if err := m.Add(seq, o.kind, o.key, o.value); err != nil {
				t.Errorf("Add(%d): %v", seq, err)
				return
			}
			published.Store(seq)
		}
	}()

	var reads, scans atomic.Int64
	for rd := range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rr := rand.New(rand.NewPCG(seed, uint64(100+rd)))
			for finished := false; !finished && !t.Failed(); {
				select {
				case <-done:
					finished = true // one last pass over the final state
				default:
				}
				s := published.Load()
				for range 64 {
					checkGet(t, m, md, keyName(rr.IntN(numKeys+10)), s)
					reads.Add(1)
				}
				if rr.IntN(8) == 0 {
					checkScan(t, m, md, s)
					scans.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("%d point reads, %d full scans", reads.Load(), scans.Load())
	if m.Len() != numOps {
		t.Errorf("Len = %d, want %d", m.Len(), numOps)
	}
}

func TestHeightDistribution(t *testing.T) {
	m := New()
	counts := make([]int, maxHeight+1)
	const n = 100000
	for range n {
		counts[m.randomHeight()]++
	}
	// Roughly 3/4 of nodes have height 1 and 3/16 have height 2.
	if f := float64(counts[1]) / n; f < 0.72 || f > 0.78 {
		t.Errorf("height 1 fraction = %.3f, want about 0.75", f)
	}
	if f := float64(counts[2]) / n; f < 0.17 || f > 0.21 {
		t.Errorf("height 2 fraction = %.3f, want about 0.1875", f)
	}
	if counts[0] != 0 || slices.Max(counts[1:]) == 0 {
		t.Errorf("unexpected height counts %v", counts)
	}
}

func BenchmarkAdd(b *testing.B) {
	r := rand.New(rand.NewPCG(1, 1))
	keys := make([][]byte, 1<<16)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%016x", r.Uint64()))
	}
	val := make([]byte, 100)
	m := New()
	b.ResetTimer()
	for i := range b.N {
		if err := m.Add(uint64(i+1), ikey.KindSet, keys[i%len(keys)], val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	m := New()
	const n = 1 << 16
	val := make([]byte, 100)
	for i := range n {
		if err := m.Add(uint64(i+1), ikey.KindSet, keyName(i), val); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := range b.N {
		if _, _, ok := m.Get(keyName(i%n), ikey.MaxSeq); !ok {
			b.Fatal("missing key")
		}
	}
}
