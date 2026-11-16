package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
)

type entry struct {
	k ikey.Key
	v []byte
}

// genEntries returns sorted entries: numKeys user keys with 1-3 versions
// each, some of them deletes.
func genEntries(r *rand.Rand, numKeys int) []entry {
	var out []entry
	seq := uint64(1)
	for i := range numKeys {
		uk := []byte(fmt.Sprintf("user-key-%07d", i*3))
		for range 1 + r.IntN(3) {
			kind := ikey.KindSet
			var v []byte
			if r.IntN(5) == 0 {
				kind = ikey.KindDelete
			} else {
				v = bytes.Repeat([]byte{byte('a' + r.IntN(26))}, r.IntN(200))
			}
			out = append(out, entry{ikey.Key{UserKey: uk, Trailer: ikey.MakeTrailer(seq+r.Uint64N(1000), kind)}, v})
			seq += 1000
		}
	}
	slices.SortFunc(out, func(a, b entry) int { return ikey.Compare(a.k, b.k) })
	return slices.CompactFunc(out, func(a, b entry) bool { return ikey.Compare(a.k, b.k) == 0 })
}

func build(t testing.TB, entries []entry, opts WriterOptions) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, opts)
	for _, e := range entries {
		if err := w.Add(e.k, e.v); err != nil {
			t.Fatal(err)
		}
	}
	props, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if int(w.Size()) != buf.Len() || props.NumEntries != uint64(len(entries)) {
		t.Fatalf("size %d vs %d, entries %d vs %d", w.Size(), buf.Len(), props.NumEntries, len(entries))
	}
	return buf.Bytes()
}

func open(t testing.TB, data []byte, cache *Cache) *Reader {
	t.Helper()
	r, err := Open(bytes.NewReader(data), int64(len(data)), ReaderOptions{Cache: cache, Name: "test.sst"})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	entries := genEntries(r, 3000)
	for _, opts := range []WriterOptions{{}, {BlockSize: 64, RestartInterval: 1}, {BlockSize: 1 << 20, BloomBitsPerKey: -1}} {
		data := build(t, entries, opts)
		rd := open(t, data, NewCache(1<<20))
		p := rd.Properties()
		if !bytes.Equal(p.SmallestKey, entries[0].k.UserKey) || !bytes.Equal(p.LargestKey, entries[len(entries)-1].k.UserKey) {
			t.Errorf("key range %q..%q", p.SmallestKey, p.LargestKey)
		}

		// Full scan.
		it := rd.NewIter()
		i := 0
		for it.First(); it.Valid(); it.Next() {
			if ikey.Compare(it.Key(), entries[i].k) != 0 || !bytes.Equal(it.Value(), entries[i].v) {
				t.Fatalf("opts %+v entry %d = %s, want %s", opts, i, it.Key(), entries[i].k)
			}
			i++
		}
		if it.Err() != nil || i != len(entries) {
			t.Fatalf("scanned %d of %d: %v", i, len(entries), it.Err())
		}

		// Seeks to existing keys and to gaps between them.
		for range 2000 {
			j := r.IntN(len(entries))
			target := entries[j].k
			if r.IntN(2) == 0 {
				target = ikey.SeekKey(append(slices.Clone(target.UserKey), 'x'), ikey.MaxSeq)
			}
			want := lowerBound(entries, target)
			it.SeekGE(target)
			if want == len(entries) {
				if it.Valid() {
					t.Fatalf("SeekGE(%s) = %s, want end", target, it.Key())
				}
				continue
			}
			if !it.Valid() || ikey.Compare(it.Key(), entries[want].k) != 0 {
				t.Fatalf("SeekGE(%s) wrong", target)
			}
		}

		// Point reads against the model at random snapshots.
		for range 2000 {
			uk := []byte(fmt.Sprintf("user-key-%07d", r.IntN(9100)))
			seq := r.Uint64N(entries[len(entries)-1].k.Seq() + 2000)
			v, tr, found, err := rd.Get(uk, seq)
			if err != nil {
				t.Fatal(err)
			}
			wantIdx := -1
			for j, e := range entries {
				if bytes.Equal(e.k.UserKey, uk) && e.k.Seq() <= seq {
					wantIdx = j
					break
				}
			}
			if (wantIdx >= 0) != found {
				t.Fatalf("Get(%q, %d) found=%v, want %v", uk, seq, found, wantIdx >= 0)
			}
			if found && (tr != entries[wantIdx].k.Trailer || !bytes.Equal(v, entries[wantIdx].v)) {
				t.Fatalf("Get(%q, %d) = %d", uk, seq, tr.Seq())
			}
		}
	}
}

func lowerBound(entries []entry, target ikey.Key) int {
	i, _ := slices.BinarySearchFunc(entries, target, func(e entry, t ikey.Key) int { return ikey.Compare(e.k, t) })
	return i
}

func TestEmptyTable(t *testing.T) {
	rd := open(t, build(t, nil, WriterOptions{}), nil)
	it := rd.NewIter()
	it.First()
	if it.Valid() || it.Err() != nil {
		t.Fatalf("empty table iterates: %v", it.Err())
	}
	if _, _, found, err := rd.Get([]byte("x"), 10); found || err != nil {
		t.Fatal("Get on empty table found something")
	}
}

func TestAddOutOfOrder(t *testing.T) {
	w := NewWriter(&bytes.Buffer{}, WriterOptions{})
	k := ikey.Key{UserKey: []byte("b"), Trailer: ikey.MakeTrailer(5, ikey.KindSet)}
	if err := w.Add(k, nil); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []ikey.Key{k, {UserKey: []byte("a"), Trailer: k.Trailer}, {UserKey: []byte("b"), Trailer: ikey.MakeTrailer(6, ikey.KindSet)}} {
		if err := w.Add(bad, nil); !errors.Is(err, ErrOutOfOrder) {
			t.Errorf("Add(%s) after %s: %v", bad, k, err)
		}
	}
}

// TestBloomFalsePositiveRate is the Phase 3 gate check that the measured
// false-positive rate is close to the configured one, for several sizes.
// It also checks there are no false negatives.
func TestBloomFalsePositiveRate(t *testing.T) {
	const n, probes = 100_000, 1_000_000
	for _, bits := range []int{6, 10, 14, 20} {
		f := &filterBuilder{bitsPerKey: bits}
		for i := range n {
			f.add([]byte(fmt.Sprintf("member-%d", i)))
		}
		filter := f.finish()
		for i := range n {
			if !filterMayContain(filter, []byte(fmt.Sprintf("member-%d", i))) {
				t.Fatalf("bits=%d: false negative for member-%d", bits, i)
			}
		}
		fp := 0
		for i := range probes {
			if filterMayContain(filter, []byte(fmt.Sprintf("absent-%d", i))) {
				fp++
			}
		}
		got, want := float64(fp)/probes, ExpectedFalsePositiveRate(bits)
		// With a million probes the sampling error is about 1% of the rate
		// at 10 bits; 25% leaves room for double hashing, which is slightly
		// worse than k independent hashes.
		if math.Abs(got-want)/want > 0.25 {
			t.Errorf("bits/key %d: measured false-positive rate %.4f%%, expected %.4f%%", bits, 100*got, 100*want)
		}
		t.Logf("bits/key %2d (k=%d): measured %.4f%%, expected %.4f%%", bits, bloomProbes(bits), 100*got, 100*want)
	}
}

// TestTableFilterRate measures the rate through a real table's Get.
func TestTableFilterRate(t *testing.T) {
	var entries []entry
	for i := range 50_000 {
		entries = append(entries, entry{ikey.Key{UserKey: []byte(fmt.Sprintf("k%08d", 2*i)), Trailer: ikey.MakeTrailer(uint64(i+1), ikey.KindSet)}, []byte("v")})
	}
	rd := open(t, build(t, entries, WriterOptions{}), NewCache(1<<20))
	for i := range 200_000 {
		k := []byte(fmt.Sprintf("k%08d", 2*(i%50_000)+1)) // odd: absent, but inside the key range
		if _, _, found, err := rd.Get(k, ikey.MaxSeq); found || err != nil {
			t.Fatalf("absent key found: %v", err)
		}
	}
	checks, excluded := rd.FilterStats()
	got, want := 1-float64(excluded)/float64(checks), ExpectedFalsePositiveRate(10)
	if math.Abs(got-want)/want > 0.3 {
		t.Errorf("table false-positive rate %.4f%%, expected %.4f%%", 100*got, 100*want)
	}
	t.Logf("table: %d lookups of absent keys, %.4f%% passed the filter (expected %.4f%%)", checks, 100*got, 100*want)
}

func TestCorruptionIsDetected(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	data := build(t, genEntries(r, 500), WriterOptions{BlockSize: 512})

	t.Run("data block", func(t *testing.T) {
		bad := slices.Clone(data)
		bad[100] ^= 0x01
		rd := open(t, bad, nil) // metadata blocks are intact
		it := rd.NewIter()
		for it.First(); it.Valid(); it.Next() {
		}
		if !errors.Is(it.Err(), ErrCorrupt) {
			t.Fatalf("scan over a damaged block: %v, want ErrCorrupt", it.Err())
		}
	})
	t.Run("footer, index, filter or properties", func(t *testing.T) {
		for _, off := range []int{len(data) - 1, len(data) - footerLen + 2, len(data) - footerLen - 20} {
			bad := slices.Clone(data)
			bad[off] ^= 0x10
			if _, err := Open(bytes.NewReader(bad), int64(len(bad)), ReaderOptions{}); !errors.Is(err, ErrCorrupt) {
				t.Errorf("damage at %d: Open = %v, want ErrCorrupt", off, err)
			}
		}
	})
	t.Run("truncated", func(t *testing.T) {
		for _, n := range []int{0, 10, len(data) / 2, len(data) - 1} {
			if _, err := Open(bytes.NewReader(data[:n]), int64(n), ReaderOptions{}); err == nil {
				t.Errorf("table truncated to %d bytes opened", n)
			}
		}
	})
}

func TestCacheEvictsLeastRecentlyUsed(t *testing.T) {
	c := NewCache(100)
	c.put(cacheKey{1, 0}, make([]byte, 40))
	c.put(cacheKey{1, 1}, make([]byte, 40))
	c.get(cacheKey{1, 0}) // now most recent
	c.put(cacheKey{1, 2}, make([]byte, 40))
	if _, ok := c.get(cacheKey{1, 1}); ok {
		t.Error("least recently used block survived")
	}
	if _, ok := c.get(cacheKey{1, 0}); !ok {
		t.Error("recently used block evicted")
	}
	c.put(cacheKey{2, 0}, make([]byte, 101)) // larger than the cache: ignored
	if s := c.Stats(); s.Used != 80 || s.Hits != 2 || s.Misses != 1 {
		t.Errorf("stats %+v", s)
	}
	c.evictTable(1)
	if s := c.Stats(); s.Used != 0 {
		t.Errorf("after evicting table 1: %+v", s)
	}
}

func TestCacheServesRepeatedReads(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	entries := genEntries(r, 1000)
	c := NewCache(1 << 20)
	rd := open(t, build(t, entries, WriterOptions{}), c)
	for range 3 {
		it := rd.NewIter()
		for it.First(); it.Valid(); it.Next() {
		}
	}
	s := c.Stats()
	if s.Hits < 2*s.Misses {
		t.Errorf("cache stats %+v: repeated scans should mostly hit", s)
	}
}

func FuzzOpenAndScan(f *testing.F) {
	r := rand.New(rand.NewPCG(7, 8))
	f.Add(build(f, genEntries(r, 20), WriterOptions{BlockSize: 128}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		rd, err := Open(bytes.NewReader(data), int64(len(data)), ReaderOptions{})
		if err != nil {
			return
		}
		it := rd.NewIter()
		n := 0
		for it.First(); it.Valid() && n < 100000; it.Next() {
			n++
		}
		it.SeekGE(ikey.SeekKey([]byte("user-key-0000030"), 5000))
		_, _, _, _ = rd.Get([]byte("user-key-0000030"), ikey.MaxSeq)
	})
}

func BenchmarkGetCached(b *testing.B) {
	r := rand.New(rand.NewPCG(9, 10))
	entries := genEntries(r, 20000)
	rd := open(b, build(b, entries, WriterOptions{}), NewCache(64<<20))
	b.ResetTimer()
	for i := range b.N {
		e := entries[i%len(entries)]
		if _, _, _, err := rd.Get(e.k.UserKey, ikey.MaxSeq); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetAbsentFiltered(b *testing.B) {
	r := rand.New(rand.NewPCG(9, 10))
	rd := open(b, build(b, genEntries(r, 20000), WriterOptions{}), NewCache(64<<20))
	key := []byte("user-key-0000001") // between keys, so only the filter can rule it out
	b.ResetTimer()
	for range b.N {
		if _, _, _, err := rd.Get(key, ikey.MaxSeq); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScan(b *testing.B) {
	r := rand.New(rand.NewPCG(9, 10))
	entries := genEntries(r, 20000)
	rd := open(b, build(b, entries, WriterOptions{}), NewCache(64<<20))
	b.ResetTimer()
	for range b.N {
		it := rd.NewIter()
		for it.First(); it.Valid(); it.Next() {
		}
	}
	b.ReportMetric(float64(len(entries)), "entries/op")
}

func BenchmarkBuild(b *testing.B) {
	r := rand.New(rand.NewPCG(9, 10))
	entries := genEntries(r, 20000)
	b.ResetTimer()
	for range b.N {
		build(b, entries, WriterOptions{})
	}
}
