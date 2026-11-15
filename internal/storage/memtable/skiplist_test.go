package memtable

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestSkiplistBasic tests basic skiplist operations
func TestSkiplistBasic(t *testing.T) {
	sl := NewSkiplist()

	// Test Set and Get
	key := []byte("test-key")
	value := []byte("test-value")

	if !sl.Set(key, value, 1, 0) {
		t.Error("Set should return true for new key")
	}

	v, seq, kind, found := sl.Get(key)
	if !found {
		t.Error("Get should find the key")
	}
	if string(v) != "test-value" {
		t.Errorf("Expected 'test-value', got '%s'", v)
	}
	if seq != 1 {
		t.Errorf("Expected seq 1, got %d", seq)
	}
	if kind != 0 {
		t.Errorf("Expected kind 0, got %d", kind)
	}

	// Test update
	if sl.Set(key, []byte("new-value"), 2, 0) {
		t.Error("Set should return false for existing key")
	}

	v, seq, kind, found = sl.Get(key)
	if !found {
		t.Error("Get should find the key after update")
	}
	if string(v) != "new-value" {
		t.Errorf("Expected 'new-value', got '%s'", v)
	}
	if seq != 2 {
		t.Errorf("Expected seq 2, got %d", seq)
	}

	// Test delete
	if !sl.Delete(key, 3) {
		t.Error("Delete should return true for existing key")
	}

	_, _, _, found = sl.Get(key)
	if found {
		t.Error("Get should not find deleted key")
	}

	// Test HasKey
	if sl.HasKey(key) {
		t.Error("HasKey should return false for deleted key")
	}

	// Test Len
	if sl.Len() != 0 {
		t.Errorf("Expected Len 0, got %d", sl.Len())
	}
}

// TestSkiplistMultipleKeys tests multiple keys
func TestSkiplistMultipleKeys(t *testing.T) {
	sl := NewSkiplist()

	keys := []string{"a", "b", "c", "d", "e"}
	for i, k := range keys {
		sl.Set([]byte(k), []byte(k+"-value"), uint64(i+1), 0)
	}

	if sl.Len() != 5 {
		t.Errorf("Expected Len 5, got %d", sl.Len())
	}

	for i, k := range keys {
		v, seq, _, found := sl.Get([]byte(k))
		if !found {
			t.Errorf("Key %s not found", k)
		}
		if string(v) != k+"-value" {
			t.Errorf("Key %s: expected '%s-value', got '%s'", k, k, v)
		}
		if seq != uint64(i+1) {
			t.Errorf("Key %s: expected seq %d, got %d", k, i+1, seq)
		}
	}
}

// TestSkiplistIterator tests the skiplist iterator
func TestSkiplistIterator(t *testing.T) {
	sl := NewSkiplist()

	// Insert keys in random order
	keys := []string{"c", "a", "e", "b", "d"}
	for i, k := range keys {
		sl.Set([]byte(k), []byte(k), uint64(i+1), 0)
	}

	// Iterate and check order (should be sorted)
	it := sl.NewIterator()
	var prev string
	count := 0
	for it.Valid() {
		k := string(it.Key())
		if prev != "" && k < prev {
			t.Errorf("Keys not in order: %s before %s", k, prev)
		}
		prev = k
		count++
		it.Next()
	}

	if count != 5 {
		t.Errorf("Expected 5 keys, got %d", count)
	}
}

// TestSkiplistSeek tests the Seek operation
func TestSkiplistSeek(t *testing.T) {
	sl := NewSkiplist()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		sl.Set([]byte(k), []byte(k), 1, 0)
	}

	it := sl.NewIterator()
	it.Seek([]byte("c"))

	if !it.Valid() {
		t.Error("Iterator should be valid after Seek")
	}
	if string(it.Key()) != "c" {
		t.Errorf("Expected key 'c', got '%s'", it.Key())
	}

	it.Next()
	if !it.Valid() || string(it.Key()) != "d" {
		t.Error("Next should be 'd'")
	}
}

// TestSkiplistConcurrent tests concurrent access
func TestSkiplistConcurrent(t *testing.T) {
	sl := NewSkiplist()
	var wg sync.WaitGroup

	// Writer goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(id), uint64(id<<32)))
			for j := 0; j < 100; j++ {
				key := []byte(randKey(r, 10))
				value := []byte(randKey(r, 20))
				sl.Set(key, value, uint64(r.Uint64()), 0)
			}
		}(i)
	}

	// Reader goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				it := sl.NewIterator()
				for it.Valid() {
					_ = it.Key()
					_ = it.Value()
					it.Next()
				}
			}
		}()
	}

	wg.Wait()
}

// TestMemtableBasic tests basic memtable operations
func TestMemtableBasic(t *testing.T) {
	mt := NewMemtable(1024 * 1024) // 1MB

	// Test Set
	mt.Set([]byte("key1"), []byte("value1"), 1, KeyKindValue)
	if mt.Len() != 1 {
		t.Errorf("Expected Len 1, got %d", mt.Len())
	}

	// Test Get
	v, seq, kind, found := mt.Get([]byte("key1"), ^uint64(0), KeyKindValue)
	if !found {
		t.Error("Get should find the key")
	}
	if string(v) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", v)
	}
	if seq != 1 {
		t.Errorf("Expected seq 1, got %d", seq)
	}
	if kind != KeyKindValue {
		t.Errorf("Expected KeyKindValue, got %d", kind)
	}

	// Test update
	mt.Set([]byte("key1"), []byte("value2"), 2, KeyKindValue)
	v, seq, _, found = mt.Get([]byte("key1"), ^uint64(0), KeyKindValue)
	if !found || string(v) != "value2" || seq != 2 {
		t.Error("Update failed")
	}

	// Test Delete
	mt.Delete([]byte("key1"), 3)
	_, _, _, found = mt.Get([]byte("key1"), ^uint64(0), KeyKindValue)
	if found {
		t.Error("Get should not find deleted key")
	}
}

// TestMemtableIterator tests the memtable iterator
func TestMemtableIterator(t *testing.T) {
	mt := NewMemtable(1024 * 1024)

	keys := []string{"c", "a", "e", "b", "d"}
	for i, k := range keys {
		mt.Set([]byte(k), []byte(k), uint64(i+1), KeyKindValue)
	}

	it := mt.NewIterator()
	var prev string
	count := 0
	for it.Valid() {
		k := string(it.Key())
		if prev != "" && k < prev {
			t.Errorf("Keys not in order: %s before %s", k, prev)
		}
		prev = k
		count++
		it.Next()
	}

	if count != 5 {
		t.Errorf("Expected 5 keys, got %d", count)
	}
}

// TestMemtableSeek tests the Seek operation
func TestMemtableSeek(t *testing.T) {
	mt := NewMemtable(1024 * 1024)

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mt.Set([]byte(k), []byte(k), 1, KeyKindValue)
	}

	it := mt.NewIterator()
	it.Seek([]byte("c"))

	if !it.Valid() {
		t.Error("Iterator should be valid after Seek")
	}
	if string(it.Key()) != "c" {
		t.Errorf("Expected key 'c', got '%s'", it.Key())
	}
}

// TestMemtableBatch tests batch operations
func TestMemtableBatch(t *testing.T) {
	mt := NewMemtable(1024 * 1024)
	batch := NewBatch()

	batch.Set([]byte("key1"), []byte("value1"), 1)
	batch.Set([]byte("key2"), []byte("value2"), 2)
	batch.Delete([]byte("key3"), 3)

	if batch.Len() != 3 {
		t.Errorf("Expected 3 entries, got %d", batch.Len())
	}

	batch.Apply(mt)

	v, _, _, found := mt.Get([]byte("key1"), ^uint64(0), KeyKindValue)
	if !found || string(v) != "value1" {
		t.Error("Batch Set failed for key1")
	}

	v, _, _, found = mt.Get([]byte("key2"), ^uint64(0), KeyKindValue)
	if !found || string(v) != "value2" {
		t.Error("Batch Set failed for key2")
	}

	_, _, _, found = mt.Get([]byte("key3"), ^uint64(0), KeyKindValue)
	if found {
		t.Error("Batch Delete failed for key3")
	}
}

// TestEncodeDecodeInternalKey tests internal key encoding/decoding
func TestEncodeDecodeInternalKey(t *testing.T) {
	testCases := []struct {
		key   []byte
		seq   uint64
		kind  byte
	}{
		{[]byte("test"), 1, 0},
		{[]byte(""), 100, 1},
		{[]byte("long-key-with-special-chars-!@#$%"), ^uint64(0), 0},
		{[]byte("a"), 0, 1},
	}

	for _, tc := range testCases {
		encoded := EncodeInternalKey(tc.key, tc.seq, tc.kind)
		decodedKey, decodedSeq, decodedKind, ok := DecodeInternalKey(encoded)
		if !ok {
			t.Errorf("Decode failed for %v", tc)
			continue
		}
		if string(decodedKey) != string(tc.key) {
			t.Errorf("Key mismatch: expected %q, got %q", tc.key, decodedKey)
		}
		if decodedSeq != tc.seq {
			t.Errorf("Seq mismatch: expected %d, got %d", tc.seq, decodedSeq)
		}
		if decodedKind != tc.kind {
			t.Errorf("Kind mismatch: expected %d, got %d", tc.kind, decodedKind)
		}
	}
}

// TestCompare tests the Compare function
func TestCompare(t *testing.T) {
	// Different user keys
	a := EncodeInternalKey([]byte("a"), 1, 0)
	b := EncodeInternalKey([]byte("b"), 1, 0)
	if Compare(a, b) >= 0 {
		t.Error("a should be < b")
	}

	// Same user key, different seq (higher seq should come first)
	a = EncodeInternalKey([]byte("key"), 2, 0)
	b = EncodeInternalKey([]byte("key"), 1, 0)
	if Compare(a, b) >= 0 {
		t.Error("seq 2 should come before seq 1")
	}

	// Same user key, same seq, different kind (tombstone should come first)
	a = EncodeInternalKey([]byte("key"), 1, 1) // tombstone
	b = EncodeInternalKey([]byte("key"), 1, 0) // value
	if Compare(a, b) >= 0 {
		t.Error("tombstone should come before value")
	}
}

// TestMemtableVersionedGet tests Get with version constraint
func TestMemtableVersionedGet(t *testing.T) {
	mt := NewMemtable(1024 * 1024)

	// Insert multiple versions of the same key
	mt.Set([]byte("key"), []byte("v1"), 1, KeyKindValue)
	mt.Set([]byte("key"), []byte("v2"), 2, KeyKindValue)
	mt.Set([]byte("key"), []byte("v3"), 3, KeyKindValue)

	// Get with seq=2 should return v2 (highest <= 2)
	v, seq, _, found := mt.Get([]byte("key"), 2, KeyKindValue)
	if !found || string(v) != "v2" || seq != 2 {
		t.Errorf("Expected v2@2, got %s@%d, found=%v", v, seq, found)
	}

	// Get with seq=3 should return v3
	v, seq, _, found = mt.Get([]byte("key"), 3, KeyKindValue)
	if !found || string(v) != "v3" || seq != 3 {
		t.Errorf("Expected v3@3, got %s@%d, found=%v", v, seq, found)
	}

	// Get with seq=1 should return v1
	v, seq, _, found = mt.Get([]byte("key"), 1, KeyKindValue)
	if !found || string(v) != "v1" || seq != 1 {
		t.Errorf("Expected v1@1, got %s@%d, found=%v", v, seq, found)
	}

	// Get with seq=0 should not find anything
	_, _, _, found = mt.Get([]byte("key"), 0, KeyKindValue)
	if found {
		t.Error("Expected not found for seq=0")
	}
}

// randKey generates a random key for testing
func randKey(r *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[r.IntN(len(letters))]
	}
	return string(b)
}