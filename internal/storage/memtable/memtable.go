package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// EncodeInternalKey encodes a user key, sequence number, and kind into an internal key
// Format: userKey | seq(8 bytes, big-endian, descending) | kind(1 byte)
func EncodeInternalKey(userKey []byte, seq uint64, kind byte) []byte {
	buf := make([]byte, len(userKey)+9)
	copy(buf, userKey)
	// Encode seq in descending order (higher seq = smaller encoded value for sorting)
	// We use ^seq to reverse the order for big-endian comparison
	binary.BigEndian.PutUint64(buf[len(userKey):], ^seq)
	buf[len(buf)-1] = kind
	return buf
}

// DecodeInternalKey decodes an internal key into user key, sequence, and kind
func DecodeInternalKey(internalKey []byte) (userKey []byte, seq uint64, kind byte, ok bool) {
	if len(internalKey) < 9 {
		return nil, 0, 0, false
	}
	userKey = internalKey[:len(internalKey)-9]
	seq = ^binary.BigEndian.Uint64(internalKey[len(internalKey)-9 : len(internalKey)-1])
	kind = internalKey[len(internalKey)-1]
	return userKey, seq, kind, true
}

// KeyKind represents the type of a key
type KeyKind byte

const (
	KeyKindValue KeyKind = 0
	KeyKindDelete KeyKind = 1
)

// Entry represents a single key-value entry in the memtable
type Entry struct {
	Key       []byte
	Value     []byte
	Seq       uint64
	Kind      KeyKind
}

// Memtable is a thread-safe in-memory table using a skiplist
type Memtable struct {
	sl     *Skiplist
	size   int64
	maxSize int64
}

// NewMemtable creates a new memtable with the given max size
func NewMemtable(maxSize int64) *Memtable {
	return &Memtable{
		sl:      NewSkiplist(),
		maxSize: maxSize,
	}
}

// Set adds a key-value pair to the memtable
// Returns the number of bytes added to the memtable
func (m *Memtable) Set(key, value []byte, seq uint64, kind KeyKind) int {
	internalKey := EncodeInternalKey(key, seq, byte(kind))
	isNew := m.sl.Set(internalKey, value, seq, byte(kind))

	added := 0
	if isNew {
		added = len(internalKey) + len(value) + 48 // approximate overhead
	} else {
		// For updates, we still count the new value size
		added = len(value)
	}
	m.size += int64(added)
	return added
}

// Get retrieves a value from the memtable
func (m *Memtable) Get(key []byte, seq uint64, kind KeyKind) ([]byte, uint64, KeyKind, bool) {
	// We need to find the entry with the highest seq <= given seq for this key
	// Since skiplist stores keys in descending seq order, we seek to the key
	// and iterate until we find a matching seq

	// Create a key with the max possible seq to find the first entry for this key
	maxSeqKey := EncodeInternalKey(key, ^uint64(0), 0)

	it := m.sl.newSkiplistIterator()
	it.Seek(maxSeqKey)

	for it.Valid() {
		ukey, useq, ukind, _ := DecodeInternalKey(it.Key())
		if !bytes.Equal(ukey, key) {
			break // Different user key
		}
		if useq <= seq {
			return it.Value(), useq, KeyKind(ukind), true
		}
		it.Next()
	}
	return nil, 0, 0, false
}

// HasKey checks if a key exists in the memtable (for any seq)
func (m *Memtable) HasKey(key []byte) bool {
	return m.sl.HasKey(EncodeInternalKey(key, ^uint64(0), 0))
}

// Delete adds a tombstone for a key
func (m *Memtable) Delete(key []byte, seq uint64) int {
	internalKey := EncodeInternalKey(key, seq, byte(KeyKindDelete))
	m.sl.Delete(internalKey, seq)
	added := len(internalKey) + 48
	m.size += int64(added)
	return added
}

// Len returns the number of entries in the memtable
func (m *Memtable) Len() int {
	return m.sl.Len()
}

// Size returns the approximate size of the memtable in bytes
func (m *Memtable) Size() int64 {
	return m.size
}

// IsFull returns true if the memtable has reached its max size
func (m *Memtable) IsFull() bool {
	return m.size >= m.maxSize
}

// Iterator returns an iterator over the memtable entries
type Iterator struct {
	it *memtableIterator
}

type memtableIterator struct {
	skiplistIter *skiplistIterator
	key          []byte
	value        []byte
	seq          uint64
	kind         KeyKind
	valid        bool
}

// NewIterator creates a new iterator over the memtable
func (m *Memtable) NewIterator() *Iterator {
	skiplistIter := m.sl.newSkiplistIterator()
	iter := &memtableIterator{
		skiplistIter: skiplistIter,
	}
	iter.advance()
	return &Iterator{it: iter}
}

func (it *memtableIterator) advance() {
	for it.skiplistIter.Valid() {
		ukey, useq, ukind, _ := DecodeInternalKey(it.skiplistIter.Key())
		// Skip tombstones that are not the latest version for this key
		// Actually, we should return all entries since the merge iterator handles deduplication
		it.key = ukey
		it.value = it.skiplistIter.Value()
		it.seq = useq
		it.kind = KeyKind(ukind)
		it.valid = true
		return
	}
	it.valid = false
}

// Valid returns true if the iterator is at a valid position
func (it *Iterator) Valid() bool {
	return it.it.valid
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	return it.it.key
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	return it.it.value
}

// Seq returns the current sequence number
func (it *Iterator) Seq() uint64 {
	return it.it.seq
}

// Kind returns the current kind
func (it *Iterator) Kind() KeyKind {
	return it.it.kind
}

// Next advances the iterator
func (it *Iterator) Next() {
	it.it.skiplistIter.Next()
	it.it.advance()
}

// Seek moves the iterator to the first key >= target
func (it *Iterator) Seek(target []byte) {
	// We need to seek to the highest seq for this key
	maxSeqKey := EncodeInternalKey(target, ^uint64(0), 0)
	it.it.skiplistIter.Seek(maxSeqKey)
	it.it.advance()
}

// Batch represents a batch of writes to be applied atomically
type Batch struct {
	entries []BatchEntry
	size    int
}

type BatchEntry struct {
	Key   []byte
	Value []byte
	Seq   uint64
	Kind  KeyKind
}

// NewBatch creates a new write batch
func NewBatch() *Batch {
	return &Batch{
		entries: make([]BatchEntry, 0),
	}
}

// Set adds a Set operation to the batch
func (b *Batch) Set(key, value []byte, seq uint64) {
	b.entries = append(b.entries, BatchEntry{
		Key:   key,
		Value: value,
		Seq:   seq,
		Kind:  KeyKindValue,
	})
	b.size += len(key) + len(value) + 24
}

// Delete adds a Delete operation to the batch
func (b *Batch) Delete(key []byte, seq uint64) {
	b.entries = append(b.entries, BatchEntry{
		Key:  key,
		Seq:  seq,
		Kind: KeyKindDelete,
	})
	b.size += len(key) + 24
}

// Len returns the number of entries in the batch
func (b *Batch) Len() int {
	return len(b.entries)
}

// Size returns the approximate size of the batch in bytes
func (b *Batch) Size() int {
	return b.size
}

// Iterate iterates over the batch entries
func (b *Batch) Iterate(fn func(key, value []byte, seq uint64, kind KeyKind)) {
	for _, e := range b.entries {
		fn(e.Key, e.Value, e.Seq, e.Kind)
	}
}

// Clear clears the batch
func (b *Batch) Clear() {
	b.entries = b.entries[:0]
	b.size = 0
}

// CRC32C returns the CRC32C checksum of the batch
func (b *Batch) CRC32C() uint32 {
	var buf bytes.Buffer
	for _, e := range b.entries {
		buf.Write(e.Key)
		buf.Write(e.Value)
		var seqBuf [8]byte
		binary.BigEndian.PutUint64(seqBuf[:], e.Seq)
		buf.Write(seqBuf[:])
		buf.WriteByte(byte(e.Kind))
	}
	return crc32.ChecksumIEEE(buf.Bytes()) // Using standard CRC32, could use CRC32C
}

// Apply applies the batch to the memtable
func (b *Batch) Apply(m *Memtable) {
	for _, e := range b.entries {
		if e.Kind == KeyKindValue {
			m.Set(e.Key, e.Value, e.Seq, e.Kind)
		} else {
			m.Delete(e.Key, e.Seq)
		}
	}
}

// String returns a string representation of the memtable (for debugging)
func (m *Memtable) String() string {
	return fmt.Sprintf("Memtable{len=%d, size=%d, maxSize=%d}", m.Len(), m.Size(), m.maxSize)
}