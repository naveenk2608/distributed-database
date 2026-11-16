// Package memtable implements the engine's in-memory sorted table: a skiplist
// of internal keys (user key, sequence number, kind).
//
// Concurrency: writers are serialized by an internal mutex; readers never
// lock. Nodes are immutable once linked, and every link is an atomic
// pointer, so a reader that loads a link observes a fully initialized node.
// Entries are never updated or removed in place: a newer version of a key
// is a new entry with a higher sequence number, and a delete is a tombstone
// entry. See docs/adr/0017-memtable-concurrency.md.
package memtable

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
)

const (
	// maxHeight bounds tower height. With branching factor 4, height 12
	// keeps searches logarithmic up to about 4^12 = 16M entries, far more
	// than a memtable holds before it is flushed.
	maxHeight = 12
	// branching is the inverse probability that a node gets one more level.
	branching = 4
)

// Errors returned by Add.
var (
	ErrDuplicate   = errors.New("memtable: internal key already present")
	ErrInvalidKind = errors.New("memtable: invalid kind")
	ErrSeqTooLarge = errors.New("memtable: sequence number exceeds ikey.MaxSeq")
)

type node struct {
	key   ikey.Key // UserKey is owned by the node
	value []byte   // owned by the node
	next  []atomic.Pointer[node]
}

// nodeOverhead approximates the fixed memory cost of one node, excluding
// its tower, key and value bytes.
const nodeOverhead = int64(unsafe.Sizeof(node{}))

// Memtable is a concurrent skiplist of internal keys. The zero value is not
// usable; call New.
type Memtable struct {
	mu  sync.Mutex // serializes Add; readers never take it
	rng *rand.Rand // guarded by mu

	head   *node
	height atomic.Int32 // current tower height in use, 1..maxHeight

	count atomic.Int64
	size  atomic.Int64
}

// New returns an empty memtable.
func New() *Memtable {
	m := &Memtable{
		// The tower shape only affects speed, never results, so a fixed
		// seed keeps the structure reproducible when debugging.
		rng:  rand.New(rand.NewPCG(0x646b76, 0x6d656d)),
		head: &node{next: make([]atomic.Pointer[node], maxHeight)},
	}
	m.height.Store(1)
	return m
}

// Add inserts the entry (key, seq, kind) -> value. Key and value are copied.
// Every (key, seq) pair must be unique, whatever the kind; adding one twice
// returns ErrDuplicate and leaves the memtable unchanged.
func (m *Memtable) Add(seq uint64, kind ikey.Kind, key, value []byte) error {
	if seq > ikey.MaxSeq {
		return fmt.Errorf("%w: %d", ErrSeqTooLarge, seq)
	}
	if !kind.Valid() {
		return fmt.Errorf("%w: %d", ErrInvalidKind, kind)
	}
	// A tombstone carries no value.
	if kind == ikey.KindDelete {
		value = nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	target := ikey.Key{UserKey: key, Trailer: ikey.MakeTrailer(seq, kind)}
	var prev, next [maxHeight]*node
	x := m.head
	h := int(m.height.Load())
	for level := h - 1; level >= 0; level-- {
		for {
			n := x.next[level].Load()
			if n == nil || ikey.Compare(n.key, target) >= 0 {
				next[level] = n
				break
			}
			x = n
		}
		prev[level] = x
	}
	// Entries with the same user key and sequence number but a different
	// kind sort right next to the insertion point, on either side.
	if sameKeySeq(next[0], key, seq) || (prev[0] != m.head && sameKeySeq(prev[0], key, seq)) {
		return fmt.Errorf("%w: %q#%d", ErrDuplicate, key, seq)
	}

	nh := m.randomHeight()
	if nh > h {
		for level := h; level < nh; level++ {
			prev[level] = m.head
			next[level] = nil
		}
		// Publishing the taller height before the node is linked is safe:
		// a reader that sees it finds nil links at the new levels and
		// simply descends.
		m.height.Store(int32(nh))
	}

	n := &node{
		key:   ikey.Key{UserKey: slices.Clone(key), Trailer: target.Trailer},
		value: slices.Clone(value),
		next:  make([]atomic.Pointer[node], nh),
	}
	// Link bottom-up. The node's own links are set before the node becomes
	// reachable at that level, so readers never follow a half-built tower.
	for level := range nh {
		n.next[level].Store(next[level])
		prev[level].next[level].Store(n)
	}

	m.count.Add(1)
	m.size.Add(nodeOverhead + int64(nh)*int64(unsafe.Sizeof(atomic.Pointer[node]{})) +
		int64(len(key)) + int64(len(value)))
	return nil
}

func sameKeySeq(n *node, userKey []byte, seq uint64) bool {
	return n != nil && n.key.Seq() == seq && bytes.Equal(n.key.UserKey, userKey)
}

// randomHeight returns a height in [1, maxHeight] with P(h > k) = branching^-k.
// Caller holds m.mu.
func (m *Memtable) randomHeight() int {
	h := 1
	for h < maxHeight && m.rng.IntN(branching) == 0 {
		h++
	}
	return h
}

// findGE returns the first node whose key is >= target, or nil.
// It is safe to call concurrently with Add.
func (m *Memtable) findGE(target ikey.Key) *node {
	x := m.head
	var n *node
	for level := int(m.height.Load()) - 1; level >= 0; level-- {
		for {
			// Use the link value that failed the comparison rather than
			// reloading it: a node inserted after x in the meantime could
			// sort before target.
			n = x.next[level].Load()
			if n == nil || ikey.Compare(n.key, target) >= 0 {
				break
			}
			x = n
		}
	}
	return n
}

// Get returns the newest entry for userKey with sequence number <= seq.
// ok is false if there is none. The caller must check the trailer's kind:
// a KindDelete entry means the key is deleted at that version. The returned
// value aliases memtable memory and must not be modified.
func (m *Memtable) Get(userKey []byte, seq uint64) (value []byte, trailer ikey.Trailer, ok bool) {
	n := m.findGE(ikey.SeekKey(userKey, seq))
	if n == nil || !bytes.Equal(n.key.UserKey, userKey) {
		return nil, 0, false
	}
	return n.value, n.key.Trailer, true
}

// Len returns the number of entries.
func (m *Memtable) Len() int { return int(m.count.Load()) }

// ApproximateSize returns an estimate of the memory used by entries, in bytes.
func (m *Memtable) ApproximateSize() int64 { return m.size.Load() }

// Iter walks the memtable in internal key order. It is safe to use while
// writers add entries; it may or may not observe entries added after it was
// positioned. Callers that need a consistent view filter by sequence number.
// An Iter is not safe for concurrent use by multiple goroutines.
type Iter struct {
	m *Memtable
	n *node
}

// NewIter returns an unpositioned iterator; call First or SeekGE.
func (m *Memtable) NewIter() *Iter { return &Iter{m: m} }

// First positions the iterator at the smallest entry.
func (it *Iter) First() { it.n = it.m.head.next[0].Load() }

// SeekGE positions the iterator at the first entry >= target.
func (it *Iter) SeekGE(target ikey.Key) { it.n = it.m.findGE(target) }

// Next advances to the next entry. The iterator must be valid.
func (it *Iter) Next() { it.n = it.n.next[0].Load() }

// Valid reports whether the iterator is positioned at an entry.
func (it *Iter) Valid() bool { return it.n != nil }

// Key returns the current internal key. Its bytes must not be modified.
func (it *Iter) Key() ikey.Key { return it.n.key }

// Value returns the current value. Its bytes must not be modified.
func (it *Iter) Value() []byte { return it.n.value }

// Err always returns nil: a memtable cannot fail to read. It lets Iter
// stand in wherever an iterator over files is expected.
func (it *Iter) Err() error { return nil }
