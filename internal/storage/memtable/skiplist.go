package memtable

import (
	"math/rand/v2"
	"sync"
	"unsafe"
)

// maxHeight is the maximum height of the skiplist
const maxHeight = 32

// probability is the probability of a node having a level i+1 given it has level i
const probability = 0.25

// Node represents a skiplist node
type Node struct {
	key       []byte
	value     []byte
	seq       uint64
	kind      byte // 0 = value, 1 = tombstone
	tower     []*Node
	height    int
	deleted   bool
}

// Skiplist is a thread-safe skiplist implementation
type Skiplist struct {
	head     *Node
	height   int
	length   int
	mu       sync.RWMutex
	rand     *rand.Rand
	pool     sync.Pool
}

// NewSkiplist creates a new skiplist
func NewSkiplist() *Skiplist {
	sl := &Skiplist{
		head: &Node{
			tower: make([]*Node, maxHeight),
		},
		height: 1,
		rand:   rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
	sl.pool.New = func() interface{} {
		return &Node{
			tower: make([]*Node, maxHeight),
		}
	}
	return sl
}

// getNode gets a node from the pool
func (sl *Skiplist) getNode() *Node {
	n := sl.pool.Get().(*Node)
	// Reset the node
	n.key = nil
	n.value = nil
	n.seq = 0
	n.kind = 0
	n.height = 0
	n.deleted = false
	for i := range n.tower {
		n.tower[i] = nil
	}
	return n
}

// putNode returns a node to the pool
func (sl *Skiplist) putNode(n *Node) {
	sl.pool.Put(n)
}

// randomHeight generates a random height for a new node
func (sl *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && sl.rand.Float64() < probability {
		h++
	}
	return h
}

// compareKeys compares two internal keys (userKey | seq | kind)
// Returns -1 if a < b, 0 if a == b, 1 if a > b
func compareKeys(aKey, aSeq uint64, aKind byte, bKey, bSeq uint64, bKind byte) int {
	if aKey != bKey {
		if aKey < bKey {
			return -1
		}
		return 1
	}
	// Same user key: higher seq comes first (newer versions first)
	if aSeq != bSeq {
		if aSeq > bSeq {
			return -1
		}
		return 1
	}
	// Same seq: tombstone (kind=1) comes before value (kind=0)
	if aKind != bKind {
		if aKind > bKind {
			return -1
		}
		return 1
	}
	return 0
}

// Compare compares two keys in the format: userKey | seq(8) | kind(1)
func Compare(a, b []byte) int {
	if len(a) < 9 || len(b) < 9 {
		// Fallback to byte comparison
		for i := 0; i < len(a) && i < len(b); i++ {
			if a[i] != b[i] {
				if a[i] < b[i] {
					return -1
				}
				return 1
			}
		}
		if len(a) < len(b) {
			return -1
		}
		if len(a) > len(b) {
			return 1
		}
		return 0
	}

	// Extract user key, seq, and kind
	aKeyLen := len(a) - 9
	bKeyLen := len(b) - 9

	// Compare user keys
	minKeyLen := aKeyLen
	if bKeyLen < minKeyLen {
		minKeyLen = bKeyLen
	}
	for i := 0; i < minKeyLen; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	if aKeyLen != bKeyLen {
		if aKeyLen < bKeyLen {
			return -1
		}
		return 1
	}

	// Compare seq (big endian, descending)
	for i := 0; i < 8; i++ {
		ai := a[aKeyLen+i]
		bi := b[bKeyLen+i]
		if ai != bi {
			if ai > bi { // descending
				return -1
			}
			return 1
		}
	}

	// Compare kind (tombstone=1 comes before value=0)
	ak := a[len(a)-1]
	bk := b[len(b)-1]
	if ak != bk {
		if ak > bk {
			return -1
		}
		return 1
	}

	return 0
}

// findGreaterOrEqual finds the first node with key >= target
// Returns the node and the update array for insertion
func (sl *Skiplist) findGreaterOrEqual(key []byte) (*Node, []*Node) {
	update := make([]*Node, maxHeight)
	current := sl.head

	for i := sl.height - 1; i >= 0; i-- {
		for current.tower[i] != nil && Compare(current.tower[i].key, key) < 0 {
			current = current.tower[i]
		}
		update[i] = current
	}

	next := current.tower[0]
	return next, update
}

// findExact finds a node with exactly the given key
func (sl *Skiplist) findExact(key []byte) *Node {
	current := sl.head
	for i := sl.height - 1; i >= 0; i-- {
		for current.tower[i] != nil && Compare(current.tower[i].key, key) < 0 {
			current = current.tower[i]
		}
	}
	next := current.tower[0]
	if next != nil && Compare(next.key, key) == 0 {
		return next
	}
	return nil
}

// Set inserts or updates a key-value pair
// Returns true if this is a new key, false if updated existing
func (sl *Skiplist) Set(key, value []byte, seq uint64, kind byte) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// Check if key already exists
	existing := sl.findExact(key)
	if existing != nil {
		// Update existing node
		existing.value = append([]byte{}, value...)
		existing.seq = seq
		existing.kind = kind
		return false
	}

	// Create new node
	height := sl.randomHeight()
	node := sl.getNode()
	node.key = append([]byte{}, key...)
	node.value = append([]byte{}, value...)
	node.seq = seq
	node.kind = kind
	node.height = height

	// If new height is greater than current skiplist height, update head
	for i := sl.height; i < height; i++ {
		sl.head.tower[i] = node
	}
	if height > sl.height {
		sl.height = height
	}

	// Find insertion point and update pointers
	_, update := sl.findGreaterOrEqual(key)
	for i := 0; i < height; i++ {
		node.tower[i] = update[i].tower[i]
		update[i].tower[i] = node
	}

	sl.length++
	return true
}

// Get retrieves a value by key
// Returns (value, seq, kind, found)
func (sl *Skiplist) Get(key []byte) ([]byte, uint64, byte, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	node := sl.findExact(key)
	if node == nil {
		return nil, 0, 0, false
	}
	if node.deleted {
		return nil, 0, 0, false
	}
	return node.value, node.seq, node.kind, true
}

// Delete marks a key as deleted (tombstone)
func (sl *Skiplist) Delete(key []byte, seq uint64) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	node := sl.findExact(key)
	if node == nil {
		return false
	}
	node.deleted = true
	node.kind = 1 // tombstone
	node.seq = seq
	return true
}

// HasKey checks if a key exists (not deleted)
func (sl *Skiplist) HasKey(key []byte) bool {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	node := sl.findExact(key)
	return node != nil && !node.deleted
}

// Len returns the number of elements in the skiplist
func (sl *Skiplist) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.length
}

// skiplistIterator provides iteration over the skiplist in order
type skiplistIterator struct {
	sl     *Skiplist
	current *Node
	valid  bool
}

// newSkiplistIterator creates a new iterator
func (sl *Skiplist) newSkiplistIterator() *skiplistIterator {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	it := &skiplistIterator{
		sl:      sl,
		current: sl.head.tower[0],
		valid:   sl.head.tower[0] != nil,
	}
	// Skip deleted nodes
	for it.valid && it.current.deleted {
		it.current = it.current.tower[0]
		it.valid = it.current != nil
	}
	return it
}

// Valid returns true if the iterator is at a valid position
func (it *skiplistIterator) Valid() bool {
	return it.valid
}

// Key returns the current key
func (it *skiplistIterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.current.key
}

// Value returns the current value
func (it *skiplistIterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.current.value
}

// Seq returns the current sequence number
func (it *skiplistIterator) Seq() uint64 {
	if !it.valid {
		return 0
	}
	return it.current.seq
}

// Kind returns the current kind (0=value, 1=tombstone)
func (it *skiplistIterator) Kind() byte {
	if !it.valid {
		return 0
	}
	return it.current.kind
}

// Next advances the iterator
func (it *skiplistIterator) Next() {
	if !it.valid {
		return
	}
	it.current = it.current.tower[0]
	it.valid = it.current != nil
	// Skip deleted nodes
	for it.valid && it.current.deleted {
		it.current = it.current.tower[0]
		it.valid = it.current != nil
	}
}

// Seek moves the iterator to the first key >= target
func (it *skiplistIterator) Seek(target []byte) {
	it.sl.mu.RLock()
	defer it.sl.mu.RUnlock()

	_, update := it.sl.findGreaterOrEqual(target)
	it.current = update[0].tower[0]
	it.valid = it.current != nil
	// Skip deleted nodes
	for it.valid && it.current.deleted {
		it.current = it.current.tower[0]
		it.valid = it.current != nil
	}
}

// EstimatedSize returns the approximate memory usage of the skiplist in bytes
func (sl *Skiplist) EstimatedSize() uint64 {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var size uint64
	current := sl.head.tower[0]
	for current != nil {
		size += uint64(unsafe.Sizeof(*current))
		size += uint64(len(current.key) + len(current.value) + len(current.tower)*8)
		current = current.tower[0]
	}
	return size
}

// Clear removes all elements from the skiplist
func (sl *Skiplist) Clear() {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	current := sl.head.tower[0]
	for current != nil {
		next := current.tower[0]
		sl.putNode(current)
		current = next
	}

	for i := range sl.head.tower {
		sl.head.tower[i] = nil
	}
	sl.height = 1
	sl.length = 0
}