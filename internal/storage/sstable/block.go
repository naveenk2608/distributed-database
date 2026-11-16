package sstable

import (
	"encoding/binary"
	"sort"
)

// Block layout (data and index blocks):
//
//	entry*   entry := shared(uvarint) | unshared(uvarint) | vlen(uvarint) | key[shared:] | value
//	restart offsets (uint32 each) | numRestarts (uint32)
//
// Every restartInterval-th entry stores its whole key (shared = 0) and its
// offset is a restart point, so a seek binary-searches the restart points
// and then scans at most restartInterval entries.

type blockBuilder struct {
	restartInterval int
	buf             []byte
	restarts        []uint32
	counter         int
	lastKey         []byte
	entries         int
}

func (b *blockBuilder) add(key, value []byte) {
	shared := 0
	if b.counter < b.restartInterval && b.entries > 0 {
		n := min(len(b.lastKey), len(key))
		for shared < n && b.lastKey[shared] == key[shared] {
			shared++
		}
	} else {
		b.restarts = append(b.restarts, uint32(len(b.buf)))
		b.counter = 0
	}
	b.buf = binary.AppendUvarint(b.buf, uint64(shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(key)-shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, key[shared:]...)
	b.buf = append(b.buf, value...)
	b.lastKey = append(b.lastKey[:0], key...)
	b.counter++
	b.entries++
}

func (b *blockBuilder) empty() bool { return b.entries == 0 }

func (b *blockBuilder) estimatedSize() int { return len(b.buf) + 4*len(b.restarts) + 4 }

// finish appends the restart array and returns the block payload. The
// builder must be reset before reuse.
func (b *blockBuilder) finish() []byte {
	if len(b.restarts) == 0 {
		b.restarts = append(b.restarts, 0)
	}
	for _, r := range b.restarts {
		b.buf = binary.LittleEndian.AppendUint32(b.buf, r)
	}
	return binary.LittleEndian.AppendUint32(b.buf, uint32(len(b.restarts)))
}

func (b *blockBuilder) reset() {
	b.buf = b.buf[:0]
	b.restarts = b.restarts[:0]
	b.counter = 0
	b.lastKey = b.lastKey[:0]
	b.entries = 0
}

// blockIter iterates over one block. Keys are compared with cmp. Values
// alias the block; the key is assembled in an owned buffer.
type blockIter struct {
	cmp         func(a, b []byte) int
	data        []byte // entries only
	restarts    []byte // restart offsets
	numRestarts int

	offset     int // start of the current entry
	nextOffset int // start of the next entry
	key        []byte
	value      []byte
	valid      bool
	err        error
}

func newBlockIter(block []byte, cmp func(a, b []byte) int) (*blockIter, error) {
	it := &blockIter{cmp: cmp}
	if err := it.init(block); err != nil {
		return nil, err
	}
	return it, nil
}

func (it *blockIter) init(block []byte) error {
	if len(block) < 4 {
		return corruptf("block of %d bytes has no restart count", len(block))
	}
	n := int(binary.LittleEndian.Uint32(block[len(block)-4:]))
	if n == 0 || n > (len(block)-4)/4 {
		return corruptf("block restart count %d does not fit in %d bytes", n, len(block))
	}
	restartsStart := len(block) - 4 - 4*n
	it.data = block[:restartsStart]
	it.restarts = block[restartsStart : len(block)-4]
	it.numRestarts = n
	for i := range n {
		if int(it.restartPoint(i)) > len(it.data) {
			return corruptf("restart point %d out of range", i)
		}
	}
	it.valid, it.err = false, nil
	return nil
}

func (it *blockIter) restartPoint(i int) uint32 {
	return binary.LittleEndian.Uint32(it.restarts[4*i:])
}

// decodeAt decodes the entry at off, given the previous key for prefix
// sharing, into it.key/it.value and sets nextOffset.
func (it *blockIter) decodeAt(off int) bool {
	p := it.data[off:]
	shared, n1 := binary.Uvarint(p)
	if n1 <= 0 {
		return it.fail("bad shared length")
	}
	unshared, n2 := binary.Uvarint(p[n1:])
	if n2 <= 0 {
		return it.fail("bad key length")
	}
	vlen, n3 := binary.Uvarint(p[n1+n2:])
	if n3 <= 0 {
		return it.fail("bad value length")
	}
	h := n1 + n2 + n3
	if shared > uint64(len(it.key)) || unshared > uint64(len(p)-h) || vlen > uint64(len(p)-h)-unshared {
		return it.fail("entry lengths out of range")
	}
	it.key = append(it.key[:shared], p[h:h+int(unshared)]...)
	if len(it.key) < 8 {
		return it.fail("key shorter than its trailer")
	}
	vs := h + int(unshared)
	it.value = p[vs : vs+int(vlen) : vs+int(vlen)]
	it.offset = off
	it.nextOffset = off + vs + int(vlen)
	it.valid = true
	return true
}

func (it *blockIter) fail(reason string) bool {
	it.err = corruptf("block entry: %s", reason)
	it.valid = false
	return false
}

func (it *blockIter) First() {
	it.key = it.key[:0]
	it.valid = false
	if it.err == nil && len(it.data) > 0 {
		it.decodeAt(0)
	}
}

func (it *blockIter) Next() {
	if !it.valid {
		return
	}
	if it.nextOffset >= len(it.data) {
		it.valid = false
		return
	}
	it.decodeAt(it.nextOffset)
}

// SeekGE positions at the first entry whose key is >= target.
func (it *blockIter) SeekGE(target []byte) {
	if it.err != nil {
		return
	}
	// Find the last restart point whose key is < target.
	var decodeErr bool
	i := sort.Search(it.numRestarts, func(i int) bool {
		if decodeErr {
			return true
		}
		it.key = it.key[:0]
		if !it.decodeAt(int(it.restartPoint(i))) {
			decodeErr = true
			return true
		}
		return it.cmp(it.key, target) >= 0
	})
	if decodeErr {
		return
	}
	start := 0
	if i > 0 {
		start = int(it.restartPoint(i - 1))
	}
	it.key = it.key[:0]
	if start >= len(it.data) {
		it.valid = false
		return
	}
	for ok := it.decodeAt(start); ok; ok = it.advance() {
		if it.cmp(it.key, target) >= 0 {
			return
		}
	}
}

func (it *blockIter) advance() bool {
	if it.nextOffset >= len(it.data) {
		it.valid = false
		return false
	}
	return it.decodeAt(it.nextOffset)
}

func (it *blockIter) Valid() bool { return it.valid }

// Key returns the current key; it is valid until the next move.
func (it *blockIter) Key() []byte { return it.key }

func (it *blockIter) Value() []byte { return it.value }

func (it *blockIter) Err() error { return it.err }
