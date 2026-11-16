package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
)

// Batch is a set of writes applied atomically by DB.Apply at one sequence
// number. The zero value is an empty batch ready to use. A Batch is not safe
// for concurrent use.
//
// Last write wins, explicitly: a batch holds at most one operation per key.
// Set or Delete on a key the batch already writes replaces that operation,
// because two operations on one key at one sequence number would have the
// same internal key and could not both be stored. So Set(k, a) then Set(k, b)
// writes b, Set(k, a) then Delete(k) deletes k, and Delete(k) then
// Set(k, a) writes a. See docs/adr/0018-batch-sequence-numbers.md.
type Batch struct {
	ops   []batchOp
	index map[string]int // key -> position in ops; built once the batch is large
	bytes int            // sum of key and value lengths
}

type batchOp struct {
	kind  ikey.Kind
	key   []byte
	value []byte
}

// indexThreshold is the batch size above which a map replaces the linear
// scan for an existing operation on a key.
const indexThreshold = 8

// Set makes the batch write value to key, replacing any earlier operation on
// key in this batch. Both are copied.
func (b *Batch) Set(key, value []byte) {
	b.put(batchOp{kind: ikey.KindSet, key: slices.Clone(key), value: slices.Clone(value)})
}

// Delete makes the batch delete key, replacing any earlier operation on key
// in this batch. The key is copied.
func (b *Batch) Delete(key []byte) {
	b.put(batchOp{kind: ikey.KindDelete, key: slices.Clone(key)})
}

func (b *Batch) put(op batchOp) {
	if op.key == nil {
		op.key = []byte{}
	}
	if i, ok := b.find(op.key); ok {
		b.bytes += len(op.value) - len(b.ops[i].value)
		b.ops[i] = op
		return
	}
	b.ops = append(b.ops, op)
	b.bytes += len(op.key) + len(op.value)
	if b.index != nil {
		b.index[string(op.key)] = len(b.ops) - 1
	} else if len(b.ops) > indexThreshold {
		b.index = make(map[string]int, 2*len(b.ops))
		for i, o := range b.ops {
			b.index[string(o.key)] = i
		}
	}
}

func (b *Batch) find(key []byte) (int, bool) {
	if b.index != nil {
		i, ok := b.index[string(key)]
		return i, ok
	}
	for i := range b.ops {
		if bytes.Equal(b.ops[i].key, key) {
			return i, true
		}
	}
	return 0, false
}

// Count returns the number of keys the batch writes.
func (b *Batch) Count() int { return len(b.ops) }

// Reset empties the batch so it can be reused.
func (b *Batch) Reset() {
	clear(b.ops)
	b.ops = b.ops[:0]
	b.index = nil
	b.bytes = 0
}

// Batch record payload (docs/adr/0019-wal-format-and-recovery.md):
//
//	seq(8) | count(4) | { kind(1) | klen(uvarint) | key | vlen(uvarint) | value }*
//
// Integers are little-endian. A delete has vlen 0. Keys are unique.
const (
	batchHeaderSize = 12
	minOpSize       = 3 // kind + empty key length + empty value length
)

func (b *Batch) encodedSize() int {
	return batchHeaderSize + len(b.ops)*(1+2*binary.MaxVarintLen32) + b.bytes
}

// encode appends the batch record payload for sequence number seq to dst.
func (b *Batch) encode(dst []byte, seq uint64) []byte {
	dst = slices.Grow(dst, b.encodedSize())
	dst = binary.LittleEndian.AppendUint64(dst, seq)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(b.ops)))
	for _, op := range b.ops {
		dst = append(dst, byte(op.kind))
		dst = binary.AppendUvarint(dst, uint64(len(op.key)))
		dst = append(dst, op.key...)
		dst = binary.AppendUvarint(dst, uint64(len(op.value)))
		dst = append(dst, op.value...)
	}
	return dst
}

var errBadBatch = errors.New("malformed batch record")

// decodeBatch parses a batch record payload. The returned keys and values
// alias p. Every structural problem is an error, never a panic: the WAL's
// checksum makes a malformed payload very unlikely, but a decoder must not
// trust its input.
func decodeBatch(p []byte) (seq uint64, ops []batchOp, err error) {
	bad := func(format string, args ...any) (uint64, []batchOp, error) {
		return 0, nil, fmt.Errorf("%w: %s", errBadBatch, fmt.Sprintf(format, args...))
	}
	if len(p) < batchHeaderSize {
		return bad("%d bytes is shorter than the header", len(p))
	}
	seq = binary.LittleEndian.Uint64(p)
	count := binary.LittleEndian.Uint32(p[8:])
	rest := p[batchHeaderSize:]
	if count == 0 {
		return bad("empty batch")
	}
	if uint64(count) > uint64(len(rest)/minOpSize) {
		return bad("count %d does not fit in %d bytes", count, len(rest))
	}
	readBytes := func(what string) ([]byte, error) {
		n, k := binary.Uvarint(rest)
		if k <= 0 {
			return nil, fmt.Errorf("%w: bad %s length", errBadBatch, what)
		}
		rest = rest[k:]
		if n > uint64(len(rest)) {
			return nil, fmt.Errorf("%w: %s length %d exceeds the %d bytes left", errBadBatch, what, n, len(rest))
		}
		out := rest[:n:n]
		rest = rest[n:]
		return out, nil
	}
	ops = make([]batchOp, 0, count)
	seen := make(map[string]struct{}, count)
	for i := range count {
		if len(rest) == 0 {
			return bad("ends after %d of %d operations", i, count)
		}
		kind := ikey.Kind(rest[0])
		rest = rest[1:]
		if !kind.Valid() {
			return bad("operation %d has invalid kind %d", i, kind)
		}
		key, err := readBytes("key")
		if err != nil {
			return 0, nil, err
		}
		value, err := readBytes("value")
		if err != nil {
			return 0, nil, err
		}
		if kind == ikey.KindDelete && len(value) != 0 {
			return bad("delete of %q carries a value", key)
		}
		if _, dup := seen[string(key)]; dup {
			return bad("key %q appears twice", key)
		}
		seen[string(key)] = struct{}{}
		ops = append(ops, batchOp{kind: kind, key: key, value: value})
	}
	if len(rest) != 0 {
		return bad("%d trailing bytes", len(rest))
	}
	return seq, ops, nil
}
