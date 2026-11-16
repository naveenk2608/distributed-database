package lsm

import (
	"slices"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
)

// Batch is an ordered list of writes applied atomically by DB.Apply. Every
// operation in a batch gets the same sequence number, so if a batch writes
// the same key more than once, the last write wins. The zero value is an
// empty batch ready to use. A Batch is not safe for concurrent use.
type Batch struct {
	ops []batchOp
}

type batchOp struct {
	kind  ikey.Kind
	key   []byte
	value []byte
}

// Set adds a write of value to key. Both are copied.
func (b *Batch) Set(key, value []byte) {
	b.ops = append(b.ops, batchOp{kind: ikey.KindSet, key: slices.Clone(key), value: slices.Clone(value)})
}

// Delete adds a deletion of key. The key is copied.
func (b *Batch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{kind: ikey.KindDelete, key: slices.Clone(key)})
}

// Count returns the number of operations in the batch.
func (b *Batch) Count() int { return len(b.ops) }

// Reset empties the batch so it can be reused.
func (b *Batch) Reset() {
	clear(b.ops)
	b.ops = b.ops[:0]
}

// effectiveOps returns the operations to apply: the last operation for each
// key, in batch order. Since a batch shares one sequence number, earlier
// writes to the same key would be invisible anyway.
func (b *Batch) effectiveOps() []batchOp {
	if len(b.ops) <= 1 {
		return b.ops
	}
	seen := make(map[string]struct{}, len(b.ops))
	keep := make([]bool, len(b.ops))
	n := 0
	for i := len(b.ops) - 1; i >= 0; i-- {
		if _, dup := seen[string(b.ops[i].key)]; !dup {
			seen[string(b.ops[i].key)] = struct{}{}
			keep[i] = true
			n++
		}
	}
	if n == len(b.ops) {
		return b.ops
	}
	out := make([]batchOp, 0, n)
	for i, op := range b.ops {
		if keep[i] {
			out = append(out, op)
		}
	}
	return out
}
