package lsm

import (
	"bytes"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
	"github.com/naveenk2608/distributed-database/internal/storage/memtable"
)

// IterOptions configures NewIterator.
type IterOptions struct {
	// Snapshot to read at; nil means the state when the iterator is created.
	Snapshot *Snapshot
}

// Iterator walks live user keys in ascending order, yielding the newest
// version of each key visible at its snapshot and skipping deleted keys.
// It is not safe for concurrent use.
type Iterator struct {
	it  *memtable.Iter
	seq uint64

	valid   bool
	key     []byte
	value   []byte
	version uint64
}

// NewIterator returns an unpositioned iterator; call First or SeekGE.
func (d *DB) NewIterator(opts IterOptions) (*Iterator, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	return &Iterator{it: d.mem.NewIter(), seq: d.readSeq(opts.Snapshot)}, nil
}

// First positions the iterator at the smallest live key.
func (i *Iterator) First() {
	i.it.First()
	i.settle()
}

// SeekGE positions the iterator at the smallest live key >= key.
func (i *Iterator) SeekGE(key []byte) {
	i.it.SeekGE(ikey.SeekKey(key, i.seq))
	i.settle()
}

// Next moves to the next live key. The iterator must be valid.
func (i *Iterator) Next() {
	i.skipUserKey(i.key)
	i.settle()
}

// Valid reports whether the iterator is positioned at a key.
func (i *Iterator) Valid() bool { return i.valid }

// Key returns the current user key. It is valid until the next positioning
// call and must not be modified.
func (i *Iterator) Key() []byte { return i.key }

// Value returns the current value. It is valid until the next positioning
// call and must not be modified.
func (i *Iterator) Value() []byte { return i.value }

// Version returns the sequence number of the write that produced the
// current value.
func (i *Iterator) Version() uint64 { return i.version }

// Close releases the iterator.
func (i *Iterator) Close() error {
	i.valid, i.key, i.value, i.it = false, nil, nil, nil
	return nil
}

// settle advances the underlying iterator to the newest visible version of
// the next live user key, starting at its current position.
func (i *Iterator) settle() {
	for i.it.Valid() {
		k := i.it.Key()
		if k.Seq() > i.seq {
			i.it.Next() // written after the snapshot
			continue
		}
		// Entries for a user key are ordered newest first, so the first one
		// visible at the snapshot is the version the snapshot sees.
		if k.Kind() == ikey.KindDelete {
			i.skipUserKey(k.UserKey)
			continue
		}
		i.valid, i.key, i.value, i.version = true, k.UserKey, i.it.Value(), k.Seq()
		return
	}
	i.valid, i.key, i.value, i.version = false, nil, nil, 0
}

// skipUserKey advances past every remaining entry for userKey.
func (i *Iterator) skipUserKey(userKey []byte) {
	for i.it.Valid() && bytes.Equal(i.it.Key().UserKey, userKey) {
		i.it.Next()
	}
}
