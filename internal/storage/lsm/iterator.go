package lsm

import (
	"bytes"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
)

// IterOptions configures NewIterator.
type IterOptions struct {
	// Snapshot to read at; nil means the state when the iterator is created.
	Snapshot *Snapshot
}

// Iterator walks live user keys in ascending order, yielding the newest
// version of each key visible at its snapshot and skipping deleted keys.
// It holds the files it reads open until Close, so always close it. It is
// not safe for concurrent use.
type Iterator struct {
	state *readState
	it    internalIterator
	seq   uint64

	valid   bool
	key     []byte // owned copy: table iterators reuse their key buffers
	value   []byte
	version uint64
}

// NewIterator returns an unpositioned iterator; call First or SeekGE.
func (d *DB) NewIterator(opts IterOptions) (*Iterator, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	seq := d.readSeq(opts.Snapshot)
	s := d.loadState()
	if s == nil {
		return nil, ErrClosed
	}
	iters := make([]internalIterator, 0, 1+len(s.imm)+len(s.tables))
	iters = append(iters, s.mem.NewIter())
	for _, m := range s.imm {
		iters = append(iters, m.mt.NewIter())
	}
	for _, t := range s.tables {
		if t.meta.minSeq <= seq {
			iters = append(iters, t.reader.NewIter())
		}
	}
	it := iters[0]
	if len(iters) > 1 {
		it = newMergingIter(iters...)
	}
	return &Iterator{state: s, it: it, seq: seq}, nil
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

// Valid reports whether the iterator is positioned at a key. After a read
// error it is false; check Err.
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

// Err returns the first read error, such as a corrupt block. A scan that
// ends with an error is incomplete.
func (i *Iterator) Err() error {
	if i.it == nil {
		return nil
	}
	if err := i.it.Err(); err != nil {
		return readError(err)
	}
	return nil
}

// Close releases the files the iterator holds.
func (i *Iterator) Close() error {
	if i.state != nil {
		i.state.unref()
		i.state = nil
	}
	i.valid, i.value = false, nil
	return nil
}

// settle advances the underlying iterator to the newest visible version of
// the next live user key, starting at its current position.
func (i *Iterator) settle() {
	for i.it.Valid() && i.it.Err() == nil {
		k := i.it.Key()
		if k.Seq() > i.seq {
			i.it.Next() // written after the snapshot
			continue
		}
		// Entries for a user key are ordered newest first, so the first one
		// visible at the snapshot is the version the snapshot sees.
		i.key = append(i.key[:0], k.UserKey...)
		if k.Kind() == ikey.KindDelete {
			i.skipUserKey(i.key)
			continue
		}
		i.valid, i.value, i.version = true, i.it.Value(), k.Seq()
		return
	}
	i.valid, i.value, i.version = false, nil, 0
}

// skipUserKey advances past every remaining entry for userKey, which must
// not alias the underlying iterator's key.
func (i *Iterator) skipUserKey(userKey []byte) {
	for i.it.Valid() && bytes.Equal(i.it.Key().UserKey, userKey) {
		i.it.Next()
	}
}
