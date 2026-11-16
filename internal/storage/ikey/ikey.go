// Package ikey defines the internal key format shared by every layer of the
// storage engine.
//
// An internal key is a user key plus an 8-byte trailer that packs a sequence
// number (high 56 bits) and a kind (low 8 bits). Internal keys sort by user
// key ascending and then by trailer descending, so for any user key the
// newest version is found first. In clustered mode the sequence number is
// the Raft log index of the write; in standalone mode the engine assigns it.
package ikey

import (
	"bytes"
	"cmp"
	"fmt"
)

// Kind says what an internal key records.
type Kind uint8

// The numeric values are part of the on-disk format from Phase 2 onwards;
// never renumber them.
const (
	// KindDelete is a tombstone: the user key has no value at this sequence.
	KindDelete Kind = 0
	// KindSet stores a value for the user key.
	KindSet Kind = 1

	// kindMax is the largest valid kind. SeekKey relies on it so that a seek
	// lands before every version visible at a sequence number.
	kindMax = KindSet
)

// Valid reports whether k is a known kind.
func (k Kind) Valid() bool { return k <= kindMax }

func (k Kind) String() string {
	switch k {
	case KindDelete:
		return "DEL"
	case KindSet:
		return "SET"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

// MaxSeq is the largest sequence number a trailer can hold.
const MaxSeq uint64 = 1<<56 - 1

// Trailer packs a sequence number and a kind: seq<<8 | kind.
type Trailer uint64

// MakeTrailer packs seq and kind. seq must be <= MaxSeq.
func MakeTrailer(seq uint64, kind Kind) Trailer {
	return Trailer(seq<<8 | uint64(kind))
}

// Seq returns the sequence number.
func (t Trailer) Seq() uint64 { return uint64(t) >> 8 }

// Kind returns the kind.
func (t Trailer) Kind() Kind { return Kind(t & 0xff) }

// Key is an internal key. UserKey is not copied by any function in this
// package; callers own the bytes.
type Key struct {
	UserKey []byte
	Trailer Trailer
}

// Seq returns the key's sequence number.
func (k Key) Seq() uint64 { return k.Trailer.Seq() }

// Kind returns the key's kind.
func (k Key) Kind() Kind { return k.Trailer.Kind() }

func (k Key) String() string {
	return fmt.Sprintf("%q#%d,%s", k.UserKey, k.Seq(), k.Kind())
}

// Compare orders internal keys: user key ascending, then trailer descending,
// so that newer versions of the same user key sort first. It returns -1, 0
// or +1.
func Compare(a, b Key) int {
	if c := bytes.Compare(a.UserKey, b.UserKey); c != 0 {
		return c
	}
	return cmp.Compare(b.Trailer, a.Trailer)
}

// SeekKey returns the smallest internal key such that every version of
// userKey with sequence number <= seq sorts at or after it, and every version
// with a larger sequence number sorts before it. Seeking to SeekKey(k, s)
// therefore lands on the newest version of k visible at s, if one exists.
func SeekKey(userKey []byte, seq uint64) Key {
	return Key{UserKey: userKey, Trailer: MakeTrailer(seq, kindMax)}
}
