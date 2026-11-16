package sstable

import (
	"bytes"
	"math"
)

// Bloom filter over the user keys of a table.
//
//	filter := bits(m/8 bytes) | k(1 byte)
//
// Each key is hashed once to 64 bits; the k probe positions come from double
// hashing, h + i*delta, which behaves like k independent hashes for this
// purpose (Kirsch and Mitzenmacher). With b bits per key and k = round(b ln 2)
// the expected false-positive rate is about (1 - e^(-k/b))^k: 0.82% at b = 10.

// bloomHash is FNV-1a over the key followed by the SplitMix64 finalizer,
// which spreads FNV's weak low bits. It is part of the file format: changing
// it makes every stored filter wrong.
func bloomHash(key []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, c := range key {
		h ^= uint64(c)
		h *= 1099511628211
	}
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

// bloomProbes returns k for a given number of bits per key.
func bloomProbes(bitsPerKey int) int {
	k := int(math.Round(float64(bitsPerKey) * math.Ln2))
	return min(max(k, 1), 30)
}

// ExpectedFalsePositiveRate is the theoretical false-positive rate of a
// filter with bitsPerKey bits per key.
func ExpectedFalsePositiveRate(bitsPerKey int) float64 {
	k := float64(bloomProbes(bitsPerKey))
	return math.Pow(1-math.Exp(-k/float64(bitsPerKey)), k)
}

type filterBuilder struct {
	bitsPerKey int
	hashes     []uint64
	lastKey    []byte
	hasLast    bool
}

// add records a user key. Consecutive versions of one key count once.
func (f *filterBuilder) add(userKey []byte) {
	if f.hasLast && bytes.Equal(f.lastKey, userKey) {
		return
	}
	f.lastKey = append(f.lastKey[:0], userKey...)
	f.hasLast = true
	f.hashes = append(f.hashes, bloomHash(userKey))
}

func (f *filterBuilder) finish() []byte {
	bits := max(len(f.hashes)*f.bitsPerKey, 64)
	nbytes := (bits + 7) / 8
	bits = nbytes * 8
	k := bloomProbes(f.bitsPerKey)
	out := make([]byte, nbytes+1)
	for _, h := range f.hashes {
		delta := h>>33 | h<<31
		for range k {
			pos := h % uint64(bits)
			out[pos/8] |= 1 << (pos % 8)
			h += delta
		}
	}
	out[nbytes] = byte(k)
	return out
}

// filterMayContain reports whether userKey may be in the filter. An empty
// or malformed filter matches everything, which is always safe.
func filterMayContain(filter, userKey []byte) bool {
	if len(filter) < 2 {
		return true
	}
	nbytes := len(filter) - 1
	k := int(filter[nbytes])
	if k < 1 || k > 30 {
		return true
	}
	bits := uint64(nbytes * 8)
	h := bloomHash(userKey)
	delta := h>>33 | h<<31
	for range k {
		pos := h % bits
		if filter[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}
