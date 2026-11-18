package main

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
)

// workloads are the YCSB core workloads dkv-bench runs: the share of
// operations that are reads; the rest are updates of existing keys.
var workloads = map[string]float64{
	"A": 0.50, // update heavy
	"B": 0.95, // read mostly
	"C": 1.00, // read only
	"W": 0.00, // write only
}

// keyName is the key of record i. Keys have one fixed length, so the
// value size alone sets a record's size.
func keyName(i uint64) []byte { return fmt.Appendf(nil, "user%012d", i) }

// keyChooser picks which record an operation touches.
type keyChooser interface {
	next(rng *rand.Rand) uint64
}

type uniform struct{ n uint64 }

func (u uniform) next(rng *rand.Rand) uint64 { return rng.Uint64N(u.n) }

// zipfian is YCSB's scrambled Zipfian generator (Gray et al., "Quickly
// generating billion-record synthetic databases", 1994): item ranks follow
// a Zipfian distribution with exponent theta, and each rank is hashed to a
// record so the popular records are spread over the key space, as YCSB
// does. theta 0.99 is YCSB's default.
type zipfian struct {
	n                         uint64
	theta, alpha, zetaN, eta  float64
	zeta2, halfPowTheta, plus float64
}

func newZipfian(n uint64, theta float64) *zipfian {
	z := &zipfian{n: n, theta: theta, alpha: 1 / (1 - theta)}
	z.zetaN = zeta(n, theta)
	z.zeta2 = zeta(2, theta)
	z.eta = (1 - math.Pow(2/float64(n), 1-theta)) / (1 - z.zeta2/z.zetaN)
	z.halfPowTheta = math.Pow(0.5, theta)
	z.plus = 1 + z.halfPowTheta
	return z
}

func zeta(n uint64, theta float64) float64 {
	var sum float64
	for i := uint64(1); i <= n; i++ {
		sum += 1 / math.Pow(float64(i), theta)
	}
	return sum
}

// rank draws a Zipfian rank in [0, n): rank 0 is the most popular.
func (z *zipfian) rank(rng *rand.Rand) uint64 {
	u := rng.Float64()
	uz := u * z.zetaN
	switch {
	case uz < 1:
		return 0
	case uz < z.plus:
		return 1
	}
	r := uint64(float64(z.n) * math.Pow(z.eta*u-z.eta+1, z.alpha))
	return min(r, z.n-1)
}

func (z *zipfian) next(rng *rand.Rand) uint64 {
	h := fnv.New64a()
	var b [8]byte
	r := z.rank(rng)
	for i := range b {
		b[i] = byte(r >> (8 * i))
	}
	_, _ = h.Write(b[:]) // hashing into memory cannot fail
	return h.Sum64() % z.n
}

func newKeyChooser(dist string, records uint64) (keyChooser, error) {
	switch dist {
	case "uniform":
		return uniform{records}, nil
	case "zipfian":
		return newZipfian(records, 0.99), nil
	default:
		return nil, fmt.Errorf("unknown key distribution %q: want uniform or zipfian", dist)
	}
}

// value fills a value of size bytes for record i, version v: printable, and
// different on every update.
func value(size int, i, v uint64) []byte {
	b := make([]byte, size)
	seed := i*1_000_003 + v
	for j := range b {
		seed = seed*6364136223846793005 + 1442695040888963407
		b[j] = 'a' + byte(seed>>59)%26
	}
	return b
}
