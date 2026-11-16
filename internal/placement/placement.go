// Package placement maps keys to shards and shards to replica sets.
//
// A key belongs to shard xxhash64(key) mod S, where S is fixed when the
// cluster is bootstrapped. Shards are placed on nodes with a consistent hash
// ring: every member owns VirtualNodes points on a 64-bit ring, each shard
// has a point of its own, and a shard's replica set is the first RF distinct
// members found walking clockwise from that point. Placement depends only on
// the member list, S and RF, so every node computes the same map without a
// metadata service, and adding a member changes only the replica slots it
// takes over. See docs/adr/0023-placement.md.
package placement

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"

	"github.com/cespare/xxhash/v2"
)

// VirtualNodes is the number of ring points per member. It is part of the
// placement algorithm: every node must use the same value.
const VirtualNodes = 128

// ShardOf returns the shard that owns key.
func ShardOf(key []byte, numShards int) int {
	return int(xxhash.Sum64(key) % uint64(numShards))
}

type point struct {
	hash   uint64
	member int
}

// Ring is a consistent hash ring over a set of members. It is immutable and
// safe for concurrent use.
type Ring struct {
	members []string // sorted, unique
	points  []point  // sorted by hash
}

// NewRing returns the ring for members. Order and duplicates do not matter.
func NewRing(members []string) *Ring {
	ms := slices.Clone(members)
	slices.Sort(ms)
	ms = slices.Compact(ms)
	r := &Ring{members: ms, points: make([]point, 0, len(ms)*VirtualNodes)}
	for i, m := range ms {
		for v := range VirtualNodes {
			r.points = append(r.points, point{xxhash.Sum64String(m + "#" + strconv.Itoa(v)), i})
		}
	}
	// Break hash ties by member so the order never depends on input order.
	slices.SortFunc(r.points, func(a, b point) int {
		return cmp.Or(cmp.Compare(a.hash, b.hash), cmp.Compare(a.member, b.member))
	})
	return r
}

// Members returns the ring's members, sorted.
func (r *Ring) Members() []string { return slices.Clone(r.members) }

func shardPoint(shard int) uint64 {
	return xxhash.Sum64String("shard/" + strconv.Itoa(shard))
}

// ReplicaSet returns the first rf distinct members clockwise from shard's
// point, in ring order; the first is the shard's primary. If the ring has
// fewer than rf members it returns all of them.
func (r *Ring) ReplicaSet(shard, rf int) []string {
	if len(r.members) == 0 {
		return nil
	}
	rf = min(rf, len(r.members))
	h := shardPoint(shard)
	i, _ := slices.BinarySearchFunc(r.points, h, func(p point, h uint64) int { return cmp.Compare(p.hash, h) })
	out := make([]string, 0, rf)
	seen := make(map[int]bool, rf)
	for n := 0; len(out) < rf && n < len(r.points); n++ {
		p := r.points[(i+n)%len(r.points)]
		if !seen[p.member] {
			seen[p.member] = true
			out = append(out, r.members[p.member])
		}
	}
	return out
}

// Map is the placement of every shard. It is immutable.
type Map struct {
	Shards int
	RF     int
	sets   [][]string
}

// Compute places shards 0..shards-1 on members with rf replicas each.
func Compute(members []string, shards, rf int) (*Map, error) {
	if shards < 1 {
		return nil, fmt.Errorf("placement: shard count %d must be positive", shards)
	}
	if rf < 1 {
		return nil, fmt.Errorf("placement: replication factor %d must be positive", rf)
	}
	r := NewRing(members)
	if len(r.members) < rf {
		return nil, fmt.Errorf("placement: replication factor %d needs at least %d members, have %d", rf, rf, len(r.members))
	}
	m := &Map{Shards: shards, RF: rf, sets: make([][]string, shards)}
	for s := range shards {
		m.sets[s] = r.ReplicaSet(s, rf)
	}
	return m, nil
}

// ReplicaSet returns shard's replicas; the first is its primary.
func (m *Map) ReplicaSet(shard int) []string { return slices.Clone(m.sets[shard]) }

// Primary returns the member that serves shard.
func (m *Map) Primary(shard int) string { return m.sets[shard][0] }

// ShardsOf returns the shards member holds a replica of, ascending.
func (m *Map) ShardsOf(member string) []int {
	var out []int
	for s, set := range m.sets {
		if slices.Contains(set, member) {
			out = append(out, s)
		}
	}
	return out
}

// Fingerprint identifies a placement: the algorithm version, shard count,
// replication factor and member list. Nodes whose fingerprints differ would
// place shards differently.
func Fingerprint(members []string, shards, rf int) string {
	ms := slices.Clone(members)
	slices.Sort(ms)
	ms = slices.Compact(ms)
	h := xxhash.New()
	for _, m := range ms {
		_, _ = h.WriteString(m) // hashing into memory cannot fail
		_, _ = h.WriteString("\n")
	}
	return fmt.Sprintf("v1/s%d/rf%d/%016x", shards, rf, h.Sum64())
}
