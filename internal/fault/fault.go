// Package fault is the in-process backend of dkv's fault injector: a
// partition table and per-link message faults consulted by the transport
// (transport.Faults) on every call between nodes. Tests build one
// Injector, give it to every node of an in-process cluster, and change
// the network while the cluster runs. Nodes are named by their internal
// addresses.
package fault

import (
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/naveenk2608/distributed-database/internal/transport"
)

// Link describes faults on a link.
type Link struct {
	// Loss and Duplicate are probabilities per call.
	Loss, Duplicate float64
	// Delay holds every call back, plus up to Jitter more at random
	// (which also reorders concurrent calls).
	Delay, Jitter time.Duration
}

type pair struct{ from, to string }

// Injector holds the current faults. It is safe for concurrent use.
type Injector struct {
	mu      sync.Mutex
	rng     *rand.Rand
	blocked map[pair]bool
	links   map[pair]Link
	all     Link
	stats   Stats
}

// Stats counts what the injector did.
type Stats struct {
	Calls, Blocked, Lost, Duplicated, Delayed int64
}

// New returns an injector with a healthy network; seed drives the random
// choices.
func New(seed uint64) *Injector {
	return &Injector{rng: rand.New(rand.NewPCG(seed, 0xfa17)), blocked: map[pair]bool{}, links: map[pair]Link{}}
}

var _ transport.Faults = (*Injector)(nil)

// Decide implements transport.Faults.
func (i *Injector) Decide(from, to, _ string) transport.Fault {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.stats.Calls++
	p := pair{from, to}
	if i.blocked[p] {
		i.stats.Blocked++
		return transport.Fault{Drop: true}
	}
	l, ok := i.links[p]
	if !ok {
		l = i.all
	}
	var f transport.Fault
	if l.Loss > 0 && i.rng.Float64() < l.Loss {
		i.stats.Lost++
		f.Drop = true
	}
	if l.Duplicate > 0 && i.rng.Float64() < l.Duplicate {
		i.stats.Duplicated++
		f.Duplicate = true
	}
	f.Delay = l.Delay
	if l.Jitter > 0 {
		f.Delay += time.Duration(i.rng.Int64N(int64(l.Jitter)))
	}
	if f.Delay > 0 {
		i.stats.Delayed++
	}
	return f
}

// Partition splits the nodes into sides that cannot reach each other.
// Nodes in no side are unaffected.
func (i *Injector) Partition(sides ...[]string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	for a, sa := range sides {
		for b, sb := range sides {
			if a == b {
				continue
			}
			for _, x := range sa {
				for _, y := range sb {
					i.blocked[pair{x, y}] = true
				}
			}
		}
	}
}

// Isolate cuts node off from others in both directions.
func (i *Injector) Isolate(node string, others []string) {
	var rest []string
	for _, o := range others {
		if o != node {
			rest = append(rest, o)
		}
	}
	i.Partition([]string{node}, rest)
}

// Block stops calls from one node to another; the reverse direction keeps
// working (a one-way partition).
func (i *Injector) Block(from, to string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.blocked[pair{from, to}] = true
}

// SetLink sets the faults of one link; SetAll those of every link without
// its own setting.
func (i *Injector) SetLink(from, to string, l Link) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.links[pair{from, to}] = l
}

// SetAll sets the faults of every link without its own setting.
func (i *Injector) SetAll(l Link) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.all = l
}

// Heal removes every partition and link fault.
func (i *Injector) Heal() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.blocked, i.links, i.all = map[pair]bool{}, map[pair]Link{}, Link{}
}

// Blocked reports whether calls from one node to another are blocked.
func (i *Injector) Blocked(from, to string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.blocked[pair{from, to}]
}

// Stats returns the counters.
func (i *Injector) Stats() Stats {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.stats
}

// Sides draws a random split of nodes into two non-empty sides.
func Sides(rng *rand.Rand, nodes []string) ([]string, []string) {
	shuffled := slices.Clone(nodes)
	rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
	k := 1 + rng.IntN(len(shuffled)-1)
	return shuffled[:k], shuffled[k:]
}
