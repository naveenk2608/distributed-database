package obs

import (
	"sync/atomic"
	"time"
)

// LatencyBuckets are the upper bounds of the request latency histogram.
// Fixed buckets let a reader compute percentiles over any interval from
// the difference of two snapshots.
var LatencyBuckets = []time.Duration{
	250 * time.Microsecond, 500 * time.Microsecond, time.Millisecond, 2 * time.Millisecond,
	4 * time.Millisecond, 8 * time.Millisecond, 16 * time.Millisecond, 32 * time.Millisecond,
	64 * time.Millisecond, 128 * time.Millisecond, 256 * time.Millisecond, 512 * time.Millisecond,
	time.Second, 2 * time.Second, 4 * time.Second,
}

// Requests counts a server's requests by operation and outcome, with a
// latency histogram per operation. It is safe for concurrent use.
type Requests struct {
	ops   []string
	stats map[string]*opStats
}

type opStats struct {
	ok, errors atomic.Uint64
	sumNanos   atomic.Uint64
	buckets    []atomic.Uint64 // one per LatencyBuckets entry, plus overflow
}

// NewRequests tracks the named operations; others are ignored.
func NewRequests(ops ...string) *Requests {
	r := &Requests{ops: ops, stats: map[string]*opStats{}}
	for _, op := range ops {
		r.stats[op] = &opStats{buckets: make([]atomic.Uint64, len(LatencyBuckets)+1)}
	}
	return r
}

// Observe records one request.
func (r *Requests) Observe(op string, d time.Duration, failed bool) {
	s := r.stats[op]
	if s == nil {
		return
	}
	if failed {
		s.errors.Add(1)
	} else {
		s.ok.Add(1)
	}
	s.sumNanos.Add(uint64(max(d, 0)))
	i := 0
	for i < len(LatencyBuckets) && d > LatencyBuckets[i] {
		i++
	}
	s.buckets[i].Add(1)
}

// OpSnapshot is one operation's counters at a moment. Cumulative[i]
// counts requests that took at most LatencyBuckets[i]; requests slower
// than every bound count only in OK+Errors.
type OpSnapshot struct {
	Op         string
	OK, Errors uint64
	Sum        time.Duration
	Cumulative []uint64
}

// Snapshot returns every tracked operation's counters, in the order given
// to NewRequests.
func (r *Requests) Snapshot() []OpSnapshot {
	out := make([]OpSnapshot, 0, len(r.ops))
	for _, op := range r.ops {
		s := r.stats[op]
		snap := OpSnapshot{Op: op, OK: s.ok.Load(), Errors: s.errors.Load(), Sum: time.Duration(s.sumNanos.Load()),
			Cumulative: make([]uint64, len(LatencyBuckets))}
		var total uint64
		for i := range LatencyBuckets {
			total += s.buckets[i].Load()
			snap.Cumulative[i] = total
		}
		out = append(out, snap)
	}
	return out
}
