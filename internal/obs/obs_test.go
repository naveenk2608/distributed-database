package obs

import (
	"slices"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// TestEventLogKeepsTheLatest: sequence numbers are gapless from 1; once the
// ring is full the oldest events go, After reports the oldest kept so a
// reader can tell it missed some, and limit bounds a page.
func TestEventLogKeepsTheLatest(t *testing.T) {
	clock := time.Unix(100, 0)
	l := NewEventLog(3, func() time.Time { clock = clock.Add(time.Second); return clock })
	if ev, oldest := l.After(0, 10); ev != nil || oldest != 0 {
		t.Fatalf("empty log: %v, oldest %d", ev, oldest)
	}
	for i := range 5 {
		e := l.Record(Event{Kind: EventLeaderElected, Shard: i})
		if e.Seq != uint64(i+1) || !e.Time.Equal(time.Unix(int64(101+i), 0)) {
			t.Fatalf("event %d stamped %d at %v", i, e.Seq, e.Time)
		}
	}
	seqs := func(ev []Event) []uint64 {
		var out []uint64
		for _, e := range ev {
			out = append(out, e.Seq)
		}
		return out
	}
	ev, oldest := l.After(0, 10)
	if !slices.Equal(seqs(ev), []uint64{3, 4, 5}) || oldest != 3 || ev[0].Shard != 2 {
		t.Fatalf("after 0: %v, oldest %d", seqs(ev), oldest)
	}
	if ev, _ := l.After(3, 10); !slices.Equal(seqs(ev), []uint64{4, 5}) {
		t.Fatalf("after 3: %v", seqs(ev))
	}
	if ev, _ := l.After(5, 10); len(ev) != 0 {
		t.Fatalf("after the newest: %v", seqs(ev))
	}
	if ev, _ := l.After(0, 2); !slices.Equal(seqs(ev), []uint64{3, 4}) {
		t.Fatalf("limit 2: %v", seqs(ev))
	}
}

// TestRequestsHistogram: each request lands in the first bucket whose bound
// it does not exceed; snapshots are cumulative; outcomes and the sum are
// counted; unknown operations are ignored.
func TestRequestsHistogram(t *testing.T) {
	r := NewRequests("put", "get")
	r.Observe("put", 250*time.Microsecond, false) // exactly the first bound
	r.Observe("put", 3*time.Millisecond, false)   // the 4 ms bucket
	r.Observe("put", 10*time.Second, true)        // beyond every bound
	r.Observe("scan", time.Millisecond, false)
	snap := r.Snapshot()
	if len(snap) != 2 || snap[0].Op != "put" || snap[1].Op != "get" {
		t.Fatalf("snapshot ops: %+v", snap)
	}
	p := snap[0]
	if p.OK != 2 || p.Errors != 1 || p.Sum != 250*time.Microsecond+3*time.Millisecond+10*time.Second {
		t.Fatalf("put counters: %+v", p)
	}
	want := []uint64{1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}
	if !slices.Equal(p.Cumulative, want) {
		t.Fatalf("cumulative %v; want %v", p.Cumulative, want)
	}
	if snap[1].OK != 0 || snap[1].Cumulative[len(LatencyBuckets)-1] != 0 {
		t.Fatalf("get counters: %+v", snap[1])
	}
}

// TestRequestsConcurrent: concurrent observers lose no count (run with
// -race).
func TestRequestsConcurrent(t *testing.T) {
	r := NewRequests("get")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				r.Observe("get", time.Millisecond, false)
			}
		})
	}
	wg.Wait()
	if s := r.Snapshot()[0]; s.OK != 8000 || s.Cumulative[2] != 8000 {
		t.Fatalf("after 8,000 observations: %+v", s)
	}
}
