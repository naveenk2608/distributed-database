package console

import (
	"log/slog"
	"math"
	"testing"
	"time"

	"go.uber.org/goleak"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func near(t *testing.T, what string, got *float64, want float64) {
	t.Helper()
	if got == nil || math.Abs(*got-want) > 1e-9 {
		t.Fatalf("%s = %v; want %v", what, got, want)
	}
}

// TestPercentiles: a percentile is interpolated inside the bucket its rank
// falls in, and capped at the last bound when it falls beyond.
func TestPercentiles(t *testing.T) {
	bounds := []float64{0.001, 0.002, 0.004}
	// 10 requests up to 1 ms, 10 more up to 2 ms, none up to 4 ms, 0 slower.
	cum := []uint64{10, 20, 20}
	near(t, "p50", percentileMs(bounds, cum, 20, 0.50), 1)        // rank 10: the top of the first bucket
	near(t, "p75", percentileMs(bounds, cum, 20, 0.75), 1.5)      // rank 15: halfway through the second
	near(t, "p25", percentileMs(bounds, cum, 20, 0.25), 0.5)      // rank 5: halfway through the first
	near(t, "p99 beyond", percentileMs(bounds, cum, 40, 0.99), 4) // half the requests slower than every bound
}

func stats(started, at int64, ok, errs uint64, cum ...uint64) *dkvv1.StatsResponse {
	return &dkvv1.StatsResponse{StartedUnixNano: started, UnixNano: at, LatencyBoundsSeconds: []float64{0.001, 0.002},
		Ops: []*dkvv1.OpStats{{Op: "put", Ok: ok, Errors: errs, LatencyCumulative: cum}, {Op: "get", LatencyCumulative: []uint64{0, 0}}}}
}

// TestDelta: rates come from the difference of two snapshots of the same
// process; across a restart, or with no earlier snapshot, there are none.
func TestDelta(t *testing.T) {
	s := int64(time.Second)
	a := stats(1, 10*s, 100, 0, 100, 100)
	b := stats(1, 12*s, 140, 4, 120, 144) // 40 writes and 4 errors in 2 s
	iv, ok := delta(a, b)
	if !ok || iv.rates.WritesPerSec != 20 || iv.rates.ErrorsPerSec != 2 || iv.rates.ReadsPerSec != 0 || iv.total != 44 {
		t.Fatalf("delta = %+v, %v", iv, ok)
	}
	near(t, "p50", iv.rates.P50Ms, 1.08333333333333) // rank 22: 2 of the 24 requests between 1 and 2 ms
	if _, ok := delta(nil, b); ok {
		t.Fatal("an interval without an earlier snapshot")
	}
	if _, ok := delta(stats(2, 9*s, 1000, 0, 1000, 1000), b); ok {
		t.Fatal("an interval across a restart")
	}
	if iv := (interval{}).add(iv).add(iv); iv.total != 88 || iv.rates.WritesPerSec != 40 {
		t.Fatalf("two nodes added: %+v", iv)
	}
}

// TestShardViews: the leader at the highest term describes a group; each
// member's lag is from the leader's progress and its applied index from
// its own node.
func TestShardViews(t *testing.T) {
	replica := func(shard int32, role, leader string, term, applied, last uint64, progress map[string]uint64) *dkvv1.ReplicaStats {
		return &dkvv1.ReplicaStats{Raft: &dkvv1.ReplicaStatus{Shard: shard, Role: role, Leader: leader, Term: term, Applied: applied,
			Commit: applied, LastIndex: last, Voters: []string{"a", "b", "c"}, Progress: progress}}
	}
	views := shardViews(map[string]*dkvv1.StatsResponse{
		"a": {Replicas: []*dkvv1.ReplicaStats{replica(0, "leader", "a", 3, 50, 50, map[string]uint64{"a": 50, "b": 48, "c": 20})}},
		"b": {Replicas: []*dkvv1.ReplicaStats{replica(0, "follower", "a", 3, 47, 48, nil)}},
		// c thinks it still leads, at an older term: it lost leadership.
		"c": {Replicas: []*dkvv1.ReplicaStats{replica(0, "leader", "c", 2, 20, 20, nil)}},
	})
	if len(views) != 1 || views[0].Leader != "a" || views[0].Term != 3 || len(views[0].Members) != 3 {
		t.Fatalf("views %+v", views)
	}
	lag := map[string]uint64{}
	applied := map[string]uint64{}
	for _, m := range views[0].Members {
		lag[m.NodeID], applied[m.NodeID] = m.Lag, *m.Applied
	}
	if lag["a"] != 0 || lag["b"] != 2 || lag["c"] != 30 || applied["b"] != 47 || applied["c"] != 20 {
		t.Fatalf("lag %v applied %v", lag, applied)
	}
}

// TestEventMerge: a membership change reported by every member is kept
// once; a restarted node's events, numbered from 1 again, are not taken
// for old ones; events are kept in time order.
func TestEventMerge(t *testing.T) {
	c := New(config.Console{PollInterval: time.Second, MemorySamples: 10}, nil, slog.New(slog.DiscardHandler))
	t0 := time.Unix(1000, 0)
	shard := 2
	added := func(reporter string, at time.Time) EventView {
		return EventView{Time: at, Kind: "replica_added", Reporter: reporter, Shard: &shard, Node: "d", Index: 42, Seq: 7}
	}
	fresh := c.addEventsLocked([]EventView{added("a", t0), added("b", t0.Add(time.Millisecond)), added("c", t0)})
	if len(fresh) != 1 || len(c.events) != 1 {
		t.Fatalf("membership change from 3 members: %d fresh, %d kept", len(fresh), len(c.events))
	}
	elected := func(seq uint64, at time.Time) EventView {
		return EventView{Time: at, Kind: "leader_elected", Reporter: "a", Shard: &shard, Seq: seq}
	}
	c.addEventsLocked([]EventView{elected(1, t0.Add(time.Second))})
	// Node a restarts: its next event is number 1 again.
	fresh = c.addEventsLocked([]EventView{elected(1, t0.Add(time.Hour)), elected(1, t0.Add(time.Second))})
	if len(fresh) != 1 || len(c.events) != 3 {
		t.Fatalf("after a restart: %d fresh, %d kept", len(fresh), len(c.events))
	}
	c.addEventsLocked([]EventView{elected(9, t0.Add(-time.Minute))})
	for i := 1; i < len(c.events); i++ {
		if c.events[i].Time.Before(c.events[i-1].Time) {
			t.Fatalf("events out of order: %+v", c.events)
		}
	}
}
