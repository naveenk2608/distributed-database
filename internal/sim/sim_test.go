package sim

import (
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"
)

var (
	seeds     = flag.Int("seeds", 0, "schedules to run (0 = 1,000, or 200 with -short)")
	firstSeed = flag.Uint64("first-seed", 1, "first seed")
	preVote   = flag.String("prevote", "random", "PreVote in every schedule: random (drawn per seed), on or off")
)

// scheduleOptions returns the options seed's schedule runs with: drawn
// from the seed's generator as Run draws them, with PreVote forced as
// -prevote says, and membership changes limited for the result. With
// -prevote=random they are exactly what Run(seed, nil) uses.
func scheduleOptions(t testing.TB, seed uint64) Options {
	t.Helper()
	o := randomShape(seedRand(seed))
	switch *preVote {
	case "random":
	case "on":
		o.PreVote = true
	case "off":
		o.PreVote = false
	default:
		t.Fatalf("-prevote=%q: want random, on or off", *preVote)
	}
	o.limitMembershipChanges()
	return o
}

// TestSimulation is the Phase 6 gate's simulator check: many seeded
// schedules with drops, duplicates, delays, reordering, partitions and
// crashes, with zero safety violations. Run the gate size with
// make sim SEEDS=10000.
func TestSimulation(t *testing.T) {
	n := *seeds
	if n == 0 {
		n = 1000
		if testing.Short() {
			n = 200
		}
	}
	start := time.Now()
	var total Result
	membershipChanges := false
	for seed := *firstSeed; seed < *firstSeed+uint64(n); seed++ {
		o := scheduleOptions(t, seed)
		membershipChanges = membershipChanges || o.ConfRate > 0
		res := Run(seed, &o)
		if len(res.Violations) > 0 {
			t.Fatalf("seed %d: %d violations; first: %s\nlast events:\n%s\nreplay: go test ./internal/sim -run TestSimulation -seeds=1 -first-seed=%d -prevote=%s",
				seed, len(res.Violations), res.Violations[0], traceTail(seed, o, 80), seed, *preVote)
		}
		total.Leaders += res.Leaders
		total.Commits += res.Commits
		total.Crashes += res.Crashes
		total.Partitions += res.Partitions
		total.Reads += res.Reads
		total.Delivered += res.Delivered
		total.Dropped += res.Dropped
		total.ConfChanges += res.ConfChanges
		total.Transfers += res.Transfers
		total.Compactions += res.Compactions
		total.Snapshots += res.Snapshots
	}
	t.Logf("%d schedules (seeds %d..%d, PreVote %s) in %v, zero violations: %d leaders elected, %d commit advances, %d crashes, %d partitions, %d reads confirmed, %d membership changes, %d leader transfers, %d compactions, %d snapshots installed, %d messages delivered, %d dropped",
		n, *firstSeed, *firstSeed+uint64(n)-1, *preVote, time.Since(start).Round(time.Millisecond),
		total.Leaders, total.Commits, total.Crashes, total.Partitions, total.Reads,
		total.ConfChanges, total.Transfers, total.Compactions, total.Snapshots, total.Delivered, total.Dropped)
	if total.Leaders == 0 || total.Commits == 0 || total.Reads == 0 || total.Transfers == 0 || total.Snapshots == 0 ||
		(membershipChanges && total.ConfChanges == 0) {
		t.Error("the schedules did no meaningful work")
	}
}

// regressionSeeds once failed. Each runs on every test run, whatever
// -seeds and -first-seed say:
//
//   - 51674: PreVote off, CheckQuorum on, with membership changes. The
//     membership came down to two voters, one of which could never win
//     (its log was behind), and the group stayed leaderless for most of
//     the calm phase (docs/adr/0026).
var regressionSeeds = []uint64{51674}

// TestRegressionSeeds runs each regression seed three ways. As drawn
// today and with PreVote forced on (dkv's configuration, membership
// changes kept), it must pass every check. As originally drawn, before
// PreVote-off schedules stopped changing membership, it may fail only the
// liveness check: no safety violation, ever.
func TestRegressionSeeds(t *testing.T) {
	for _, seed := range regressionSeeds {
		drawn := randomShape(seedRand(seed))
		limited, preVoteOn := drawn, drawn
		limited.limitMembershipChanges()
		preVoteOn.PreVote = true
		preVoteOn.limitMembershipChanges()
		for _, tc := range []struct {
			name         string
			o            Options
			livenessOnly bool
		}{
			{"as drawn", limited, false},
			{"prevote on", preVoteOn, false},
			{"original", drawn, true},
		} {
			t.Run(fmt.Sprintf("%d/%s", seed, tc.name), func(t *testing.T) {
				res := Run(seed, &tc.o)
				for _, v := range res.Violations {
					if !tc.livenessOnly || !strings.HasPrefix(v, "Liveness:") {
						t.Fatalf("%s\nlast events:\n%s", v, traceTail(seed, tc.o, 80))
					}
					t.Logf("allowed: %s", v)
				}
			})
		}
	}
}

// TestReplayIsIdentical: running a seed twice produces the same trace.
func TestReplayIsIdentical(t *testing.T) {
	for seed := range uint64(50) {
		a, b := Run(seed, nil), Run(seed, nil)
		if a.Trace != b.Trace || a.Delivered != b.Delivered || a.Leaders != b.Leaders {
			t.Fatalf("seed %d: two runs differ (trace %x vs %x)", seed, a.Trace, b.Trace)
		}
	}
	if Run(1, nil).Trace == Run(2, nil).Trace {
		t.Error("different seeds produced the same trace")
	}
}

// traceTail reruns seed with options o and tracing, and returns its last
// n events.
func traceTail(seed uint64, o Options, n int) string {
	var buf strings.Builder
	o.Trace = &buf
	Run(seed, &o)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	return fmt.Sprintf("options %+v\n%s", o, strings.Join(lines[max(0, len(lines)-n):], "\n"))
}
