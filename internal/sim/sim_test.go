package sim

import (
	"flag"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/naveenk2608/distributed-database/internal/raft"
)

var (
	seeds     = flag.Int("seeds", 0, "schedules to run (0 = 1,000, or 200 with -short)")
	firstSeed = flag.Uint64("first-seed", 1, "first seed")
	preVote   = flag.String("prevote", "random", "PreVote in every schedule: random (drawn per seed), on or off")
)

// scheduleOptions returns the options seed's schedule runs with under
// -prevote.
func scheduleOptions(t testing.TB, seed uint64) Options {
	t.Helper()
	o, err := optionsFor(seed, *preVote)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// optionsFor draws seed's options from the seed's generator as Run draws
// them, forces PreVote as mode ("random", "on" or "off") says, and limits
// membership changes for the result. With mode "random" they are exactly
// what Run(seed, nil) uses (TestForcedPreVoteChangesNothingElse).
func optionsFor(seed uint64, mode string) (Options, error) {
	o := randomShape(seedRand(seed))
	switch mode {
	case "random":
	case "on":
		o.PreVote = true
	case "off":
		o.PreVote = false
	default:
		return o, fmt.Errorf("-prevote=%q: want random, on or off", mode)
	}
	o.limitMembershipChanges()
	return o, nil
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
	// A replay of one seed (the failure message's hint) may well install no
	// snapshot; across 100 schedules or more, every kind of event happens.
	if n >= 100 && (total.Leaders == 0 || total.Commits == 0 || total.Reads == 0 || total.Transfers == 0 ||
		total.Snapshots == 0 || (membershipChanges && total.ConfChanges == 0)) {
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
// today and with PreVote forced on (dkv's configuration), it must pass
// every check, and the PreVote-on run must still change membership. As
// originally drawn, before PreVote-off schedules stopped changing
// membership, it must still fail the liveness check and nothing else: if
// it passes, a change to how options or events are drawn has turned the
// seed into a different schedule, and the regression test into nothing.
func TestRegressionSeeds(t *testing.T) {
	for _, seed := range regressionSeeds {
		drawn := randomShape(seedRand(seed))
		limited, preVoteOn := drawn, drawn
		limited.limitMembershipChanges()
		preVoteOn.PreVote = true
		preVoteOn.limitMembershipChanges()
		for _, tc := range []struct {
			name     string
			o        Options
			original bool
		}{
			{"as drawn", limited, false},
			{"prevote on", preVoteOn, false},
			{"original", drawn, true},
		} {
			t.Run(fmt.Sprintf("%d/%s", seed, tc.name), func(t *testing.T) {
				res := Run(seed, &tc.o)
				for _, v := range res.Violations {
					if !tc.original || !strings.HasPrefix(v, "Liveness:") {
						t.Fatalf("%s\nlast events:\n%s", v, traceTail(seed, tc.o, 80))
					}
					t.Logf("expected: %s", v)
				}
				switch {
				case tc.original && len(res.Violations) == 0:
					t.Fatal("the original schedule no longer fails the liveness check: it is not the schedule that failed any more")
				case tc.o.ConfRate > 0 && res.ConfChanges == 0:
					t.Fatal("no membership change happened: the schedule no longer exercises what failed")
				}
			})
		}
	}
}

// TestMembershipChangesNeedPreVoteAndCheckQuorum: no schedule drawn for
// seeds 1 to 10,000 changes membership unless both PreVote and
// CheckQuorum are on, and with both on, some do.
func TestMembershipChangesNeedPreVoteAndCheckQuorum(t *testing.T) {
	changing := 0
	for seed := range uint64(10000) {
		o := RandomOptions(seedRand(seed + 1))
		if o.ConfRate > 0 {
			if !o.PreVote || !o.CheckQuorum {
				t.Fatalf("seed %d: membership changes with PreVote %v, CheckQuorum %v", seed+1, o.PreVote, o.CheckQuorum)
			}
			changing++
		}
	}
	if changing == 0 {
		t.Fatal("no schedule changes membership")
	}
}

// TestForcedPreVoteChangesNothingElse: -prevote=random gives exactly
// Run(seed, nil), and forcing PreVote on or off changes no option but
// PreVote and, through limitMembershipChanges, ConfRate.
func TestForcedPreVoteChangesNothingElse(t *testing.T) {
	for seed := uint64(1); seed <= 2000; seed++ {
		random, _ := optionsFor(seed, "random")
		on, _ := optionsFor(seed, "on")
		off, _ := optionsFor(seed, "off")
		if want := RandomOptions(seedRand(seed)); !reflect.DeepEqual(random, want) {
			t.Fatalf("seed %d: -prevote=random gives %+v; Run uses %+v", seed, random, want)
		}
		off.PreVote, off.ConfRate = on.PreVote, on.ConfRate
		if !reflect.DeepEqual(on, off) {
			t.Fatalf("seed %d: forcing PreVote changed other options: %+v vs %+v", seed, on, off)
		}
		if seed <= 20 {
			if a, b := Run(seed, nil), Run(seed, &random); a.Trace != b.Trace {
				t.Fatalf("seed %d: Run with -prevote=random options differs from Run(seed, nil)", seed)
			}
		}
	}
	if _, err := optionsFor(1, "sometimes"); err == nil {
		t.Fatal("a bad -prevote value was accepted")
	}
}

// TestScheduleStopsAtFirstViolation: once a check fails, the schedule
// takes no further step and the calm phase does not run, so the first
// violation is the only one reported.
func TestScheduleStopsAtFirstViolation(t *testing.T) {
	o := RandomOptions(seedRand(7))
	s := newSim(7, seedRand(7), o)
	const at = 50
	s.intercept = func(raft.Message) bool {
		if s.now == at && !s.failed() {
			s.check.violate("planted at step %d", s.now)
		}
		return true
	}
	res := s.run()
	if len(res.Violations) != 1 || res.Violations[0] != fmt.Sprintf("planted at step %d", at) {
		t.Fatalf("violations %q; want only the planted one", res.Violations)
	}
	if s.now != at+1 {
		t.Fatalf("the schedule ran on to step %d after a violation at step %d", s.now, at)
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
