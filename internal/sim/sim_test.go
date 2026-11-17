package sim

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

var (
	seeds     = flag.Int("seeds", 0, "schedules to run (0 = 1,000, or 200 with -short)")
	firstSeed = flag.Uint64("first-seed", 1, "first seed")
)

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
	for seed := *firstSeed; seed < *firstSeed+uint64(n); seed++ {
		res := Run(seed, nil)
		if len(res.Violations) > 0 {
			t.Fatalf("seed %d: %d violations; first: %s\nlast events:\n%s\nreplay: go test ./internal/sim -run TestSimulation -seeds=1 -first-seed=%d",
				seed, len(res.Violations), res.Violations[0], traceTail(seed, 80), seed)
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
	t.Logf("%d schedules (seeds %d..%d) in %v, zero violations: %d leaders elected, %d commit advances, %d crashes, %d partitions, %d reads confirmed, %d membership changes, %d leader transfers, %d compactions, %d snapshots installed, %d messages delivered, %d dropped",
		n, *firstSeed, *firstSeed+uint64(n)-1, time.Since(start).Round(time.Millisecond),
		total.Leaders, total.Commits, total.Crashes, total.Partitions, total.Reads,
		total.ConfChanges, total.Transfers, total.Compactions, total.Snapshots, total.Delivered, total.Dropped)
	if total.Leaders == 0 || total.Commits == 0 || total.Reads == 0 || total.ConfChanges == 0 ||
		total.Transfers == 0 || total.Snapshots == 0 {
		t.Error("the schedules did no meaningful work")
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

// traceTail reruns seed with tracing and returns its last n events.
func traceTail(seed uint64, n int) string {
	var buf strings.Builder
	rng := rand.New(rand.NewPCG(seed, 0x5ee1))
	o := RandomOptions(rng)
	o.Trace = &buf
	Run(seed, &o)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	return fmt.Sprintf("options %+v\n%s", o, strings.Join(lines[max(0, len(lines)-n):], "\n"))
}
