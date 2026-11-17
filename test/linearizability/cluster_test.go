package linearizability

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"go.uber.org/goleak"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/fault"
	"github.com/naveenk2608/distributed-database/internal/meta"
	"github.com/naveenk2608/distributed-database/internal/node/nodetest"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

var (
	runs     = flag.Int("lin-runs", 1, "runs of every scenario")
	duration = flag.Duration("lin-duration", 3*time.Second, "how long each scenario's faults last")
	keep     = flag.String("lin-out", "", "directory for visualizations of failing histories")
	baseSeed = flag.Uint64("lin-seed", 1, "seed of the first run; run r of scenario i uses lin-seed + 1000*r + i")
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const (
	nodes   = 3
	shards  = 4
	keys    = 8
	workers = 4
)

// world is a cluster under test with its fault injector and history.
type world struct {
	t   *testing.T
	c   *nodetest.Cluster
	inj *fault.Injector
	rng *rand.Rand
	h   *History
	wl  *Workload
}

func newWorld(t *testing.T, seed uint64, opts nodetest.Options, configure func(*config.Config)) *world {
	inj := fault.New(seed)
	opts.Faults = inj
	c := nodetest.StartWith(t, nodes, opts, func(_ int, cfg *config.Config) {
		cfg.Cluster.Shards = shards
		cfg.Raft.CompactEntries = 200
		cfg.Liveness.DeadAfter = 2 * time.Second
		if configure != nil {
			configure(cfg)
		}
	})
	nodetest.WaitFor(t, 30*time.Second, "every group to elect a leader", c.Settled)
	return &world{t: t, c: c, inj: inj, rng: rand.New(rand.NewPCG(seed, 11)), h: NewHistory()}
}

func (w *world) addrs() []string {
	var out []string
	for _, m := range w.c.Members {
		out = append(out, m.InternalAddr)
	}
	return out
}

func (w *world) indexOf(id string) int {
	for i, m := range w.c.Members {
		if m.NodeID() == id {
			return i
		}
	}
	return -1
}

// leaderOf returns the member index leading a random data shard, or -1.
func (w *world) leaderOf() int {
	g := w.c.Groups()[w.rng.IntN(shards)]
	if g == nil {
		return -1
	}
	return w.indexOf(g.GetLeader())
}

// sleep waits d or until ctx ends.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// workload starts the clients; call the returned function after ending
// ctx to wait for them.
func (w *world) workload(ctx context.Context, seed uint64, dup bool) func() {
	var pubs []string
	for _, m := range w.c.Members {
		pubs = append(pubs, m.PublicAddr)
	}
	w.wl = &Workload{Addrs: pubs, Workers: workers, Keys: keys, Dup: dup, History: w.h}
	wait, err := w.wl.Start(ctx, seed)
	if err != nil {
		w.t.Fatal(err)
	}
	return wait
}

// finalReads reads every key once more after the faults ended; once the
// cluster has recovered every read must succeed.
func (w *world) finalReads() {
	failed, err := w.wl.FinalReads(5 * time.Second)
	if err != nil {
		w.t.Fatal(err)
	}
	if failed > 0 {
		w.t.Errorf("%d of %d final reads failed after recovery", failed, keys)
	}
}

// recover heals the network and restarts every stopped node.
func (w *world) recover() {
	w.inj.Heal()
	for _, m := range w.c.Members {
		if n := m.Node(); n != nil {
			n.SetRaftFilter(nil)
		}
	}
	for i, m := range w.c.Members {
		if !m.Running() {
			w.c.Restart(i)
		}
	}
	nodetest.WaitFor(w.t, 60*time.Second, "every group to elect a leader after the faults", w.c.Settled)
}

// check runs the checker and reports the result.
func (w *world) check(name string) porcupine.CheckResult {
	res, info := w.h.Check(60 * time.Second)
	w.t.Logf("%s: %s: %s; injector %+v", name, w.h.Summary(), res, w.inj.Stats())
	if res != porcupine.Ok && *keep != "" {
		path := filepath.Join(*keep, strings.ReplaceAll(name, "/", "_")+".html")
		if err := os.MkdirAll(*keep, 0o755); err == nil {
			if err := porcupine.VisualizePath(Model.ToModel(), info, path); err == nil {
				w.t.Logf("visualization: %s", path)
			}
		}
	}
	return res
}

// scenario is one row of the failure matrix: a nemesis that injects
// faults until ctx ends.
type scenario struct {
	name    string
	dup     bool // clients duplicate writes
	opts    nodetest.Options
	nemesis func(ctx context.Context, w *world)
}

var scenarios = []scenario{
	{name: "kill-leader", nemesis: func(ctx context.Context, w *world) {
		for ctx.Err() == nil {
			if i := w.leaderOf(); i >= 0 {
				w.c.Stop(i)
				sleep(ctx, 600*time.Millisecond)
				w.c.Restart(i)
			}
			sleep(ctx, 600*time.Millisecond)
		}
	}},
	{name: "kill-minority", nemesis: func(ctx context.Context, w *world) {
		for ctx.Err() == nil {
			i := w.rng.IntN(nodes)
			w.c.Stop(i)
			sleep(ctx, time.Second)
			w.c.Restart(i)
			sleep(ctx, 500*time.Millisecond)
		}
	}},
	{name: "kill-majority", nemesis: func(ctx context.Context, w *world) {
		alive := w.rng.IntN(nodes)
		for i := range nodes {
			if i != alive {
				w.c.Stop(i)
			}
		}
		// Linearizable operations fail; stale reads from the survivor's
		// own replicas still answer.
		kv := w.c.KV(alive)
		for k := range keys {
			cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := kv.Get(cctx, &dkvv1.GetRequest{Key: fmt.Appendf(nil, "k%d", k), Consistency: dkvv1.Consistency_CONSISTENCY_STALE})
			cancel()
			if err != nil {
				w.t.Errorf("stale read with a majority down: %v", err)
			}
		}
		sleep(ctx, time.Second)
		for i := range nodes {
			if i != alive {
				w.c.Restart(i)
			}
		}
		<-ctx.Done()
	}},
	{name: "isolate-leader", nemesis: func(ctx context.Context, w *world) {
		for ctx.Err() == nil {
			if i := w.leaderOf(); i >= 0 {
				w.inj.Isolate(w.c.Members[i].InternalAddr, w.addrs())
				sleep(ctx, time.Second)
				w.inj.Heal()
			}
			sleep(ctx, 400*time.Millisecond)
		}
	}},
	{name: "paused-leader", nemesis: func(ctx context.Context, w *world) {
		// A leader whose clock stops (a long GC pause, a frozen VM) cannot
		// notice it lost its quorum; a new leader takes over elsewhere,
		// and the old one must not answer reads from its stale state.
		for ctx.Err() == nil {
			if i := w.leaderOf(); i >= 0 {
				n := w.c.Members[i].Node()
				n.PauseTicks(true)
				w.inj.Isolate(w.c.Members[i].InternalAddr, w.addrs())
				sleep(ctx, 1500*time.Millisecond)
				n.PauseTicks(false)
				w.inj.Heal()
			}
			sleep(ctx, 400*time.Millisecond)
		}
	}},
	{name: "random-partitions", nemesis: func(ctx context.Context, w *world) {
		for ctx.Err() == nil {
			a, b := fault.Sides(w.rng, w.addrs())
			w.inj.Partition(a, b)
			sleep(ctx, 700*time.Millisecond)
			w.inj.Heal()
			sleep(ctx, 300*time.Millisecond)
		}
	}},
	{name: "flapping-one-way", nemesis: func(ctx context.Context, w *world) {
		addrs := w.addrs()
		for ctx.Err() == nil {
			from, to := addrs[w.rng.IntN(nodes)], addrs[w.rng.IntN(nodes)]
			if from != to {
				w.inj.Block(from, to)
			}
			sleep(ctx, 150*time.Millisecond)
			if w.rng.IntN(3) == 0 {
				w.inj.Heal()
			}
		}
	}},
	{name: "rolling-restart", nemesis: func(ctx context.Context, w *world) {
		for i := 0; ctx.Err() == nil; i = (i + 1) % nodes {
			w.c.Stop(i)
			sleep(ctx, 300*time.Millisecond)
			w.c.Restart(i)
			sleep(ctx, 700*time.Millisecond)
		}
	}},
	{name: "wipe-node", nemesis: func(ctx context.Context, w *world) {
		i := w.rng.IntN(nodes)
		w.c.Stop(i)
		w.c.Wipe(i)
		w.c.Restart(i)
		<-ctx.Done()
	}},
	{name: "slow-network", nemesis: func(ctx context.Context, w *world) {
		w.inj.SetAll(fault.Link{Delay: 2 * time.Millisecond, Jitter: 25 * time.Millisecond})
		<-ctx.Done()
	}},
	{name: "slow-disk", opts: nodetest.Options{WrapFS: func(i int, fs vfs.FS) vfs.FS {
		if i == 0 {
			return vfs.Slow(fs, 30*time.Millisecond)
		}
		return fs
	}}, nemesis: func(ctx context.Context, _ *world) { <-ctx.Done() }},
	{name: "lossy-network", nemesis: func(ctx context.Context, w *world) {
		w.inj.SetAll(fault.Link{Loss: 0.1, Duplicate: 0.1, Jitter: 10 * time.Millisecond})
		<-ctx.Done()
	}},
	{name: "meta-quorum-loss", nemesis: func(ctx context.Context, w *world) {
		isMeta := func(shard int) bool { return shard == meta.Shard }
		for _, m := range w.c.Members {
			m.Node().SetRaftFilter(isMeta)
		}
		<-ctx.Done()
	}},
	{name: "client-retries", dup: true, nemesis: func(ctx context.Context, _ *world) { <-ctx.Done() }},
}

// TestFailureMatrix is the Phase 8 gate check: every scenario of the
// failure matrix, under a concurrent workload, produces a history the
// checker finds linearizable. -lin-runs repeats each scenario and
// -lin-duration sets how long its faults last.
func TestFailureMatrix(t *testing.T) {
	for i, sc := range scenarios {
		for run := range *runs {
			t.Run(fmt.Sprintf("%s/%d", sc.name, run), func(t *testing.T) {
				seed := *baseSeed + uint64(run)*1000 + uint64(i)
				t.Logf("seed %d", seed)
				w := newWorld(t, seed, sc.opts, nil)
				ctx, stopLoad := context.WithCancel(context.Background())
				wait := w.workload(ctx, seed, sc.dup)
				time.Sleep(300 * time.Millisecond)
				nctx, stopFaults := context.WithTimeout(context.Background(), *duration)
				sc.nemesis(nctx, w)
				<-nctx.Done()
				stopFaults()
				w.recover()
				time.Sleep(500 * time.Millisecond)
				stopLoad()
				wait()
				w.finalReads()
				if res := w.check(fmt.Sprintf("%s/%d", sc.name, run)); res != porcupine.Ok {
					t.Fatalf("history is %s", res)
				}
			})
		}
	}
}

// powerLossOfMajority partitions one node away while writes go on, cuts
// the power of the other two (which hold every acknowledged write) while
// it is still cut off, then restarts everything and reads every key.
func powerLossOfMajority(t *testing.T, sync string) porcupine.CheckResult {
	w := newWorld(t, 42, nodetest.Options{MemFS: true}, func(cfg *config.Config) { cfg.Storage.Sync = sync })
	lagging := 2
	ctx, stopLoad := context.WithCancel(context.Background())
	wait := w.workload(ctx, 42, false)
	time.Sleep(500 * time.Millisecond)
	w.inj.Isolate(w.c.Members[lagging].InternalAddr, w.addrs())
	time.Sleep(1500 * time.Millisecond) // writes commit without the lagging node
	stopLoad()
	wait()
	for i := range nodes {
		if i != lagging {
			w.c.PowerLoss(i, nil) // exactly what was made durable survives
		}
	}
	w.inj.Heal()
	for i := range nodes {
		if i != lagging {
			w.c.Restart(i)
		}
	}
	nodetest.WaitFor(t, 60*time.Second, "every group to elect a leader after the power loss", w.c.Settled)
	w.finalReads()
	return w.check("power-loss-of-majority/" + sync)
}

// TestPowerLossOfMajorityLosesNothing: the powered-off majority recovers
// every acknowledged write. With storage.sync interval the engine loses
// its unsynced tail, and the Raft log, still fsynced, replays it.
func TestPowerLossOfMajorityLosesNothing(t *testing.T) {
	for _, sync := range []string{"always", "interval"} {
		t.Run(sync, func(t *testing.T) {
			if res := powerLossOfMajority(t, sync); res != porcupine.Ok {
				t.Fatalf("history is %s", res)
			}
		})
	}
}

// TestNegativeControlFailsTheChecker is the Phase 8 gate's negative
// control: with fsync turned off (storage.sync never, which also skips the
// Raft log's fsyncs), the same power loss destroys acknowledged writes;
// the lagging node then leads without them, and the checker must find the
// history illegal. A checker that accepted it could not catch real bugs.
func TestNegativeControlFailsTheChecker(t *testing.T) {
	if res := powerLossOfMajority(t, "never"); res != porcupine.Illegal {
		t.Fatalf("history is %s; the broken configuration must fail the checker", res)
	}
}
