package linearizability

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
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
	// wiped and wipedID: the member the wipe-node scenario wiped, and its
	// node ID before.
	wiped   int
	wipedID string
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

// evidence is what a run leaves besides its history: what the injector
// did, how far the data shards' terms moved, and how many operations
// completed, with a known outcome, while the faults lasted. A scenario
// checks it to show that its faults took effect and that the cluster
// stayed as available as the failure matrix promises; without it, a
// scenario whose faults did nothing would pass.
type evidence struct {
	stats                   fault.Stats
	termsBefore, termsAfter uint64
	reads, writes           int
}

// termSum adds up, over the data shards, the highest term any running
// member reports. With PreVote, a term grows only when a real election
// starts (or a leadership transfer, which is one).
func (w *world) termSum() uint64 {
	terms := map[int32]uint64{}
	for i, m := range w.c.Members {
		if !m.Running() {
			continue
		}
		st, err := w.c.NodeStatus(i)
		if err != nil {
			continue
		}
		for _, r := range st.GetReplicas() {
			if r.GetShard() >= 0 {
				terms[r.GetShard()] = max(terms[r.GetShard()], r.GetTerm())
			}
		}
	}
	var sum uint64
	for _, t := range terms {
		sum += t
	}
	return sum
}

// ledBy returns the data shards member i leads, with their terms.
func (w *world) ledBy(i int) map[int32]uint64 {
	out := map[int32]uint64{}
	if st, err := w.c.NodeStatus(i); err == nil {
		for _, r := range st.GetReplicas() {
			if r.GetShard() >= 0 && r.GetRole() == "leader" {
				out[r.GetShard()] = r.GetTerm()
			}
		}
	}
	return out
}

func requireElections(t *testing.T, ev evidence) {
	t.Helper()
	if ev.termsAfter <= ev.termsBefore {
		t.Errorf("no data shard's term moved (summed %d -> %d): the faults forced no election", ev.termsBefore, ev.termsAfter)
	}
}

// requireServed checks that at least minReads reads and minWrites writes
// completed with a known outcome while the faults lasted.
func requireServed(t *testing.T, ev evidence, minReads, minWrites int) {
	t.Helper()
	if ev.reads < minReads || ev.writes < minWrites {
		t.Errorf("during the faults %d reads and %d writes completed; want at least %d and %d", ev.reads, ev.writes, minReads, minWrites)
	}
}

// slowSyncs counts the syncs that reach node 0's slow filesystem in the
// slow-disk scenario.
var slowSyncs atomic.Int64

// scenario is one row of the failure matrix: a nemesis that injects
// faults until ctx ends, and what must hold afterwards besides
// linearizability.
type scenario struct {
	name      string
	dup       bool // clients duplicate writes
	opts      nodetest.Options
	configure func(*config.Config)
	nemesis   func(ctx context.Context, w *world)
	expect    func(t *testing.T, w *world, ev evidence)
}

// minServed is the availability floor for the rows where reads and writes
// continue: far below what a 3-second run completes even under the race
// detector (77 reads at the least without it, 9 in one flapping run with
// it; docs/PROGRESS.md, Phase 8 follow-up), but enough to fail a run in
// which the faults stopped everything.
const minServed = 10

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
	}, expect: func(t *testing.T, _ *world, ev evidence) {
		requireElections(t, ev)
		requireServed(t, ev, 1, 1)
	}},
	{name: "kill-minority", nemesis: func(ctx context.Context, w *world) {
		for ctx.Err() == nil {
			i := w.rng.IntN(nodes)
			w.c.Stop(i)
			sleep(ctx, time.Second)
			w.c.Restart(i)
			sleep(ctx, 500*time.Millisecond)
		}
	}, expect: func(t *testing.T, _ *world, ev evidence) { requireServed(t, ev, minServed, minServed) }},
	{name: "kill-majority", nemesis: func(ctx context.Context, w *world) {
		alive := w.rng.IntN(nodes)
		for i := range nodes {
			if i != alive {
				w.c.Stop(i)
			}
		}
		kv := w.c.KV(alive)
		// Linearizable reads and writes fail, within their deadline.
		const deadline = time.Second
		for _, op := range []string{"get", "put"} {
			cctx, cancel := context.WithTimeout(context.Background(), deadline)
			start := time.Now()
			var err error
			if op == "get" {
				_, err = kv.Get(cctx, &dkvv1.GetRequest{Key: []byte("k0")})
			} else {
				_, err = kv.Put(cctx, &dkvv1.PutRequest{Key: []byte("k0"), Value: []byte("x")})
			}
			cancel()
			if took := time.Since(start); err == nil || took > deadline+500*time.Millisecond {
				w.t.Errorf("linearizable %s with a majority down: error %v after %v; want an error within %v", op, err, took, deadline)
			}
		}
		// Stale reads from the survivor's own replicas still answer.
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
				if sleep(ctx, time.Second) {
					// Cut off from its quorum, it steps down (CheckQuorum).
					if led := w.ledBy(i); len(led) > 0 {
						w.t.Errorf("node %d still leads shards %v after 1 s cut off from its quorum", i, led)
					}
				}
				w.inj.Heal()
			}
			sleep(ctx, 400*time.Millisecond)
		}
	}, expect: func(t *testing.T, _ *world, ev evidence) {
		requireElections(t, ev)
		requireServed(t, ev, 1, 1)
	}},
	{name: "paused-leader", nemesis: func(ctx context.Context, w *world) {
		// A leader whose clock stops (a long GC pause, a frozen VM) cannot
		// notice it lost its quorum; a new leader takes over elsewhere,
		// and the old one must not answer reads from its stale state.
		for ctx.Err() == nil {
			if i := w.leaderOf(); i >= 0 {
				led := w.ledBy(i)
				n := w.c.Members[i].Node()
				n.PauseTicks(true)
				w.inj.Isolate(w.c.Members[i].InternalAddr, w.addrs())
				if sleep(ctx, 1500*time.Millisecond) {
					groups := w.c.Groups()
					for shard, term := range led {
						if g := groups[int(shard)]; g == nil || g.GetTerm() <= term {
							w.t.Errorf("shard %d, led by paused node %d at term %d, has no newer leader after 1.5 s: %v", shard, i, term, g)
						}
					}
				}
				n.PauseTicks(false)
				w.inj.Heal()
			}
			sleep(ctx, 400*time.Millisecond)
		}
	}, expect: func(t *testing.T, _ *world, ev evidence) { requireElections(t, ev) }},
	{name: "random-partitions", nemesis: func(ctx context.Context, w *world) {
		for ctx.Err() == nil {
			a, b := fault.Sides(w.rng, w.addrs())
			w.inj.Partition(a, b)
			sleep(ctx, 700*time.Millisecond)
			w.inj.Heal()
			sleep(ctx, 300*time.Millisecond)
		}
	}, expect: func(t *testing.T, _ *world, ev evidence) {
		if ev.stats.Blocked == 0 {
			t.Error("the partitions blocked no call")
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
	}, expect: func(t *testing.T, _ *world, ev evidence) {
		if ev.stats.Blocked == 0 {
			t.Error("the one-way blocks blocked no call")
		}
		// PreVote keeps terms from running away: see maxFlappingElections.
		if n := ev.termsAfter - ev.termsBefore; n > maxFlappingElections(*duration) {
			t.Errorf("one-way partitions raised the summed shard terms by %d in %v; want at most %d", n, *duration, maxFlappingElections(*duration))
		}
	}},
	{name: "rolling-restart", nemesis: func(ctx context.Context, w *world) {
		for i := 0; ctx.Err() == nil; i = (i + 1) % nodes {
			w.c.Stop(i)
			sleep(ctx, 300*time.Millisecond)
			w.c.Restart(i)
			sleep(ctx, 700*time.Millisecond)
		}
	}, expect: func(t *testing.T, w *world, ev evidence) {
		requireElections(t, ev)
		// Every restarted node replays its log or snapshot and catches up.
		nodetest.WaitFor(t, 30*time.Second, "every replica to apply its leader's commit index", w.caughtUp)
	}},
	{name: "wipe-node", configure: func(cfg *config.Config) { cfg.Raft.CompactEntries = 20 }, nemesis: func(ctx context.Context, w *world) {
		// Wipe only once every data shard has compacted its log, so the
		// new node can catch up through snapshots alone.
		for !w.compacted() {
			if !sleep(ctx, 50*time.Millisecond) {
				w.t.Error("the data shards' logs were not compacted before the faults ended")
				return
			}
		}
		i := w.rng.IntN(nodes)
		w.wiped, w.wipedID = i, w.c.Members[i].NodeID()
		w.c.Stop(i)
		w.c.Wipe(i)
		w.c.Restart(i)
		<-ctx.Done()
	}, expect: func(t *testing.T, w *world, _ evidence) {
		// It rejoins as a new node and receives snapshots.
		id := w.c.Members[w.wiped].NodeID()
		if id == w.wipedID {
			t.Fatalf("the wiped node came back with its old ID %s", id)
		}
		for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(100 * time.Millisecond) {
			voter, snaps := w.votesEverywhere(id), w.snapshotsInstalled(w.wiped)
			if voter && snaps > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("60 s after recovery the wiped node (now %s) is a voter of every data shard: %v; snapshots it installed: %d", id, voter, snaps)
			}
		}
	}},
	{name: "slow-network", nemesis: func(ctx context.Context, w *world) {
		w.inj.SetAll(fault.Link{Delay: 2 * time.Millisecond, Jitter: 25 * time.Millisecond})
		<-ctx.Done()
	}, expect: func(t *testing.T, _ *world, ev evidence) {
		if ev.stats.Delayed == 0 {
			t.Error("no call was delayed")
		}
		requireServed(t, ev, minServed, minServed)
	}},
	{name: "slow-disk", opts: nodetest.Options{WrapFS: func(i int, fs vfs.FS) vfs.FS {
		if i == 0 {
			return vfs.Slow(vfs.CountSyncs(fs, &slowSyncs), 30*time.Millisecond)
		}
		return fs
	}}, nemesis: func(ctx context.Context, _ *world) { <-ctx.Done() }, expect: func(t *testing.T, _ *world, ev evidence) {
		if n := slowSyncs.Swap(0); n == 0 {
			t.Error("no sync reached node 0's slow filesystem")
		}
		requireServed(t, ev, minServed, minServed)
	}},
	{name: "lossy-network", nemesis: func(ctx context.Context, w *world) {
		w.inj.SetAll(fault.Link{Loss: 0.1, Duplicate: 0.1, Jitter: 10 * time.Millisecond})
		<-ctx.Done()
	}, expect: func(t *testing.T, _ *world, ev evidence) {
		if ev.stats.Lost == 0 || ev.stats.Duplicated == 0 {
			t.Errorf("injector lost %d and duplicated %d calls; want both above zero", ev.stats.Lost, ev.stats.Duplicated)
		}
		requireServed(t, ev, minServed, minServed)
	}},
	{name: "meta-quorum-loss", nemesis: func(ctx context.Context, w *world) {
		isMeta := func(shard int) bool { return shard == meta.Shard }
		for _, m := range w.c.Members {
			m.Node().SetRaftFilter(isMeta)
		}
		// The meta group loses its leader: its leader steps down once it
		// hears from no quorum, and no election can succeed.
		deadline := time.Now().Add(2 * time.Second)
		for w.c.Groups()[meta.Shard] != nil {
			if time.Now().After(deadline) {
				w.t.Errorf("the meta group still has a leader 2 s after its traffic was cut")
				break
			}
			if !sleep(ctx, 50*time.Millisecond) {
				break
			}
		}
		<-ctx.Done()
	}, expect: func(t *testing.T, _ *world, ev evidence) { requireServed(t, ev, minServed, minServed) }},
	{name: "client-retries", dup: true, nemesis: func(ctx context.Context, _ *world) { <-ctx.Done() }, expect: func(t *testing.T, w *world, ev evidence) {
		if w.wl.Duplicated.Load() == 0 {
			t.Error("no write was sent twice")
		}
		requireServed(t, ev, minServed, minServed)
	}},
}

// maxFlappingElections bounds how far one-way partitions may raise the
// summed terms of the data shards in d: 1.5 per shard per second. With
// PreVote a node that stops hearing its leader fails its pre-vote and
// keeps its term, so terms move only when a leader really loses its
// quorum. Measured over 3 s on 4 shards (docs/PROGRESS.md, Phase 8
// follow-up): growth 1 to 11 with PreVote (0 to 10 under the race
// detector), 21 to 34 with PreVote turned off in internal/replica; the
// bound is 18.
func maxFlappingElections(d time.Duration) uint64 {
	return uint64(1.5 * shards * d.Seconds())
}

// caughtUp reports whether every running replica of every data shard has
// applied its leader's commit index.
func (w *world) caughtUp() bool {
	groups := w.c.Groups()
	for i, m := range w.c.Members {
		if !m.Running() {
			continue
		}
		st, err := w.c.NodeStatus(i)
		if err != nil {
			return false
		}
		for _, r := range st.GetReplicas() {
			g := groups[int(r.GetShard())]
			if r.GetShard() >= 0 && (g == nil || r.GetApplied() < g.GetCommit()) {
				return false
			}
		}
	}
	return true
}

// compacted reports whether every data shard's leader has compacted its
// log, so a member starting from nothing needs a snapshot.
func (w *world) compacted() bool {
	groups := w.c.Groups()
	for shard := range shards {
		if g := groups[shard]; g == nil || g.GetFirstIndex() <= 1 {
			return false
		}
	}
	return true
}

// votesEverywhere reports whether node id is a voter of every data shard.
func (w *world) votesEverywhere(id string) bool {
	groups := w.c.Groups()
	for shard := range shards {
		if g := groups[shard]; g == nil || !slices.Contains(g.GetVoters(), id) {
			return false
		}
	}
	return true
}

// snapshotsInstalled sums the snapshots member i's replicas installed.
func (w *world) snapshotsInstalled(i int) int64 {
	st, err := w.c.NodeStatus(i)
	if err != nil {
		return 0
	}
	var n int64
	for _, r := range st.GetReplicas() {
		n += r.GetSnapshotsInstalled()
	}
	return n
}

// TestFailureMatrix is the Phase 8 gate check: every scenario of the
// failure matrix, under a concurrent workload, produces a history the
// checker finds linearizable, and its faults visibly took effect.
// -lin-runs repeats each scenario and -lin-duration sets how long its
// faults last.
func TestFailureMatrix(t *testing.T) {
	for i, sc := range scenarios {
		for run := range *runs {
			t.Run(fmt.Sprintf("%s/%d", sc.name, run), func(t *testing.T) {
				seed := *baseSeed + uint64(run)*1000 + uint64(i)
				t.Logf("seed %d", seed)
				w := newWorld(t, seed, sc.opts, sc.configure)
				ctx, stopLoad := context.WithCancel(context.Background())
				wait := w.workload(ctx, seed, sc.dup)
				time.Sleep(300 * time.Millisecond)
				var ev evidence
				ev.termsBefore = w.termSum()
				from := w.h.Mark()
				nctx, stopFaults := context.WithTimeout(context.Background(), *duration)
				sc.nemesis(nctx, w)
				<-nctx.Done()
				stopFaults()
				ev.reads, ev.writes = w.h.KnownBetween(from, w.h.Mark())
				ev.stats = w.inj.Stats()
				w.recover()
				time.Sleep(500 * time.Millisecond)
				stopLoad()
				wait()
				w.finalReads()
				ev.termsAfter = w.termSum()
				t.Logf("during the faults: %d reads, %d writes with known outcomes; summed shard terms %d -> %d",
					ev.reads, ev.writes, ev.termsBefore, ev.termsAfter)
				if res := w.check(fmt.Sprintf("%s/%d", sc.name, run)); res != porcupine.Ok {
					t.Fatalf("history is %s", res)
				}
				if sc.expect != nil {
					sc.expect(t, w, ev)
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
// fsync of every Raft log write), the same power loss destroys acknowledged writes;
// the lagging node then leads without them, and the checker must find the
// history illegal. A checker that accepted it could not catch real bugs.
func TestNegativeControlFailsTheChecker(t *testing.T) {
	if res := powerLossOfMajority(t, "never"); res != porcupine.Illegal {
		t.Fatalf("history is %s; the broken configuration must fail the checker", res)
	}
}
