// Package chaos runs the failure matrix against a dkv cluster in Docker
// Compose: real processes killed with SIGKILL, stopped with SIGTERM or
// frozen with SIGSTOP, wiped volumes, partitions made with iptables, and
// latency, loss and duplication from tc netem. The clients and the
// linearizability check are those of test/linearizability. It needs
// Docker and runs only with -chaos (make chaos); see ADR 0025.
package chaos

import (
	"context"
	"errors"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/pkg/client"
	lin "github.com/naveenk2608/distributed-database/test/linearizability"
)

var (
	enabled  = flag.Bool("chaos", false, "run the Docker chaos scenarios (needs docker compose)")
	files    = flag.String("chaos-compose", "../../deploy/compose/docker-compose.yml,../../deploy/compose/chaos.yml", "comma-separated Compose files")
	project  = flag.String("chaos-project", "dkv-chaos", "Compose project name")
	runs     = flag.Int("chaos-runs", 1, "runs of every scenario")
	duration = flag.Duration("chaos-duration", 10*time.Second, "how long each scenario's faults last")
	only     = flag.String("chaos-only", "", "run only the scenarios whose name contains this")
	outDir   = flag.String("chaos-out", "", "directory for visualizations of failing histories")
	baseSeed = flag.Uint64("chaos-seed", 1, "seed of the first run")
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const (
	keys    = 8
	workers = 4
)

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// world is one scenario run: a cluster, a random source for the nemesis
// and the clients' history.
type world struct {
	t   *testing.T
	c   *cluster
	rng *rand.Rand
}

// randomLeader returns the leader of a random shard, or "".
func (w *world) randomLeader() string {
	n := w.c.shardCount()
	if n == 0 {
		return ""
	}
	return w.c.leaderOf(uint32(w.rng.IntN(int(n))))
}

func (w *world) randomNode() string { return w.c.services[w.rng.IntN(len(w.c.services))] }

// waitReady waits until every node answers a linearizable read of every
// workload key.
func (w *world) waitReady(timeout time.Duration) {
	w.t.Helper()
	deadline := time.Now().Add(timeout)
	for _, addr := range w.c.addrs() {
		cl, err := client.New(addr)
		if err != nil {
			w.t.Fatal(err)
		}
		for k := 0; k < keys; {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _, err := cl.Get(ctx, fmt.Appendf(nil, "k%d", k))
			cancel()
			switch {
			case err == nil || errors.Is(err, client.ErrNotFound):
				k++
			case time.Now().After(deadline):
				_ = cl.Close()
				w.t.Fatalf("%s cannot read k%d after %v: %v", addr, k, timeout, err)
			default:
				time.Sleep(200 * time.Millisecond)
			}
		}
		_ = cl.Close()
	}
}

type scenario struct {
	name    string
	dup     bool
	nemesis func(ctx context.Context, w *world)
}

var scenarios = []scenario{
	{name: "kill-leader", nemesis: func(ctx context.Context, w *world) {
		for ctx.Err() == nil {
			if s := w.randomLeader(); s != "" {
				w.c.kill(s)
				sleep(ctx, 2*time.Second)
				w.c.start(s)
			}
			sleep(ctx, 2*time.Second)
		}
	}},
	{name: "kill-minority", nemesis: func(ctx context.Context, w *world) {
		for ctx.Err() == nil {
			s := w.randomNode()
			w.c.kill(s)
			sleep(ctx, 3*time.Second)
			w.c.start(s)
			sleep(ctx, 2*time.Second)
		}
	}},
	{name: "kill-majority", nemesis: func(ctx context.Context, w *world) {
		alive := w.randomNode()
		for _, s := range w.c.services {
			if s != alive {
				w.c.kill(s)
			}
		}
		// Stale reads from the survivor's own replicas still answer.
		conn, err := grpc.NewClient(w.c.public[alive], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			w.t.Fatal(err)
		}
		kv := dkvv1.NewKVServiceClient(conn)
		for k := range keys {
			cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := kv.Get(cctx, &dkvv1.GetRequest{Key: fmt.Appendf(nil, "k%d", k), Consistency: dkvv1.Consistency_CONSISTENCY_STALE})
			cancel()
			if err != nil {
				w.t.Errorf("stale read from %s with a majority down: %v", alive, err)
			}
		}
		_ = conn.Close()
		sleep(ctx, 3*time.Second)
		for _, s := range w.c.services {
			if s != alive {
				w.c.start(s)
			}
		}
		<-ctx.Done()
	}},
	{name: "freeze-leader", nemesis: func(ctx context.Context, w *world) {
		// SIGSTOP: the leader's clock and network stop together; when it
		// resumes it must not answer from its stale state.
		for ctx.Err() == nil {
			if s := w.randomLeader(); s != "" {
				w.c.freeze(s, true)
				sleep(ctx, 3*time.Second)
				w.c.freeze(s, false)
			}
			sleep(ctx, 2*time.Second)
		}
	}},
	{name: "isolate-leader", nemesis: func(ctx context.Context, w *world) {
		for ctx.Err() == nil {
			if s := w.randomLeader(); s != "" {
				var rest []string
				for _, o := range w.c.services {
					if o != s {
						rest = append(rest, o)
					}
				}
				w.c.partition([]string{s}, rest)
				sleep(ctx, 3*time.Second)
				w.c.heal()
			}
			sleep(ctx, time.Second)
		}
	}},
	{name: "random-partitions", nemesis: func(ctx context.Context, w *world) {
		for ctx.Err() == nil {
			nodes := append([]string(nil), w.c.services...)
			w.rng.Shuffle(len(nodes), func(a, b int) { nodes[a], nodes[b] = nodes[b], nodes[a] })
			k := 1 + w.rng.IntN(len(nodes)-1)
			w.c.partition(nodes[:k], nodes[k:])
			sleep(ctx, 2*time.Second)
			w.c.heal()
			sleep(ctx, time.Second)
		}
	}},
	{name: "flapping-one-way", nemesis: func(ctx context.Context, w *world) {
		for ctx.Err() == nil {
			from, to := w.randomNode(), w.randomNode()
			if from != to {
				w.c.block(from, to)
			}
			sleep(ctx, 500*time.Millisecond)
			if w.rng.IntN(3) == 0 {
				w.c.heal()
			}
		}
	}},
	{name: "rolling-restart", nemesis: func(ctx context.Context, w *world) {
		for i := 0; ctx.Err() == nil; i = (i + 1) % len(w.c.services) {
			s := w.c.services[i]
			w.c.stop(s)
			w.c.start(s)
			sleep(ctx, 3*time.Second)
		}
	}},
	{name: "wipe-node", nemesis: func(ctx context.Context, w *world) {
		s := w.randomNode()
		w.c.kill(s)
		w.c.wipe(s)
		w.c.start(s)
		<-ctx.Done()
	}},
	{name: "slow-network", nemesis: func(ctx context.Context, w *world) {
		w.c.netem("delay 20ms 15ms")
		<-ctx.Done()
	}},
	{name: "lossy-network", nemesis: func(ctx context.Context, w *world) {
		w.c.netem("delay 5ms 5ms loss 10% duplicate 5% reorder 25%")
		<-ctx.Done()
	}},
	{name: "client-retries", dup: true, nemesis: func(ctx context.Context, _ *world) { <-ctx.Done() }},
}

// TestChaosFailureMatrix runs every scenario against a fresh Compose
// cluster and checks the clients' history for linearizability.
func TestChaosFailureMatrix(t *testing.T) {
	if !*enabled {
		t.Skip("needs Docker; run with -chaos (make chaos)")
	}
	built := false
	for i, sc := range scenarios {
		if *only != "" && !strings.Contains(sc.name, *only) {
			continue
		}
		for r := range *runs {
			name := fmt.Sprintf("%s/%d", sc.name, r)
			t.Run(name, func(t *testing.T) {
				seed := *baseSeed + uint64(r)*1000 + uint64(i)
				t.Logf("seed %d", seed)
				c := up(t, strings.Split(*files, ","), *project, !built)
				built = true
				w := &world{t: t, c: c, rng: rand.New(rand.NewPCG(seed, 11))}
				w.waitReady(2 * time.Minute)

				h := lin.NewHistory()
				wl := &lin.Workload{Addrs: c.addrs(), Workers: workers, Keys: keys, Dup: sc.dup, OpTimeout: 5 * time.Second, History: h}
				ctx, stopLoad := context.WithCancel(context.Background())
				wait, err := wl.Start(ctx, seed)
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				nctx, stopFaults := context.WithTimeout(context.Background(), *duration)
				sc.nemesis(nctx, w)
				<-nctx.Done()
				stopFaults()
				c.recover()
				w.waitReady(2 * time.Minute)
				time.Sleep(time.Second)
				stopLoad()
				wait()
				if failed, err := wl.FinalReads(10 * time.Second); err != nil || failed > 0 {
					t.Errorf("final reads: %d failed, %v", failed, err)
				}

				res, info := h.Check(5 * time.Minute)
				t.Logf("%s: %s: %s", name, h.Summary(), res)
				if res != porcupine.Ok {
					if *outDir != "" {
						path := filepath.Join(*outDir, "chaos-"+strings.ReplaceAll(name, "/", "_")+".html")
						if err := os.MkdirAll(*outDir, 0o755); err == nil && porcupine.VisualizePath(lin.Model.ToModel(), info, path) == nil {
							t.Logf("visualization: %s", path)
						}
					}
					t.Fatalf("history is %s", res)
				}
			})
		}
	}
}
