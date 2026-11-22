// Package dashboard holds the Phase 9 gate tests: a real in-process
// cluster watched by a real dkv-console, checked through the console's own
// API, which is all the dashboard sees.
package dashboard

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/protobuf/encoding/protojson"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/console"
	"github.com/naveenk2608/distributed-database/internal/node/nodetest"
	"github.com/naveenk2608/distributed-database/pkg/client"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const (
	pollInterval = 250 * time.Millisecond
	pollTimeout  = 200 * time.Millisecond
)

// startConsole runs a console polling c's nodes; stop ends it and waits.
func startConsole(t *testing.T, c *nodetest.Cluster) (base string, stop func()) {
	t.Helper()
	cfg := config.DefaultConsole().Console
	cfg.PollInterval, cfg.PollTimeout = pollInterval, pollTimeout
	for _, m := range c.Members {
		cfg.Nodes = append(cfg.Nodes, m.AdminAddr)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- console.New(cfg, nil, slog.New(slog.DiscardHandler)).Run(ctx, lis) }()
	once := sync.OnceFunc(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("console: %v", err)
		}
	})
	t.Cleanup(once)
	return "http://" + lis.Addr().String(), once
}

func getJSON[T any](t *testing.T, url string) T {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx // a test against a local server
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var v T
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return v
}

func nodeStats(t *testing.T, adminAddr string) *dkvv1.StatsResponse {
	t.Helper()
	resp, err := http.Get("http://" + adminAddr + "/v1/stats") //nolint:noctx // a test against a local server
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	st := &dkvv1.StatsResponse{}
	if err := protojson.Unmarshal(b, st); err != nil {
		t.Fatal(err)
	}
	return st
}

// writer puts keys through every node until stopped, counting outcomes and
// keeping the slowest write, to explain a stall.
type writer struct {
	ok, failed atomic.Int64
	stop       func()

	mu        sync.Mutex
	slowest   time.Duration
	slowestAt time.Time // when the slowest write started
	lastErr   error
}

// slowestWrite describes the slowest write so far, relative to t0.
func (w *writer) slowestWrite(t0 time.Time) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return fmt.Sprintf("slowest write %v, started %v after the console stopped; last error %v",
		w.slowest.Round(time.Millisecond), w.slowestAt.Sub(t0).Round(time.Millisecond), w.lastErr)
}

func startWriter(t *testing.T, c *nodetest.Cluster) *writer {
	t.Helper()
	var addrs []string
	for _, m := range c.Members {
		addrs = append(addrs, m.PublicAddr)
	}
	cl, err := client.New(addrs...)
	if err != nil {
		t.Fatal(err)
	}
	w := &writer{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; ctx.Err() == nil; i++ {
			octx, ocancel := context.WithTimeout(ctx, 2*time.Second)
			began := time.Now()
			_, err := cl.Put(octx, fmt.Appendf(nil, "key%04d", i%500), []byte("value"))
			took := time.Since(began)
			ocancel()
			w.mu.Lock()
			if took > w.slowest {
				w.slowest, w.slowestAt = took, began
			}
			if err != nil && ctx.Err() == nil {
				w.lastErr = err
			}
			w.mu.Unlock()
			switch {
			case err == nil:
				w.ok.Add(1)
			case ctx.Err() == nil:
				w.failed.Add(1)
			}
		}
	})
	w.stop = sync.OnceFunc(func() { cancel(); wg.Wait(); _ = cl.Close() })
	t.Cleanup(w.stop)
	return w
}

// TestDashboardGate is the Phase 9 gate, in process:
//
//  1. every number the console shows for a node is that node's own
//     metric, and a node that does not answer shows none;
//  2. stopping the console has no effect on the database;
//  3. a killed node shows as down within one poll interval (plus the
//     poll's timeout), on the stream the dashboard follows;
//  4. the election that replaces it, the meta leader's verdict that it is
//     dead, and the re-replication of its replicas appear in that order.
func TestDashboardGate(t *testing.T) {
	c := nodetest.StartWith(t, 4, nodetest.Options{Admin: true}, func(_ int, cfg *config.Config) {
		cfg.Cluster.Shards = 4
		cfg.Liveness.DeadAfter = 2 * time.Second
	})
	nodetest.WaitFor(t, 30*time.Second, "every group to elect a leader", c.Settled)
	base, stopConsole := startConsole(t, c)
	nodetest.WaitFor(t, 30*time.Second, "the console to see every node up and every shard led", func() bool {
		ov := getJSON[console.Overview](t, base+"/api/overview")
		up := 0
		for _, n := range ov.Nodes {
			if n.Up {
				up++
			}
		}
		led := 0
		for _, s := range ov.Shards {
			if s.Leader != "" {
				led++
			}
		}
		return up == 4 && led == 5 // 4 data shards and the meta group
	})

	t.Run("numbers come from the nodes", func(t *testing.T) {
		// With no traffic the nodes' numbers hold still; once two direct
		// reads of every node agree, the console's next round must show
		// exactly them.
		nodetest.WaitFor(t, 30*time.Second, "the console to show what the nodes report", func() bool {
			direct := map[string]*dkvv1.StatsResponse{}
			for _, m := range c.Members {
				direct[m.AdminAddr] = nodeStats(t, m.AdminAddr)
			}
			time.Sleep(2 * pollInterval)
			ov := getJSON[console.Overview](t, base+"/api/overview")
			for _, m := range c.Members {
				if !sameReplicas(direct[m.AdminAddr], nodeStats(t, m.AdminAddr)) {
					return false // still changing (leadership balancing, compactions)
				}
			}
			for _, n := range ov.Nodes {
				st := direct[n.Addr]
				if n.NodeID != st.GetNodeId() || !n.StartedAt.Equal(time.Unix(0, st.GetStartedUnixNano())) {
					t.Fatalf("node %s shown as %s started %v; it reports %s started %v", n.Addr, n.NodeID, n.StartedAt,
						st.GetNodeId(), time.Unix(0, st.GetStartedUnixNano()))
				}
				if !matches(n, st) {
					return false
				}
			}
			return true
		})
	})

	t.Run("stopping the console changes nothing", func(t *testing.T) {
		// Writes go on, none failing (each has 2 s), while the console runs
		// and after it stops. They are counted rather than timed: on a
		// starved machine one write can take over a second with no retry
		// and no leader change, which says nothing about the console.
		const writes = 10
		w := startWriter(t, c)
		nodetest.WaitFor(t, 30*time.Second, "writes while the console runs", func() bool { return w.ok.Load() >= writes })
		before := w.ok.Load()
		stopConsole()
		stopped := time.Now()
		nodetest.WaitFor(t, 30*time.Second, "writes after the console stopped", func() bool { return w.ok.Load()-before >= writes })
		during := w.ok.Load() - before
		w.stop()
		if f := w.failed.Load(); f != 0 {
			t.Fatalf("writes: %d before the console stopped, %d after, %d failed; want no failure (%s)", before, during, f, w.slowestWrite(stopped))
		}
		t.Logf("writes: %d before the console stopped, %d after (%s)", before, during, w.slowestWrite(stopped))
	})

	base, _ = startConsole(t, c) // a new console, as after a restart
	t.Run("a killed node shows down within a poll interval, then the events in order", func(t *testing.T) {
		nodetest.WaitFor(t, 30*time.Second, "the new console to see every node", func() bool {
			ov := getJSON[console.Overview](t, base+"/api/overview")
			return len(ov.Nodes) == 4 && !slices.ContainsFunc(ov.Nodes, func(n console.NodeView) bool { return !n.Up })
		})
		stream := follow(t, base+"/api/stream")
		victim, _ := c.Leader(0)
		victimID := c.Members[victim].NodeID()
		// The data shards the victim leads, as the nodes report them just
		// before the kill. (The console's overview can be a poll behind: a
		// leadership it still showed on the victim, or one it did not show
		// yet, left the test waiting for an election that was not coming.)
		led := map[int]bool{}
		for s := range 4 {
			if l, _ := c.Leader(s); l == victim {
				led[s] = true
			}
		}
		killed := time.Now()
		c.Stop(victim)
		stopped := time.Since(killed)
		var shownDown time.Duration
		for ov := range stream {
			if i := slices.IndexFunc(ov.Nodes, func(n console.NodeView) bool { return n.NodeID == victimID }); i >= 0 && !ov.Nodes[i].Up {
				shownDown = time.Since(killed)
				if ov.Nodes[i].Rates != (console.Rates{}) || len(ov.Nodes[i].Replicas) != 0 || ov.Nodes[i].Storage != (console.StorageView{}) {
					t.Fatalf("the down node still shows numbers: %+v", ov.Nodes[i])
				}
				break
			}
			if time.Since(killed) > 10*time.Second {
				t.Fatal("the killed node never showed as down")
			}
		}
		limit := pollInterval + pollTimeout + 250*time.Millisecond // + scheduling slack
		if shownDown > limit {
			t.Fatalf("the killed node showed as down after %v (it took %v to stop); want within one poll interval (%v) and the poll timeout (%v)",
				shownDown, stopped, pollInterval, pollTimeout)
		}
		t.Logf("node %s killed (stopped in %v); shown as down on the stream %v later", victimID,
			stopped.Round(time.Millisecond), shownDown.Round(time.Millisecond))

		var elected, dead, replicated time.Time
		defer func() {
			if t.Failed() {
				t.Logf("after the kill of %s (leader of data shards %v): election %v, dead verdict %v, re-replication %v (zero: not seen)",
					victimID, led, elected, dead, replicated)
			}
		}()
		nodetest.WaitFor(t, 60*time.Second, "the election, the dead verdict and the re-replication", func() bool {
			evs := getJSON[[]console.EventView](t, base+"/api/events?limit=5000")
			elected, dead, replicated = time.Time{}, time.Time{}, time.Time{}
			for i, e := range evs {
				if i > 0 && e.Time.Before(evs[i-1].Time) {
					t.Fatalf("events out of time order at %d: %+v", i, evs[i-1:i+1])
				}
				if e.Time.Before(killed) {
					continue
				}
				switch {
				case e.Kind == "leader_elected" && e.Shard != nil && led[*e.Shard] && e.Reporter != victimID && elected.IsZero():
					elected = e.Time
				case e.Kind == "node_dead" && e.Node == victimID && dead.IsZero():
					dead = e.Time
				case e.Kind == "replica_added" && !dead.IsZero() && replicated.IsZero():
					replicated = e.Time
				}
			}
			return !elected.IsZero() && !dead.IsZero() && !replicated.IsZero()
		})
		if !elected.Before(dead) || !dead.Before(replicated) {
			t.Fatalf("event order: elected %v, node_dead %v, replica_added %v; want in that order", elected, dead, replicated)
		}
		t.Logf("after the kill: new leader %v, declared dead %v, first replica added %v",
			elected.Sub(killed).Round(time.Millisecond), dead.Sub(killed).Round(time.Millisecond), replicated.Sub(killed).Round(time.Millisecond))
		writeFixtures(t, base, c.Members[(victim+1)%len(c.Members)].AdminAddr)
	})
}

// writeFixtures saves what the console serves after the kill, with
// DKV_DASHBOARD_FIXTURE=dir, for the dashboard's own tests (web/src/fixtures).
func writeFixtures(t *testing.T, base, liveNode string) {
	dir := os.Getenv("DKV_DASHBOARD_FIXTURE")
	if dir == "" {
		return
	}
	for name, path := range map[string]string{
		"overview.json": "/api/overview",
		"events.json":   "/api/events?limit=5000",
		"history.json":  "/api/history?addr=" + liveNode,
	} {
		resp, err := http.Get(base + path) //nolint:noctx // a test against a local server
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("fixtures written to %s", dir)
}

// sameReplicas reports whether two statistics snapshots of one node agree
// on every replica's Raft state and engine.
func sameReplicas(a, b *dkvv1.StatsResponse) bool {
	if len(a.GetReplicas()) != len(b.GetReplicas()) {
		return false
	}
	for i := range a.GetReplicas() {
		x, y := a.GetReplicas()[i], b.GetReplicas()[i]
		if x.GetRaft().GetTerm() != y.GetRaft().GetTerm() || x.GetRaft().GetRole() != y.GetRaft().GetRole() ||
			x.GetRaft().GetCommit() != y.GetRaft().GetCommit() || x.GetRaft().GetApplied() != y.GetRaft().GetApplied() ||
			x.GetRaft().GetLastIndex() != y.GetRaft().GetLastIndex() || x.GetEngine().GetTableBytes() != y.GetEngine().GetTableBytes() ||
			x.GetEngine().GetMemtableBytes() != y.GetEngine().GetMemtableBytes() || x.GetEngine().GetTables() != y.GetEngine().GetTables() {
			return false
		}
	}
	return true
}

// matches reports whether the console's view of a node equals the node's
// own statistics.
func matches(n console.NodeView, st *dkvv1.StatsResponse) bool {
	if len(n.Replicas) != len(st.GetReplicas()) {
		return false
	}
	var tableBytes uint64
	var memBytes int64
	leaders := 0
	for i, r := range st.GetReplicas() {
		rs, e, v := r.GetRaft(), r.GetEngine(), n.Replicas[i]
		if v.Shard != int(rs.GetShard()) || v.Role != rs.GetRole() || v.Term != rs.GetTerm() || v.Commit != rs.GetCommit() ||
			v.Applied != rs.GetApplied() || v.LastIndex != rs.GetLastIndex() || v.TableBytes != e.GetTableBytes() ||
			v.MemtableBytes != e.GetMemtableBytes() || v.Tables != int(e.GetTables()) {
			return false
		}
		tableBytes += e.GetTableBytes()
		memBytes += e.GetMemtableBytes()
		if rs.GetRole() == "leader" {
			leaders++
		}
	}
	return n.Storage.TableBytes == tableBytes && n.Storage.MemtableBytes == memBytes && n.Leaders == leaders &&
		n.Storage.CacheCapacityBytes == st.GetBlockCache().GetCapacityBytes()
}

// follow reads the console's server-sent stream, yielding each overview.
func follow(t *testing.T, url string) <-chan console.Overview {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		cancel()
		t.Fatalf("stream content type %q", ct)
	}
	out := make(chan console.Overview)
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(out)
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
		for sc.Scan() {
			data, ok := strings.CutPrefix(sc.Text(), "data: ")
			if !ok {
				continue
			}
			var ov console.Overview
			if err := json.Unmarshal([]byte(data), &ov); err != nil {
				return
			}
			select {
			case out <- ov:
			case <-ctx.Done():
				return
			}
		}
	})
	t.Cleanup(func() {
		cancel()
		_ = resp.Body.Close()
		for range out { //nolint:revive // drain so the reader can exit
		}
		wg.Wait()
	})
	return out
}
