// Package nodetest runs in-process dkv clusters for tests: N full nodes
// (public API, internal API, membership, storage) on ephemeral loopback
// ports, each with its own data directory. Nodes can be stopped and
// restarted on the same addresses and data.
package nodetest

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/node"
)

// testLogger discards node logs unless NODETEST_LOG is set, in which case
// they go to stderr as text (the variable avoids the DKV_ prefix, which
// the server's configuration reserves).
func testLogger() *slog.Logger {
	if os.Getenv("NODETEST_LOG") == "" {
		return slog.New(slog.DiscardHandler)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// Cluster is a set of in-process nodes that list each other as peers.
type Cluster struct {
	t       testing.TB
	Members []*Member
}

// Member is one node of a Cluster.
type Member struct {
	Index        int
	Config       config.Config
	PublicAddr   string
	InternalAddr string

	mu     sync.Mutex
	nodeID string
	cancel context.CancelFunc
	done   chan error
}

// Start runs n nodes. Every node gets fast membership timings (ping every
// 100ms, down after 1s), a fast Raft clock (20ms ticks, so election
// timeouts of 200 to 400ms) and 8 shards rather than the default 64, to
// keep the background load of idle Raft groups low; configure may change
// any of them.
func Start(t testing.TB, n int, configure func(i int, c *config.Config)) *Cluster {
	t.Helper()
	publics, internals := listenN(t, n), listenN(t, n)
	var peers []string
	for _, l := range internals {
		peers = append(peers, l.Addr().String())
	}
	c := &Cluster{t: t}
	for i := range n {
		cfg := config.Default()
		cfg.Server.GRPCAddr = publics[i].Addr().String()
		cfg.Server.ShutdownTimeout = 2 * time.Second
		cfg.Cluster.InternalAddr = internals[i].Addr().String()
		cfg.Cluster.Peers = peers
		cfg.Cluster.PingInterval = 100 * time.Millisecond
		cfg.Cluster.PingTimeout = 100 * time.Millisecond
		cfg.Cluster.DownAfter = time.Second
		cfg.Raft.TickInterval = 20 * time.Millisecond
		cfg.Cluster.Shards = 8
		cfg.Storage.Dir = t.TempDir()
		if configure != nil {
			configure(i, &cfg)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("nodetest: node %d: %v", i, err)
		}
		m := &Member{Index: i, Config: cfg, PublicAddr: cfg.Server.GRPCAddr, InternalAddr: cfg.Cluster.InternalAddr}
		c.Members = append(c.Members, m)
		m.start(t, publics[i], internals[i])
	}
	t.Cleanup(func() {
		for i := range c.Members {
			c.Stop(i)
		}
	})
	return c
}

func listenN(t testing.TB, n int) []net.Listener {
	t.Helper()
	out := make([]net.Listener, n)
	for i := range out {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("nodetest: listen: %v", err)
		}
		out[i] = l
	}
	return out
}

// listenAgain rebinds an address a stopped node used; the port can stay
// busy for a moment after the old listener closes.
func listenAgain(t testing.TB, addr string) net.Listener {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		l, err := net.Listen("tcp", addr)
		if err == nil {
			return l
		}
		if time.Now().After(deadline) {
			t.Fatalf("nodetest: rebind %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (m *Member) start(t testing.TB, public, internal net.Listener) {
	t.Helper()
	cfg := m.Config
	n, err := node.Open(&cfg, testLogger().With("node", m.Index))
	if err != nil {
		_ = public.Close()
		_ = internal.Close()
		t.Fatalf("nodetest: open node %d: %v", m.Index, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx, public, internal) }()
	m.mu.Lock()
	m.nodeID, m.cancel, m.done = n.ID(), cancel, done
	m.mu.Unlock()
}

// NodeID returns the member's node ID (from its latest start).
func (m *Member) NodeID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nodeID
}

// Running reports whether the member is started.
func (m *Member) Running() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cancel != nil
}

// Stop stops member i and waits until it has released its ports and data
// directory. Peers see it vanish like a crashed process. Stopping a stopped
// member does nothing.
func (c *Cluster) Stop(i int) {
	m := c.Members[i]
	m.mu.Lock()
	cancel, done := m.cancel, m.done
	m.cancel, m.done = nil, nil
	m.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	if err := <-done; err != nil {
		c.t.Errorf("nodetest: node %d exited with: %v", i, err)
	}
}

// Restart starts a stopped member again on the same addresses and data
// directory.
func (c *Cluster) Restart(i int) {
	c.t.Helper()
	m := c.Members[i]
	if m.Running() {
		c.t.Fatalf("nodetest: node %d is running", i)
	}
	m.start(c.t, listenAgain(c.t, m.PublicAddr), listenAgain(c.t, m.InternalAddr))
}

// Status asks member i for its view of the cluster.
func (c *Cluster) Status(i int) (*dkvv1.ClusterStatusResponse, error) {
	conn, err := grpc.NewClient(c.Members[i].PublicAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return dkvv1.NewAdminServiceClient(conn).ClusterStatus(ctx, &dkvv1.ClusterStatusRequest{})
}

// StateOf returns the state member observer reports for member target.
func (c *Cluster) StateOf(observer, target int) (dkvv1.MemberState, error) {
	resp, err := c.Status(observer)
	if err != nil {
		return 0, err
	}
	for _, m := range resp.GetMembers() {
		if m.GetAddr() == c.Members[target].InternalAddr {
			return m.GetState(), nil
		}
	}
	return 0, fmt.Errorf("node %d does not list node %d (%s)", observer, target, c.Members[target].InternalAddr)
}

// WaitFor polls until cond holds or timeout passes, and returns how long it
// took.
func WaitFor(t testing.TB, timeout time.Duration, what string, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for !cond() {
		if time.Since(start) > timeout {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return time.Since(start)
}

// ShardMap asks member i for the shard map and its local replicas.
func (c *Cluster) ShardMap(i int) (*dkvv1.ShardMapResponse, error) {
	conn, err := grpc.NewClient(c.Members[i].PublicAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return dkvv1.NewAdminServiceClient(conn).ShardMap(ctx, &dkvv1.ShardMapRequest{})
}

// KV returns a KV client for member i, closed at test cleanup.
func (c *Cluster) KV(i int) dkvv1.KVServiceClient {
	conn, err := grpc.NewClient(c.Members[i].PublicAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.t.Fatalf("nodetest: dial node %d: %v", i, err)
	}
	c.t.Cleanup(func() { _ = conn.Close() })
	return dkvv1.NewKVServiceClient(conn)
}

// Leader returns the index of the member that leads shard according to
// the running members, and its term; -1 if no running member is leader.
// With several claimants (an old leader that has not yet noticed), the
// highest term wins.
func (c *Cluster) Leader(shard int) (int, uint64) {
	best, term := -1, uint64(0)
	for i, m := range c.Members {
		if !m.Running() {
			continue
		}
		sm, err := c.ShardMap(i)
		if err != nil {
			continue
		}
		for _, info := range sm.GetShards() {
			if int(info.GetShard()) == shard && info.GetLocal() && info.GetRole() == "leader" && info.GetTerm() > term {
				best, term = i, info.GetTerm()
			}
		}
	}
	return best, term
}

// Settled reports whether every shard has a running leader that every
// running replica of the shard knows of.
func (c *Cluster) Settled() bool {
	running := map[string]bool{}
	for _, m := range c.Members {
		running[m.InternalAddr] = m.Running()
	}
	leaders := map[uint32]string{}
	for i, m := range c.Members {
		if !m.Running() {
			continue
		}
		sm, err := c.ShardMap(i)
		if err != nil {
			return false
		}
		for _, info := range sm.GetShards() {
			if !info.GetLocal() {
				continue
			}
			l := info.GetLeader()
			if !running[l] {
				return false
			}
			if prev, ok := leaders[info.GetShard()]; ok && prev != l {
				return false
			}
			leaders[info.GetShard()] = l
		}
	}
	return true
}
