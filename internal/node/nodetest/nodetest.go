// Package nodetest runs in-process dkv clusters for tests: N full nodes
// (public API, internal API, membership, storage) on ephemeral loopback
// ports, each with its own data directory. Nodes can be stopped and
// restarted on the same addresses and data.
package nodetest

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
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
	"github.com/naveenk2608/distributed-database/internal/porttest"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
	"github.com/naveenk2608/distributed-database/internal/transport"
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
	// configure is applied to every node, including ones added later.
	configure func(i int, c *config.Config)
	opts      Options
}

// Options extend a cluster for fault-injection tests.
type Options struct {
	// Faults, if set, is consulted on every call between nodes.
	Faults transport.Faults
	// MemFS keeps each node's replicas on its own in-memory filesystem,
	// which PowerLoss can crash.
	MemFS bool
	// WrapFS, if set, wraps member i's replica filesystem (for example in
	// a slow disk).
	WrapFS func(i int, fs vfs.FS) vfs.FS
	// Admin serves every member's admin HTTP API (Member.AdminAddr).
	Admin bool
}

// Member is one node of a Cluster.
type Member struct {
	Index        int
	Config       config.Config
	PublicAddr   string
	InternalAddr string
	AdminAddr    string // with Options.Admin

	mu     sync.Mutex
	nodeID string
	node   *node.Node
	fs     *vfs.MemFS // with Options.MemFS
	cancel context.CancelFunc
	done   chan error
}

// testConfig returns a node configuration with the fast timings described
// on Start.
func testConfig(t testing.TB) config.Config {
	cfg := config.Default()
	cfg.Server.ShutdownTimeout = 2 * time.Second
	cfg.Cluster.PingInterval = 100 * time.Millisecond
	cfg.Cluster.PingTimeout = 100 * time.Millisecond
	cfg.Cluster.DownAfter = time.Second
	cfg.Raft.TickInterval = 20 * time.Millisecond
	cfg.Cluster.Shards = 8
	cfg.Liveness.HeartbeatInterval = 200 * time.Millisecond
	cfg.Liveness.SuspectAfter = 600 * time.Millisecond
	cfg.Liveness.DeadAfter = 5 * time.Second
	cfg.Rebalance.Interval = 200 * time.Millisecond
	cfg.Storage.Dir = t.TempDir()
	return cfg
}

// Start runs n nodes. Every node gets fast membership timings (ping every
// 100ms, down after 1s), a fast Raft clock (20ms ticks, so election
// timeouts of 200 to 400ms), fast liveness and rebalancing (heartbeats
// every 200ms, suspect after 600ms, dead after 5s, a rebalance round every
// 200ms), and 8 shards rather than the default 64, to keep the background
// load of idle Raft groups low; configure may change any of them.
func Start(t testing.TB, n int, configure func(i int, c *config.Config)) *Cluster {
	t.Helper()
	return StartWith(t, n, Options{}, configure)
}

// StartWith is Start with fault-injection options.
func StartWith(t testing.TB, n int, opts Options, configure func(i int, c *config.Config)) *Cluster {
	t.Helper()
	publics, internals := listenN(t, n), listenN(t, n)
	admins := make([]net.Listener, n)
	if opts.Admin {
		admins = listenN(t, n)
	}
	var peers []string
	for _, l := range internals {
		peers = append(peers, l.Addr().String())
	}
	c := &Cluster{t: t, configure: configure, opts: opts}
	for i := range n {
		cfg := testConfig(t)
		cfg.Server.GRPCAddr = publics[i].Addr().String()
		cfg.Cluster.InternalAddr = internals[i].Addr().String()
		cfg.Cluster.Peers = peers
		if admins[i] != nil {
			cfg.Server.AdminAddr = admins[i].Addr().String()
		}
		if configure != nil {
			configure(i, &cfg)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("nodetest: node %d: %v", i, err)
		}
		m := &Member{Index: i, Config: cfg, PublicAddr: cfg.Server.GRPCAddr, InternalAddr: cfg.Cluster.InternalAddr,
			AdminAddr: cfg.Server.AdminAddr}
		if opts.MemFS {
			m.fs = vfs.NewMemFS()
		}
		c.Members = append(c.Members, m)
		m.start(t, publics[i], internals[i], admins[i], opts)
	}
	t.Cleanup(func() {
		for i := range c.Members {
			c.Stop(i)
		}
	})
	return c
}

// listenN opens n listeners on ports outside the kernel's ephemeral range,
// so a member restarted on its addresses (listenAgain) cannot find them
// taken by some connection's local port in the meantime.
func listenN(t testing.TB, n int) []net.Listener {
	t.Helper()
	out := make([]net.Listener, n)
	for i := range out {
		out[i] = porttest.Listen(t)
	}
	return out
}

// listenAgain rebinds an address a stopped node used; the port can stay
// busy for a moment after the old listener closes.
func listenAgain(t testing.TB, addr string) net.Listener {
	t.Helper()
	return porttest.Relisten(t, addr, 5*time.Second)
}

func (m *Member) start(t testing.TB, public, internal, admin net.Listener, opts Options) {
	t.Helper()
	cfg := m.Config
	var nopts []node.Option
	if opts.Faults != nil {
		nopts = append(nopts, node.WithFaults(opts.Faults))
	}

	m.mu.Lock()
	var fs vfs.FS
	if m.fs != nil {
		fs = m.fs
	}
	m.mu.Unlock()
	if opts.WrapFS != nil {
		if fs == nil {
			fs = vfs.Default
		}
		fs = opts.WrapFS(m.Index, fs)
	}
	if fs != nil {
		nopts = append(nopts, node.WithFS(fs))
	}
	n, err := node.Open(&cfg, testLogger().With("node", m.Index), nopts...)
	if err != nil {
		_ = public.Close()
		_ = internal.Close()
		if admin != nil {
			_ = admin.Close()
		}
		t.Fatalf("nodetest: open node %d: %v", m.Index, err)
	}
	if admin != nil {
		n.ServeAdminOn(admin)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx, public, internal) }()
	m.mu.Lock()
	m.nodeID, m.node, m.cancel, m.done = n.ID(), n, cancel, done
	m.mu.Unlock()
}

// Node returns the member's running node, or nil when it is stopped.
func (m *Member) Node() *node.Node {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel == nil {
		return nil
	}
	return m.node
}

// Add starts a new node that joins the cluster through the existing
// members (it is not in their bootstrap peer list) and returns its index.
func (c *Cluster) Add() int {
	c.t.Helper()
	pub, in := listenN(c.t, 1)[0], listenN(c.t, 1)[0]
	var admin net.Listener
	cfg := testConfig(c.t)
	cfg.Server.GRPCAddr, cfg.Cluster.InternalAddr = pub.Addr().String(), in.Addr().String()
	if c.opts.Admin {
		admin = listenN(c.t, 1)[0]
		cfg.Server.AdminAddr = admin.Addr().String()
	}
	for _, m := range c.Members {
		cfg.Cluster.Join = append(cfg.Cluster.Join, m.InternalAddr)
	}
	i := len(c.Members)
	if c.configure != nil {
		c.configure(i, &cfg)
	}
	cfg.Cluster.Peers = nil
	if err := cfg.Validate(); err != nil {
		c.t.Fatalf("nodetest: node %d: %v", i, err)
	}
	m := &Member{Index: i, Config: cfg, PublicAddr: cfg.Server.GRPCAddr, InternalAddr: cfg.Cluster.InternalAddr,
		AdminAddr: cfg.Server.AdminAddr}
	if c.opts.MemFS {
		m.fs = vfs.NewMemFS()
	}
	c.Members = append(c.Members, m)
	m.start(c.t, pub, in, admin, c.opts)
	return i
}

// PowerLoss stops member i as a power cut would: its in-memory filesystem
// (Options.MemFS) crashes first, so nothing more reaches it, and the
// member keeps only what had been made durable. Restart brings it back on
// that state.
func (c *Cluster) PowerLoss(i int, rng *rand.Rand) {
	c.t.Helper()
	m := c.Members[i]
	if m.fs == nil {
		c.t.Fatal("nodetest: PowerLoss needs Options.MemFS")
	}
	m.fs.Crash()
	c.stop(i, false)
	m.mu.Lock()
	m.fs = m.fs.CrashImage(rng)
	m.mu.Unlock()
}

// Wipe deletes a stopped member's data, as if its disk were replaced; on
// its next start it gets a new node ID.
func (c *Cluster) Wipe(i int) {
	c.t.Helper()
	m := c.Members[i]
	if m.Running() {
		c.t.Fatalf("nodetest: node %d is running", i)
	}
	dir := m.Config.Storage.Dir
	if err := os.RemoveAll(dir); err != nil {
		c.t.Fatal(err)
	}
	if m.fs != nil {
		m.fs = vfs.NewMemFS()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		c.t.Fatal(err)
	}
}

// NodeStatus asks member i for every replica it holds.
func (c *Cluster) NodeStatus(i int) (*dkvv1.NodeStatusResponse, error) {
	conn, err := grpc.NewClient(c.Members[i].InternalAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return dkvv1.NewInternalServiceClient(conn).NodeStatus(ctx, &dkvv1.NodeStatusRequest{})
}

// Groups returns, for every group (shard -1 is the meta group), the
// status reported by its leader among the running members.
func (c *Cluster) Groups() map[int]*dkvv1.ReplicaStatus {
	out := map[int]*dkvv1.ReplicaStatus{}
	for i, m := range c.Members {
		if !m.Running() {
			continue
		}
		resp, err := c.NodeStatus(i)
		if err != nil {
			continue
		}
		for _, r := range resp.GetReplicas() {
			if r.GetRole() != "leader" {
				continue
			}
			if cur := out[int(r.GetShard())]; cur == nil || r.GetTerm() > cur.GetTerm() {
				out[int(r.GetShard())] = r
			}
		}
	}
	return out
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
func (c *Cluster) Stop(i int) { c.stop(i, true) }

func (c *Cluster) stop(i int, check bool) {
	m := c.Members[i]
	m.mu.Lock()
	cancel, done := m.cancel, m.done
	m.cancel, m.done = nil, nil
	m.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	if err := <-done; err != nil && check {
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
	var admin net.Listener
	if m.AdminAddr != "" {
		admin = listenAgain(c.t, m.AdminAddr)
	}
	m.start(c.t, listenAgain(c.t, m.PublicAddr), listenAgain(c.t, m.InternalAddr), admin, c.opts)
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
	running := map[string]bool{} // by node ID, which names Raft members
	for _, m := range c.Members {
		running[m.NodeID()] = m.Running()
	}
	leaders := map[uint32]string{}
	shards := 0
	for i, m := range c.Members {
		if !m.Running() {
			continue
		}
		sm, err := c.ShardMap(i)
		if err != nil {
			return false
		}
		shards = int(sm.GetShardCount())
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
	return shards > 0 && len(leaders) == shards
}
