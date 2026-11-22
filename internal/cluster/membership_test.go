package cluster

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/obs"
	"github.com/naveenk2608/distributed-database/internal/porttest"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

var tcfg = transport.Config{DialTimeout: time.Second, KeepaliveTime: 10 * time.Second, KeepaliveTimeout: time.Second}

// node runs a ping server for a Membership on a free loopback port.
func node(t *testing.T, name, id string) (*Membership, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	m := New(Config{ClusterName: name, NodeID: id, Advertise: addr, PingInterval: 50 * time.Millisecond,
		PingTimeout: 50 * time.Millisecond, DownAfter: time.Second}, nil, nil, slog.New(slog.DiscardHandler))
	srv := transport.NewServer(tcfg)
	dkvv1.RegisterInternalServiceServer(srv, NewPingService(m))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); wg.Wait() })
	return m, addr
}

func observer(t *testing.T, clock Clock, peers ...string) *Membership {
	t.Helper()
	pool := transport.NewPool(tcfg)
	t.Cleanup(func() { _ = pool.Close() })
	return New(Config{ClusterName: "c", NodeID: "observer", Advertise: "127.0.0.1:1", Peers: peers,
		PingInterval: 50 * time.Millisecond, PingTimeout: time.Second, DownAfter: 5 * time.Second},
		pool, clock, slog.New(slog.DiscardHandler))
}

func member(resp *dkvv1.ClusterStatusResponse, addr string) *dkvv1.MemberStatus {
	for _, m := range resp.GetMembers() {
		if m.GetAddr() == addr {
			return m
		}
	}
	return nil
}

func TestDownWindowIsExact(t *testing.T) {
	_, addr := node(t, "c", "peer-1")
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	m := observer(t, clock, addr)
	ctx := context.Background()

	if st := member(m.Status(), addr).GetState(); st != dkvv1.MemberState_MEMBER_STATE_UNKNOWN {
		t.Fatalf("before any ping: %v", st)
	}
	m.pingOnce(ctx, addr)
	got := member(m.Status(), addr)
	if got.GetState() != dkvv1.MemberState_MEMBER_STATE_UP || got.GetNode().GetNodeId() != "peer-1" {
		t.Fatalf("after a good ping: %v", got)
	}
	clock.Advance(5 * time.Second) // exactly the window: still up
	if st := member(m.Status(), addr).GetState(); st != dkvv1.MemberState_MEMBER_STATE_UP {
		t.Fatalf("at the edge of the window: %v", st)
	}
	clock.Advance(time.Nanosecond)
	if st := member(m.Status(), addr).GetState(); st != dkvv1.MemberState_MEMBER_STATE_DOWN {
		t.Fatalf("past the window: %v", st)
	}
	m.pingOnce(ctx, addr)
	if st := member(m.Status(), addr).GetState(); st != dkvv1.MemberState_MEMBER_STATE_UP {
		t.Fatalf("answering again: %v", st)
	}
}

func TestNeverReachedPeerGoesDown(t *testing.T) {
	dead := porttest.Refusing(t) // never answers a ping
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	m := observer(t, clock, dead)
	m.pingOnce(context.Background(), dead)
	got := member(m.Status(), dead)
	if got.GetState() != dkvv1.MemberState_MEMBER_STATE_UNKNOWN || got.GetConsecutiveFailures() != 1 || got.GetLastError() == "" {
		t.Fatalf("within the first window: %v", got)
	}
	clock.Advance(5*time.Second + time.Nanosecond)
	if st := member(m.Status(), dead).GetState(); st != dkvv1.MemberState_MEMBER_STATE_DOWN {
		t.Fatalf("after a window without an answer: %v", st)
	}
}

func TestOtherClusterIsNeverUp(t *testing.T) {
	_, addr := node(t, "other", "stranger")
	m := observer(t, nil, addr)
	m.pingOnce(context.Background(), addr)
	got := member(m.Status(), addr)
	if got.GetState() == dkvv1.MemberState_MEMBER_STATE_UP || !strings.Contains(got.GetLastError(), `cluster "other"`) {
		t.Fatalf("node from another cluster: %v", got)
	}
}

func TestSelfListedUnderAnotherAddress(t *testing.T) {
	self, addr := node(t, "c", "me")
	_, port, _ := net.SplitHostPort(addr)
	alias := "localhost:" + port
	pool := transport.NewPool(tcfg)
	defer func() { _ = pool.Close() }()
	// A generous ping timeout: this checks alias detection, and a loaded
	// machine can take over a second to connect.
	m := New(Config{ClusterName: "c", NodeID: "me", Advertise: addr, Peers: []string{alias},
		PingInterval: 5 * time.Second, PingTimeout: 5 * time.Second, DownAfter: 10 * time.Second}, pool, nil, slog.New(slog.DiscardHandler))
	if m.pingOnce(context.Background(), alias) {
		t.Fatal("pinging this node's own alias should stop the loop")
	}
	got := member(m.Status(), alias)
	if !got.GetSelf() || got.GetState() != dkvv1.MemberState_MEMBER_STATE_UP {
		t.Fatalf("alias of self: %v", got)
	}
	_ = self
}

func TestRunPingsUntilCancelled(t *testing.T) {
	_, a := node(t, "c", "a")
	_, b := node(t, "c", "b")
	m := observer(t, nil, a, b)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s := m.Status()
		if member(s, a).GetState() == dkvv1.MemberState_MEMBER_STATE_UP && member(s, b).GetState() == dkvv1.MemberState_MEMBER_STATE_UP {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peers not up: %v", s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if s := m.Status(); len(s.GetMembers()) != 3 || !member(s, "127.0.0.1:1").GetSelf() {
		t.Errorf("status should list both peers and this node: %v", s)
	}
	cancel()
	<-done
}

func TestPlacementMismatchIsReported(t *testing.T) {
	peer, addr := node(t, "c", "peer-1")
	peer.self.Placement = "v1/s64/rf3/aaaa"
	m := observer(t, nil, addr)
	m.cfg.Placement = "v1/s64/rf3/bbbb"
	m.pingOnce(context.Background(), addr)
	got := member(m.Status(), addr)
	if got.GetState() != dkvv1.MemberState_MEMBER_STATE_UP || !strings.Contains(got.GetLastError(), "placement differs") {
		t.Fatalf("peer with another placement: %v", got)
	}
}

// TestPeerEvents: the failure detector reports a peer going up (named by
// its node ID) and going down (named by its address when it never
// answered), once per change.
func TestPeerEvents(t *testing.T) {
	_, live := node(t, "c", "peer-1")
	dead := porttest.Refusing(t) // nothing answers there
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	pool := transport.NewPool(tcfg)
	t.Cleanup(func() { _ = pool.Close() })
	var events []obs.Event
	m := New(Config{ClusterName: "c", NodeID: "observer", Advertise: "127.0.0.1:1", Peers: []string{live, dead},
		PingInterval: 50 * time.Millisecond, PingTimeout: time.Second, DownAfter: 5 * time.Second,
		OnEvent: func(e obs.Event) { events = append(events, e) }}, pool, clock, slog.New(slog.DiscardHandler))
	ctx := context.Background()
	m.pingOnce(ctx, live)
	m.pingOnce(ctx, live) // still up: no second event
	clock.Advance(5*time.Second + time.Nanosecond)
	m.pingOnce(ctx, dead)
	if len(events) != 2 || events[0].Kind != obs.EventPeerUp || events[0].Node != "peer-1" ||
		events[1].Kind != obs.EventPeerDown || events[1].Node != dead || events[1].Shard != obs.NoShard {
		t.Fatalf("events %+v; want peer-1 up, then %s down", events, dead)
	}
}
