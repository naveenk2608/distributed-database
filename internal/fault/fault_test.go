package fault

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestPartitionAndOneWayBlocks(t *testing.T) {
	i := New(1)
	i.Partition([]string{"a", "b"}, []string{"c"})
	for _, c := range []struct {
		from, to string
		drop     bool
	}{{"a", "c", true}, {"c", "b", true}, {"a", "b", false}, {"b", "a", false}} {
		if got := i.Decide(c.from, c.to, "").Drop; got != c.drop {
			t.Errorf("%s->%s dropped=%v, want %v", c.from, c.to, got, c.drop)
		}
	}
	i.Heal()
	i.Block("a", "b")
	if !i.Decide("a", "b", "").Drop || i.Decide("b", "a", "").Drop {
		t.Error("a one-way block must stop a->b only")
	}
	i.Heal()
	i.SetAll(Link{Loss: 0.5, Duplicate: 0.5, Delay: time.Millisecond, Jitter: time.Millisecond})
	lost, dup := 0, 0
	for range 1000 {
		f := i.Decide("x", "y", "")
		if f.Drop {
			lost++
		}
		if f.Duplicate {
			dup++
		}
		if f.Delay < time.Millisecond || f.Delay >= 2*time.Millisecond {
			t.Fatalf("delay %v outside [1ms, 2ms)", f.Delay)
		}
	}
	if lost < 400 || lost > 600 || dup < 400 || dup > 600 {
		t.Errorf("lost %d, duplicated %d of 1000 at probability 0.5", lost, dup)
	}
	a, b := Sides(rand.New(rand.NewPCG(3, 3)), []string{"n1", "n2", "n3", "n4"})
	if len(a) == 0 || len(b) == 0 || len(a)+len(b) != 4 {
		t.Errorf("sides %v %v", a, b)
	}
}

type pingServer struct {
	dkvv1.UnimplementedInternalServiceServer
	mu             sync.Mutex
	calls, streams int
}

// InstallSnapshot counts the streams opened and drains each one.
func (s *pingServer) InstallSnapshot(stream grpc.ClientStreamingServer[dkvv1.InstallSnapshotRequest, dkvv1.InstallSnapshotResponse]) error {
	s.mu.Lock()
	s.streams++
	s.mu.Unlock()
	for {
		if _, err := stream.Recv(); errors.Is(err, io.EOF) {
			return stream.SendAndClose(&dkvv1.InstallSnapshotResponse{})
		} else if err != nil {
			return err
		}
	}
}

func (s *pingServer) Ping(context.Context, *dkvv1.PingRequest) (*dkvv1.PingResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return &dkvv1.PingResponse{}, nil
}

// TestTransportAppliesFaults: the transport's interceptors drop, delay and
// duplicate real calls as the injector decides, unary calls and streams
// alike (streams are never duplicated), and a delay ends when the caller's
// context does.
func TestTransportAppliesFaults(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := transport.Config{DialTimeout: time.Second, KeepaliveTime: 10 * time.Second, KeepaliveTimeout: time.Second}
	srv := transport.NewServer(cfg)
	ps := &pingServer{}
	dkvv1.RegisterInternalServiceServer(srv, ps)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = srv.Serve(lis) }()
	defer func() { srv.Stop(); wg.Wait() }()

	inj := New(7)
	cfg.Self, cfg.Faults = "me", inj
	pool := transport.NewPool(cfg)
	defer func() { _ = pool.Close() }()
	addr := lis.Addr().String()
	conn, err := pool.Conn(addr)
	if err != nil {
		t.Fatal(err)
	}
	c := dkvv1.NewInternalServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Ping(ctx, &dkvv1.PingRequest{}); err != nil {
		t.Fatal(err)
	}
	inj.Block("me", addr)
	if _, err := c.Ping(ctx, &dkvv1.PingRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("blocked call: %v", err)
	}
	inj.Heal()
	inj.SetLink("me", addr, Link{Duplicate: 1, Delay: 50 * time.Millisecond})
	start := time.Now()
	if _, err := c.Ping(ctx, &dkvv1.PingRequest{}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Error("the delay was not applied")
	}
	ps.mu.Lock()
	if ps.calls != 3 { // one plain, none blocked, one duplicated pair
		t.Errorf("server saw %d calls, want 3", ps.calls)
	}
	ps.mu.Unlock()

	// Streams (snapshot transfers) are blocked and delayed the same way,
	// but never duplicated: a stream cannot be replayed.
	snapshot := func() error {
		st, err := c.InstallSnapshot(ctx)
		if err != nil {
			return err
		}
		if err := st.Send(&dkvv1.InstallSnapshotRequest{}); err != nil {
			return err
		}
		_, err = st.CloseAndRecv()
		return err
	}
	inj.Heal()
	inj.Block("me", addr)
	if err := snapshot(); status.Code(err) != codes.Unavailable {
		t.Fatalf("blocked stream: %v", err)
	}
	inj.Heal()
	inj.SetLink("me", addr, Link{Duplicate: 1, Delay: 50 * time.Millisecond})
	start = time.Now()
	if err := snapshot(); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Error("the delay was not applied to the stream")
	}
	ps.mu.Lock()
	if ps.streams != 1 {
		t.Errorf("server saw %d streams, want 1 (none blocked, none duplicated)", ps.streams)
	}
	ps.mu.Unlock()

	// A delay ends early when the caller gives up.
	inj.SetLink("me", addr, Link{Delay: time.Minute})
	short, cancelShort := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelShort()
	start = time.Now()
	if _, err := c.Ping(short, &dkvv1.PingRequest{}); status.Code(err) != codes.DeadlineExceeded || time.Since(start) > 2*time.Second {
		t.Errorf("delayed call with a 100 ms deadline: %v after %v; want DeadlineExceeded at once", err, time.Since(start))
	}
}

// TestLinkSettingsAndStats: a link's own setting takes precedence over
// SetAll, in its direction only; nodes in no side of a partition are
// unaffected; Heal clears links and blocks; Stats counts every decision.
func TestLinkSettingsAndStats(t *testing.T) {
	i := New(1)
	i.SetAll(Link{Loss: 1})
	i.SetLink("a", "b", Link{Delay: time.Millisecond, Duplicate: 1})
	if f := i.Decide("a", "b", ""); f.Drop || !f.Duplicate || f.Delay != time.Millisecond {
		t.Errorf("a->b with its own link: %+v; want delayed and duplicated, not dropped", f)
	}
	if f := i.Decide("b", "a", ""); !f.Drop {
		t.Errorf("b->a falls back to SetAll: %+v; want dropped", f)
	}
	i.Heal()
	i.SetLink("a", "b", Link{Loss: 1})
	if !i.Decide("a", "b", "").Drop || i.Decide("b", "a", "").Drop || i.Decide("a", "c", "").Drop {
		t.Error("a per-link loss must drop a->b only")
	}
	i.Heal()
	i.Partition([]string{"a"}, []string{"b"})
	for _, c := range []struct {
		from, to string
		drop     bool
	}{{"a", "b", true}, {"b", "a", true}, {"a", "c", false}, {"c", "b", false}, {"c", "a", false}} {
		if got := i.Decide(c.from, c.to, "").Drop; got != c.drop {
			t.Errorf("partition {a} {b}: %s->%s dropped=%v, want %v", c.from, c.to, got, c.drop)
		}
	}
	i.Heal()
	if i.Blocked("a", "b") || i.Decide("a", "b", "") != (transport.Fault{}) {
		t.Error("Heal left a fault behind")
	}
	want := Stats{Calls: 11, Blocked: 2, Lost: 2, Duplicated: 1, Delayed: 1}
	if got := i.Stats(); got != want {
		t.Errorf("stats %+v; want %+v", got, want)
	}
}
