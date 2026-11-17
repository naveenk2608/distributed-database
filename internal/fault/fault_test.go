package fault

import (
	"context"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
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
	mu    sync.Mutex
	calls int
}

func (s *pingServer) Ping(context.Context, *dkvv1.PingRequest) (*dkvv1.PingResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return &dkvv1.PingResponse{}, nil
}

// TestTransportAppliesFaults: the transport's interceptors drop, delay and
// duplicate real calls as the injector decides.
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
	defer ps.mu.Unlock()
	if ps.calls != 3 { // one plain, none blocked, one duplicated pair
		t.Errorf("server saw %d calls, want 3", ps.calls)
	}
}
