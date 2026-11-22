package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/porttest"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var testCfg = Config{
	DialTimeout:      time.Second,
	MaxBackoff:       200 * time.Millisecond,
	KeepaliveTime:    10 * time.Second,
	KeepaliveTimeout: time.Second,
}

// pinger answers Ping, optionally blocking until released or cancelled.
type pinger struct {
	dkvv1.UnimplementedInternalServiceServer
	id    string
	block chan struct{} // if non-nil, Ping waits on it
}

func (p *pinger) Ping(ctx context.Context, _ *dkvv1.PingRequest) (*dkvv1.PingResponse, error) {
	if p.block != nil {
		select {
		case <-p.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &dkvv1.PingResponse{Node: &dkvv1.NodeInfo{NodeId: p.id}}, nil
}

// serve runs an internal server on addr ("" = any free loopback port) and
// returns the bound address and a stop function.
func serve(t *testing.T, addr string, svc dkvv1.InternalServiceServer) (string, func()) {
	t.Helper()
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	var lis net.Listener
	var err error
	// Rebinding a port that was just closed can briefly fail; retry.
	for deadline := time.Now().Add(5 * time.Second); ; {
		if lis, err = net.Listen("tcp", addr); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(testCfg)
	dkvv1.RegisterInternalServiceServer(srv, svc)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = srv.Serve(lis) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			srv.Stop() // abrupt, like a crashed peer
			wg.Wait()
		})
	}
	t.Cleanup(stop)
	return lis.Addr().String(), stop
}

func ping(pool *Pool, addr string, timeout time.Duration) (*dkvv1.PingResponse, error) {
	conn, err := pool.Conn(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return dkvv1.NewInternalServiceClient(conn).Ping(ctx, &dkvv1.PingRequest{})
}

func newPool(t *testing.T) *Pool {
	t.Helper()
	p := NewPool(testCfg)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestPoolSharesOneConnectionPerPeer(t *testing.T) {
	addr, _ := serve(t, "", &pinger{id: "n1"})
	pool := newPool(t)
	c1, err := pool.Conn(addr)
	if err != nil {
		t.Fatal(err)
	}
	c2, _ := pool.Conn(addr)
	if c1 != c2 {
		t.Error("two connections to the same peer")
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if resp, err := ping(pool, addr, 5*time.Second); err != nil || resp.GetNode().GetNodeId() != "n1" {
				t.Errorf("concurrent ping: %v, %v", resp, err)
			}
		}()
	}
	wg.Wait()
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Conn(addr); !errors.Is(err, ErrClosed) {
		t.Errorf("Conn after Close: %v", err)
	}
}

// TestDeadlineIsEnforced: a peer that accepts the call but never answers
// costs the caller its deadline, not more, and the handler sees the
// cancellation.
func TestDeadlineIsEnforced(t *testing.T) {
	addr, _ := serve(t, "", &pinger{block: make(chan struct{})})
	pool := newPool(t)
	start := time.Now()
	_, err := ping(pool, addr, 200*time.Millisecond)
	elapsed := time.Since(start)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("blocked peer: %v, want DeadlineExceeded", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("call took %v with a 200ms deadline", elapsed)
	}
}

// TestSilentPeerTimesOut: a peer whose TCP port accepts connections but
// never speaks HTTP/2 (a hung process) also fails within the deadline.
func TestSilentPeerTimesOut(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	defer func() {
		_ = lis.Close()
		<-done
		mu.Lock()
		for _, c := range held {
			_ = c.Close()
		}
		mu.Unlock()
	}()
	pool := newPool(t)
	start := time.Now()
	_, err = ping(pool, lis.Addr().String(), 300*time.Millisecond)
	if code := status.Code(err); code != codes.DeadlineExceeded && code != codes.Unavailable {
		t.Fatalf("silent peer: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("call took %v with a 300ms deadline", elapsed)
	}
}

func TestUnreachablePeerFailsFast(t *testing.T) {
	addr := porttest.Refusing(t) // closes every connection, as a stopped peer's port would
	pool := newPool(t)
	start := time.Now()
	_, err := ping(pool, addr, 5*time.Second)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("closed port: %v, want Unavailable", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("refused connection took %v to report", elapsed)
	}
}

// TestReconnectAfterPeerRestart: when a peer dies, calls fail fast; when it
// comes back on the same address, the same pooled connection recovers on
// its own within the backoff bound.
func TestReconnectAfterPeerRestart(t *testing.T) {
	addr, stop := serve(t, "", &pinger{id: "first"})
	pool := newPool(t)
	conn, _ := pool.Conn(addr)
	if _, err := ping(pool, addr, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	stop()
	failed := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if _, err := ping(pool, addr, 200*time.Millisecond); err != nil {
			if code := status.Code(err); code != codes.Unavailable && code != codes.DeadlineExceeded {
				t.Fatalf("dead peer: unexpected error %v", err)
			}
			failed = true
			break
		}
	}
	if !failed {
		t.Fatal("calls kept succeeding after the peer stopped")
	}

	serve(t, addr, &pinger{id: "second"})
	start := time.Now()
	for {
		resp, err := ping(pool, addr, 200*time.Millisecond)
		if err == nil {
			if resp.GetNode().GetNodeId() != "second" {
				t.Fatalf("answered by %q", resp.GetNode().GetNodeId())
			}
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("no reconnect within 5s: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("reconnected %v after the peer came back", time.Since(start).Round(time.Millisecond))
	if again, _ := pool.Conn(addr); again != conn {
		t.Error("the pool replaced the connection instead of letting it reconnect")
	}
}
