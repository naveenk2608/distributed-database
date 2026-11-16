package router

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/kvstore"
	"github.com/naveenk2608/distributed-database/internal/placement"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const shards = 8

var tcfg = transport.Config{DialTimeout: time.Second, KeepaliveTime: 10 * time.Second, KeepaliveTimeout: time.Second}

type internalAPI struct {
	dkvv1.UnimplementedInternalServiceServer
	r *Router
}

func (a internalAPI) Forward(ctx context.Context, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	return a.r.Forward(ctx, req)
}

// testNode is a router with its own internal server and in-memory stores
// for every shard (so a misrouted request would be visible as data in the
// wrong place).
type testNode struct {
	addr   string
	router *Router
	stores map[int]*kvstore.Store
}

// startNodes starts len(views) routers; views[i] gives node i's idea of
// who owns each shard, as an index into the returned nodes.
func startNodes(t *testing.T, views []func(shard int) int) []*testNode {
	t.Helper()
	nodes := make([]*testNode, len(views))
	lis := make([]net.Listener, len(views))
	for i := range views {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis[i] = l
		nodes[i] = &testNode{addr: l.Addr().String(), stores: map[int]*kvstore.Store{}}
	}
	for i, view := range views {
		n := nodes[i]
		for s := range shards {
			db := lsm.OpenInMemory()
			t.Cleanup(func() { _ = db.Close() })
			n.stores[s] = kvstore.New(db)
		}
		pool := transport.NewPool(tcfg)
		t.Cleanup(func() { _ = pool.Close() })
		n.router = New(Config{
			Self: n.addr, Shards: shards, Local: n.stores, Pool: pool, Log: slog.New(slog.DiscardHandler),
			Primary: func(s int) string { return nodes[view(s)].addr },
		})
		srv := transport.NewServer(tcfg)
		dkvv1.RegisterInternalServiceServer(srv, internalAPI{r: n.router})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); _ = srv.Serve(lis[i]) }()
		t.Cleanup(func() { srv.Stop(); wg.Wait() })
	}
	return nodes
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func keyIn(shard int) []byte {
	for i := 0; ; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		if placement.ShardOf(k, shards) == shard {
			return k
		}
	}
}

func holds(n *testNode, shard int, key []byte) bool {
	_, _, found, _ := n.stores[shard].Get(context.Background(), key)
	return found
}

func TestRequestsReachTheOwner(t *testing.T) {
	owner := func(s int) int { return s % 2 } // even shards on node 0, odd on node 1
	nodes := startNodes(t, []func(int) int{owner, owner})
	for s := range shards {
		key := keyIn(s)
		for via := range nodes {
			ver, err := nodes[via].router.Put(ctx(t), key, []byte(fmt.Sprint("v", via)))
			if err != nil {
				t.Fatalf("put shard %d via node %d: %v", s, via, err)
			}
			v, gotVer, found, err := nodes[1-via].router.Get(ctx(t), key)
			if err != nil || !found || string(v) != fmt.Sprint("v", via) || gotVer != ver {
				t.Fatalf("get shard %d via node %d: %q@%d found=%v %v", s, 1-via, v, gotVer, found, err)
			}
		}
		if !holds(nodes[owner(s)], s, key) || holds(nodes[1-owner(s)], s, key) {
			t.Fatalf("shard %d: data is not (only) on its owner", s)
		}
		found, _, err := nodes[1-owner(s)].router.Delete(ctx(t), key)
		if err != nil || !found {
			t.Fatalf("delete via non-owner: found=%v %v", found, err)
		}
	}
	if st := nodes[0].router.Stats(); st.Forwarded == 0 || st.NotOwner != 0 {
		t.Errorf("stats %+v", st)
	}
}

// TestNotOwnerHintIsFollowed: node 0 believes node 1 owns shard 3; node 1
// knows node 2 does and answers NotOwner with that hint; node 0 retries
// there.
func TestNotOwnerHintIsFollowed(t *testing.T) {
	nodes := startNodes(t, []func(int) int{
		func(int) int { return 1 },
		func(int) int { return 2 },
		func(int) int { return 2 },
	})
	key := keyIn(3)
	if _, err := nodes[0].router.Put(ctx(t), key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if !holds(nodes[2], 3, key) || holds(nodes[1], 3, key) || holds(nodes[0], 3, key) {
		t.Fatal("the write did not land on the hinted owner only")
	}
	if st := nodes[0].router.Stats(); st.NotOwner != 1 || st.Forwarded != 2 {
		t.Errorf("stats %+v; want one NotOwner and two forwards", st)
	}
}

func TestForwardRefusesShardsItDoesNotOwn(t *testing.T) {
	owner := func(int) int { return 0 }
	nodes := startNodes(t, []func(int) int{owner, owner})
	resp, err := nodes[1].router.Forward(ctx(t), &dkvv1.ForwardRequest{Shard: 2,
		Op: &dkvv1.ForwardRequest_Put{Put: &dkvv1.PutRequest{Key: keyIn(2)}}})
	if err != nil || resp.GetNotOwner().GetOwnerHint() != nodes[0].addr {
		t.Fatalf("Forward to a non-owner = %v, %v; want NotOwner pointing at node 0", resp, err)
	}
	if holds(nodes[1], 2, keyIn(2)) {
		t.Error("non-owner applied a forwarded write")
	}
	if _, err := nodes[1].router.Forward(ctx(t), &dkvv1.ForwardRequest{Shard: shards}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("out-of-range shard: %v", err)
	}
}

// TestDeadOwnerFailsWithinDeadline: requests for a shard whose owner is down
// are retried with backoff but return when the caller's deadline expires.
func TestDeadOwnerFailsWithinDeadline(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := lis.Addr().String()
	_ = lis.Close()
	db := lsm.OpenInMemory()
	defer func() { _ = db.Close() }()
	pool := transport.NewPool(tcfg)
	defer func() { _ = pool.Close() }()
	r := New(Config{Self: "127.0.0.1:1", Shards: shards, Pool: pool, Log: slog.New(slog.DiscardHandler),
		Primary: func(int) string { return dead }, Local: map[int]*kvstore.Store{}})
	c, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := r.Put(c, []byte("k"), []byte("v"))
	elapsed := time.Since(start)
	if code := status.Code(err); code != codes.DeadlineExceeded && code != codes.Unavailable {
		t.Fatalf("dead owner: %v", err)
	}
	if elapsed > time.Second {
		t.Errorf("took %v with a 400ms deadline", elapsed)
	}
	if r.Stats().Retries == 0 {
		t.Error("no retries before giving up")
	}
	// Without a deadline, the attempt limit still ends the request.
	start = time.Now()
	if _, err := r.Put(context.Background(), []byte("k"), []byte("v")); status.Code(err) != codes.Unavailable {
		t.Fatalf("no deadline: %v", err)
	}
	t.Logf("gave up after %d attempts in %v", maxAttempts, time.Since(start).Round(time.Millisecond))
}
