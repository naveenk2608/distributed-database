package router

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/placement"
	"github.com/naveenk2608/distributed-database/internal/raft"
	"github.com/naveenk2608/distributed-database/internal/replica"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const shards = 2

var tcfg = transport.Config{DialTimeout: time.Second, KeepaliveTime: 10 * time.Second, KeepaliveTimeout: time.Second}

type internalAPI struct {
	dkvv1.UnimplementedInternalServiceServer
	r *Router
}

func (a internalAPI) Forward(ctx context.Context, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	return a.r.Forward(ctx, req)
}

// loopback carries Raft messages between the replicas of a test cluster
// in process. Forwarded client requests still go over real gRPC.
type loopback struct {
	mu   sync.Mutex
	reps map[string]map[int]*replica.Replica // node address -> shard -> replica
}

func (l *loopback) Send(shard int, msgs []raft.Message) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range msgs {
		if r := l.reps[m.To][shard]; r != nil {
			r.Step(m)
		}
	}
}

type testNode struct {
	addr   string
	router *Router
	reps   map[int]*replica.Replica // nil for a node without replicas
}

type testCluster struct {
	t     *testing.T
	nodes []*testNode
	// order is the replica set every router sees, in placement order;
	// tests may reorder it before sending requests.
	mu    sync.Mutex
	order []string
}

func (c *testCluster) replicas(int) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.order)
}

// startCluster starts n routers; the first holders of them hold a replica
// of every shard, in one Raft group per shard.
func startCluster(t *testing.T, n, holders int) *testCluster {
	t.Helper()
	c := &testCluster{t: t}
	lis := make([]net.Listener, n)
	for i := range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis[i] = l
		c.nodes = append(c.nodes, &testNode{addr: l.Addr().String()})
		if i < holders {
			c.order = append(c.order, l.Addr().String())
		}
	}
	lb := &loopback{reps: map[string]map[int]*replica.Replica{}}
	fs := vfs.NewMemFS()
	for i, nd := range c.nodes[:holders] {
		nd.reps = map[int]*replica.Replica{}
		for s := range shards {
			rp, err := replica.Open(replica.Config{Shard: s, ID: nd.addr, Peers: c.order,
				Dir: fmt.Sprintf("/n%d/s%d", i, s), FS: fs, Engine: lsm.Options{Sync: lsm.SyncNever},
				ElectionTick: 10, HeartbeatTick: 2, SessionTTL: time.Hour, Sender: lb,
				Log: slog.New(slog.DiscardHandler)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = rp.Close() })
			nd.reps[s] = rp
		}
		lb.mu.Lock()
		lb.reps[nd.addr] = nd.reps
		lb.mu.Unlock()
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // one ticker for every replica
		defer wg.Done()
		tk := time.NewTicker(2 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				for _, nd := range c.nodes {
					for _, rp := range nd.reps {
						rp.Tick()
					}
				}
			}
		}
	}()
	t.Cleanup(func() { close(stop); wg.Wait() }) // runs before the replicas close
	for i, nd := range c.nodes {
		local := map[int]Replica{}
		for s, rp := range nd.reps {
			local[s] = rp
		}
		pool := transport.NewPool(tcfg)
		t.Cleanup(func() { _ = pool.Close() })
		nd.router = New(Config{Self: nd.addr, Shards: shards, Replicas: c.replicas, Resolve: identity,
			Local: localOf(local), Pool: pool, Log: slog.New(slog.DiscardHandler)})
		srv := transport.NewServer(tcfg)
		dkvv1.RegisterInternalServiceServer(srv, internalAPI{r: nd.router})
		var swg sync.WaitGroup
		swg.Add(1)
		go func() { defer swg.Done(); _ = srv.Serve(lis[i]) }()
		t.Cleanup(func() { srv.Stop(); swg.Wait() })
	}
	return c
}

// leader waits until every replica of shard knows the same leader and
// returns that leader's node.
func (c *testCluster) leader(shard int) *testNode {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var lead *testNode
		agreed := true
		for _, nd := range c.nodes {
			rp := nd.reps[shard]
			if rp == nil {
				continue
			}
			if rp.Status().Role == raft.StateLeader {
				lead = nd
			}
			agreed = agreed && rp.Leader() != "" && rp.Leader() == c.nodes[0].reps[shard].Leader()
		}
		if lead != nil && agreed && lead.addr == c.nodes[0].reps[shard].Leader() {
			return lead
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("shard %d: replicas did not agree on a leader", shard)
	return nil
}

// In these tests node IDs are the nodes' addresses.
func identity(id string) string { return id }

func localOf(m map[int]Replica) func(int) Replica {
	return func(s int) Replica { return m[s] }
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func keyIn(shard int) []byte {
	for i := 0; ; i++ {
		k := fmt.Appendf(nil, "key-%d", i)
		if placement.ShardOf(k, shards) == shard {
			return k
		}
	}
}

func put(t *testing.T, r *Router, key, value string, reqID uint64) uint64 {
	t.Helper()
	resp, err := r.Put(ctx(t), &dkvv1.PutRequest{Key: []byte(key), Value: []byte(value), ClientId: "c", RequestId: reqID})
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return resp.GetVersion()
}

// TestRequestsReachTheLeader: every operation sent to any node, holding a
// replica or not, runs on the shard's leader and is replicated to every
// replica.
func TestRequestsReachTheLeader(t *testing.T) {
	c := startCluster(t, 4, 3) // node 3 holds no replica
	var reqID uint64
	for s := range shards {
		key := keyIn(s)
		for via, nd := range c.nodes {
			reqID++
			resp, err := nd.router.Put(ctx(t), &dkvv1.PutRequest{Key: key, Value: fmt.Append(nil, "v", via), ClientId: "c", RequestId: reqID})
			if err != nil {
				t.Fatalf("put shard %d via node %d: %v", s, via, err)
			}
			other := c.nodes[(via+1)%len(c.nodes)]
			got, err := other.router.Get(ctx(t), &dkvv1.GetRequest{Key: key})
			if err != nil || !got.GetFound() || string(got.GetValue()) != fmt.Sprint("v", via) || got.GetVersion() != resp.GetVersion() {
				t.Fatalf("get shard %d after put via node %d: %v %v", s, via, got, err)
			}
		}
		reqID++
		cas, err := c.nodes[3].router.CompareAndSet(ctx(t), &dkvv1.CompareAndSetRequest{Key: key, Value: []byte("cas"),
			ExpectedVersion: 1, ClientId: "c", RequestId: reqID})
		if err != nil || cas.GetSucceeded() || cas.GetVersion() == 0 {
			t.Fatalf("CAS with a wrong version: %v %v", cas, err)
		}
		reqID++
		del, err := c.nodes[3].router.Delete(ctx(t), &dkvv1.DeleteRequest{Key: key, ClientId: "c", RequestId: reqID})
		if err != nil || !del.GetFound() {
			t.Fatalf("delete via the node without replicas: %v %v", del, err)
		}
		// Every replica applies the delete.
		for i, nd := range c.nodes[:3] {
			got, err := nd.router.Get(ctx(t), &dkvv1.GetRequest{Key: key,
				Consistency: dkvv1.Consistency_CONSISTENCY_STALE, MinVersion: del.GetVersion()})
			if err != nil || got.GetFound() || got.GetAppliedIndex() < del.GetVersion() {
				t.Fatalf("replica on node %d after delete: %v %v", i, got, err)
			}
		}
	}
	if st := c.nodes[3].router.Stats(); st.Forwarded == 0 {
		t.Errorf("node without replicas forwarded nothing: %+v", st)
	}
}

// TestNotLeaderHintIsFollowed: a router that sends to a follower gets the
// leader's address back and retries there at once; afterwards it goes to
// the leader directly.
func TestNotLeaderHintIsFollowed(t *testing.T) {
	c := startCluster(t, 4, 3)
	l := c.leader(0)
	c.mu.Lock()
	slices.SortFunc(c.order, func(a, b string) int { // the leader last
		switch {
		case a == l.addr:
			return 1
		case b == l.addr:
			return -1
		}
		return 0
	})
	c.mu.Unlock()
	r := c.nodes[3].router
	put(t, r, string(keyIn(0)), "v", 1)
	if st := r.Stats(); st.NotLeader != 1 || st.Forwarded != 2 || st.Retries != 1 {
		t.Errorf("first request: %+v; want one NotLeader answer, two forwards, one retry", st)
	}
	put(t, r, string(keyIn(0)), "v", 2)
	if st := r.Stats(); st.NotLeader != 1 || st.Forwarded != 3 {
		t.Errorf("second request: %+v; want it sent straight to the cached leader", st)
	}
}

func TestForwardAnswersNotOwner(t *testing.T) {
	c := startCluster(t, 4, 3)
	l := c.leader(1)
	follower := c.nodes[0]
	if follower == l {
		follower = c.nodes[1]
	}
	req := &dkvv1.ForwardRequest{Shard: 1, Op: &dkvv1.ForwardRequest_Put{Put: &dkvv1.PutRequest{Key: keyIn(1)}}}
	resp, err := follower.router.Forward(ctx(t), req)
	if err != nil || resp.GetNotOwner().GetOwnerHint() != l.addr {
		t.Fatalf("Forward to a follower = %v, %v; want NotOwner pointing at the leader %s", resp, err, l.addr)
	}
	resp, err = c.nodes[3].router.Forward(ctx(t), req)
	if err != nil || resp.GetNotOwner().GetOwnerHint() != c.replicas(1)[0] {
		t.Fatalf("Forward to a node without a replica = %v, %v; want NotOwner pointing at the first replica", resp, err)
	}
	if _, err := l.router.Forward(ctx(t), &dkvv1.ForwardRequest{Shard: shards}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("out-of-range shard: %v", err)
	}
	if _, err := l.router.Forward(ctx(t), &dkvv1.ForwardRequest{Shard: 1}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("no operation: %v", err)
	}
}

// TestStaleReadIsServedLocally: a node holding a follower replica answers
// a stale read itself, while a linearizable read goes to the leader.
func TestStaleReadIsServedLocally(t *testing.T) {
	c := startCluster(t, 3, 3)
	l := c.leader(0)
	ver := put(t, l.router, string(keyIn(0)), "v", 1)
	var f *testNode
	for _, nd := range c.nodes {
		if nd != l {
			f = nd
		}
	}
	before := f.router.Stats().Forwarded
	got, err := f.router.Get(ctx(t), &dkvv1.GetRequest{Key: keyIn(0), Consistency: dkvv1.Consistency_CONSISTENCY_STALE, MinVersion: ver})
	if err != nil || string(got.GetValue()) != "v" {
		t.Fatalf("stale read: %v %v", got, err)
	}
	if n := f.router.Stats().Forwarded - before; n != 0 {
		t.Errorf("stale read was forwarded %d times", n)
	}
	if _, err := f.router.Get(ctx(t), &dkvv1.GetRequest{Key: keyIn(0)}); err != nil {
		t.Fatal(err)
	}
	if f.router.Stats().Forwarded == before {
		t.Error("linearizable read on a follower node was not sent to the leader")
	}
}

// TestLeaderErrorsAreNotRetried: an answer from the leader that is not
// about routing goes straight back to the client.
func TestLeaderErrorsAreNotRetried(t *testing.T) {
	c := startCluster(t, 3, 3)
	c.leader(0)
	r := c.nodes[0].router
	put(t, r, string(keyIn(0)), "v", 5) // also teaches r the leader
	before := r.Stats().Retries
	_, err := r.Put(ctx(t), &dkvv1.PutRequest{Key: keyIn(0), Value: []byte("old"), ClientId: "c", RequestId: 4})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("request older than the session's latest: %v", err)
	}
	if n := r.Stats().Retries - before; n != 0 {
		t.Errorf("retried %d times", n)
	}
}

// TestDeadReplicasFailWithinDeadline: requests for a shard none of whose
// replicas answer are retried with backoff but return when the caller's
// deadline expires, or after the attempt limit without one.
func TestDeadReplicasFailWithinDeadline(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := lis.Addr().String()
	_ = lis.Close()
	pool := transport.NewPool(tcfg)
	defer func() { _ = pool.Close() }()
	r := New(Config{Self: "127.0.0.1:1", Shards: shards, Pool: pool, Log: slog.New(slog.DiscardHandler),
		Replicas: func(int) []string { return []string{dead} }, Resolve: identity, Local: localOf(nil)})
	c, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := r.Put(c, &dkvv1.PutRequest{Key: []byte("k"), Value: []byte("v")})
	elapsed := time.Since(start)
	if code := status.Code(err); code != codes.DeadlineExceeded && code != codes.Unavailable {
		t.Fatalf("dead replicas: %v", err)
	}
	if elapsed > time.Second {
		t.Errorf("took %v with a 400ms deadline", elapsed)
	}
	if r.Stats().Retries == 0 {
		t.Error("no retries before giving up")
	}
	start = time.Now()
	if _, err := r.Put(context.Background(), &dkvv1.PutRequest{Key: []byte("k"), Value: []byte("v")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("no deadline: %v", err)
	}
	t.Logf("gave up after %d attempts in %v", maxAttempts, time.Since(start).Round(time.Millisecond))
}
