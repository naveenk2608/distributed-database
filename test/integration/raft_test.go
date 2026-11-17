package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/node/nodetest"
	"github.com/naveenk2608/distributed-database/internal/placement"
	"github.com/naveenk2608/distributed-database/pkg/client"
)

func publicAddrs(c *nodetest.Cluster, except int) []string {
	var out []string
	for i, m := range c.Members {
		if i != except {
			out = append(out, m.PublicAddr)
		}
	}
	return out
}

// TestLeaderFailover is the Phase 6 gate check that a three-node RF=3
// cluster keeps serving after a leader is killed, elects a new leader
// within about two election timeouts, and loses no acknowledged write.
// Writers run throughout; the node leading shard 0 is stopped mid-stream.
// Every shard that node led must get a new leader in time, and every write
// any client saw acknowledged must be readable afterwards, from the
// survivors and, once it rejoins, from the stopped node too.
func TestLeaderFailover(t *testing.T) {
	const shards, writers = 4, 4
	c := shardCluster(t, 3, shards, 3)
	tick := c.Members[0].Config.Raft.TickInterval
	electionTimeout := time.Duration(c.Members[0].Config.Raft.ElectionTicks) * tick
	old, _ := c.Leader(0)
	if old < 0 {
		t.Fatal("shard 0 has no leader")
	}
	var led []int // every shard the victim leads
	for s := range shards {
		if l, _ := c.Leader(s); l == old {
			led = append(led, s)
		}
	}

	cl, err := client.New(publicAddrs(c, old)...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Close() }()
	var mu sync.Mutex
	acked := map[string]uint64{} // key -> acknowledged version
	failed := 0
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("w%d/%06d", w, i)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				ver, err := cl.Put(ctx, []byte(key), []byte(key))
				cancel()
				mu.Lock()
				if err == nil {
					acked[key] = ver
				} else {
					failed++
				}
				mu.Unlock()
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)

	killed := time.Now()
	c.Stop(old)
	stopped := time.Since(killed)
	elected := map[int]time.Duration{}
	nodetest.WaitFor(t, 10*time.Second, "new leaders for the stopped node's shards", func() bool {
		for _, s := range led {
			if _, ok := elected[s]; ok {
				continue
			}
			if l, _ := c.Leader(s); l >= 0 { // only running members are asked
				elected[s] = time.Since(killed)
			}
		}
		return len(elected) == len(led)
	})
	time.Sleep(300 * time.Millisecond) // keep writing under the new leaders
	close(stop)
	wg.Wait()

	slowest := time.Duration(0)
	for _, d := range elected {
		slowest = max(slowest, d)
	}
	t.Logf("node %d (leader of shards %v) stopped in %v; new leaders after %v (slowest %v; election timeout %v-%v)",
		old, led, stopped.Round(time.Millisecond), elected, slowest.Round(time.Millisecond), electionTimeout, 2*electionTimeout)
	// The followers' timers start at the victim's last heartbeat; the
	// first to fire, within [timeout, 2*timeout), wins after one round of
	// pre-votes and votes. Allow a few ticks for those rounds and for
	// polling the shard maps.
	if limit := 2*electionTimeout + 10*tick; slowest > limit {
		t.Errorf("slowest new leader after %v; want within about two election timeouts (%v)", slowest, limit)
	}

	mu.Lock()
	defer mu.Unlock()
	perShard := make([]int, shards)
	for k := range acked {
		perShard[placement.ShardOf([]byte(k), shards)]++
	}
	t.Logf("%d writes acknowledged (per shard %v), %d failed", len(acked), perShard, failed)
	for _, s := range led {
		if perShard[s] == 0 {
			t.Errorf("no write to shard %d was acknowledged", s)
		}
	}
	check := func(via int) {
		kv := c.KV(via)
		for k, ver := range acked {
			resp, err := kv.Get(reqCtx(t, 5*time.Second), &dkvv1.GetRequest{Key: []byte(k)})
			if err != nil || !resp.GetFound() || string(resp.GetValue()) != k || resp.GetVersion() != ver {
				t.Fatalf("acknowledged write %s@%d read via node %d: %v, %v", k, ver, via, resp, err)
			}
		}
	}
	check((old + 1) % 3)
	c.Restart(old)
	nodetest.WaitFor(t, 10*time.Second, "the stopped node to rejoin", c.Settled)
	check(old)
	// The rejoined node's own replicas caught up as well.
	kv := c.KV(old)
	for k, ver := range acked {
		resp, err := kv.Get(reqCtx(t, 5*time.Second), &dkvv1.GetRequest{Key: []byte(k),
			Consistency: dkvv1.Consistency_CONSISTENCY_STALE, MinVersion: ver})
		if err != nil || string(resp.GetValue()) != k {
			t.Fatalf("rejoined node's replica: %s: %v, %v", k, resp, err)
		}
	}
}

// dropFirstResponse is a client interceptor that lets the first Put reach
// the server and then reports it as failed, as if the response had been
// lost on the way back. It records every Put it sees.
type dropFirstResponse struct {
	mu      sync.Mutex
	puts    []*dkvv1.PutRequest
	dropped *dkvv1.PutResponse
}

func (d *dropFirstResponse) intercept(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	put, ok := req.(*dkvv1.PutRequest)
	if !ok {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.puts = append(d.puts, put)
	if err == nil && d.dropped == nil {
		d.dropped = reply.(*dkvv1.PutResponse)
		return status.Error(codes.Unavailable, "test: response lost")
	}
	return err
}

// TestRetriedPutAppliedOnce is the Phase 6 gate check that a retried PUT
// is applied exactly once: when the response to an applied write is lost
// and the client retries, and when the retry reaches a new leader after
// the old one died.
func TestRetriedPutAppliedOnce(t *testing.T) {
	c := shardCluster(t, 3, 4, 3)

	t.Run("lost response", func(t *testing.T) {
		d := &dropFirstResponse{}
		cl, err := client.NewWithOptions(client.Options{Addrs: publicAddrs(c, -1),
			DialOptions: []grpc.DialOption{grpc.WithUnaryInterceptor(d.intercept)}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cl.Close() }()
		ver, err := cl.Put(reqCtx(t, 5*time.Second), []byte("once"), []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if len(d.puts) != 2 || d.puts[0].GetClientId() == "" ||
			d.puts[0].GetClientId() != d.puts[1].GetClientId() || d.puts[0].GetRequestId() != d.puts[1].GetRequestId() {
			t.Fatalf("puts sent: %v; want the same request twice", d.puts)
		}
		if ver != d.dropped.GetVersion() {
			t.Fatalf("the retry returned version %d, but the first try was applied at %d: applied twice", ver, d.dropped.GetVersion())
		}
		v, gotVer, err := cl.Get(reqCtx(t, 5*time.Second), []byte("once"))
		if err != nil || string(v) != "v" || gotVer != ver {
			t.Fatalf("after the retry: %q@%d, %v; want v@%d", v, gotVer, err, ver)
		}
	})

	t.Run("retry after leader change", func(t *testing.T) {
		key := keyInShard(1, 4)
		leader, _ := c.Leader(1)
		via := (leader + 1) % 3
		req := &dkvv1.PutRequest{Key: key, Value: []byte("first"), ClientId: "retry-test", RequestId: 1}
		first, err := c.KV(via).Put(reqCtx(t, 5*time.Second), req)
		if err != nil {
			t.Fatal(err)
		}
		c.Stop(leader)
		nodetest.WaitFor(t, 10*time.Second, "a new leader for shard 1", func() bool {
			l, _ := c.Leader(1)
			return l >= 0 && l != leader
		})
		retry := &dkvv1.PutRequest{Key: key, Value: []byte("retry"), ClientId: "retry-test", RequestId: 1}
		again, err := c.KV(via).Put(reqCtx(t, 5*time.Second), retry)
		if err != nil || again.GetVersion() != first.GetVersion() {
			t.Fatalf("retry via the new leader: %v, %v; want the original version %d", again, err, first.GetVersion())
		}
		got, err := c.KV(via).Get(reqCtx(t, 5*time.Second), &dkvv1.GetRequest{Key: key})
		if err != nil || string(got.GetValue()) != "first" || got.GetVersion() != first.GetVersion() {
			t.Fatalf("after the retry: %v, %v; want the first write only", got, err)
		}
		// Once the session has moved on, an older request is refused
		// rather than applied.
		_, err = c.KV(via).Put(reqCtx(t, 5*time.Second), &dkvv1.PutRequest{Key: key, Value: []byte("y"), ClientId: "retry-test", RequestId: 2})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.KV(via).Put(reqCtx(t, 5*time.Second), &dkvv1.PutRequest{Key: key, Value: []byte("z"), ClientId: "retry-test", RequestId: 1})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("a request older than the session's latest: %v; want FailedPrecondition", err)
		}
	})
}
