package node_test

import (
	"context"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/meta"
	"github.com/naveenk2608/distributed-database/internal/node/nodetest"
)

func httpGet(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx // a test against a local server
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, b
}

func getJSON(t *testing.T, url string, m proto.Message) {
	t.Helper()
	code, b := httpGet(t, url)
	if code != http.StatusOK {
		t.Fatalf("GET %s: %d %s", url, code, b)
	}
	if err := protojson.Unmarshal(b, m); err != nil {
		t.Fatalf("GET %s: %v in %s", url, err, b)
	}
}

func adminClient(t *testing.T, c *nodetest.Cluster, i int) dkvv1.AdminServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(c.Members[i].PublicAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return dkvv1.NewAdminServiceClient(conn)
}

// TestAdminHTTPAPI: every node serves its status, shard map, statistics
// (request counters, Raft and engine state per replica), events and
// Prometheus metrics as read-only HTTP; elections appear as events on the
// node that won them.
func TestAdminHTTPAPI(t *testing.T) {
	c := nodetest.StartWith(t, 3, nodetest.Options{Admin: true}, func(_ int, cfg *config.Config) { cfg.Cluster.Shards = 2 })
	nodetest.WaitFor(t, 30*time.Second, "every group to elect a leader", c.Settled)
	kv := c.KV(0)
	const puts = 25
	for i := range puts {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := kv.Put(ctx, &dkvv1.PutRequest{Key: []byte{byte('a' + i)}, Value: []byte("v")})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}

	elected := map[int32]bool{}
	for i, m := range c.Members {
		base := "http://" + m.AdminAddr
		var st dkvv1.StatsResponse
		getJSON(t, base+"/v1/stats", &st)
		if st.GetNodeId() != m.NodeID() || len(st.GetReplicas()) == 0 || len(st.GetLatencyBoundsSeconds()) == 0 {
			t.Fatalf("node %d stats: %v", i, &st)
		}
		for _, r := range st.GetReplicas() {
			if r.GetRaft().GetTerm() == 0 || r.GetEngine() == nil {
				t.Fatalf("node %d replica stats: %v", i, r)
			}
		}
		if i == 0 {
			var put *dkvv1.OpStats
			for _, o := range st.GetOps() {
				if o.GetOp() == "put" {
					put = o
				}
			}
			if put.GetOk() != puts || put.GetLatencyCumulative()[len(put.GetLatencyCumulative())-1] != puts {
				t.Fatalf("node 0 counted puts %v; want %d", put, puts)
			}
		}
		var ev dkvv1.EventsResponse
		getJSON(t, base+"/v1/events?after=0&limit=100", &ev)
		for _, e := range ev.GetEvents() {
			if e.GetKind() == "leader_elected" && e.GetNode() == m.NodeID() && e.GetHasShard() {
				elected[e.GetShard()] = true
			}
		}
		var cs dkvv1.ClusterStatusResponse
		getJSON(t, base+"/v1/status", &cs)
		var sm dkvv1.ShardMapResponse
		getJSON(t, base+"/v1/shards", &sm)
		if cs.GetSelf().GetNodeId() != m.NodeID() || sm.GetShardCount() != 2 {
			t.Fatalf("node %d status %v, shard map %v", i, &cs, &sm)
		}
		code, metrics := httpGet(t, base+"/metrics")
		for _, want := range []string{"dkv_raft_term{shard=", "dkv_engine_tables{shard=", "dkv_request_duration_seconds_bucket{op=\"put\""} {
			if code != http.StatusOK || !strings.Contains(string(metrics), want) {
				t.Fatalf("node %d /metrics (%d) lacks %s", i, code, want)
			}
		}
		if code, _ := httpGet(t, base+"/v1/events?after=x"); code != http.StatusBadRequest {
			t.Fatalf("bad after: %d", code)
		}
		if code, b := httpGet(t, base+"/healthz"); code != http.StatusOK || string(b) != "ok\n" {
			t.Fatalf("healthz: %d %q", code, b)
		}
		if code, _ := httpGet(t, base+"/v1/nothing"); code != http.StatusNotFound {
			t.Fatalf("unknown path: %d", code)
		}
	}
	var commits, syncs uint64
	var userBytes, raftLogBytes int64
	for _, m := range c.Members {
		var st dkvv1.StatsResponse
		getJSON(t, "http://"+m.AdminAddr+"/v1/stats", &st)
		for _, o := range st.GetInternal() {
			switch o.GetOp() {
			case "commit":
				commits += o.GetOk()
			case "raft_log_sync":
				syncs += o.GetOk()
			}
		}
		for _, r := range st.GetReplicas() {
			userBytes += r.GetEngine().GetUserBytes()
			raftLogBytes += r.GetRaftLogBytes()
			if r.GetEngine().GetWalBytes() != 0 {
				t.Fatalf("replica %d wrote %d bytes to an engine log on the single-log path", r.GetRaft().GetShard(), r.GetEngine().GetWalBytes())
			}
		}
	}
	if commits < puts || syncs == 0 || userBytes == 0 || raftLogBytes < userBytes {
		t.Fatalf("internal latencies: %d commits for %d puts, %d raft log syncs; %d user bytes, %d Raft log bytes",
			commits, puts, syncs, userBytes, raftLogBytes)
	}
	for _, shard := range []int32{0, 1, meta.Shard} {
		if !elected[shard] {
			t.Errorf("no node recorded winning the election of shard %d: %v", shard, elected)
		}
	}
}

// TestTransferLeaderAndDrain: transfer-leader moves one shard's leadership
// to the voter asked for; drain empties a node of leaderships and keeps
// them away, undrain lets it lead again.
func TestTransferLeaderAndDrain(t *testing.T) {
	c := nodetest.StartWith(t, 3, nodetest.Options{Admin: true}, func(_ int, cfg *config.Config) { cfg.Cluster.Shards = 4 })
	nodetest.WaitFor(t, 30*time.Second, "every group to elect a leader", c.Settled)
	nodetest.WaitFor(t, 30*time.Second, "the meta group to record every node", func() bool {
		st, err := c.Status(0)
		known := 0
		for _, m := range st.GetMembers() {
			if m.GetNodeState() == dkvv1.NodeState_NODE_STATE_ALIVE {
				known++
			}
		}
		return err == nil && known == 3
	})
	admin := adminClient(t, c, 0)
	ctx := func() context.Context {
		cx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)
		return cx
	}

	// Leader balancing moves a leadership, every rebalance round, off a node
	// that leads two more groups than another, so it may race a transfer or
	// undo one (transfer-leader is a nudge). The transfer below starts from
	// a balance that has held for three rounds and moves a leadership from a
	// node leading the most groups to the node leading the fewest: the
	// result is as balanced, so the rebalancer has nothing to do.
	const groups = 5 // four data shards and the meta group
	led := func(leaders map[int]string) map[string]int {
		n := map[string]int{}
		for _, m := range c.Members {
			n[m.NodeID()] = 0
		}
		for _, id := range leaders {
			n[id]++
		}
		return n
	}
	round := c.Members[0].Config.Rebalance.Interval
	var leaders map[int]string // group -> leader ID
	since := time.Now()
	nodetest.WaitFor(t, 30*time.Second, "leadership to stay balanced for three rebalance rounds", func() bool {
		now := map[int]string{}
		for shard, r := range c.Groups() {
			now[shard] = r.GetLeader()
		}
		if len(now) != groups || !maps.Equal(now, leaders) {
			leaders, since = now, time.Now()
			return false
		}
		n := slices.Collect(maps.Values(led(leaders)))
		return slices.Max(n)-slices.Min(n) <= 1 && time.Since(since) >= 3*round
	})
	n := led(leaders)
	most := slices.Max(slices.Collect(maps.Values(n)))
	shard := slices.IndexFunc([]int{0, 1, 2, 3}, func(s int) bool { return n[leaders[s]] == most })
	var to string
	for _, id := range slices.Sorted(maps.Keys(n)) {
		if to == "" || n[id] < n[to] {
			to = id
		}
	}
	sm, err := c.ShardMap(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range sm.GetShards() {
		if int(s.GetShard()) == shard && !slices.Contains(s.GetVoterIds(), to) {
			t.Fatalf("%s, leading the fewest groups, is not a voter of shard %d (voters %v)", to, shard, s.GetVoterIds())
		}
	}
	if shard < 0 || leaders[shard] == to {
		t.Fatalf("no data shard to move from a node leading the most groups to one leading the fewest: leaders %v", leaders)
	}
	if _, err := admin.TransferLeader(ctx(), &dkvv1.AdminServiceTransferLeaderRequest{Shard: int32(shard), NodeId: to}); err != nil {
		t.Fatal(err)
	}
	// The transfer returns once the old leader sees the new one, which may
	// not yet report itself (see nodetest.Cluster.WaitLeader).
	if i, _ := c.WaitLeader(shard, 10*time.Second); c.Members[i].NodeID() != to {
		t.Fatalf("after transfer-leader to %s, shard %d is led by %s", to, shard, c.Members[i].NodeID())
	}
	if _, err := admin.TransferLeader(ctx(), &dkvv1.AdminServiceTransferLeaderRequest{Shard: 0, NodeId: "nobody"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("transfer to a non-voter: %v", err)
	}
	if _, err := admin.TransferLeader(ctx(), &dkvv1.AdminServiceTransferLeaderRequest{Shard: 99, NodeId: to}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("transfer of a shard that does not exist: %v", err)
	}

	target := c.Members[1].NodeID()
	leads := func(id string) int {
		n := 0
		for shard := range 4 {
			if i, _ := c.Leader(shard); i >= 0 && c.Members[i].NodeID() == id {
				n++
			}
		}
		if g := c.Groups()[meta.Shard]; g != nil && g.GetLeader() == id {
			n++
		}
		return n
	}
	if _, err := admin.Drain(ctx(), &dkvv1.DrainRequest{NodeId: target, Drain: true}); err != nil {
		t.Fatal(err)
	}
	nodetest.WaitFor(t, 30*time.Second, "the drained node to lead nothing", func() bool { return leads(target) == 0 })
	time.Sleep(time.Second) // five rebalance rounds: the leaderships stay away
	if n := leads(target); n != 0 {
		t.Fatalf("drained node leads %d groups again", n)
	}
	if _, err := admin.Drain(ctx(), &dkvv1.DrainRequest{NodeId: target, Drain: false}); err != nil {
		t.Fatal(err)
	}
	nodetest.WaitFor(t, 30*time.Second, "the undrained node to lead again", func() bool { return leads(target) > 0 })
	if _, err := admin.Drain(ctx(), &dkvv1.DrainRequest{NodeId: "nobody", Drain: true}); status.Code(err) != codes.NotFound {
		t.Fatalf("drain of an unknown node: %v", err)
	}
	if !slices.Contains(sm.GetShards()[0].GetVoterIds(), to) {
		t.Fatal("setup: the transferee was not a voter")
	}
}
