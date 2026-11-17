package integration

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/meta"
	"github.com/naveenk2608/distributed-database/internal/node/nodetest"
	"github.com/naveenk2608/distributed-database/pkg/client"
)

// placed reports whether every group (the shards and the meta group) has
// a leader, no learner, and voters satisfying ok.
func placed(c *nodetest.Cluster, shards int, ok func(shard int, voters []string) bool) bool {
	groups := c.Groups()
	if len(groups) != shards+1 {
		return false
	}
	for shard, g := range groups {
		if len(g.GetLearners()) > 0 || !ok(shard, g.GetVoters()) {
			return false
		}
	}
	return true
}

func writeKeys(t *testing.T, c *nodetest.Cluster, via, n int, prefix string) map[string]uint64 {
	t.Helper()
	kv := c.KV(via)
	acked := map[string]uint64{}
	for k := range n {
		key := fmt.Sprintf("%s/%05d", prefix, k)
		resp, err := kv.Put(reqCtx(t, 10*time.Second), &dkvv1.PutRequest{Key: []byte(key), Value: []byte(key)})
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		acked[key] = resp.GetVersion()
	}
	return acked
}

func checkKeys(t *testing.T, c *nodetest.Cluster, via int, acked map[string]uint64) {
	t.Helper()
	kv := c.KV(via)
	for key, ver := range acked {
		resp, err := kv.Get(reqCtx(t, 10*time.Second), &dkvv1.GetRequest{Key: []byte(key)})
		if err != nil || string(resp.GetValue()) != key || resp.GetVersion() < ver {
			t.Fatalf("acknowledged %s@%d read via node %d: %v, %v", key, ver, via, resp, err)
		}
	}
}

func nodeState(t *testing.T, c *nodetest.Cluster, observer int, id string) dkvv1.NodeState {
	t.Helper()
	resp, err := c.Status(observer)
	if err != nil {
		return dkvv1.NodeState_NODE_STATE_UNSPECIFIED
	}
	for _, m := range resp.GetMembers() {
		if m.GetNode().GetNodeId() == id {
			return m.GetNodeState()
		}
	}
	return dkvv1.NodeState_NODE_STATE_UNSPECIFIED
}

// TestDeadNodeReplicasAreRecreated is a Phase 7 gate check: a node down
// past liveness.dead_after is declared dead, its replicas are recreated on
// the other nodes, and every group returns to RF voters, all of them live.
// No acknowledged write is lost.
func TestDeadNodeReplicasAreRecreated(t *testing.T) {
	const shards, deadAfter = 8, 1500 * time.Millisecond
	c := nodetest.Start(t, 4, func(_ int, cfg *config.Config) {
		cfg.Cluster.Shards = shards
		cfg.Liveness.DeadAfter = deadAfter
	})
	nodetest.WaitFor(t, 15*time.Second, "every group to elect a leader", c.Settled)
	acked := writeKeys(t, c, 0, 100, "before")
	victim := c.Members[3].NodeID()
	held := 0
	for _, g := range c.Groups() {
		if slices.Contains(g.GetVoters(), victim) {
			held++
		}
	}
	c.Stop(3)
	stopped := time.Now()
	took := nodetest.WaitFor(t, 60*time.Second, "every group back to 3 live voters", func() bool {
		return placed(c, shards, func(_ int, voters []string) bool {
			return len(voters) == 3 && !slices.Contains(voters, victim)
		})
	})
	t.Logf("node %s (voter in %d of %d groups) stopped; every group had 3 live voters again %v later (dead_after %v)",
		victim, held, shards+1, took.Round(time.Millisecond), deadAfter)
	if st := nodeState(t, c, 0, victim); st != dkvv1.NodeState_NODE_STATE_DEAD {
		t.Errorf("the stopped node is %v, want dead", st)
	}
	if time.Since(stopped) < deadAfter {
		t.Errorf("replicas moved after %v, before the dead timeout %v", time.Since(stopped), deadAfter)
	}
	// The descriptors (used for routing) caught up too.
	nodetest.WaitFor(t, 10*time.Second, "descriptors to drop the dead node", func() bool {
		sm, err := c.ShardMap(0)
		if err != nil {
			return false
		}
		for _, s := range sm.GetShards() {
			if slices.Contains(s.GetVoterIds(), victim) || len(s.GetVoterIds()) != 3 {
				return false
			}
		}
		return true
	})
	checkKeys(t, c, 1, acked)
	writeKeys(t, c, 2, 20, "after")
}

// TestWipedNodeRejoinsThroughSnapshots is a Phase 7 gate check: a node
// whose disk is wiped restarts with a new identity, notices that the
// cluster exists without it, joins, and receives its replicas through
// snapshots (the groups' logs were compacted), while its old identity is
// declared dead and removed.
func TestWipedNodeRejoinsThroughSnapshots(t *testing.T) {
	const shards = 4
	c := nodetest.Start(t, 3, func(_ int, cfg *config.Config) {
		cfg.Cluster.Shards = shards
		cfg.Raft.CompactEntries = 50
		cfg.Liveness.DeadAfter = 1500 * time.Millisecond
	})
	nodetest.WaitFor(t, 15*time.Second, "every group to elect a leader", c.Settled)
	acked := writeKeys(t, c, 0, 400, "k")
	nodetest.WaitFor(t, 10*time.Second, "every shard's log to be compacted", func() bool {
		groups := c.Groups()
		for s := range shards {
			if groups[s].GetFirstIndex() <= 2 {
				return false
			}
		}
		return true
	})
	old := c.Members[2].NodeID()
	c.Stop(2)
	c.Wipe(2)
	c.Restart(2)
	fresh := c.Members[2].NodeID()
	if fresh == old {
		t.Fatal("a wiped node kept its ID")
	}
	took := nodetest.WaitFor(t, 60*time.Second, "the rejoined node to hold every group again", func() bool {
		return placed(c, shards, func(_ int, voters []string) bool {
			return len(voters) == 3 && slices.Contains(voters, fresh) && !slices.Contains(voters, old)
		})
	})
	resp, err := c.NodeStatus(2)
	if err != nil {
		t.Fatal(err)
	}
	installed := int64(0)
	for _, r := range resp.GetReplicas() {
		installed += r.GetSnapshotsInstalled()
	}
	t.Logf("wiped node rejoined as %s (was %s); holds every group again after %v, having installed %d snapshots",
		fresh, old, took.Round(time.Millisecond), installed)
	if installed == 0 {
		t.Error("the rejoined node caught up without any snapshot")
	}
	// Its own replicas serve the data.
	kv := c.KV(2)
	for key, ver := range acked {
		r, err := kv.Get(reqCtx(t, 10*time.Second), &dkvv1.GetRequest{Key: []byte(key),
			Consistency: dkvv1.Consistency_CONSISTENCY_STALE, MinVersion: ver})
		if err != nil || string(r.GetValue()) != key {
			t.Fatalf("rejoined node's replica: %s: %v, %v", key, r, err)
		}
	}
}

// TestAddedNodeTakesItsShare is a Phase 7 gate check: a node that joins a
// running cluster receives its share of replicas and leaderships. Writers
// run throughout; every write must succeed (the client retries a leader
// that is handing over), and the slowest writes, the pauses of leadership
// transfers, are reported.
func TestAddedNodeTakesItsShare(t *testing.T) {
	const shards = 8
	c := nodetest.Start(t, 3, func(_ int, cfg *config.Config) { cfg.Cluster.Shards = shards })
	nodetest.WaitFor(t, 15*time.Second, "every group to elect a leader", c.Settled)

	var addrs []string
	for _, m := range c.Members {
		addrs = append(addrs, m.PublicAddr)
	}
	cl, err := client.New(addrs...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Close() }()
	var mu sync.Mutex
	var lat []time.Duration
	acked := map[string]uint64{}
	failed := 0
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
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
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				ver, err := cl.Put(ctx, []byte(key), []byte(key))
				cancel()
				mu.Lock()
				if err != nil {
					failed++
				} else {
					acked[key] = ver
					lat = append(lat, time.Since(start))
				}
				mu.Unlock()
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)
	idx := c.Add()
	added := c.Members[idx].NodeID()
	var ids []string
	for _, m := range c.Members {
		ids = append(ids, m.NodeID())
	}
	want := meta.InitialLayout(ids, shards, 3, 3)
	took := nodetest.WaitFor(t, 90*time.Second, "the placement to include the new node", func() bool {
		return placed(c, shards, func(shard int, voters []string) bool {
			return slices.Equal(slices.Sorted(slices.Values(voters)), want[shard])
		})
	})
	// Leader balancing may still be moving leaders; let it settle.
	leaders := map[string]int{}
	nodetest.WaitFor(t, 30*time.Second, "leaders to be balanced", func() bool {
		leaders = map[string]int{}
		for _, g := range c.Groups() {
			leaders[g.GetLeader()]++
		}
		most, least := 0, 1<<30
		for _, id := range ids {
			most, least = max(most, leaders[id]), min(least, leaders[id])
		}
		return most-least <= 2 && leaders[added] > 0
	})
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	held := 0
	for _, v := range want {
		if slices.Contains(v, added) {
			held++
		}
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration { return lat[int(float64(len(lat)-1)*p)] }
	slow := 0
	for _, l := range lat {
		if l > 100*time.Millisecond {
			slow++
		}
	}
	t.Logf("new node %s holds %d of %d groups after %v; leaders per node %v", added, held, shards+1,
		took.Round(time.Millisecond), leaders)
	t.Logf("%d writes during the rebalance, %d failed; latency p50 %v p99 %v max %v; %d writes over 100ms",
		len(lat), failed, pct(0.5).Round(time.Microsecond), pct(0.99).Round(time.Microsecond),
		lat[len(lat)-1].Round(time.Millisecond), slow)
	if failed > 0 {
		t.Errorf("%d writes failed during the rebalance", failed)
	}
	if held == 0 {
		t.Error("the new node holds no group")
	}
	if maxLat := lat[len(lat)-1]; maxLat > 2*time.Second {
		t.Errorf("a write took %v: more than a brief leadership-transfer pause", maxLat)
	}
	checkKeys(t, c, idx, acked)
}

// TestMetaQuorumLossStopsRebalancingNotData is a Phase 7 gate check: with
// the meta group unable to reach a quorum (its Raft traffic is dropped on
// every node), a dead data node is not replaced, but reads and writes to
// every shard continue on the cached routes. Once the meta group recovers,
// the dead node is replaced.
func TestMetaQuorumLossStopsRebalancingNotData(t *testing.T) {
	const shards = 8
	c := nodetest.Start(t, 4, func(_ int, cfg *config.Config) {
		cfg.Cluster.Shards = shards
		cfg.Liveness.DeadAfter = time.Second
	})
	nodetest.WaitFor(t, 15*time.Second, "every group to elect a leader", c.Settled)
	writeKeys(t, c, 0, 20, "warm")
	isMeta := func(shard int) bool { return shard == meta.Shard }
	for _, m := range c.Members {
		m.Node().SetRaftFilter(isMeta)
	}
	nodetest.WaitFor(t, 10*time.Second, "the meta group to lose its leader", func() bool {
		_, ok := c.Groups()[meta.Shard]
		return !ok
	})
	victim := c.Members[3].NodeID()
	before := map[int][]string{}
	for s, g := range c.Groups() {
		before[s] = g.GetVoters()
	}
	c.Stop(3)
	// Three dead timeouts with no meta leader: nothing may move, and data
	// keeps flowing.
	deadline := time.Now().Add(3 * time.Second)
	acked := map[string]uint64{}
	for i := 0; time.Now().Before(deadline); i++ {
		maps.Copy(acked, writeKeys(t, c, i%3, 8, fmt.Sprintf("outage%d", i)))
		time.Sleep(100 * time.Millisecond)
	}
	checkKeys(t, c, 1, acked)
	for s, g := range c.Groups() {
		if s == meta.Shard {
			t.Fatal("the meta group elected a leader with its traffic dropped")
		}
		if slices.Contains(before[s], victim) && !slices.Contains(g.GetVoters(), victim) {
			t.Fatalf("shard %d was changed without a meta leader: %v -> %v", s, before[s], g.GetVoters())
		}
	}
	t.Logf("with the meta group down, %d writes and their reads succeeded and no group changed", len(acked))
	for _, m := range c.Members[:3] {
		m.Node().SetRaftFilter(nil)
	}
	took := nodetest.WaitFor(t, 60*time.Second, "the dead node to be replaced once the meta group recovers", func() bool {
		return placed(c, shards, func(_ int, voters []string) bool {
			return len(voters) == 3 && !slices.Contains(voters, victim)
		})
	})
	t.Logf("after the meta group recovered, the dead node was replaced in %v", took.Round(time.Millisecond))
}

// TestDecommissionDrainsNode: an operator decommissions a node; the
// rebalancer moves every replica off it while it keeps running, and then
// marks it decommissioned.
func TestDecommissionDrainsNode(t *testing.T) {
	const shards = 8
	c := nodetest.Start(t, 4, func(_ int, cfg *config.Config) { cfg.Cluster.Shards = shards })
	nodetest.WaitFor(t, 15*time.Second, "every group to elect a leader", c.Settled)
	acked := writeKeys(t, c, 0, 50, "d")
	target := c.Members[1].NodeID()
	cl, err := client.New(c.Members[0].PublicAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Close() }()
	if err := cl.Decommission(reqCtx(t, 10*time.Second), "no-such-node"); err == nil {
		t.Fatal("decommissioning an unknown node succeeded")
	}
	if err := cl.Decommission(reqCtx(t, 10*time.Second), target); err != nil {
		t.Fatal(err)
	}
	took := nodetest.WaitFor(t, 60*time.Second, "the node to be drained", func() bool {
		return placed(c, shards, func(_ int, voters []string) bool {
			return len(voters) == 3 && !slices.Contains(voters, target)
		}) && nodeState(t, c, 0, target) == dkvv1.NodeState_NODE_STATE_DECOMMISSIONED
	})
	t.Logf("node %s drained and decommissioned in %v", target, took.Round(time.Millisecond))
	checkKeys(t, c, 2, acked)
	// The drained node deletes its replicas once their groups' leaders
	// confirm it is no longer a member.
	took = nodetest.WaitFor(t, 30*time.Second, "the drained node to delete its replicas", func() bool {
		resp, err := c.NodeStatus(1)
		return err == nil && len(resp.GetReplicas()) == 0
	})
	t.Logf("the drained node deleted its replicas %v later", took.Round(time.Millisecond))
}
