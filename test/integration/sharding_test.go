package integration

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/node"
	"github.com/naveenk2608/distributed-database/internal/node/nodetest"
	"github.com/naveenk2608/distributed-database/internal/placement"
)

func shardCluster(t *testing.T, n, shards, rf int) *nodetest.Cluster {
	t.Helper()
	c := nodetest.Start(t, n, func(_ int, cfg *config.Config) {
		cfg.Cluster.Shards, cfg.Cluster.ReplicationFactor = shards, rf
		cfg.Storage.MemtableBytes = 256 << 10
	})
	nodetest.WaitFor(t, 10*time.Second, "cluster to form", func() bool { return allUp(c) })
	nodetest.WaitFor(t, 10*time.Second, "every shard to elect a leader", c.Settled)
	return c
}

func reqCtx(t *testing.T, d time.Duration) context.Context {
	c, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return c
}

// lastSeqs returns, per node, each shard's local last sequence number (-1
// if the node holds no replica of it).
func lastSeqs(t *testing.T, c *nodetest.Cluster, shards int) [][]int64 {
	t.Helper()
	out := make([][]int64, len(c.Members))
	for i := range c.Members {
		sm, err := c.ShardMap(i)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = make([]int64, shards)
		for _, info := range sm.GetShards() {
			out[i][info.GetShard()] = -1
			if info.GetLocal() {
				out[i][info.GetShard()] = int64(info.GetLastSeq())
			}
		}
	}
	return out
}

// TestAnyNodeAcceptsAnyKey is the Phase 5 gate check that any node accepts
// any key and the request reaches the shard's replicas, updated for Raft.
// Four nodes, 16 shards, RF 2: every key is written through a different
// node in turn and then read back through every node. Afterwards every
// replica of each shard, and no other node, has applied every write to
// that shard: a stale read served by each replica's own node sees it.
func TestAnyNodeAcceptsAnyKey(t *testing.T) {
	const n, shards, rf, keys = 4, 16, 2, 400
	c := shardCluster(t, n, shards, rf)
	var addrs []string
	for _, m := range c.Members {
		addrs = append(addrs, m.InternalAddr)
	}
	want, err := placement.Compute(addrs, shards, rf)
	if err != nil {
		t.Fatal(err)
	}
	for i := range c.Members {
		sm, err := c.ShardMap(i)
		if err != nil {
			t.Fatal(err)
		}
		for _, info := range sm.GetShards() {
			s := int(info.GetShard())
			if !slices.Equal(info.GetReplicas(), want.ReplicaSet(s)) {
				t.Fatalf("node %d places shard %d on %v; placement computes %v", i, s, info.GetReplicas(), want.ReplicaSet(s))
			}
			if info.GetLocal() != slices.Contains(want.ReplicaSet(s), addrs[i]) {
				t.Fatalf("node %d local=%v for shard %d with replicas %v", i, info.GetLocal(), s, want.ReplicaSet(s))
			}
		}
	}

	kv := make([]dkvv1.KVServiceClient, n)
	for i := range kv {
		kv[i] = c.KV(i)
	}
	versions := make([]uint64, keys)
	writes := make([]int, shards)
	for k := range keys {
		key := []byte(fmt.Sprintf("user/%05d", k))
		resp, err := kv[k%n].Put(reqCtx(t, 5*time.Second), &dkvv1.PutRequest{Key: key, Value: []byte(fmt.Sprint(k))})
		if err != nil {
			t.Fatalf("put %s via node %d: %v", key, k%n, err)
		}
		versions[k] = resp.GetVersion()
		writes[placement.ShardOf(key, shards)]++
	}
	for k := range keys {
		key := []byte(fmt.Sprintf("user/%05d", k))
		s := placement.ShardOf(key, shards)
		for i := range n {
			resp, err := kv[i].Get(reqCtx(t, 5*time.Second), &dkvv1.GetRequest{Key: key})
			if err != nil || !resp.GetFound() || string(resp.GetValue()) != fmt.Sprint(k) || resp.GetVersion() != versions[k] {
				t.Fatalf("get %s via node %d: %v, %v", key, i, resp, err)
			}
			if !slices.Contains(want.ReplicaSet(s), addrs[i]) {
				continue
			}
			// A stale read is served by the node's own replica.
			resp, err = kv[i].Get(reqCtx(t, 5*time.Second), &dkvv1.GetRequest{Key: key,
				Consistency: dkvv1.Consistency_CONSISTENCY_STALE, MinVersion: versions[k]})
			if err != nil || string(resp.GetValue()) != fmt.Sprint(k) || resp.GetAppliedIndex() < versions[k] {
				t.Fatalf("stale get %s from node %d's replica of shard %d: %v, %v", key, i, s, resp, err)
			}
		}
	}
	seqs := lastSeqs(t, c, shards)
	for s := range shards {
		for i, addr := range addrs {
			if !slices.Contains(want.ReplicaSet(s), addr) && seqs[i][s] != -1 {
				t.Errorf("shard %d: node %d holds a replica it should not", s, i)
			}
		}
	}
	busy := 0
	for _, w := range writes {
		if w > 0 {
			busy++
		}
	}
	t.Logf("%d keys over %d shards (%d with writes), written through 4 nodes, read through each and from every replica", keys, shards, busy)
}

// TestMinorityDownKeepsEveryShardWorking: with one of three nodes stopped,
// every shard still has a majority, so every key keeps working through the
// survivors once the stopped node's shards have elected new leaders.
func TestMinorityDownKeepsEveryShardWorking(t *testing.T) {
	const n, shards = 3, 12
	c := shardCluster(t, n, shards, 3)
	c.Stop(2)
	kv := c.KV(0)
	for s := range shards {
		key := keyInShard(s, shards)
		if _, err := kv.Put(reqCtx(t, 5*time.Second), &dkvv1.PutRequest{Key: key, Value: []byte("v")}); err != nil {
			t.Errorf("shard %d with one replica down: %v", s, err)
		}
	}
}

// TestMajorityDownFailsWithinDeadline: with two of three nodes stopped no
// shard has a majority. Writes fail within the client's deadline instead
// of hanging, and nothing is acknowledged.
func TestMajorityDownFailsWithinDeadline(t *testing.T) {
	const n, shards = 3, 4
	c := shardCluster(t, n, shards, 3)
	c.Stop(1)
	c.Stop(2)
	kv := c.KV(0)
	for s := range shards {
		start := time.Now()
		_, err := kv.Put(reqCtx(t, 500*time.Millisecond), &dkvv1.PutRequest{Key: keyInShard(s, shards), Value: []byte("v")})
		if code := status.Code(err); code != codes.Unavailable && code != codes.DeadlineExceeded {
			t.Errorf("shard %d without a majority: %v", s, err)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Errorf("shard %d: failed only after %v with a 500ms deadline", s, took)
		}
	}
}

func keyInShard(s, shards int) []byte {
	for k := 0; ; k++ {
		if key := []byte(fmt.Sprintf("k%d", k)); placement.ShardOf(key, shards) == s {
			return key
		}
	}
}

func TestShardDataSurvivesRestart(t *testing.T) {
	c := shardCluster(t, 3, 8, 3)
	kv := c.KV(1)
	for k := range 50 {
		if _, err := kv.Put(reqCtx(t, 5*time.Second), &dkvv1.PutRequest{Key: []byte(fmt.Sprint("k", k)), Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range c.Members {
		c.Stop(i)
	}
	for i := range c.Members {
		c.Restart(i)
	}
	nodetest.WaitFor(t, 10*time.Second, "cluster to re-form", func() bool { return allUp(c) })
	kv = c.KV(2)
	for k := range 50 {
		resp, err := kv.Get(reqCtx(t, 5*time.Second), &dkvv1.GetRequest{Key: []byte(fmt.Sprint("k", k))})
		if err != nil || !resp.GetFound() {
			t.Fatalf("k%d after restarting every node: %v, %v", k, resp, err)
		}
	}
}

func TestClusterSettingsAreFixedAtBootstrap(t *testing.T) {
	c := shardCluster(t, 3, 8, 3)
	c.Stop(0)
	cfg := c.Members[0].Config
	cfg.Cluster.Shards = 16
	if _, err := node.Open(&cfg, slogDiscard()); err == nil || !strings.Contains(err.Error(), "cannot change once data exists") {
		t.Fatalf("reopening with another shard count: %v", err)
	}
	cfg = c.Members[0].Config
	cfg.Cluster.InternalAddr, cfg.Cluster.Peers = "", nil // standalone on cluster data
	if _, err := node.Open(&cfg, slogDiscard()); err == nil || !strings.Contains(err.Error(), "belongs to a cluster") {
		t.Fatalf("standalone start on cluster data: %v", err)
	}
}

func TestPlacementMismatchIsVisible(t *testing.T) {
	c := nodetest.Start(t, 3, func(i int, cfg *config.Config) {
		cfg.Cluster.Shards = 8
		if i == 2 {
			cfg.Cluster.Shards = 16
		}
	})
	nodetest.WaitFor(t, 10*time.Second, "node 0 to flag node 2's placement", func() bool {
		resp, err := c.Status(0)
		if err != nil {
			return false
		}
		for _, m := range resp.GetMembers() {
			if m.GetAddr() == c.Members[2].InternalAddr {
				return strings.Contains(m.GetLastError(), "placement differs")
			}
		}
		return false
	})
}
