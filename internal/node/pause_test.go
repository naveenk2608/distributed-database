package node_test

import (
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/fault"
	"github.com/naveenk2608/distributed-database/internal/node/nodetest"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// TestPausedLeaderCannotNoticeLostQuorum: a leader whose Raft clock is
// stopped with PauseTicks, and cut off from its peers, still believes it
// leads: CheckQuorum runs on ticks. The others elect a new leader at a
// higher term. Once its clock runs again it notices and steps down, still
// cut off.
func TestPausedLeaderCannotNoticeLostQuorum(t *testing.T) {
	inj := fault.New(1)
	c := nodetest.StartWith(t, 3, nodetest.Options{Faults: inj}, func(_ int, cfg *config.Config) { cfg.Cluster.Shards = 1 })
	nodetest.WaitFor(t, 30*time.Second, "shard 0 to elect a leader", c.Settled)
	old, term := c.Leader(0)
	if old < 0 {
		t.Fatal("no leader of shard 0")
	}
	var addrs []string
	for _, m := range c.Members {
		addrs = append(addrs, m.InternalAddr)
	}
	role := func() (string, uint64) {
		sm, err := c.ShardMap(old)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range sm.GetShards() {
			if s.GetShard() == 0 && s.GetLocal() {
				return s.GetRole(), s.GetTerm()
			}
		}
		t.Fatalf("node %d holds no replica of shard 0", old)
		return "", 0
	}

	c.Members[old].Node().PauseTicks(true)
	inj.Isolate(c.Members[old].InternalAddr, addrs)
	nodetest.WaitFor(t, 10*time.Second, "another node to lead shard 0 at a higher term", func() bool {
		i, tm := c.Leader(0)
		return i >= 0 && i != old && tm > term
	})
	time.Sleep(time.Second) // several election timeouts (200-400 ms)
	if r, tm := role(); r != "leader" || tm != term {
		t.Fatalf("paused node %d is %s at term %d; want still leader at term %d: without ticks it cannot notice", old, r, tm, term)
	}

	c.Members[old].Node().PauseTicks(false)
	nodetest.WaitFor(t, 10*time.Second, "the old leader to step down once its clock runs", func() bool {
		r, _ := role()
		return r != "leader"
	})
	inj.Heal()
}
