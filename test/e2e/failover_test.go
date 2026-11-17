package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/naveenk2608/distributed-database/pkg/client"
)

// shardLeader asks each live process for its view of shard and returns
// the index of the process whose replica leads it with the highest term,
// or -1.
func shardLeader(procs []*serverProc, alive []bool, shard uint32) int {
	best, term := -1, uint64(0)
	for i, p := range procs {
		if !alive[i] {
			continue
		}
		c, err := client.New(p.addr)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		sm, err := c.ShardMap(ctx)
		cancel()
		_ = c.Close()
		if err != nil {
			continue
		}
		for _, s := range sm.GetShards() {
			if s.GetShard() == shard && s.GetRole() == "leader" && s.GetTerm() > term {
				best, term = i, s.GetTerm()
			}
		}
	}
	return best
}

// TestSIGKILLedLeaderIsReplaced is the process-level Phase 6 gate check:
// three dkv-server processes, RF 3. The leader of shard 0 is killed with
// SIGKILL while acknowledged writes exist; the survivors elect a new
// leader within about two election timeouts, keep accepting writes, and
// still return every acknowledged write. The killed process then restarts
// on its data directory, replays its Raft log and serves them too.
func TestSIGKILLedLeaderIsReplaced(t *testing.T) {
	serverBin, _ := binaries(t)
	internal := freeAddrs(t, 3)
	const tick, electionTicks = 20 * time.Millisecond, 10
	electionTimeout := electionTicks * tick
	args := func(i int) []string {
		return []string{"--cluster.internal-addr=" + internal[i], "--cluster.peers=" + strings.Join(internal, ","),
			"--cluster.shards=4", "--cluster.replication-factor=3",
			"--raft.tick-interval=" + tick.String(), fmt.Sprintf("--raft.election-ticks=%d", electionTicks)}
	}
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	procs := make([]*serverProc, 3)
	alive := []bool{true, true, true}
	for i := range procs {
		procs[i] = startServer(t, serverBin, dirs[i], false, args(i)...)
	}
	deadline := time.Now().Add(30 * time.Second)
	for shardLeader(procs, alive, 0) < 0 {
		if time.Now().After(deadline) {
			t.Fatal("shard 0 elected no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	addrs := func() []string {
		var out []string
		for i, p := range procs {
			if alive[i] {
				out = append(out, p.addr)
			}
		}
		return out
	}
	acked := map[string]uint64{}
	write := func(c *client.Client, n int, prefix string) {
		for k := range n {
			key := fmt.Sprintf("%s-%03d", prefix, k)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			ver, err := c.Put(ctx, []byte(key), []byte(key))
			cancel()
			if err != nil {
				t.Fatalf("put %s: %v", key, err)
			}
			acked[key] = ver
		}
	}
	verify := func(c *client.Client, who string) {
		for key, ver := range acked {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			v, got, err := c.Get(ctx, []byte(key))
			cancel()
			if err != nil || string(v) != key || got != ver {
				t.Fatalf("%s: acknowledged %s@%d read as %q@%d, %v", who, key, ver, v, got, err)
			}
		}
	}
	c, err := client.New(addrs()...)
	if err != nil {
		t.Fatal(err)
	}
	write(c, 40, "before")
	_ = c.Close()

	victim := shardLeader(procs, alive, 0)
	procs[victim].kill()
	alive[victim] = false
	killed := time.Now()
	for {
		if l := shardLeader(procs, alive, 0); l >= 0 {
			break
		}
		if time.Since(killed) > 10*time.Second {
			t.Fatal("no new leader for shard 0 after SIGKILL")
		}
		time.Sleep(5 * time.Millisecond)
	}
	took := time.Since(killed)
	t.Logf("SIGKILLed node %d; shard 0 had a new leader %v later (election timeout %v-%v; includes polling each survivor over gRPC)",
		victim, took.Round(time.Millisecond), electionTimeout, 2*electionTimeout)
	if limit := 2*electionTimeout + 25*tick; took > limit {
		t.Errorf("new leader after %v; want within about two election timeouts (%v)", took, limit)
	}

	c, err = client.New(addrs()...)
	if err != nil {
		t.Fatal(err)
	}
	write(c, 40, "after")
	verify(c, "survivors")
	_ = c.Close()

	procs[victim] = startServer(t, serverBin, dirs[victim], false, args(victim)...)
	alive[victim] = true
	c, err = client.New(procs[victim].addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	verify(c, "restarted node")
	for key, ver := range acked { // its own replicas caught up
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		v, _, err := c.GetStale(ctx, []byte(key), ver)
		cancel()
		if err != nil || string(v) != key {
			t.Fatalf("restarted node's replica: %s: %q, %v", key, v, err)
		}
	}
	for _, p := range procs {
		p.stop()
	}
}
