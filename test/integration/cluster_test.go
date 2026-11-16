// Package integration runs in-process multi-node clusters over real gRPC on
// loopback (make test-integration).
package integration

import (
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/node/nodetest"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const up, down = dkvv1.MemberState_MEMBER_STATE_UP, dkvv1.MemberState_MEMBER_STATE_DOWN

// allUp reports whether every member sees every member up, each with the
// node ID that member actually has, and itself marked as self.
func allUp(c *nodetest.Cluster) bool {
	for i := range c.Members {
		resp, err := c.Status(i)
		if err != nil || len(resp.GetMembers()) != len(c.Members) || resp.GetSelf().GetNodeId() != c.Members[i].NodeID() {
			return false
		}
		for j, m := range resp.GetMembers() {
			want := c.Members[j]
			if m.GetAddr() != want.InternalAddr || m.GetState() != up || m.GetNode().GetNodeId() != want.NodeID() || m.GetSelf() != (i == j) {
				return false
			}
		}
	}
	return true
}

// TestThreeNodesFormACluster is the Phase 4 gate check (in-process part):
// three nodes started from a shared bootstrap peer list all see each other
// up, with distinct persistent node IDs.
func TestThreeNodesFormACluster(t *testing.T) {
	c := nodetest.Start(t, 3, nil)
	took := nodetest.WaitFor(t, 10*time.Second, "all three nodes to see each other up", func() bool { return allUp(c) })
	ids := map[string]bool{}
	for _, m := range c.Members {
		ids[m.NodeID()] = true
	}
	if len(ids) != 3 {
		t.Errorf("node IDs are not distinct: %v", ids)
	}
	t.Logf("cluster formed in %v", took.Round(time.Millisecond))
}

// TestKilledPeerIsReportedDownWithinWindow is the Phase 4 gate check that a
// killed peer is reported down within the configured window
// (cluster.down_after), by every surviving node, and not much before it.
func TestKilledPeerIsReportedDownWithinWindow(t *testing.T) {
	const window, interval = time.Second, 100 * time.Millisecond
	c := nodetest.Start(t, 3, func(_ int, cfg *config.Config) {
		cfg.Cluster.DownAfter, cfg.Cluster.PingInterval = window, interval
	})
	nodetest.WaitFor(t, 10*time.Second, "cluster to form", func() bool { return allUp(c) })

	c.Stop(2)
	killed := time.Now()
	detected := map[int]time.Duration{}
	nodetest.WaitFor(t, 5*time.Second, "survivors to report node 2 down", func() bool {
		for _, obs := range []int{0, 1} {
			if _, ok := detected[obs]; ok {
				continue
			}
			if st, err := c.StateOf(obs, 2); err == nil && st == down {
				detected[obs] = time.Since(killed)
			}
		}
		return len(detected) == 2
	})
	for obs, d := range detected {
		t.Logf("node %d reported node 2 down %v after it stopped", obs, d.Round(time.Millisecond))
		// The last good ping was at most one interval (plus its round trip)
		// before the stop, so detection lands between window-interval and
		// window; allow scheduling slack on top.
		if d > window+300*time.Millisecond {
			t.Errorf("node %d took %v to report node 2 down; window is %v", obs, d, window)
		}
		if d < window-interval-50*time.Millisecond {
			t.Errorf("node %d reported node 2 down after only %v; window is %v", obs, d, window)
		}
	}
	for _, pair := range [][2]int{{0, 1}, {1, 0}} {
		if st, err := c.StateOf(pair[0], pair[1]); err != nil || st != up {
			t.Errorf("node %d sees node %d as %v (%v); the survivors must stay up", pair[0], pair[1], st, err)
		}
	}
}

// TestRestartedPeerRejoinsWithItsID: a stopped node that comes back on the
// same address and data directory is up again, with the same node ID.
func TestRestartedPeerRejoinsWithItsID(t *testing.T) {
	c := nodetest.Start(t, 3, nil)
	nodetest.WaitFor(t, 10*time.Second, "cluster to form", func() bool { return allUp(c) })
	id := c.Members[1].NodeID()
	c.Stop(1)
	nodetest.WaitFor(t, 5*time.Second, "node 1 reported down", func() bool {
		st, err := c.StateOf(0, 1)
		return err == nil && st == down
	})
	c.Restart(1)
	took := nodetest.WaitFor(t, 10*time.Second, "node 1 back up everywhere", func() bool { return allUp(c) })
	if c.Members[1].NodeID() != id {
		t.Errorf("node 1 came back as %s, was %s", c.Members[1].NodeID(), id)
	}
	t.Logf("restarted node seen up by everyone %v after it started", took.Round(time.Millisecond))
}

// TestNodeOfAnotherClusterIsNotAMember: a node configured with a different
// cluster name is never reported up, and the reason is visible.
func TestNodeOfAnotherClusterIsNotAMember(t *testing.T) {
	c := nodetest.Start(t, 3, func(i int, cfg *config.Config) {
		if i == 2 {
			cfg.Cluster.Name = "other"
		}
	})
	nodetest.WaitFor(t, 10*time.Second, "node 0 to judge node 2", func() bool {
		resp, err := c.Status(0)
		if err != nil {
			return false
		}
		for _, m := range resp.GetMembers() {
			if m.GetAddr() == c.Members[2].InternalAddr {
				return m.GetState() == down && strings.Contains(m.GetLastError(), `"other"`)
			}
		}
		return false
	})
	if st, err := c.StateOf(0, 1); err != nil || st != up {
		t.Errorf("node 0 sees node 1 as %v (%v)", st, err)
	}
}
