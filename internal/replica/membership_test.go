package replica

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/naveenk2608/distributed-database/internal/raft"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

// discard closes a replica whose filesystem may have crashed, ignoring
// the errors that causes.
func (g *group) discard(id string) {
	g.net.mu.Lock()
	r := g.net.reps[id]
	delete(g.net.reps, id)
	g.net.mu.Unlock()
	if r != nil {
		_ = r.Close()
	}
}

func (g *group) waitFor(what string, cond func() bool) {
	g.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			g.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// changeMembership retries while another change is pending.
func changeMembership(t *testing.T, l *Replica, ct raft.ConfChangeType, node string) {
	t.Helper()
	for {
		err := l.ChangeMembership(ctx(t), ct, node)
		if errors.Is(err, raft.ErrConfChangePending) {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("%v %s: %v", ct, node, err)
		}
		return
	}
}

func fill(t *testing.T, l *Replica, n int, prefix string) uint64 {
	t.Helper()
	var last uint64
	for i := range n {
		var err error
		if last, err = l.Put(ctx(t), fmt.Appendf(nil, "%s%04d", prefix, i), []byte("v"), "", 0); err != nil {
			t.Fatal(err)
		}
	}
	return last
}

// TestLearnerCatchesUpBySnapshot: after the leader compacted its log, a
// new, empty replica added as a learner receives a snapshot, then the
// entries after it, and once promoted counts as a voter.
func TestLearnerCatchesUpBySnapshot(t *testing.T) {
	g := newGroupCompacting(t, 3, 50)
	l := g.leader()
	last := fill(t, l, 200, "k")
	g.waitFor("the leader to compact", func() bool { return l.Status().FirstIndex > 2 })
	g.open("n4") // no bootstrap peers: it waits for the leader
	if st := g.rep("n4").Status(); st.Role != raft.StateFollower || len(st.Conf.Voters) != 0 {
		t.Fatalf("a new replica: %+v", st)
	}
	changeMembership(t, l, raft.ConfAddLearner, "n4")
	n4 := g.rep("n4")
	g.waitFor("the learner to catch up", func() bool { return n4.Status().Applied >= last })
	if st := n4.Status(); st.SnapshotsInstalled == 0 || !st.Conf.IsLearner("n4") {
		t.Fatalf("learner status: %+v", st)
	}
	v, ver, found, _, err := n4.Get(ctx(t), []byte("k0007"), true, last)
	if err != nil || !found || string(v) != "v" || ver == 0 {
		t.Fatalf("learner's data: %q@%d %v %v", v, ver, found, err)
	}
	after := fill(t, l, 10, "after")
	changeMembership(t, l, raft.ConfPromote, "n4")
	g.waitFor("the promotion to reach n4", func() bool { return n4.Status().Conf.IsVoter("n4") && n4.Status().Applied >= after })
	if st := l.Status(); !slices.Equal(st.Conf.Voters, []string{"n1", "n2", "n3", "n4"}) {
		t.Fatalf("leader's membership: %v", st.Conf)
	}
	// Restarting the learner keeps the snapshot's data and the membership.
	g.close("n4")
	g.open("n4")
	n4 = g.rep("n4")
	if v, _, found, _, err := n4.Get(ctx(t), []byte("after0003"), true, after); err != nil || !found || string(v) != "v" {
		t.Fatalf("after restart: %q %v %v", v, found, err)
	}
	if st := n4.Status(); !st.Conf.IsVoter("n4") {
		t.Fatalf("membership after restart: %v", st.Conf)
	}
}

// TestRemovedReplicaAndLeaderTransfer: leadership moves to a named voter,
// and a removed voter no longer counts.
func TestRemovedReplicaAndLeaderTransfer(t *testing.T) {
	g := newGroup(t, 3)
	l := g.leader()
	var old, target string
	for _, id := range g.peers {
		if g.rep(id) == l {
			old = id
		} else if target == "" {
			target = id
		}
	}
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := l.TransferLeader(c, target); err != nil {
		t.Fatal(err)
	}
	t.Logf("leadership moved from %s to %s in %v", old, target, time.Since(start).Round(time.Millisecond))
	nl := g.rep(target)
	if nl.Status().Role != raft.StateLeader {
		g.waitFor("the transferee to lead", func() bool { return nl.Status().Role == raft.StateLeader })
	}
	changeMembership(t, nl, raft.ConfRemove, old)
	g.close(old)
	// Two voters remain; both are needed, and both are up.
	if _, err := nl.Put(ctx(t), []byte("k"), []byte("v"), "", 0); err != nil {
		t.Fatal(err)
	}
	if st := nl.Status(); st.Conf.IsMember(old) || len(st.Conf.Voters) != 2 {
		t.Fatalf("after removal: %v", st.Conf)
	}
}

// TestCrashDuringSnapshotInstall crashes a learner's filesystem after each
// filesystem operation of receiving and installing a snapshot. Reopened
// from the crash image, it must start (finishing an interrupted install
// if needed) and catch up with every write.
func TestCrashDuringSnapshotInstall(t *testing.T) {
	g := newGroupCompacting(t, 3, 40)
	l := g.leader()
	last := fill(t, l, 120, "k")
	g.waitFor("the leader to compact", func() bool { return l.Status().FirstIndex > 2 })
	for k := int64(0); ; k++ {
		id := fmt.Sprintf("j%d", k)
		fs := vfs.NewMemFS()
		g.fss[id] = fs
		g.open(id)
		fs.CrashAfter(k)
		changeMembership(t, l, raft.ConfAddLearner, id)
		j := g.rep(id)
		g.waitFor(id+" to install or crash", func() bool { return fs.Crashed() || j.Status().Applied >= last })
		crashed := fs.Crashed()
		if !crashed {
			fs.CrashAfter(1 << 62) // disarm before a clean close
		}
		g.discard(id)
		if crashed {
			g.fss[id] = fs.CrashImage(nil)
			if err := g.tryOpen(id); err != nil {
				t.Fatalf("crash after %d operations: reopen: %v", k, err)
			}
			j = g.rep(id)
			g.waitFor(id+" to catch up after the crash", func() bool { return j.Status().Applied >= last })
			if v, _, found, _, err := j.Get(ctx(t), []byte("k0100"), true, last); err != nil || !found || string(v) != "v" {
				t.Fatalf("crash after %d operations: data %q %v %v", k, v, found, err)
			}
			g.close(id)
		}
		changeMembership(t, l, raft.ConfRemove, id)
		if !crashed {
			t.Logf("crashed at each of %d filesystem operations of a snapshot install", k)
			return
		}
	}
}
