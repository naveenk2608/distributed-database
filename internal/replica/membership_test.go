package replica

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/naveenk2608/distributed-database/internal/obs"
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
	// The snapshot may end before the entry that adds n4, which then
	// follows by append: catching up means applying that entry too.
	added := l.Status().Applied
	n4 := g.rep("n4")
	g.waitFor("the learner to catch up", func() bool { return n4.Status().Applied >= max(last, added) })
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
		g.setFS(id, fs)
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
			g.setFS(id, fs.CrashImage(nil))
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

// TestReplicaEvents: a replica reports its elections and step-downs, the
// membership changes it applies (every member reports each one at the
// same index), the snapshots it installs and the transfers it starts.
func TestReplicaEvents(t *testing.T) {
	g := newGroupCompacting(t, 3, 50)
	l := g.leader()
	var leader string
	for _, id := range g.peers {
		if g.rep(id) == l {
			leader = id
		}
	}
	if ev := g.eventsOf(obs.EventLeaderElected); len(ev) == 0 || ev[len(ev)-1].rep != leader ||
		ev[len(ev)-1].Node != leader || ev[len(ev)-1].Shard != 7 || ev[len(ev)-1].Term != l.Status().Term {
		t.Fatalf("leader_elected events %+v; want the last from %s at term %d", ev, leader, l.Status().Term)
	}

	last := fill(t, l, 200, "k")
	g.waitFor("the leader to compact", func() bool { return l.Status().FirstIndex > 2 })
	g.open("n4")
	changeMembership(t, l, raft.ConfAddLearner, "n4")
	g.waitFor("the learner to catch up", func() bool { return g.rep("n4").Status().Applied >= last })
	// Every member that applies the entry reports it. n4 applies it only if
	// its snapshot ended before it; a snapshot that already holds it does
	// not replay it. The console keeps one event per shard and index.
	reported := func(kind string) func() bool {
		return func() bool {
			by := map[string]bool{}
			for _, e := range g.eventsOf(kind) {
				by[e.rep] = true
			}
			return by["n1"] && by["n2"] && by["n3"]
		}
	}
	g.waitFor("every voter to report the learner", reported(obs.EventReplicaAdded))
	added := g.eventsOf(obs.EventReplicaAdded)
	for _, e := range added {
		if e.Node != "n4" || e.Index != added[0].Index || e.Index == 0 {
			t.Fatalf("replica_added events %+v; want n4 at one index", added)
		}
	}
	// The learner caught up by snapshot and must say so. A voter that fell
	// behind the leader's compaction point may have installed one too.
	snaps := g.eventsOf(obs.EventSnapshotInstalled)
	if !slices.ContainsFunc(snaps, func(e seenEvent) bool { return e.rep == "n4" && e.Node == "n4" && e.Index > 0 }) {
		t.Fatalf("snapshot_installed events %+v; want one from n4", snaps)
	}

	changeMembership(t, l, raft.ConfPromote, "n4")
	g.waitFor("every voter to report the promotion", reported(obs.EventReplicaPromoted))

	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.TransferLeader(c, "n4"); err != nil {
		t.Fatal(err)
	}
	if ev := g.eventsOf(obs.EventLeaderTransfer); len(ev) != 1 || ev[0].rep != leader || ev[0].Node != "n4" {
		t.Fatalf("leader_transfer events %+v", ev)
	}
	g.waitFor("n4 to report its election and the old leader its step-down", func() bool {
		won, left := false, false
		for _, e := range g.eventsOf(obs.EventLeaderElected) {
			won = won || e.rep == "n4"
		}
		for _, e := range g.eventsOf(obs.EventLeaderSteppedDown) {
			left = left || e.rep == leader
		}
		return won && left
	})
	changeMembership(t, g.rep("n4"), raft.ConfRemove, leader)
	g.waitFor("the removal to be reported", func() bool {
		for _, e := range g.eventsOf(obs.EventReplicaRemoved) {
			if e.Node == leader {
				return true
			}
		}
		return false
	})
}
