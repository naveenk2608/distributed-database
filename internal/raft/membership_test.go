package raft

import (
	"errors"
	"slices"
	"testing"
)

func (nw *network) confChange(id string, t ConfChangeType, node string) uint64 {
	nw.t.Helper()
	idx, err := nw.nodes[id].ProposeConfChange(t, node)
	if err != nil {
		nw.t.Fatalf("%v %s on %s: %v", t, node, id, err)
	}
	nw.settle()
	return idx
}

func (nw *network) heartbeat(id string) {
	nw.nodes[id].bcastHeartbeat(0)
	nw.settle()
}

func TestConfChangeEncoding(t *testing.T) {
	cc := ConfChange{Type: ConfPromote, Node: "n4", Conf: ConfState{Voters: []string{"n1", "n2", "n4"}, Learners: []string{"n9"}}}
	got, err := DecodeConfChange(EncodeConfChange(cc))
	if err != nil || got.Type != cc.Type || got.Node != cc.Node || !got.Conf.Equal(cc.Conf) {
		t.Fatalf("round trip: %+v, %v", got, err)
	}
	b := EncodeConfChange(cc)
	for i := range len(b) {
		if _, err := DecodeConfChange(b[:i]); err == nil {
			t.Fatalf("truncated to %d bytes: decoded without error", i)
		}
	}
	if _, err := DecodeConfChange(append(b, 0)); err == nil {
		t.Fatal("trailing byte accepted")
	}
}

// TestLearnerJoinsCatchesUpAndIsPromoted: an uninitialized node added as
// a learner gets the whole log, including the bootstrap entry, does not
// count toward commit, and votes once promoted.
func TestLearnerJoinsCatchesUpAndIsPromoted(t *testing.T) {
	nw := newNetwork(t, 3, func(c *Config) { c.PreVote, c.CheckQuorum = true, true })
	nw.join("s4")
	if nw.nodes["s4"].Initialized() {
		t.Fatal("a node without peers or state claims to know its group")
	}
	for range 100 { // an uninitialized node never campaigns
		nw.nodes["s4"].Tick()
	}
	if s := nw.nodes["s4"].State(); s != StateFollower {
		t.Fatalf("uninitialized node became %v", s)
	}
	nw.campaign("s1")
	nw.propose("s1", "a")
	nw.confChange("s1", ConfAddLearner, "s4")
	nw.propose("s1", "b")
	l4 := nw.nodes["s4"]
	if !l4.Conf().IsLearner("s4") || !slices.Equal(l4.Conf().Voters, []string{"s1", "s2", "s3"}) {
		t.Fatalf("learner's view: %v", l4.Conf())
	}
	if !slices.EqualFunc(l4.Entries(), nw.nodes["s1"].Entries(), func(a, b Entry) bool { return a.Index == b.Index && a.Term == b.Term }) {
		t.Fatalf("learner log %v, leader %v", l4.Entries(), nw.nodes["s1"].Entries())
	}
	// A learner does not count: with s2 and s3 cut off, nothing commits
	// even though s4 has every entry.
	nw.drop = isolate("s2", "s3")
	before := nw.nodes["s1"].Committed()
	nw.propose("s1", "c")
	if c := nw.nodes["s1"].Committed(); c != before {
		t.Fatalf("committed %d -> %d with only a learner acknowledging", before, c)
	}
	nw.drop = nil
	nw.heartbeat("s1")
	nw.confChange("s1", ConfPromote, "s4")
	nw.heartbeat("s1")
	if !nw.nodes["s3"].Conf().IsVoter("s4") || !l4.Conf().IsVoter("s4") {
		t.Fatalf("after promotion: %v / %v", nw.nodes["s3"].Conf(), l4.Conf())
	}
	// Four voters: a quorum is three, so s1 and s4 alone cannot commit.
	nw.drop = isolate("s2", "s3")
	before = nw.nodes["s1"].Committed()
	nw.propose("s1", "d")
	if c := nw.nodes["s1"].Committed(); c != before {
		t.Fatalf("two of four voters committed %d -> %d", before, c)
	}
	nw.drop = isolate("s3")
	nw.heartbeat("s1")
	if c := nw.nodes["s1"].Committed(); c == before {
		t.Fatal("three of four voters could not commit")
	}
}

// TestRemovedLeaderStepsDownAfterCommit: a leader that removes itself
// keeps leading until the removal commits, without counting itself, and
// then steps down; the others elect a leader among themselves.
func TestRemovedLeaderStepsDownAfterCommit(t *testing.T) {
	nw := newNetwork(t, 3, nil)
	nw.campaign("s1")
	idx := nw.confChange("s1", ConfRemove, "s1")
	if c := nw.nodes["s2"].Conf(); c.IsMember("s1") {
		t.Fatalf("s2's membership after the removal: %v", c)
	}
	if s := nw.nodes["s1"].State(); s == StateLeader {
		t.Fatalf("removed leader still leads after its removal (index %d) committed at %d", idx, nw.nodes["s1"].Committed())
	}
	nw.campaign("s2")
	if l := nw.leaders(); !slices.Equal(l, []string{"s2"}) {
		t.Fatalf("leaders %v", l)
	}
	nw.propose("s2", "x")
	if nw.nodes["s3"].Committed() < nw.nodes["s2"].Committed()-1 {
		t.Fatal("remaining voters do not make progress")
	}
	for range 100 { // the removed node never campaigns
		nw.nodes["s1"].Tick()
	}
	if nw.nodes["s1"].State() != StateFollower {
		t.Fatalf("removed node is %v", nw.nodes["s1"].State())
	}
}

// TestOneConfChangeAtATime: a change is refused while another is
// uncommitted, and by a new leader before an entry of its term commits.
func TestOneConfChangeAtATime(t *testing.T) {
	nw := newNetwork(t, 3, nil)
	nw.campaign("s1")
	nw.drop = isolate("s2", "s3") // the next change cannot commit
	if _, err := nw.nodes["s1"].ProposeConfChange(ConfAddLearner, "s4"); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	if _, err := nw.nodes["s1"].ProposeConfChange(ConfAddLearner, "s5"); !errors.Is(err, ErrConfChangePending) {
		t.Fatalf("second change while one is pending: %v", err)
	}
	if _, err := nw.nodes["s1"].ProposeConfChange(ConfRemove, "s9"); err == nil {
		t.Fatal("removing a non-member was accepted")
	}
	// s2 leads a new term; until its no-op commits it refuses changes.
	nw.drop = nil
	nw.nodes["s1"].becomeFollower(nw.nodes["s1"].Term(), "")
	nw.drop = func(m Message) bool { return m.Type == MsgApp || m.Type == MsgAppResp }
	nw.campaign("s2")
	if nw.nodes["s2"].State() != StateLeader {
		t.Fatal("setup: s2 did not win")
	}
	if _, err := nw.nodes["s2"].ProposeConfChange(ConfAddLearner, "s5"); !errors.Is(err, ErrConfChangePending) {
		t.Fatalf("change before the new leader's first commit: %v", err)
	}
	nw.drop = nil
	nw.heartbeat("s2")
	nw.confChange("s2", ConfAddLearner, "s5")
	if c := nw.nodes["s3"].Conf(); !c.IsLearner("s5") {
		t.Fatalf("s3's membership: %v", c)
	}
}

// TestOverwrittenConfChangeIsUndone: a membership entry a follower
// appended but a later leader overwrote stops being in effect there.
func TestOverwrittenConfChangeIsUndone(t *testing.T) {
	nw := newNetwork(t, 5, nil)
	nw.campaign("s1")
	// The change reaches s2 only and cannot commit.
	nw.drop = func(m Message) bool { return m.From != "s1" || m.To != "s2" || m.Type != MsgApp }
	if _, err := nw.nodes["s1"].ProposeConfChange(ConfAddLearner, "s9"); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	if !nw.nodes["s2"].Conf().IsLearner("s9") {
		t.Fatalf("setup: s2 did not append the change: %v", nw.nodes["s2"].Conf())
	}
	// s1 dies; s3 wins the next term with s4 and s5 and overwrites s2's
	// entry with its no-op.
	nw.nodes["s1"] = nil
	nw.drop = func(m Message) bool { return m.To == "s2" && (m.Type == MsgVote || m.Type == MsgPreVote) }
	nw.campaign("s3")
	nw.drop = nil
	if nw.nodes["s3"].State() != StateLeader {
		t.Fatalf("setup: s3 is %v", nw.nodes["s3"].State())
	}
	nw.propose("s3", "y")
	if c := nw.nodes["s2"].Conf(); c.IsMember("s9") || len(c.Voters) != 5 {
		t.Fatalf("s2 still has the overwritten change in effect: %v", c)
	}
	if nw.nodes["s2"].LastIndex() != nw.nodes["s3"].LastIndex() {
		t.Fatal("setup: s2 did not catch up with s3")
	}
}

// TestLeaderTransfer: the transferee takes over at once, even while the
// other followers are inside their lease; proposals are refused during
// the transfer; a transfer to an unreachable voter gives up.
func TestLeaderTransfer(t *testing.T) {
	nw := newNetwork(t, 3, func(c *Config) { c.PreVote, c.CheckQuorum = true, true })
	nw.campaign("s1")
	nw.propose("s1", "a")
	if err := nw.nodes["s1"].TransferLeader("s9"); err == nil {
		t.Fatal("transfer to a non-member accepted")
	}
	term := nw.nodes["s1"].Term()
	if err := nw.nodes["s1"].TransferLeader("s2"); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	if l := nw.leaders(); !slices.Equal(l, []string{"s2"}) || nw.nodes["s2"].Term() != term+1 {
		t.Fatalf("after transfer: leaders %v term %d", l, nw.nodes["s2"].Term())
	}

	// s3 cannot be reached: the transfer is abandoned after an election
	// timeout and the leader accepts proposals again.
	nw.drop = isolate("s3")
	nw.propose("s2", "b") // s3 falls behind
	if err := nw.nodes["s2"].TransferLeader("s3"); err != nil {
		t.Fatal(err)
	}
	if err := nw.nodes["s2"].Propose([]byte("c")); !errors.Is(err, ErrTransferring) {
		t.Fatalf("proposal during a transfer: %v", err)
	}
	for range nw.cfg.ElectionTick {
		nw.nodes["s2"].Tick()
		nw.settle()
	}
	if nw.nodes["s2"].Transferee() != "" || nw.nodes["s2"].State() != StateLeader {
		t.Fatalf("transfer not abandoned: transferee %q state %v", nw.nodes["s2"].Transferee(), nw.nodes["s2"].State())
	}
	if err := nw.nodes["s2"].Propose([]byte("c")); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotCatchUp: a follower that missed entries the leader has
// compacted away is sent a snapshot, installs it, and then receives the
// entries after it; a restart from the snapshot keeps the membership.
func TestSnapshotCatchUp(t *testing.T) {
	nw := newNetwork(t, 3, nil)
	nw.campaign("s1")
	nw.drop = isolate("s3")
	for range 20 {
		nw.propose("s1", "x")
	}
	nw.heartbeat("s1")
	nw.compact("s1")
	if nw.nodes["s1"].FirstIndex() <= 2 {
		t.Fatalf("setup: compaction left first index %d", nw.nodes["s1"].FirstIndex())
	}
	nw.drop = nil
	nw.propose("s1", "after")
	// The appends pipelined to s3 while it was cut off were lost; the
	// second heartbeat response without progress makes the leader probe
	// again, and the probe needs compacted entries.
	nw.heartbeat("s1")
	nw.heartbeat("s1")
	if nw.snapsSent["s3"] != 1 {
		t.Fatalf("snapshots sent to s3: %d", nw.snapsSent["s3"])
	}
	l, f := nw.nodes["s1"], nw.nodes["s3"]
	if f.Committed() != l.Committed() || f.LastIndex() != l.LastIndex() || nw.snaps["s3"].Index == 0 {
		t.Fatalf("follower at commit %d last %d (snapshot %d); leader at %d %d",
			f.Committed(), f.LastIndex(), nw.snaps["s3"].Index, l.Committed(), l.LastIndex())
	}
	if !f.Conf().Equal(l.Conf()) {
		t.Fatalf("membership from the snapshot: %v, want %v", f.Conf(), l.Conf())
	}
	// A stale snapshot changes nothing.
	old := nw.snaps["s3"]
	if err := f.Step(Message{Type: MsgSnap, From: "s1", To: "s3", Term: l.Term(), Snapshot: &SnapshotMeta{Index: 2, Term: 2}}); err != nil {
		t.Fatal(err)
	}
	if rd := f.Ready(); rd.Snapshot != nil {
		t.Fatal("a snapshot below the commit index was installed")
	}
	nw.start("s3", 77) // restart from the persisted snapshot and entries
	f = nw.nodes["s3"]
	if f.FirstIndex() != old.Index+1 || !f.Conf().Equal(l.Conf()) || f.LastIndex() != l.LastIndex() {
		t.Fatalf("restarted: first %d last %d conf %v", f.FirstIndex(), f.LastIndex(), f.Conf())
	}
}

// TestCompactRules: compaction cannot pass the applied index, and a
// snapshot description needs an index the log covers.
func TestCompactRules(t *testing.T) {
	nw := newNetwork(t, 3, nil)
	nw.campaign("s1")
	nw.propose("s1", "x")
	r := nw.nodes["s1"]
	if err := r.Compact(r.Applied() + 1); err == nil {
		t.Fatal("compacted past the applied index")
	}
	nw.compact("s1")
	if _, err := r.SnapshotAt(r.FirstIndex() - 2); err == nil {
		t.Fatal("described a snapshot below the compaction point")
	}
	if m, err := r.SnapshotAt(r.Applied()); err != nil || m.Conf.Voters == nil {
		t.Fatalf("snapshot at the applied index: %+v, %v", m, err)
	}
}

// TestRemovedNodeCampaignsUntilItsRemovalCommits: a leader removes
// itself, then loses leadership before the removal commits. The remaining
// voter never got the removal and needs the old leader's vote, which its
// longer log withholds; the old leader must still campaign (without
// counting itself) so the removal can commit (Raft thesis §4.2.2).
func TestRemovedNodeCampaignsUntilItsRemovalCommits(t *testing.T) {
	nw := newNetwork(t, 2, nil)
	nw.campaign("s1")
	nw.drop = func(m Message) bool { return true } // the removal reaches nobody
	if _, err := nw.nodes["s1"].ProposeConfChange(ConfRemove, "s1"); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	nw.nodes["s1"].becomeFollower(nw.nodes["s1"].Term(), "") // loses leadership
	nw.drop = nil
	nw.campaign("s2") // s1's longer log withholds its vote
	if nw.nodes["s2"].State() == StateLeader {
		t.Fatal("setup: s2 won without s1's vote")
	}
	for range 3 * nw.cfg.ElectionTick {
		nw.nodes["s1"].Tick()
		nw.settle()
	}
	if c := nw.nodes["s2"].Conf(); c.IsMember("s1") || nw.nodes["s2"].Committed() < nw.nodes["s1"].LastIndex() {
		t.Fatalf("the removal did not commit: s2 has %v, commit %d", c, nw.nodes["s2"].Committed())
	}
	nw.campaign("s2")
	if nw.nodes["s2"].State() != StateLeader || nw.nodes["s1"].State() == StateLeader {
		t.Fatalf("s2 is %v, s1 is %v", nw.nodes["s2"].State(), nw.nodes["s1"].State())
	}
}

// TestLearnerLeaseExpires: a learner whose leader went silent must not
// stay inside its lease, or it would refuse votes forever; a voter it
// does not know about yet (its promotion may not have reached it) could
// then never win.
func TestLearnerLeaseExpires(t *testing.T) {
	nw := newNetwork(t, 2, func(c *Config) { c.PreVote, c.CheckQuorum = true, true })
	nw.join("s3")
	nw.campaign("s1")
	nw.confChange("s1", ConfAddLearner, "s3")
	nw.heartbeat("s1")
	l := nw.nodes["s3"]
	if l.Leader() != "s1" {
		t.Fatalf("setup: learner's leader is %q", l.Leader())
	}
	nw.nodes["s1"] = nil // the leader dies
	for range 2 * nw.cfg.ElectionTick {
		l.Tick() // longer than any randomized election timeout
	}
	if err := l.Step(Message{Type: MsgPreVote, From: "s2", To: "s3", Term: l.Term() + 1,
		Index: l.LastIndex(), LogTerm: l.Term()}); err != nil {
		t.Fatal(err)
	}
	for _, m := range l.Ready().Messages {
		if m.Type == MsgPreVoteResp && m.Reject {
			t.Fatal("learner refused a pre-vote two election timeouts after its leader went silent")
		}
	}
}
