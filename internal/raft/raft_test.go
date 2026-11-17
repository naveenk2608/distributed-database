package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// network is a minimal synchronous harness: messages are delivered in
// order until none are left, filtered by drop.
type network struct {
	t     *testing.T
	ids   []string
	nodes map[string]*Raft
	cfg   Config
	// persisted state per node, as a runner would keep it: the snapshot,
	// the HardState, the entries after the snapshot and the applied index
	snaps   map[string]SnapshotMeta
	hs      map[string]HardState
	logs    map[string][]Entry
	applied map[string]uint64
	// snapshots streamed, by receiver
	snapsSent map[string]int
	drop      func(Message) bool
	edit      func(Message) Message // applied to delivered messages
	sent      int
	reads     map[string][]ReadState // confirmed reads per node
}

func newNetwork(t *testing.T, n int, configure func(*Config)) *network {
	t.Helper()
	nw := &network{t: t, nodes: map[string]*Raft{}, hs: map[string]HardState{}, logs: map[string][]Entry{},
		reads: map[string][]ReadState{}, snaps: map[string]SnapshotMeta{}, applied: map[string]uint64{},
		snapsSent: map[string]int{}}
	for i := range n {
		nw.ids = append(nw.ids, fmt.Sprintf("s%d", i+1))
	}
	nw.cfg = Config{Peers: nw.ids, ElectionTick: 10, HeartbeatTick: 1, MaxEntriesPerMsg: 64}
	if configure != nil {
		configure(&nw.cfg)
	}
	for i, id := range nw.ids {
		nw.start(id, uint64(i))
	}
	return nw
}

func (nw *network) start(id string, seed uint64) {
	cfg := nw.cfg
	cfg.ID, cfg.Rand = id, rand.New(rand.NewPCG(seed, 99))
	cfg.Snapshot, cfg.HardState, cfg.Entries, cfg.Applied = nw.snaps[id], nw.hs[id], slices.Clone(nw.logs[id]), nw.applied[id]
	r, err := New(cfg)
	if err != nil {
		nw.t.Fatal(err)
	}
	nw.nodes[id] = r
}

// join adds an uninitialized node: no bootstrap peers and no state.
func (nw *network) join(id string) {
	cfg := nw.cfg
	cfg.ID, cfg.Rand, cfg.Peers = id, rand.New(rand.NewPCG(uint64(len(nw.ids)), 99)), nil
	r, err := New(cfg)
	if err != nil {
		nw.t.Fatal(err)
	}
	nw.ids = append(nw.ids, id)
	nw.nodes[id] = r
}

// compact compacts id's log up to its applied index, as a runner would
// after making the state machine durable.
func (nw *network) compact(id string) {
	r := nw.nodes[id]
	if err := r.Compact(r.Applied()); err != nil {
		nw.t.Fatal(err)
	}
	nw.snaps[id] = r.Snapshot()
	nw.logs[id] = slices.Clone(r.Entries())
}

// settle processes Ready and delivers messages until quiescent.
func (nw *network) settle() {
	for range 10000 {
		progressed := false
		for _, id := range nw.ids {
			r := nw.nodes[id]
			if r == nil || !r.HasReady() {
				continue
			}
			progressed = true
			rd := r.Ready()
			if rd.Snapshot != nil {
				nw.snaps[id], nw.logs[id], nw.applied[id] = *rd.Snapshot, nil, rd.Snapshot.Index
			}
			if !rd.HardState.IsEmpty() {
				nw.hs[id] = rd.HardState
			}
			if len(rd.Entries) > 0 {
				off := nw.snaps[id].Index
				nw.logs[id] = append(nw.logs[id][:rd.Entries[0].Index-1-off], rd.Entries...)
			}
			if n := len(rd.CommittedEntries); n > 0 {
				nw.applied[id] = rd.CommittedEntries[n-1].Index
			}
			nw.reads[id] = append(nw.reads[id], rd.ReadStates...)
			r.Advance(rd)
			for _, m := range rd.Messages {
				if m.Type == MsgSnap && m.Snapshot == nil {
					// Act as the leader's runner: describe a snapshot at
					// the applied index and stream it, then report.
					meta, err := r.SnapshotAt(r.Applied())
					if err != nil {
						nw.t.Fatal(err)
					}
					m.Snapshot = &meta
					delivered := nw.nodes[m.To] != nil && (nw.drop == nil || !nw.drop(m))
					if delivered {
						nw.snapsSent[m.To]++
						if err := nw.nodes[m.To].Step(m); err != nil {
							nw.t.Fatal(err)
						}
					}
					r.ReportSnapshot(m.To, delivered)
					continue
				}
				if to := nw.nodes[m.To]; to != nil && (nw.drop == nil || !nw.drop(m)) {
					if nw.edit != nil {
						m = nw.edit(m)
					}
					nw.sent++
					if err := to.Step(m); err != nil {
						nw.t.Fatal(err)
					}
				}
			}
		}
		if !progressed {
			return
		}
	}
	nw.t.Fatal("network did not settle")
}

func (nw *network) campaign(id string) {
	nw.nodes[id].hup()
	nw.settle()
}

func (nw *network) propose(id string, data string) {
	if err := nw.nodes[id].Propose([]byte(data)); err != nil {
		nw.t.Fatalf("propose on %s: %v", id, err)
	}
	nw.settle()
}

func (nw *network) leaders() []string {
	var out []string
	for _, id := range nw.ids {
		if r := nw.nodes[id]; r != nil && r.State() == StateLeader {
			out = append(out, id)
		}
	}
	return out
}

// isolate drops every message to or from the given nodes.
func isolate(ids ...string) func(Message) bool {
	return func(m Message) bool { return slices.Contains(ids, m.From) || slices.Contains(ids, m.To) }
}

func TestElectionAndReplication(t *testing.T) {
	for _, pre := range []bool{false, true} {
		nw := newNetwork(t, 3, func(c *Config) { c.PreVote = pre })
		nw.campaign("s1")
		// Term 1 is the bootstrap entry's; the first election is term 2.
		if l := nw.leaders(); !slices.Equal(l, []string{"s1"}) || nw.nodes["s1"].Term() != 2 {
			t.Fatalf("prevote=%v: leaders %v term %d", pre, l, nw.nodes["s1"].Term())
		}
		nw.propose("s1", "a")
		nw.propose("s1", "b")
		for _, id := range nw.ids {
			ents := nw.nodes[id].Entries() // bootstrap, no-op, a, b
			if len(ents) != 4 || ents[0].Type != EntryConfChange || string(ents[3].Data) != "b" {
				t.Fatalf("%s log %v", id, ents)
			}
		}
		if nw.nodes["s1"].Committed() != 4 {
			t.Fatalf("leader committed %d", nw.nodes["s1"].Committed())
		}
		nw.nodes["s1"].bcastHeartbeat(0) // carries the commit index to followers
		nw.settle()
		for _, id := range nw.ids {
			if c := nw.nodes[id].Committed(); c != 4 {
				t.Errorf("%s committed %d, want 4", id, c)
			}
		}
	}
}

func TestNoLeaderWithoutQuorum(t *testing.T) {
	nw := newNetwork(t, 3, nil)
	nw.drop = func(m Message) bool { return m.From != "s1" || m.To == "s2" || m.To == "s3" }
	nw.campaign("s1")
	if nw.nodes["s1"].State() == StateLeader {
		t.Fatal("s1 became leader without any votes")
	}
}

// TestFigure8 reconstructs Figure 8 of the Raft paper. A leader re-elected
// in a later term replicates an entry from its earlier term to a majority;
// counting replicas must not commit it, because another node whose log
// ends in a newer term could still be elected and overwrite it.
func TestFigure8(t *testing.T) {
	nw := newNetwork(t, 5, func(c *Config) { c.MaxEntriesPerMsg = 1 })
	nw.campaign("s1") // term A; bootstrap entry and no-op committed everywhere
	termA, base := nw.nodes["s1"].Term(), nw.nodes["s1"].Committed()
	if base != nw.nodes["s1"].LastIndex() {
		t.Fatalf("setup: committed %d of %d", base, nw.nodes["s1"].LastIndex())
	}
	xi := base + 1 // X's index

	// s1 replicates entry X (index xi, term A) to s2 only.
	nw.drop = func(m Message) bool { return (m.From != "s1" || m.To != "s2") && (m.From != "s2" || m.To != "s1") }
	nw.propose("s1", "X")

	// s5 becomes leader of term A+1 with votes from s3 and s4, appends its
	// no-op at index xi (term A+1), and replicates nothing.
	nw.drop = func(m Message) bool {
		if m.Type == MsgVote || m.Type == MsgVoteResp {
			return isolate("s1", "s2")(m)
		}
		return true
	}
	nw.campaign("s5")
	if nw.nodes["s5"].State() != StateLeader || nw.nodes["s5"].Term() != termA+1 {
		t.Fatalf("setup: s5 is %v in term %d", nw.nodes["s5"].State(), nw.nodes["s5"].Term())
	}

	// s1 wins term A+2 with votes from s2 and s3, then replicates X (term
	// A) to s3, one entry per message, but its term-(A+2) no-op reaches
	// nobody: it is stripped from every append, so followers acknowledge X
	// and the leader learns that a majority holds it.
	nw.drop = func(m Message) bool { return isolate("s4", "s5")(m) }
	nw.nodes["s1"].becomeFollower(nw.nodes["s5"].Term(), "") // learns term A+1 exists
	nw.nodes["s1"].hup()
	l := nw.nodes["s1"]
	termC := termA + 2
	nw.drop = func(m Message) bool {
		if isolate("s4", "s5")(m) {
			return true
		}
		// Once the leader knows a follower holds X, stop delivering the
		// stripped appends it keeps retrying.
		return m.Type == MsgApp && m.From == "s1" && l.prs[m.To].match >= xi &&
			slices.ContainsFunc(m.Entries, func(e Entry) bool { return e.Term == termC })
	}
	nw.edit = func(m Message) Message {
		if m.Type == MsgApp {
			m.Entries = slices.DeleteFunc(slices.Clone(m.Entries), func(e Entry) bool { return e.Term == termC })
		}
		return m
	}
	nw.settle()
	if l.State() != StateLeader || l.Term() != termC {
		t.Fatalf("setup: s1 is %v in term %d", l.State(), l.Term())
	}
	holders := 0
	for _, id := range nw.ids {
		if ents := nw.nodes[id].Entries(); uint64(len(ents)) >= xi && string(ents[xi-1].Data) == "X" &&
			(id == "s1" || l.prs[id].match >= xi) {
			holders++
		}
	}
	if holders < 3 {
		for _, id := range nw.ids {
			t.Logf("%s term %d state %v log %v match %d", id, nw.nodes[id].Term(), nw.nodes[id].State(), nw.nodes[id].Entries(), l.prs[id].match)
		}
		t.Fatalf("setup: the leader knows X is on %d nodes, want a majority", holders)
	}

	// The crux: X (term A) is on a majority but must not be committed by
	// counting, since s5 (last entry term A+1) could still win and
	// overwrite index xi.
	if c := nw.nodes["s1"].Committed(); c >= xi {
		t.Fatalf("leader of term %d committed index %d holding a term-%d entry by counting replicas (Figure 8)", termC, c, termA)
	}
}

func TestPreVoteKeepsTermStableAcrossPartition(t *testing.T) {
	nw := newNetwork(t, 3, func(c *Config) { c.PreVote, c.CheckQuorum = true, true })
	nw.campaign("s1")
	term := nw.nodes["s1"].Term()
	nw.drop = isolate("s3")
	for range 100 { // s3 times out over and over while cut off
		nw.nodes["s3"].Tick()
		nw.settle()
	}
	if got := nw.nodes["s3"].Term(); got != term {
		t.Fatalf("isolated node's term grew from %d to %d despite PreVote", term, got)
	}
	nw.drop = nil
	nw.nodes["s3"].hup()
	nw.settle()
	if l := nw.leaders(); !slices.Equal(l, []string{"s1"}) || nw.nodes["s1"].Term() != term {
		t.Fatalf("rejoining node disrupted the leader: leaders %v, term %d -> %d", l, term, nw.nodes["s1"].Term())
	}
}

func TestCheckQuorumLeaderStepsDown(t *testing.T) {
	nw := newNetwork(t, 3, func(c *Config) { c.CheckQuorum = true })
	nw.campaign("s1")
	nw.drop = isolate("s1")
	for range nw.cfg.ElectionTick * 2 {
		nw.nodes["s1"].Tick()
		nw.settle()
	}
	if nw.nodes["s1"].State() == StateLeader {
		t.Fatal("leader cut off from its quorum kept leading")
	}
}

func TestReadIndexNeedsAQuorum(t *testing.T) {
	nw := newNetwork(t, 3, nil)
	nw.campaign("s1")
	nw.propose("s1", "a")
	nw.drop = isolate("s1")
	if err := nw.nodes["s1"].ReadIndex(7); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	if len(nw.reads["s1"]) != 0 {
		t.Fatalf("read confirmed without any heartbeat acks: %v", nw.reads["s1"])
	}
	nw.drop = nil
	if err := nw.nodes["s1"].ReadIndex(8); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	var ids []uint64
	for _, rs := range nw.reads["s1"] {
		ids = append(ids, rs.RequestID)
		if rs.Index != nw.nodes["s1"].Committed() {
			t.Errorf("read %d at index %d, commit is %d", rs.RequestID, rs.Index, nw.nodes["s1"].Committed())
		}
	}
	// A quorum of acks for read 8 also confirms read 7, which was waiting.
	if !slices.Equal(ids, []uint64{7, 8}) {
		t.Fatalf("confirmed reads %v, want [7 8]", ids)
	}
	if err := nw.nodes["s2"].ReadIndex(9); !errors.Is(err, ErrNotLeader) {
		t.Errorf("ReadIndex on a follower: %v", err)
	}
}

func TestConflictHintsConvergeQuickly(t *testing.T) {
	nw := newNetwork(t, 3, func(c *Config) { c.MaxEntriesPerMsg = 1000 })
	nw.campaign("s1")
	// s3 is cut off and, as a would-be leader, piles up 200 entries of its
	// own term that never commit.
	nw.drop = isolate("s3")
	nw.nodes["s3"].becomeCandidate()
	nw.nodes["s3"].becomeLeader()
	for i := range 200 {
		_ = nw.nodes["s3"].Propose([]byte(fmt.Sprint("junk", i)))
	}
	nw.settle()
	for i := range 50 {
		nw.propose("s1", fmt.Sprint("good", i))
	}
	nw.drop = nil
	nw.nodes["s3"].becomeFollower(nw.nodes["s1"].Term(), "s1")
	before := nw.sent
	// The leader's appends to s3 were lost; two heartbeat rounds without
	// progress make it resend from s3's match.
	for range 2 {
		nw.nodes["s1"].Tick()
		nw.settle()
	}
	nw.propose("s1", "last")
	if got, want := nw.nodes["s3"].Entries(), nw.nodes["s1"].Entries(); !slices.EqualFunc(got, want, func(a, b Entry) bool {
		return a.Index == b.Index && a.Term == b.Term && string(a.Data) == string(b.Data)
	}) {
		t.Fatalf("s3 did not converge: %d entries vs %d", len(got), len(want))
	}
	// Stepping back one entry per rejection would take over 100 messages
	// for 200 junk entries; the conflict hint skips the junk term at once.
	msgs := nw.sent - before
	if msgs > 24 {
		t.Errorf("convergence took %d messages; conflict hints should skip the junk term at once", msgs)
	}
	t.Logf("s3 converged past 200 conflicting entries in %d messages", msgs)
}

func TestVoteSurvivesRestart(t *testing.T) {
	nw := newNetwork(t, 3, nil)
	nw.drop = isolate("s3") // s2 votes for s1 in term 2
	nw.campaign("s1")
	if nw.hs["s2"].Vote != "s1" || nw.hs["s2"].Term != 2 {
		t.Fatalf("persisted state of s2: %+v", nw.hs["s2"])
	}
	nw.start("s2", 1234) // crash and restart from persisted state
	if err := nw.nodes["s2"].Step(Message{Type: MsgVote, From: "s3", To: "s2", Term: 2}); err != nil {
		t.Fatal(err)
	}
	rd := nw.nodes["s2"].Ready()
	for _, m := range rd.Messages {
		if m.Type == MsgVoteResp && !m.Reject {
			t.Fatal("restarted node voted twice in term 2")
		}
	}
}

func TestSingleNodeGroup(t *testing.T) {
	nw := newNetwork(t, 1, func(c *Config) { c.PreVote = true })
	for range 25 {
		nw.nodes["s1"].Tick()
		nw.settle()
	}
	if nw.nodes["s1"].State() != StateLeader {
		t.Fatal("single node did not elect itself")
	}
	nw.propose("s1", "x") // after the bootstrap entry and the no-op
	if nw.nodes["s1"].Committed() != 3 {
		t.Fatalf("committed %d", nw.nodes["s1"].Committed())
	}
	if err := nw.nodes["s1"].ReadIndex(1); err != nil {
		t.Fatal(err)
	}
	if rd := nw.nodes["s1"].Ready(); len(rd.ReadStates) != 1 || rd.ReadStates[0].Index != 3 {
		t.Fatalf("single-node read: %v", rd.ReadStates)
	}
}

func TestRejectsBadConfig(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 1))
	for _, cfg := range []Config{
		{ID: "x", Peers: []string{"a"}, ElectionTick: 10, HeartbeatTick: 1, Rand: r},
		{ID: "a", Peers: []string{"a"}, ElectionTick: 1, HeartbeatTick: 1, Rand: r},
		{ID: "a", Peers: []string{"a"}, ElectionTick: 10, HeartbeatTick: 1},
		{ID: "a", Peers: []string{"a"}, ElectionTick: 10, HeartbeatTick: 1, Rand: r, Entries: []Entry{{Index: 2, Term: 1}}},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("New(%+v) succeeded", cfg)
		}
	}
}

// TestMustSync: only new entries, a new term or a new vote need an fsync
// before messages go out; a commit index alone does not.
func TestMustSync(t *testing.T) {
	nw := newNetwork(t, 3, nil)
	nw.campaign("s1")
	l := nw.nodes["s1"]
	if err := l.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if rd := l.Ready(); !rd.MustSync || len(rd.Entries) == 0 {
		t.Fatalf("a Ready with new entries: MustSync=%v", rd.MustSync)
	} else {
		l.Advance(rd)
	}
	f := nw.nodes["s2"]
	commit, last := f.Committed(), f.LastIndex()
	app := Message{Type: MsgApp, From: "s1", To: "s2", Term: l.Term(), Index: last, LogTerm: l.Term(),
		Entries: []Entry{{Index: last + 1, Term: l.Term()}}, Commit: commit}
	if err := f.Step(app); err != nil {
		t.Fatal(err)
	}
	if rd := f.Ready(); !rd.MustSync {
		t.Fatal("a follower's Ready with appended entries does not need a sync")
	} else {
		f.Advance(rd)
	}
	if err := f.Step(Message{Type: MsgHeartbeat, From: "s1", To: "s2", Term: l.Term(), Commit: last + 1}); err != nil {
		t.Fatal(err)
	}
	rd := f.Ready()
	if rd.HardState.Commit != last+1 || rd.MustSync {
		t.Fatalf("commit-only Ready: HardState %+v, MustSync %v", rd.HardState, rd.MustSync)
	}
	f.Advance(rd)
	// A new term (from a vote request refused for its stale log), then a
	// vote in that term on its own: both must be synced.
	next := l.Term() + 1
	if err := f.Step(Message{Type: MsgVote, From: "s3", To: "s2", Term: next}); err != nil {
		t.Fatal(err)
	}
	rd = f.Ready()
	if !rd.MustSync || rd.HardState.Term != next || rd.HardState.Vote != "" {
		t.Fatalf("a Ready with a new term: %+v, MustSync %v", rd.HardState, rd.MustSync)
	}
	f.Advance(rd)
	if err := f.Step(Message{Type: MsgVote, From: "s1", To: "s2", Term: next, Index: 100, LogTerm: 100}); err != nil {
		t.Fatal(err)
	}
	if rd := f.Ready(); !rd.MustSync || rd.HardState.Term != next || rd.HardState.Vote != "s1" {
		t.Fatalf("a Ready with only a new vote: %+v, MustSync %v", rd.HardState, rd.MustSync)
	}
}

// TestRestartWithAppliedBeyondCommit: the commit index is persisted
// without fsync, so after a power loss the state machine may have applied
// more than the persisted commit index. Raft must treat those entries as
// committed and never hand them out again.
func TestRestartWithAppliedBeyondCommit(t *testing.T) {
	ents := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1}}
	cfg := Config{ID: "s1", Peers: []string{"s1", "s2", "s3"}, ElectionTick: 10, HeartbeatTick: 1,
		Rand: rand.New(rand.NewPCG(1, 1)), HardState: HardState{Term: 1, Commit: 1}, Entries: ents, Applied: 3}
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Committed() != 3 {
		t.Fatalf("committed %d; want 3 (what was applied)", r.Committed())
	}
	if rd := r.Ready(); len(rd.CommittedEntries) != 0 {
		t.Fatalf("applied entries handed out again: %v", rd.CommittedEntries)
	}
	cfg.Applied = 5
	if _, err := New(cfg); err == nil {
		t.Fatal("applied beyond the persisted log was accepted")
	}
}

// TestSimultaneousPreCandidatesDoNotSplit: when the leader of a three-node
// group dies and both followers time out together, the pre-vote tie-break
// lets exactly one of them run a real election, which it wins: the more
// up-to-date one, or with equal logs the lower ID.
func TestSimultaneousPreCandidatesDoNotSplit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		s3Ahead  bool
		wantLead string
	}{
		{"equal logs", false, "s2"},
		{"s3 ahead", true, "s3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nw := newNetwork(t, 3, func(c *Config) { c.PreVote, c.CheckQuorum = true, true })
			nw.campaign("s1")
			nw.propose("s1", "a")
			if tc.s3Ahead {
				nw.drop = func(m Message) bool { return m.To == "s2" }
				nw.propose("s1", "b") // reaches s3 only
				nw.drop = nil
			}
			nw.nodes["s1"] = nil // the leader dies
			term := nw.nodes["s2"].Term()
			nw.nodes["s2"].hup()
			nw.nodes["s3"].hup()
			nw.settle()
			if l := nw.leaders(); !slices.Equal(l, []string{tc.wantLead}) {
				t.Fatalf("leaders %v; want only %s", l, tc.wantLead)
			}
			if got := nw.nodes[tc.wantLead].Term(); got != term+1 {
				t.Fatalf("new leader's term %d; want %d (one real election)", got, term+1)
			}
		})
	}
}

// TestPreCandidateAsksAgainAfterLease: the first follower to time out is
// refused by the other, still inside its lease; when the other times out
// in turn it loses the tie-break, and the first asks it again instead of
// waiting out another election timeout.
func TestPreCandidateAsksAgainAfterLease(t *testing.T) {
	nw := newNetwork(t, 3, func(c *Config) { c.PreVote, c.CheckQuorum = true, true })
	nw.campaign("s1")
	nw.propose("s1", "a")
	nw.nodes["s1"] = nil // the leader dies
	nw.nodes["s2"].hup()
	nw.settle()
	if s := nw.nodes["s2"].State(); s != StatePreCandidate || len(nw.leaders()) != 0 {
		t.Fatalf("s2 is %v, leaders %v; s3 should have refused within its lease", s, nw.leaders())
	}
	nw.nodes["s3"].hup()
	nw.settle()
	if l := nw.leaders(); !slices.Equal(l, []string{"s2"}) {
		t.Fatalf("leaders %v; want s2 without another timeout", l)
	}
}

// voteFrom is a vote request from a candidate whose log matches r's last
// entry, so only the lease can make r refuse it.
func voteFrom(r *Raft, from string, t MsgType, term uint64) Message {
	return Message{Type: t, From: from, To: r.id, Term: term, Index: r.LastIndex(), LogTerm: r.log.lastTerm()}
}

// TestLeaseIgnoresDisruptiveVotes: with CheckQuorum on, a node that heard
// from a leader within the minimum election timeout ignores a vote request
// of a higher term: its term and vote stay as they were and it sends
// nothing. It refuses a pre-vote, also without changing its term (Raft
// thesis §4.2.3). This holds for followers and for the leader itself. Once
// the lease has run out, or with CheckQuorum off, the same request raises
// the term and wins the vote.
func TestLeaseIgnoresDisruptiveVotes(t *testing.T) {
	for _, pre := range []bool{false, true} {
		setup := func(t *testing.T, checkQuorum bool) *network {
			nw := newNetwork(t, 3, func(c *Config) { c.PreVote, c.CheckQuorum = pre, checkQuorum })
			nw.campaign("s1")
			nw.propose("s1", "a") // s2 and s3 hear from the leader
			return nw
		}
		t.Run(fmt.Sprintf("prevote=%v/inside the lease", pre), func(t *testing.T) {
			nw := setup(t, true)
			for _, id := range []string{"s1", "s2"} { // the leader, a follower
				r := nw.nodes[id]
				hs, soft := r.hardState(), r.softState()
				for _, typ := range []MsgType{MsgVote, MsgPreVote} {
					if err := r.Step(voteFrom(r, "s3", typ, hs.Term+5)); err != nil {
						t.Fatal(err)
					}
					if r.hardState() != hs || r.softState() != soft {
						t.Fatalf("%s: %v changed %+v %+v to %+v %+v", id, typ, hs, soft, r.hardState(), r.softState())
					}
					switch {
					case typ == MsgVote && len(r.msgs) != 0:
						t.Fatalf("%s answered a vote request inside its lease: %+v", id, r.msgs)
					case typ == MsgPreVote && (len(r.msgs) != 1 || r.msgs[0].Type != MsgPreVoteResp || !r.msgs[0].Reject):
						t.Fatalf("%s: pre-vote inside the lease answered with %+v; want one refusal", id, r.msgs)
					}
					r.msgs = nil
				}
			}
		})
		t.Run(fmt.Sprintf("prevote=%v/lease expired", pre), func(t *testing.T) {
			nw := setup(t, true)
			r := nw.nodes["s2"]
			r.randomTimeout = 2*nw.cfg.ElectionTick - 1 // stay a follower while the lease runs out
			term := r.Term()
			for range nw.cfg.ElectionTick - 1 {
				r.Tick()
			}
			if err := r.Step(voteFrom(r, "s3", MsgVote, term+5)); err != nil {
				t.Fatal(err)
			}
			if r.Term() != term || len(r.msgs) != 0 {
				t.Fatalf("one tick before the lease ends: term %d -> %d, sent %+v", term, r.Term(), r.msgs)
			}
			r.Tick() // the minimum election timeout has passed
			if err := r.Step(voteFrom(r, "s3", MsgVote, term+5)); err != nil {
				t.Fatal(err)
			}
			if r.Term() != term+5 || r.hardState().Vote != "s3" {
				t.Fatalf("after the lease: term %d vote %q; want term %d, vote for s3", r.Term(), r.hardState().Vote, term+5)
			}
		})
		t.Run(fmt.Sprintf("prevote=%v/checkquorum off", pre), func(t *testing.T) {
			nw := setup(t, false)
			r := nw.nodes["s2"]
			term := r.Term()
			if err := r.Step(voteFrom(r, "s3", MsgVote, term+5)); err != nil {
				t.Fatal(err)
			}
			if r.Term() != term+5 || r.hardState().Vote != "s3" {
				t.Fatalf("without CheckQuorum: term %d vote %q; want term %d, vote for s3", r.Term(), r.hardState().Vote, term+5)
			}
		})
	}
}

// TestTransferCampaignBypassesLease: the campaign a leader starts with
// TimeoutNow skips the pre-vote and marks its vote requests, and nodes
// inside their lease grant them, so the transfer completes at once. The
// mark is what gets it through: the same requests unmarked are ignored. A
// campaign started by a timeout is not marked, and a TimeoutNow from
// anyone but the leader starts nothing.
func TestTransferCampaignBypassesLease(t *testing.T) {
	for _, pre := range []bool{false, true} {
		t.Run(fmt.Sprintf("prevote=%v", pre), func(t *testing.T) {
			setup := func() (*network, uint64) {
				nw := newNetwork(t, 3, func(c *Config) { c.PreVote, c.CheckQuorum = pre, true })
				nw.campaign("s1")
				nw.propose("s1", "a")
				return nw, nw.nodes["s1"].Term()
			}
			timeoutNow := func(from string, term uint64) Message {
				return Message{Type: MsgTimeoutNow, From: from, To: "s2", Term: term}
			}

			nw, term := setup()
			s2 := nw.nodes["s2"]
			if err := s2.Step(timeoutNow("s3", term)); err != nil {
				t.Fatal(err)
			}
			if s2.State() != StateFollower || len(s2.msgs) != 0 {
				t.Fatalf("TimeoutNow from a follower: s2 is %v and sent %+v", s2.State(), s2.msgs)
			}
			if err := s2.Step(timeoutNow("s1", term)); err != nil {
				t.Fatal(err)
			}
			if s2.State() != StateCandidate || s2.Term() != term+1 || len(s2.msgs) != 2 {
				t.Fatalf("after the leader's TimeoutNow: s2 is %v at term %d, sent %+v", s2.State(), s2.Term(), s2.msgs)
			}
			for _, m := range s2.msgs {
				if m.Type != MsgVote || !m.Transfer {
					t.Fatalf("transfer campaign sent %v with Transfer=%v; want marked MsgVote", m.Type, m.Transfer)
				}
			}
			nw.settle()
			if l := nw.leaders(); !slices.Equal(l, []string{"s2"}) || s2.Term() != term+1 {
				t.Fatalf("transfer campaign: leaders %v, s2 term %d", l, s2.Term())
			}

			nw, term = setup()
			nw.edit = func(m Message) Message { m.Transfer = false; return m }
			if err := nw.nodes["s2"].Step(timeoutNow("s1", term)); err != nil {
				t.Fatal(err)
			}
			nw.settle()
			if l := nw.leaders(); !slices.Equal(l, []string{"s1"}) || nw.nodes["s1"].Term() != term || nw.nodes["s3"].Term() != term {
				t.Fatalf("unmarked votes inside the lease: leaders %v, s1 term %d, s3 term %d; want s1 leading term %d",
					l, nw.nodes["s1"].Term(), nw.nodes["s3"].Term(), term)
			}

			nw, _ = setup()
			s3 := nw.nodes["s3"]
			s3.hup() // an election timeout
			if len(s3.msgs) == 0 {
				t.Fatal("timeout campaign sent nothing")
			}
			for _, m := range s3.msgs {
				if m.Transfer {
					t.Fatalf("timeout campaign marked its %v as a transfer", m.Type)
				}
			}
		})
	}
}

// TestNoLeaseWithoutALeader: a node that has heard from no leader has no
// lease to protect, so even with CheckQuorum on it grants a higher-term
// vote at once. Otherwise a group that lost its leader could not elect
// another.
func TestNoLeaseWithoutALeader(t *testing.T) {
	nw := newNetwork(t, 3, func(c *Config) { c.CheckQuorum = true })
	r := nw.nodes["s2"]
	if r.Leader() != "" {
		t.Fatalf("setup: s2 already follows %q", r.Leader())
	}
	term := r.Term()
	if err := r.Step(voteFrom(r, "s3", MsgVote, term+1)); err != nil {
		t.Fatal(err)
	}
	if r.Term() != term+1 || r.hardState().Vote != "s3" || len(r.msgs) != 1 || r.msgs[0].Reject {
		t.Fatalf("leaderless follower: term %d vote %q sent %+v; want term %d, vote and grant for s3",
			r.Term(), r.hardState().Vote, r.msgs, term+1)
	}
}

// TestPreVoteKeepsTermStableUnderOneWayPartition: the leader's messages to
// s3 are lost but s3's own still arrive. s3 hears no leader and times out
// again and again; its pre-votes reach the leader and s2, both inside
// their lease, and fail, so no node's term moves, and s1 still leads once
// the link heals. Without PreVote, s3 would raise its own term at every
// timeout and, once healed, its answer to the leader's next append would
// depose it (Raft thesis §9.6).
func TestPreVoteKeepsTermStableUnderOneWayPartition(t *testing.T) {
	nw := newNetwork(t, 3, func(c *Config) { c.PreVote, c.CheckQuorum = true, true })
	nw.campaign("s1")
	nw.propose("s1", "a")
	term := nw.nodes["s1"].Term()
	tickAll := func(n int) {
		for range n {
			for _, id := range nw.ids {
				nw.nodes[id].Tick()
			}
			nw.settle()
		}
	}
	campaigns := 0
	nw.drop = func(m Message) bool {
		if m.From == "s3" && (m.Type == MsgPreVote || m.Type == MsgVote) {
			campaigns++
		}
		return m.From == "s1" && m.To == "s3"
	}
	tickAll(10 * nw.cfg.ElectionTick)
	if campaigns == 0 {
		t.Fatal("setup: s3 never campaigned")
	}
	nw.drop = nil
	tickAll(2 * nw.cfg.ElectionTick)
	for _, id := range nw.ids {
		if got := nw.nodes[id].Term(); got != term {
			t.Fatalf("%s is at term %d after a one-way partition; want %d", id, got, term)
		}
	}
	if l := nw.leaders(); !slices.Equal(l, []string{"s1"}) {
		t.Fatalf("leaders %v after the partition healed; want s1", l)
	}
}
