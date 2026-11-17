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
	// persisted state per node, as a runner would keep it
	hs    map[string]HardState
	logs  map[string][]Entry
	drop  func(Message) bool
	edit  func(Message) Message // applied to delivered messages
	sent  int
	reads map[string][]ReadState // confirmed reads per node
}

func newNetwork(t *testing.T, n int, configure func(*Config)) *network {
	t.Helper()
	nw := &network{t: t, nodes: map[string]*Raft{}, hs: map[string]HardState{}, logs: map[string][]Entry{},
		reads: map[string][]ReadState{}}
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
	cfg.HardState, cfg.Entries = nw.hs[id], slices.Clone(nw.logs[id])
	r, err := New(cfg)
	if err != nil {
		nw.t.Fatal(err)
	}
	nw.nodes[id] = r
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
			if !rd.HardState.IsEmpty() {
				nw.hs[id] = rd.HardState
			}
			if len(rd.Entries) > 0 {
				nw.logs[id] = append(nw.logs[id][:rd.Entries[0].Index-1], rd.Entries...)
			}
			nw.reads[id] = append(nw.reads[id], rd.ReadStates...)
			r.Advance(rd)
			for _, m := range rd.Messages {
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
		if l := nw.leaders(); !slices.Equal(l, []string{"s1"}) || nw.nodes["s1"].Term() != 1 {
			t.Fatalf("prevote=%v: leaders %v term %d", pre, l, nw.nodes["s1"].Term())
		}
		nw.propose("s1", "a")
		nw.propose("s1", "b")
		for _, id := range nw.ids {
			ents := nw.nodes[id].Entries()
			if len(ents) != 3 || string(ents[2].Data) != "b" {
				t.Fatalf("%s log %v", id, ents)
			}
		}
		if nw.nodes["s1"].Committed() != 3 {
			t.Fatalf("leader committed %d", nw.nodes["s1"].Committed())
		}
		nw.nodes["s1"].bcastHeartbeat(0) // carries the commit index to followers
		nw.settle()
		for _, id := range nw.ids {
			if c := nw.nodes[id].Committed(); c != 3 {
				t.Errorf("%s committed %d, want 3", id, c)
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
	nw.campaign("s1") // term 1; no-op at index 1 everywhere
	if nw.nodes["s1"].Committed() != 1 {
		t.Fatalf("setup: committed %d", nw.nodes["s1"].Committed())
	}

	// s1 replicates entry X (index 2, term 1) to s2 only.
	nw.drop = func(m Message) bool { return (m.From != "s1" || m.To != "s2") && (m.From != "s2" || m.To != "s1") }
	nw.propose("s1", "X")

	// s5 becomes leader of term 2 with votes from s3 and s4, appends its
	// no-op at index 2 (term 2), and replicates nothing.
	nw.drop = func(m Message) bool {
		if m.Type == MsgVote || m.Type == MsgVoteResp {
			return isolate("s1", "s2")(m)
		}
		return true
	}
	nw.campaign("s5")
	if nw.nodes["s5"].State() != StateLeader {
		t.Fatalf("setup: s5 is %v", nw.nodes["s5"].State())
	}

	// s1 wins term 3 with votes from s2 and s3, then replicates X (term 1)
	// to s3, one entry per message, but its term-3 no-op reaches nobody:
	// it is stripped from every append, so followers acknowledge X and the
	// leader learns that a majority holds it.
	nw.drop = func(m Message) bool { return isolate("s4", "s5")(m) }
	nw.nodes["s1"].becomeFollower(nw.nodes["s5"].Term(), "") // learns term 2 exists
	nw.nodes["s1"].hup()
	l := nw.nodes["s1"]
	nw.drop = func(m Message) bool {
		if isolate("s4", "s5")(m) {
			return true
		}
		// Once the leader knows a follower holds X, stop delivering the
		// stripped appends it keeps retrying.
		return m.Type == MsgApp && m.From == "s1" && l.prs[m.To].match >= 2 &&
			slices.ContainsFunc(m.Entries, func(e Entry) bool { return e.Term == 3 })
	}
	nw.edit = func(m Message) Message {
		if m.Type == MsgApp {
			m.Entries = slices.DeleteFunc(slices.Clone(m.Entries), func(e Entry) bool { return e.Term == 3 })
		}
		return m
	}
	nw.settle()
	if l.State() != StateLeader || l.Term() != 3 {
		t.Fatalf("setup: s1 is %v in term %d", l.State(), l.Term())
	}
	holders := 0
	for _, id := range nw.ids {
		if ents := nw.nodes[id].Entries(); len(ents) >= 2 && string(ents[1].Data) == "X" &&
			(id == "s1" || l.prs[id].match >= 2) {
			holders++
		}
	}
	if holders < 3 {
		for _, id := range nw.ids {
			t.Logf("%s term %d state %v log %v match %d", id, nw.nodes[id].Term(), nw.nodes[id].State(), nw.nodes[id].Entries(), l.prs[id].match)
		}
		t.Fatalf("setup: the leader knows X is on %d nodes, want a majority", holders)
	}

	// The crux: X (term 1) is on a majority but must not be committed by
	// counting, since s5 (last entry term 2) could still win and overwrite
	// index 2.
	if c := nw.nodes["s1"].Committed(); c >= 2 {
		t.Fatalf("leader of term 3 committed index %d holding a term-1 entry by counting replicas (Figure 8)", c)
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
	nw.drop = isolate("s3") // s2 votes for s1 in term 1
	nw.campaign("s1")
	if nw.hs["s2"].Vote != "s1" || nw.hs["s2"].Term != 1 {
		t.Fatalf("persisted state of s2: %+v", nw.hs["s2"])
	}
	nw.start("s2", 1234) // crash and restart from persisted state
	if err := nw.nodes["s2"].Step(Message{Type: MsgVote, From: "s3", To: "s2", Term: 1}); err != nil {
		t.Fatal(err)
	}
	rd := nw.nodes["s2"].Ready()
	for _, m := range rd.Messages {
		if m.Type == MsgVoteResp && !m.Reject {
			t.Fatal("restarted node voted twice in term 1")
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
	nw.propose("s1", "x")
	if nw.nodes["s1"].Committed() != 2 {
		t.Fatalf("committed %d", nw.nodes["s1"].Committed())
	}
	if err := nw.nodes["s1"].ReadIndex(1); err != nil {
		t.Fatal(err)
	}
	if rd := nw.nodes["s1"].Ready(); len(rd.ReadStates) != 1 || rd.ReadStates[0].Index != 2 {
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
