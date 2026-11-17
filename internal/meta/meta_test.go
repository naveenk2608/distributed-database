package meta

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
)

func ids(n int) []string {
	var out []string
	for i := range n {
		out = append(out, fmt.Sprintf("node%02d", i))
	}
	return out
}

func TestInitialLayoutIsDeterministic(t *testing.T) {
	a := InitialLayout(ids(5), 16, 3, 3)
	rev := ids(5)
	slices.Reverse(rev)
	b := InitialLayout(rev, 16, 3, 3)
	for s := Shard; s < 16; s++ {
		if !slices.Equal(a[s], b[s]) || len(a[s]) != 3 || len(slices.Compact(slices.Clone(a[s]))) != 3 {
			t.Fatalf("group %d: %v vs %v", s, a[s], b[s])
		}
	}
	if got := InitialLayout(ids(2), 4, 3, 3); len(got[0]) != 2 || len(got[Shard]) != 2 {
		t.Fatalf("fewer nodes than replicas: %v", got)
	}
}

// encode turns a bootstrap transaction into the records it writes.
func encode(txn *dkvv1.Txn) []KV {
	var out []KV
	for _, w := range txn.GetWrites() {
		out = append(out, KV{Key: w.GetKey(), Value: w.GetValue(), Version: 1})
	}
	return out
}

func newState(t *testing.T, nodes []string, shards, rf int) State {
	t.Helper()
	var recs []*dkvv1.NodeRecord
	for _, id := range nodes {
		recs = append(recs, &dkvv1.NodeRecord{NodeId: id, Addr: id + ":1", State: dkvv1.NodeState_NODE_STATE_ALIVE})
	}
	settings := &dkvv1.ClusterSettings{ClusterName: "t", Shards: uint32(shards), ReplicationFactor: uint32(rf), MetaReplicas: 3}
	s, err := Decode(encode(BootstrapTxn(settings, recs, InitialLayout(nodes, shards, rf, 3))))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Nodes) != len(nodes) || len(s.Descs) != shards+1 || s.Settings.GetShards() != uint32(shards) {
		t.Fatalf("decoded %d nodes, %d descriptors, settings %v", len(s.Nodes), len(s.Descs), s.Settings)
	}
	return s
}

// model is a set of groups the plan is applied to, with every learner
// catching up at once and every leadership transfer succeeding.
type model struct {
	t      *testing.T
	s      State
	groups map[int]Group
}

func newModel(t *testing.T, s State) *model {
	m := &model{t: t, s: s, groups: map[int]Group{}}
	for shard, d := range s.Descs {
		prog := map[string]uint64{}
		for _, v := range d.GetVoters() {
			prog[v] = 10 // the followers have caught up
		}
		m.groups[shard] = Group{Shard: shard, Leader: d.GetVoters()[0], Voters: slices.Clone(d.GetVoters()),
			Progress: prog, Commit: 10}
	}
	return m
}

func (m *model) alive(id string) bool {
	n := m.s.Nodes[id]
	return n != nil && n.GetState() != dkvv1.NodeState_NODE_STATE_DEAD
}

// apply performs a, checking the safety rules of a single step.
func (m *model) apply(a Action) {
	g := m.groups[a.Shard]
	switch a.Kind {
	case AddLearner:
		if len(g.Learners) > 0 || slices.Contains(g.Voters, a.Node) {
			m.t.Fatalf("%v with learners %v voters %v", a, g.Learners, g.Voters)
		}
		g.Learners = append(g.Learners, a.Node)
		g.Progress[a.Node] = g.Commit // catches up at once
	case Promote:
		g.Learners = slices.DeleteFunc(g.Learners, func(s string) bool { return s == a.Node })
		g.Voters = append(g.Voters, a.Node)
	case Remove:
		if slices.Contains(g.Learners, a.Node) {
			g.Learners = slices.DeleteFunc(g.Learners, func(s string) bool { return s == a.Node })
			break
		}
		if a.Node == g.Leader {
			m.t.Fatalf("%v removes the leader", a)
		}
		g.Voters = slices.DeleteFunc(g.Voters, func(s string) bool { return s == a.Node })
		live := 0
		for _, v := range g.Voters {
			if m.alive(v) {
				live++
			}
		}
		if live < len(g.Voters)/2+1 {
			m.t.Fatalf("%v leaves voters %v without a live majority", a, g.Voters)
		}
	case TransferLeader:
		if !slices.Contains(g.Voters, a.Node) || !m.alive(a.Node) {
			m.t.Fatalf("%v to a non-voter or dead node", a)
		}
		g.Leader = a.Node
	case UpdateDescriptor:
		m.s.Descs[a.Shard] = &dkvv1.ShardDescriptor{Shard: int32(a.Shard), Voters: slices.Clone(g.Voters), Learners: slices.Clone(g.Learners)}
	case MarkDecommissioned:
		m.s.Nodes[a.Node].State = dkvv1.NodeState_NODE_STATE_DECOMMISSIONED
	}
	if len(g.Voters) > int(m.s.Settings.GetReplicationFactor())+1 && a.Shard != Shard {
		m.t.Fatalf("%v leaves %d voters", a, len(g.Voters))
	}
	m.groups[a.Shard] = g
}

// converge applies plans until none is left and returns the rounds taken.
func (m *model) converge(maxRounds int) int {
	for round := 1; round <= maxRounds; round++ {
		acts := Plan(m.s, m.groups)
		if len(acts) == 0 {
			return round
		}
		for _, a := range acts {
			m.apply(a)
		}
		// A dead leader loses leadership to a live voter.
		for shard, g := range m.groups {
			if !m.alive(g.Leader) {
				for _, v := range g.Voters {
					if m.alive(v) {
						g.Leader = v
						break
					}
				}
				m.groups[shard] = g
			}
		}
	}
	m.t.Fatalf("no convergence in %d rounds", maxRounds)
	return 0
}

func (m *model) checkPlacement() {
	m.t.Helper()
	want := m.s.Desired()
	for shard, g := range m.groups {
		if !slices.Equal(sorted(g.Voters), want[shard]) || len(g.Learners) > 0 {
			m.t.Fatalf("group %d: voters %v learners %v; want %v", shard, g.Voters, g.Learners, want[shard])
		}
		d := m.s.Descs[shard]
		if !slices.Equal(sorted(d.GetVoters()), want[shard]) {
			m.t.Fatalf("group %d: descriptor %v; want %v", shard, d.GetVoters(), want[shard])
		}
	}
}

func TestDeadNodeIsReplaced(t *testing.T) {
	s := newState(t, ids(4), 8, 3)
	m := newModel(t, s)
	victim := "node01"
	m.s.Nodes[victim].State = dkvv1.NodeState_NODE_STATE_DEAD
	rounds := m.converge(20)
	m.checkPlacement()
	for shard, g := range m.groups {
		if slices.Contains(g.Voters, victim) || len(g.Voters) != 3 && shard != Shard {
			t.Fatalf("group %d: %v", shard, g.Voters)
		}
	}
	t.Logf("converged in %d rounds", rounds)
}

func TestNewNodeGetsItsShareAndLeaders(t *testing.T) {
	s := newState(t, ids(3), 16, 3)
	m := newModel(t, s)
	for shard, g := range m.groups { // every group led by one node
		g.Leader = "node00"
		m.groups[shard] = g
	}
	m.s.Nodes["node03"] = &dkvv1.NodeRecord{NodeId: "node03", State: dkvv1.NodeState_NODE_STATE_ALIVE}
	m.converge(40)
	m.checkPlacement()
	led := map[string]int{}
	held := 0
	for _, g := range m.groups {
		led[g.Leader]++
		if slices.Contains(g.Voters, "node03") {
			held++
		}
	}
	if held == 0 {
		t.Fatal("the new node holds no replica")
	}
	most, least := 0, 1<<30
	for _, id := range m.s.Eligible() {
		most, least = max(most, led[id]), min(least, led[id])
	}
	if most-least > 1 {
		t.Fatalf("leaders per node %v", led)
	}
}

func TestDecommission(t *testing.T) {
	s := newState(t, ids(4), 8, 3)
	m := newModel(t, s)
	m.s.Nodes["node02"].State = dkvv1.NodeState_NODE_STATE_DECOMMISSIONING
	m.converge(40)
	m.checkPlacement()
	if st := m.s.Nodes["node02"].GetState(); st != dkvv1.NodeState_NODE_STATE_DECOMMISSIONED {
		t.Fatalf("node02 is %v", st)
	}
}

func TestLearnerWaitsUntilCaughtUp(t *testing.T) {
	s := newState(t, ids(4), 1, 3)
	g := Group{Shard: 0, Leader: "a", Voters: []string{"node00", "node01"}, Learners: []string{"node02"},
		Progress: map[string]uint64{"node02": 5}, Commit: 9}
	g.Leader = "node00"
	want := s.Desired()[0]
	if !slices.Contains(want, "node02") {
		t.Skip("layout puts node02 elsewhere")
	}
	for _, a := range Plan(s, map[int]Group{0: g}) {
		if a.Shard == 0 && a.Kind != UpdateDescriptor {
			t.Fatalf("acted on a learner that is behind: %v", a)
		}
	}
	g.Progress["node02"] = 9
	acts := Plan(s, map[int]Group{0: g})
	if !slices.ContainsFunc(acts, func(a Action) bool { return a.Kind == Promote && a.Node == "node02" }) {
		t.Fatalf("caught-up learner not promoted: %v", acts)
	}
}

// TestPlanConvergesSafely: from random deaths, joins and decommissions,
// repeatedly applying the plan reaches the desired placement, one change
// per group at a time, never removing a leader, never leaving a group
// without a live majority, and never going beyond RF+1 voters.
func TestPlanConvergesSafely(t *testing.T) {
	for seed := range uint64(300) {
		rng := rand.New(rand.NewPCG(seed, 7))
		n := 3 + rng.IntN(5)
		s := newState(t, ids(n), 4+rng.IntN(12), 3)
		m := newModel(t, s)
		for range 1 + rng.IntN(3) {
			id := fmt.Sprintf("node%02d", rng.IntN(n+2))
			switch rec := m.s.Nodes[id]; {
			case rec == nil:
				m.s.Nodes[id] = &dkvv1.NodeRecord{NodeId: id, State: dkvv1.NodeState_NODE_STATE_ALIVE}
			case len(m.s.Eligible()) > 3 && rng.IntN(2) == 0:
				rec.State = dkvv1.NodeState_NODE_STATE_DEAD
			case len(m.s.Eligible()) > 3:
				rec.State = dkvv1.NodeState_NODE_STATE_DECOMMISSIONING
			}
		}
		// A group with no live majority cannot change (Raft needs a
		// quorum); skip those layouts.
		viable := true
		for _, g := range m.groups {
			live := 0
			for _, v := range g.Voters {
				if m.alive(v) {
					live++
				}
			}
			viable = viable && live >= len(g.Voters)/2+1
		}
		if !viable {
			continue
		}
		m.converge(60)
		m.checkPlacement()
	}
}

func TestLiveness(t *testing.T) {
	t0 := time.Unix(1000, 0)
	l := NewLiveness(time.Second, 3*time.Second, t0)
	if l.Dead("x", t0.Add(2*time.Second)) || !l.Suspect("x", t0.Add(2*time.Second)) {
		t.Fatal("a new leader must wait a full timeout before declaring death")
	}
	if !l.Dead("x", t0.Add(4*time.Second)) {
		t.Fatal("silent past the timeout is dead")
	}
	l.Heartbeat("y", t0.Add(3*time.Second))
	if l.Dead("y", t0.Add(5*time.Second)) || l.Suspect("y", t0.Add(3500*time.Millisecond)) || !l.Heard("y") {
		t.Fatal("a heartbeat restarts the clock")
	}
}

// TestDrainMovesLeadershipAway: a drained node keeps its replicas but ends
// up leading nothing, with leadership balanced over the others; undrained,
// it gets its share back.
func TestDrainMovesLeadershipAway(t *testing.T) {
	m := newModel(t, newState(t, ids(4), 16, 3))
	m.converge(40)
	m.s.Nodes["node00"].Drained = true
	m.converge(40)
	m.checkPlacement() // replicas stay where they were
	led := map[string]int{}
	for _, g := range m.groups {
		led[g.Leader]++
	}
	if led["node00"] != 0 {
		t.Fatalf("drained node00 still leads %d groups: %v", led["node00"], led)
	}
	most, least := 0, 1<<30
	for _, id := range []string{"node01", "node02", "node03"} {
		most, least = max(most, led[id]), min(least, led[id])
	}
	if most-least > 1 {
		t.Fatalf("leaders per undrained node %v", led)
	}
	m.s.Nodes["node00"].Drained = false
	m.converge(40)
	clear(led)
	for _, g := range m.groups {
		led[g.Leader]++
	}
	if led["node00"] == 0 {
		t.Fatalf("undrained node00 leads nothing: %v", led)
	}
}

// TestDrainKeepsALoneLeader: a group whose other voters are all drained or
// behind keeps its drained leader rather than lose leadership.
func TestDrainKeepsALoneLeader(t *testing.T) {
	m := newModel(t, newState(t, ids(3), 1, 3))
	for _, id := range ids(3) {
		m.s.Nodes[id].Drained = true
	}
	for _, a := range Plan(m.s, m.groups) {
		if a.Kind == TransferLeader {
			t.Fatalf("planned %v with every voter drained", a)
		}
	}
}
