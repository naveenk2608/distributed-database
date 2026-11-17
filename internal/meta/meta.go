// Package meta holds the logic of dkv's meta group, kept free of I/O so it
// can be tested on its own: the keys and records the meta group stores,
// the initial layout of a bootstrapped cluster, node liveness as the meta
// leader tracks it, and the rebalancer's plan, which compares the
// placement the hash ring wants for the live nodes with each group's
// actual membership and picks the next single step for each group.
//
// The meta group is an ordinary Raft group (shard Shard = -1) whose state
// machine is the same key-value store as a data shard; its records are
// changed only by conditional transactions. Each data group's own Raft
// membership is authoritative; descriptors in the meta group cache it for
// routing. See docs/adr/0011, 0012 and 0013.
package meta

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/placement"
)

// Shard identifies the meta group among the shard numbers.
const Shard = -1

// Key prefixes of the meta group's records.
const (
	settingsKey = "c/settings"
	nodePrefix  = "n/"
	descPrefix  = "d/"
)

// SettingsKey is the key of the cluster settings.
func SettingsKey() []byte { return []byte(settingsKey) }

// NodeKey is the key of a node's record.
func NodeKey(id string) []byte { return []byte(nodePrefix + id) }

// DescKey is the key of a group's descriptor.
func DescKey(shard int) []byte {
	if shard == Shard {
		return []byte(descPrefix + "meta")
	}
	return fmt.Appendf(nil, "%s%05d", descPrefix, shard)
}

// KV is one stored record with its version.
type KV struct {
	Key, Value []byte
	Version    uint64
}

// State is the decoded content of the meta group.
type State struct {
	Settings        *dkvv1.ClusterSettings
	SettingsVersion uint64
	Nodes           map[string]*dkvv1.NodeRecord
	NodeVersions    map[string]uint64
	Descs           map[int]*dkvv1.ShardDescriptor
	DescVersions    map[int]uint64
}

// Decode builds a State from every record of the meta group.
func Decode(kvs []KV) (State, error) {
	s := State{Nodes: map[string]*dkvv1.NodeRecord{}, NodeVersions: map[string]uint64{},
		Descs: map[int]*dkvv1.ShardDescriptor{}, DescVersions: map[int]uint64{}}
	for _, kv := range kvs {
		k := string(kv.Key)
		switch {
		case k == settingsKey:
			st := &dkvv1.ClusterSettings{}
			if err := proto.Unmarshal(kv.Value, st); err != nil {
				return s, fmt.Errorf("meta: settings: %w", err)
			}
			s.Settings, s.SettingsVersion = st, kv.Version
		case strings.HasPrefix(k, nodePrefix):
			n := &dkvv1.NodeRecord{}
			if err := proto.Unmarshal(kv.Value, n); err != nil {
				return s, fmt.Errorf("meta: %s: %w", k, err)
			}
			s.Nodes[n.GetNodeId()], s.NodeVersions[n.GetNodeId()] = n, kv.Version
		case strings.HasPrefix(k, descPrefix):
			d := &dkvv1.ShardDescriptor{}
			if err := proto.Unmarshal(kv.Value, d); err != nil {
				return s, fmt.Errorf("meta: %s: %w", k, err)
			}
			s.Descs[int(d.GetShard())], s.DescVersions[int(d.GetShard())] = d, kv.Version
		}
	}
	return s, nil
}

func mustMarshal(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(err) // the meta records are plain messages that always marshal
	}
	return b
}

func put(key []byte, m proto.Message) *dkvv1.TxnWrite {
	return &dkvv1.TxnWrite{Key: key, Value: mustMarshal(m)}
}

// Layout is a cluster's initial placement: each group's voters.
type Layout map[int][]string

// InitialLayout places shards and the meta group on the bootstrap nodes:
// the hash ring over their IDs gives each data shard rf voters and the
// meta group metaReplicas voters. Every bootstrap node computes the same
// layout from the same IDs.
func InitialLayout(ids []string, shards, rf, metaReplicas int) Layout {
	ring := placement.NewRing(ids)
	out := Layout{Shard: sorted(ring.ReplicaSet(Shard, metaReplicas))}
	for s := range shards {
		out[s] = sorted(ring.ReplicaSet(s, rf))
	}
	return out
}

func sorted(ids []string) []string {
	out := slices.Clone(ids)
	slices.Sort(out)
	return out
}

// BootstrapTxn writes a new cluster's settings, node records and
// descriptors, only if no settings exist yet; the first meta leader of a
// new cluster proposes it, and a repeat is a no-op.
func BootstrapTxn(settings *dkvv1.ClusterSettings, nodes []*dkvv1.NodeRecord, layout Layout) *dkvv1.Txn {
	t := &dkvv1.Txn{Conditions: []*dkvv1.TxnCondition{{Key: SettingsKey(), Version: 0}}}
	t.Writes = append(t.Writes, put(SettingsKey(), settings))
	for _, n := range nodes {
		t.Writes = append(t.Writes, put(NodeKey(n.GetNodeId()), n))
	}
	for _, s := range slices.Sorted(maps.Keys(layout)) {
		t.Writes = append(t.Writes, put(DescKey(s), &dkvv1.ShardDescriptor{Shard: int32(s), Voters: layout[s]}))
	}
	return t
}

// PutNodeTxn writes a node record if the stored one is still at version.
func PutNodeTxn(n *dkvv1.NodeRecord, version uint64) *dkvv1.Txn {
	return &dkvv1.Txn{
		Conditions: []*dkvv1.TxnCondition{{Key: NodeKey(n.GetNodeId()), Version: version}},
		Writes:     []*dkvv1.TxnWrite{put(NodeKey(n.GetNodeId()), n)},
	}
}

// PutDescTxn writes a descriptor if the stored one is still at version.
func PutDescTxn(d *dkvv1.ShardDescriptor, version uint64) *dkvv1.Txn {
	return &dkvv1.Txn{
		Conditions: []*dkvv1.TxnCondition{{Key: DescKey(int(d.GetShard())), Version: version}},
		Writes:     []*dkvv1.TxnWrite{put(DescKey(int(d.GetShard())), d)},
	}
}

// Liveness is the meta leader's soft state about node heartbeats. Only
// decisions (a node is dead, or alive again) are written to the meta log.
type Liveness struct {
	SuspectAfter, DeadAfter time.Duration
	since                   time.Time
	last                    map[string]time.Time
}

// NewLiveness starts tracking at now, when this node became meta leader:
// nobody is declared dead before a full DeadAfter has passed, so a leader
// change cannot cause false failures.
func NewLiveness(suspectAfter, deadAfter time.Duration, now time.Time) *Liveness {
	return &Liveness{SuspectAfter: suspectAfter, DeadAfter: deadAfter, since: now, last: map[string]time.Time{}}
}

// Heartbeat records that id was heard from at now.
func (l *Liveness) Heartbeat(id string, now time.Time) {
	if now.After(l.last[id]) {
		l.last[id] = now
	}
}

func (l *Liveness) silence(id string, now time.Time) time.Duration {
	last := l.last[id]
	if last.Before(l.since) {
		last = l.since
	}
	return now.Sub(last)
}

// Suspect reports whether id has been silent for SuspectAfter.
func (l *Liveness) Suspect(id string, now time.Time) bool { return l.silence(id, now) > l.SuspectAfter }

// Dead reports whether id has been silent for DeadAfter.
func (l *Liveness) Dead(id string, now time.Time) bool { return l.silence(id, now) > l.DeadAfter }

// Heard reports whether id has heartbeated since tracking started.
func (l *Liveness) Heard(id string) bool { return !l.last[id].IsZero() }

// Group is a group's actual state, as its leader reports it.
type Group struct {
	Shard    int
	Leader   string
	Voters   []string
	Learners []string
	// Progress is each member's match index; Commit the leader's commit.
	Progress map[string]uint64
	Commit   uint64
}

// ActionKind is one step the rebalancer can take.
type ActionKind int

const (
	AddLearner ActionKind = iota + 1
	Promote
	Remove
	TransferLeader
	UpdateDescriptor
	MarkDecommissioned
)

func (k ActionKind) String() string {
	return [...]string{"", "add-learner", "promote", "remove", "transfer-leader", "update-descriptor", "mark-decommissioned"}[k]
}

// Action is one step: Node is the member added, promoted, removed or
// marked, or the transferee.
type Action struct {
	Kind  ActionKind
	Shard int
	Node  string
}

func (a Action) String() string {
	return a.Kind.String() + " " + shardName(a.Shard) + " " + a.Node
}

func shardName(s int) string {
	if s == Shard {
		return "meta"
	}
	return "shard " + strconv.Itoa(s)
}

// Eligible returns the nodes placement may use: alive and not being
// decommissioned.
func (s State) Eligible() []string {
	var out []string
	for id, n := range s.Nodes {
		if n.GetState() == dkvv1.NodeState_NODE_STATE_ALIVE {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// drained reports whether an operator asked node id to lead no group.
func (s State) drained(id string) bool { return s.Nodes[id].GetDrained() }

// Desired returns the voters the ring wants for every group over the
// eligible nodes.
func (s State) Desired() Layout {
	if s.Settings == nil {
		return nil
	}
	return InitialLayout(s.Eligible(), int(s.Settings.GetShards()), int(s.Settings.GetReplicationFactor()),
		int(s.Settings.GetMetaReplicas()))
}

// Plan returns at most one step for each group whose actual membership
// (groups, keyed by shard) differs from the desired placement, plus
// descriptor repairs, decommission completions, a leader transfer off each
// drained node's groups and at most one more for balance. Groups without a
// known leader are left alone.
//
// Each group changes one server at a time: a learner is added, promoted
// once it has caught up, and only then is a surplus voter removed (after
// moving leadership off it). A voter on a dead node is removed first, so
// a group never needs every remaining member for its quorum while it is
// being repaired.
func Plan(s State, groups map[int]Group) []Action {
	desired := s.Desired()
	if desired == nil {
		return nil
	}
	alive := func(id string) bool {
		n := s.Nodes[id]
		return n != nil && (n.GetState() == dkvv1.NodeState_NODE_STATE_ALIVE ||
			n.GetState() == dkvv1.NodeState_NODE_STATE_DECOMMISSIONING)
	}
	var out []Action
	busy := map[int]bool{}
	for _, shard := range slices.Sorted(maps.Keys(desired)) {
		g, ok := groups[shard]
		if !ok || g.Leader == "" || len(g.Voters) == 0 {
			continue
		}
		if a, ok := planGroup(g, desired[shard], alive, s.drained); ok {
			out = append(out, a)
			busy[shard] = true
			continue
		}
		if d := s.Descs[shard]; d == nil || !slices.Equal(sorted(d.GetVoters()), sorted(g.Voters)) ||
			!slices.Equal(sorted(d.GetLearners()), sorted(g.Learners)) {
			out = append(out, Action{Kind: UpdateDescriptor, Shard: shard})
		}
	}
	// A decommissioning node that no group holds any more is done.
	for _, id := range slices.Sorted(maps.Keys(s.Nodes)) {
		if s.Nodes[id].GetState() != dkvv1.NodeState_NODE_STATE_DECOMMISSIONING || len(groups) < len(desired) {
			continue
		}
		held := false
		for _, g := range groups {
			held = held || slices.Contains(g.Voters, id) || slices.Contains(g.Learners, id)
		}
		if !held {
			out = append(out, Action{Kind: MarkDecommissioned, Node: id})
		}
	}
	out = append(out, drainLeaders(s, groups, busy, alive)...)
	if a, ok := balanceLeaders(s, groups, busy, alive); ok {
		out = append(out, a)
	}
	return out
}

// drainLeaders moves leadership off drained nodes: for each stable group a
// drained node leads, a transfer to the caught-up, undrained voter that
// leads the fewest groups. A group with no such voter keeps its leader.
func drainLeaders(s State, groups map[int]Group, busy map[int]bool, alive func(string) bool) []Action {
	led := map[string]int{}
	for _, g := range groups {
		led[g.Leader]++
	}
	var out []Action
	for _, shard := range slices.Sorted(maps.Keys(groups)) {
		g := groups[shard]
		if busy[shard] || g.Leader == "" || !s.drained(g.Leader) || len(g.Learners) > 0 {
			continue
		}
		to := ""
		for _, v := range g.Voters {
			if v == g.Leader || !alive(v) || s.drained(v) || g.Progress[v] < g.Commit {
				continue
			}
			if to == "" || led[v] < led[to] {
				to = v
			}
		}
		if to != "" {
			out = append(out, Action{Kind: TransferLeader, Shard: shard, Node: to})
			busy[shard] = true
			led[to]++
		}
	}
	return out
}

func planGroup(g Group, want []string, alive, drained func(string) bool) (Action, bool) {
	act := func(k ActionKind, node string) (Action, bool) {
		return Action{Kind: k, Shard: g.Shard, Node: node}, true
	}
	// 1. A learner: promote it once caught up if it belongs, else drop it.
	if len(g.Learners) > 0 {
		l := g.Learners[0]
		if !slices.Contains(want, l) || !alive(l) || len(g.Voters) > len(want) {
			return act(Remove, l)
		}
		if g.Progress[l] >= g.Commit {
			return act(Promote, l)
		}
		return Action{}, false // still catching up
	}
	// 2. A voter on a dead or unknown node goes first.
	if len(g.Voters) > 1 {
		for _, v := range g.Voters {
			if !alive(v) && v != g.Leader {
				return act(Remove, v)
			}
		}
	}
	// 3. More voters than wanted (after a promotion): move leadership off
	// a voter that does not belong, then remove it.
	if len(g.Voters) > len(want) {
		for _, v := range g.Voters {
			if slices.Contains(want, v) {
				continue
			}
			if v != g.Leader {
				return act(Remove, v)
			}
			// Hand leadership to a wanted voter, an undrained one if any.
			to := ""
			for _, w := range g.Voters {
				if w != v && slices.Contains(want, w) && alive(w) && (to == "" || drained(to) && !drained(w)) {
					to = w
				}
			}
			if to != "" {
				return act(TransferLeader, to)
			}
		}
		return Action{}, false
	}
	// 4. Too few of the wanted nodes: add one as a learner. With at most
	// as many voters as wanted, its promotion leaves at most one extra.
	for _, w := range want {
		if !slices.Contains(g.Voters, w) && alive(w) {
			return act(AddLearner, w)
		}
	}
	return Action{}, false
}

// balanceLeaders moves one leadership off a node that leads at least two
// more stable groups than one of the group's other voters, preferring the
// busiest node and the least busy transferee. Drained nodes take no part:
// drainLeaders empties them.
func balanceLeaders(s State, groups map[int]Group, busy map[int]bool, alive func(string) bool) (Action, bool) {
	count := map[string]int{}
	for _, id := range s.Eligible() {
		if !s.drained(id) {
			count[id] = 0
		}
	}
	for _, g := range groups {
		if _, ok := count[g.Leader]; ok {
			count[g.Leader]++
		}
	}
	best, bestGap := Action{}, 1
	for _, shard := range slices.Sorted(maps.Keys(groups)) {
		g := groups[shard]
		lc, ok := count[g.Leader]
		if !ok || busy[shard] || len(g.Learners) > 0 {
			continue
		}
		for _, v := range g.Voters {
			vc, ok := count[v]
			if !ok || v == g.Leader || !alive(v) || g.Progress[v] < g.Commit {
				continue
			}
			if gap := lc - vc; gap > bestGap {
				best, bestGap = Action{Kind: TransferLeader, Shard: shard, Node: v}, gap
			}
		}
	}
	return best, best.Kind != 0
}
