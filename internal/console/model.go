// Package console is the core of dkv-console, the dashboard backend: it
// polls every node's admin HTTP API, derives what the dashboard shows from
// the nodes' own numbers, keeps a history, and serves it all as JSON and
// server-sent events. It only reads from the cluster; the cluster never
// depends on it (docs/adr/0014, 0015).
package console

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
)

// Overview is everything the dashboard shows, as of one poll round.
type Overview struct {
	GeneratedAt    time.Time   `json:"generated_at"`
	PollIntervalMs int64       `json:"poll_interval_ms"`
	Nodes          []NodeView  `json:"nodes"`
	Shards         []ShardView `json:"shards"`
	// Totals add up the rates of the nodes that answered.
	Totals Rates       `json:"totals"`
	Events []EventView `json:"events"` // the most recent, newest last
}

// NodeView is one polled node.
type NodeView struct {
	Addr     string    `json:"addr"` // admin address the console polls
	NodeID   string    `json:"node_id"`
	Up       bool      `json:"up"` // answered the last poll
	Error    string    `json:"error,omitempty"`
	LastSeen time.Time `json:"last_seen,omitzero"`
	// State is the meta group's record of the node (alive, dead,
	// decommissioning, decommissioned), as another node that answered
	// reports it; Drained and Suspect likewise.
	State     string      `json:"state"`
	Drained   bool        `json:"drained"`
	Suspect   bool        `json:"suspect"`
	StartedAt time.Time   `json:"started_at,omitzero"`
	Rates     Rates       `json:"rates"`
	Leaders   int         `json:"leaders"` // groups it leads
	Storage   StorageView `json:"storage"`
	Replicas  []Replica   `json:"replicas"`
}

// Rates are a node's (or the cluster's) requests over the last poll
// interval, from the difference of two snapshots of its counters.
type Rates struct {
	ReadsPerSec  float64 `json:"reads_per_sec"`
	WritesPerSec float64 `json:"writes_per_sec"`
	ErrorsPerSec float64 `json:"errors_per_sec"`
	// P50Ms and P99Ms are latency percentiles over the interval, from the
	// node's histogram buckets; nil when there were no requests.
	P50Ms *float64 `json:"p50_ms"`
	P99Ms *float64 `json:"p99_ms"`
}

// StorageView adds up a node's engines, with its block cache.
type StorageView struct {
	MemtableBytes          int64  `json:"memtable_bytes"`
	TableBytes             uint64 `json:"table_bytes"`
	Tables                 int    `json:"tables"`
	Flushes                int64  `json:"flushes"`
	Compactions            int64  `json:"compactions"`
	CompactionBytesRead    int64  `json:"compaction_bytes_read"`
	CompactionBytesWritten int64  `json:"compaction_bytes_written"`
	WriteStalls            int64  `json:"write_stalls"`
	CacheUsedBytes         int64  `json:"cache_used_bytes"`
	CacheCapacityBytes     int64  `json:"cache_capacity_bytes"`
	CacheHits              int64  `json:"cache_hits"`
	CacheMisses            int64  `json:"cache_misses"`
}

// Replica is one replica on a node.
type Replica struct {
	Shard              int    `json:"shard"` // -1: the meta group
	Role               string `json:"role"`
	Term               uint64 `json:"term"`
	Leader             string `json:"leader"`
	Commit             uint64 `json:"commit"`
	Applied            uint64 `json:"applied"`
	FirstIndex         uint64 `json:"first_index"`
	LastIndex          uint64 `json:"last_index"`
	SnapshotsInstalled int64  `json:"snapshots_installed"`
	SnapshotsSent      int64  `json:"snapshots_sent"`
	MemtableBytes      int64  `json:"memtable_bytes"`
	TableBytes         uint64 `json:"table_bytes"`
	Tables             int    `json:"tables"`
	Compactions        int64  `json:"compactions"`
}

// ShardView is one Raft group as its leader reports it.
type ShardView struct {
	Shard    int      `json:"shard"`
	Leader   string   `json:"leader"` // node ID; empty when no answering node knows one
	Term     uint64   `json:"term"`
	Commit   uint64   `json:"commit"`
	Voters   []string `json:"voters"`
	Learners []string `json:"learners"`
	Members  []Member `json:"members"`
}

// Member is one member of a group. Match and Lag come from the leader's
// progress (entries the member's log is behind the leader's); Applied
// from the member's own report, when its node answered.
type Member struct {
	NodeID  string  `json:"node_id"`
	Voter   bool    `json:"voter"`
	Match   uint64  `json:"match"`
	Lag     uint64  `json:"lag"`
	Applied *uint64 `json:"applied"`
}

// EventView is one event, merged from the nodes' logs.
type EventView struct {
	Time     time.Time `json:"time"`
	Kind     string    `json:"kind"`
	Reporter string    `json:"reporter"` // the node that recorded it
	Shard    *int      `json:"shard"`    // nil for events about no group
	Node     string    `json:"node,omitempty"`
	Term     uint64    `json:"term,omitempty"`
	Index    uint64    `json:"index,omitempty"`
	Detail   string    `json:"detail,omitempty"`
	Seq      uint64    `json:"seq"`
}

// Key identifies an event across the nodes' logs. A membership change is
// applied, and reported, by every member of its group, so one key covers
// all of them. Every other event is its reporter's own: its sequence
// number, with its time because a restarted node numbers from 1 again.
func (e EventView) Key() string {
	if isMembership(e.Kind) && e.Shard != nil {
		return fmt.Sprintf("%s/%d/%d", e.Kind, *e.Shard, e.Index)
	}
	return fmt.Sprintf("%s#%d@%d", e.Reporter, e.Seq, e.Time.UnixNano())
}

func eventView(reporter string, e *dkvv1.Event) EventView {
	v := EventView{Time: time.Unix(0, e.GetUnixNano()), Kind: e.GetKind(), Reporter: reporter, Node: e.GetNode(),
		Term: e.GetTerm(), Index: e.GetIndex(), Detail: e.GetDetail(), Seq: e.GetSeq()}
	if e.GetHasShard() {
		s := int(e.GetShard())
		v.Shard = &s
	}
	return v
}

// interval is what a node's counters did between two snapshots.
type interval struct {
	rates  Rates
	bounds []float64 // latency bucket bounds, seconds
	cum    []uint64  // requests at most bounds[i], all operations together
	total  uint64    // requests, all operations together
}

// delta derives a node's requests over the interval between two of its
// statistics snapshots. With no previous snapshot, or across a restart
// (counters reset), there is no interval.
func delta(prev, cur *dkvv1.StatsResponse) (interval, bool) {
	var iv interval
	if prev == nil || cur == nil || prev.GetStartedUnixNano() != cur.GetStartedUnixNano() {
		return iv, false
	}
	secs := float64(cur.GetUnixNano()-prev.GetUnixNano()) / 1e9
	if secs <= 0 {
		return iv, false
	}
	before := map[string]*dkvv1.OpStats{}
	for _, o := range prev.GetOps() {
		before[o.GetOp()] = o
	}
	iv.bounds = cur.GetLatencyBoundsSeconds()
	iv.cum = make([]uint64, len(iv.bounds))
	for _, o := range cur.GetOps() {
		p := before[o.GetOp()]
		ok, errs := o.GetOk()-min(p.GetOk(), o.GetOk()), o.GetErrors()-min(p.GetErrors(), o.GetErrors())
		if o.GetOp() == "get" {
			iv.rates.ReadsPerSec += float64(ok) / secs
		} else {
			iv.rates.WritesPerSec += float64(ok) / secs
		}
		iv.rates.ErrorsPerSec += float64(errs) / secs
		iv.total += ok + errs
		pc := p.GetLatencyCumulative()
		for i, c := range o.GetLatencyCumulative() {
			if i >= len(iv.cum) {
				break
			}
			if i < len(pc) {
				c -= min(pc[i], c)
			}
			iv.cum[i] += c
		}
	}
	iv.rates.P50Ms, iv.rates.P99Ms = iv.percentiles()
	return iv, true
}

// percentiles returns the interval's p50 and p99 latency, nil without
// requests.
func (iv interval) percentiles() (p50, p99 *float64) {
	if iv.total == 0 {
		return nil, nil
	}
	return percentileMs(iv.bounds, iv.cum, iv.total, 0.50), percentileMs(iv.bounds, iv.cum, iv.total, 0.99)
}

// add sums two nodes' intervals; their buckets must match.
func (iv interval) add(o interval) interval {
	if len(iv.bounds) == 0 {
		iv.bounds, iv.cum = o.bounds, make([]uint64, len(o.bounds))
	}
	if !slices.Equal(iv.bounds, o.bounds) {
		return iv // a node with other buckets: leave it out of the percentiles
	}
	for i := range iv.cum {
		iv.cum[i] += o.cum[i]
	}
	iv.total += o.total
	iv.rates.ReadsPerSec += o.rates.ReadsPerSec
	iv.rates.WritesPerSec += o.rates.WritesPerSec
	iv.rates.ErrorsPerSec += o.rates.ErrorsPerSec
	return iv
}

// percentileMs estimates the q-quantile of total requests whose cumulative
// bucket counts are cum (cum[i]: at most bounds[i] seconds), interpolating
// linearly inside the bucket it falls in. Past the last bound it reports
// the last bound: the histogram knows no more.
func percentileMs(bounds []float64, cum []uint64, total uint64, q float64) *float64 {
	rank := q * float64(total)
	lower, below := 0.0, uint64(0)
	for i, b := range bounds {
		if float64(cum[i]) >= rank {
			in := cum[i] - below
			ms := lower * 1000
			if in > 0 {
				ms += (b - lower) * 1000 * (rank - float64(below)) / float64(in)
			}
			return &ms
		}
		lower, below = b, cum[i]
	}
	ms := lower * 1000
	return &ms
}

// storageAndReplicas sums a node's engines and lists its replicas.
func storageAndReplicas(st *dkvv1.StatsResponse) (StorageView, []Replica, int) {
	var s StorageView
	var reps []Replica
	leaders := 0
	for _, r := range st.GetReplicas() {
		rs, e := r.GetRaft(), r.GetEngine()
		s.MemtableBytes += e.GetMemtableBytes()
		s.TableBytes += e.GetTableBytes()
		s.Tables += int(e.GetTables())
		s.Flushes += e.GetFlushes()
		s.Compactions += e.GetCompactions()
		s.CompactionBytesRead += e.GetCompactionBytesRead()
		s.CompactionBytesWritten += e.GetCompactionBytesWritten()
		s.WriteStalls += e.GetWriteStalls()
		if rs.GetRole() == "leader" {
			leaders++
		}
		reps = append(reps, Replica{Shard: int(rs.GetShard()), Role: rs.GetRole(), Term: rs.GetTerm(), Leader: rs.GetLeader(),
			Commit: rs.GetCommit(), Applied: rs.GetApplied(), FirstIndex: rs.GetFirstIndex(), LastIndex: rs.GetLastIndex(),
			SnapshotsInstalled: rs.GetSnapshotsInstalled(), SnapshotsSent: rs.GetSnapshotsSent(),
			MemtableBytes: e.GetMemtableBytes(), TableBytes: e.GetTableBytes(), Tables: int(e.GetTables()), Compactions: e.GetCompactions()})
	}
	if c := st.GetBlockCache(); c != nil {
		s.CacheUsedBytes, s.CacheCapacityBytes, s.CacheHits, s.CacheMisses = c.GetUsedBytes(), c.GetCapacityBytes(), c.GetHits(), c.GetMisses()
	}
	slices.SortFunc(reps, func(a, b Replica) int { return cmp.Compare(a.Shard, b.Shard) })
	return s, reps, leaders
}

// shardViews builds every group's view from the nodes' statistics: the
// leader at the highest term describes the group; every answering member
// adds its own applied index.
func shardViews(stats map[string]*dkvv1.StatsResponse) []ShardView {
	type leaderReport struct {
		rs *dkvv1.ReplicaStatus
	}
	leaders := map[int]leaderReport{}
	applied := map[int]map[string]uint64{}
	seen := map[int]bool{}
	for nodeID, st := range stats {
		for _, r := range st.GetReplicas() {
			rs := r.GetRaft()
			shard := int(rs.GetShard())
			seen[shard] = true
			if applied[shard] == nil {
				applied[shard] = map[string]uint64{}
			}
			applied[shard][nodeID] = rs.GetApplied()
			if rs.GetRole() == "leader" && (leaders[shard].rs == nil || rs.GetTerm() > leaders[shard].rs.GetTerm()) {
				leaders[shard] = leaderReport{rs}
			}
		}
	}
	var out []ShardView
	for _, shard := range slices.Sorted(maps.Keys(seen)) {
		v := ShardView{Shard: shard, Voters: []string{}, Learners: []string{}, Members: []Member{}}
		if l := leaders[shard].rs; l != nil {
			v.Leader, v.Term, v.Commit = l.GetLeader(), l.GetTerm(), l.GetCommit()
			v.Voters, v.Learners = slices.Clone(l.GetVoters()), slices.Clone(l.GetLearners())
			for _, id := range append(slices.Clone(l.GetVoters()), l.GetLearners()...) {
				m := Member{NodeID: id, Voter: slices.Contains(l.GetVoters(), id), Match: l.GetProgress()[id]}
				m.Lag = l.GetLastIndex() - min(m.Match, l.GetLastIndex())
				if a, ok := applied[shard][id]; ok {
					m.Applied = &a
				}
				v.Members = append(v.Members, m)
			}
		}
		out = append(out, v)
	}
	return out
}

// stateName shortens a node state for display.
func stateName(s dkvv1.NodeState) string {
	if s == dkvv1.NodeState_NODE_STATE_UNSPECIFIED {
		return "unknown"
	}
	return strings.ToLower(strings.TrimPrefix(s.String(), "NODE_STATE_"))
}
