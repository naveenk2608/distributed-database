package sim

import (
	"bytes"
	"fmt"
	"maps"
	"slices"

	"github.com/naveenk2608/distributed-database/internal/raft"
)

// Checker verifies Raft's safety properties (Raft paper, Figure 3) as a
// schedule runs:
//
//   - Election Safety: at most one leader per term.
//   - Log Matching: two logs with an entry of the same index and term are
//     identical up to that index.
//   - Leader Completeness: a committed entry is in the log of every leader
//     of a later term.
//   - State Machine Safety: no two nodes apply different entries at the
//     same index.
//
// It also checks ReadIndex safety (a confirmed read index covers every
// entry committed before the read started) and that every installed
// snapshot agrees with the committed entry at its index. Logs may be
// compacted: checks cover the entries a log still holds.
type Checker struct {
	leaders    map[uint64]string     // term -> leader
	committed  map[uint64]raft.Entry // index -> entry, from any node's commit index
	applied    map[uint64]raft.Entry // index -> entry, from any node's applied entries
	maxCommit  uint64
	readFloor  map[uint64]uint64 // read id -> highest commit index when it started
	appliedBy  map[string]map[string]bool
	appliedAt  map[string]uint64 // entry data -> its index
	violations []string
}

// LogView is a node's log after compaction: Entries starts at index First.
type LogView struct {
	First   uint64
	Entries []raft.Entry
}

// NewChecker returns an empty Checker.
func NewChecker() *Checker {
	return &Checker{
		leaders:   map[uint64]string{},
		committed: map[uint64]raft.Entry{},
		applied:   map[uint64]raft.Entry{},
		readFloor: map[uint64]uint64{},
		appliedBy: map[string]map[string]bool{},
		appliedAt: map[string]uint64{},
	}
}

func (c *Checker) violate(format string, args ...any) {
	c.violations = append(c.violations, fmt.Sprintf(format, args...))
}

// Violations returns every violation seen.
func (c *Checker) Violations() []string { return c.violations }

func sameEntry(a, b raft.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && a.Type == b.Type && bytes.Equal(a.Data, b.Data)
}

// LeaderElected records that id leads term with log (whose entries start
// at index first; earlier ones are in its snapshot), and checks Election
// Safety and Leader Completeness.
func (c *Checker) LeaderElected(term uint64, id string, first uint64, log []raft.Entry) {
	if prev, ok := c.leaders[term]; ok && prev != id {
		c.violate("Election Safety: %s and %s both lead term %d", prev, id, term)
	}
	c.leaders[term] = id
	for _, i := range slices.Sorted(maps.Keys(c.committed)) {
		e := c.committed[i]
		if e.Term >= term || i < first {
			continue
		}
		if i >= first+uint64(len(log)) || !sameEntry(log[i-first], e) {
			c.violate("Leader Completeness: leader %s of term %d lacks committed entry %d (term %d)", id, term, i, e.Term)
			return
		}
	}
}

// SnapshotInstalled records that node replaced its state with a snapshot,
// which must match the committed entry at its index.
func (c *Checker) SnapshotInstalled(node string, s raft.SnapshotMeta) {
	if e, ok := c.committed[s.Index]; ok && e.Term != s.Term {
		c.violate("State Machine Safety: %s installed a snapshot at %d (term %d) but the committed entry has term %d", node, s.Index, s.Term, e.Term)
	}
	if s.Index > c.maxCommit {
		c.violate("State Machine Safety: %s installed a snapshot at %d beyond any commit index (%d)", node, s.Index, c.maxCommit)
	}
}

// Committed records that node considers e committed.
func (c *Checker) Committed(node string, e raft.Entry) {
	if prev, ok := c.committed[e.Index]; ok && !sameEntry(prev, e) {
		c.violate("State Machine Safety: %s committed index %d with term %d, another node with term %d", node, e.Index, e.Term, prev.Term)
		return
	}
	c.committed[e.Index] = e
	c.maxCommit = max(c.maxCommit, e.Index)
}

// Applied records that node applied e.
func (c *Checker) Applied(node string, e raft.Entry) {
	if prev, ok := c.applied[e.Index]; ok && !sameEntry(prev, e) {
		c.violate("State Machine Safety: %s applied index %d (term %d %q), another node applied term %d %q",
			node, e.Index, e.Term, e.Data, prev.Term, prev.Data)
		return
	}
	if com, ok := c.committed[e.Index]; ok && !sameEntry(com, e) {
		c.violate("State Machine Safety: %s applied index %d, which differs from the committed entry", node, e.Index)
	}
	c.applied[e.Index] = e
	if len(e.Data) > 0 {
		key := string(e.Data)
		if c.appliedBy[key] == nil {
			c.appliedBy[key] = map[string]bool{}
		}
		c.appliedBy[key][node] = true
		c.appliedAt[key] = e.Index
	}
}

// AppliedEverywhere reports whether every node in ids applied the entry
// carrying data, directly or through a snapshot covering it (snaps holds
// each node's snapshot index).
func (c *Checker) AppliedEverywhere(data []byte, ids []string, snaps map[string]uint64) bool {
	idx, ok := c.appliedAt[string(data)]
	if !ok {
		return false
	}
	for _, id := range ids {
		if !c.appliedBy[string(data)][id] && snaps[id] < idx {
			return false
		}
	}
	return true
}

// ReadStarted records the commit floor a read must cover.
func (c *Checker) ReadStarted(id uint64) { c.readFloor[id] = c.maxCommit }

// ReadConfirmed checks a confirmed read index.
func (c *Checker) ReadConfirmed(node string, rs raft.ReadState) {
	floor, ok := c.readFloor[rs.RequestID]
	if !ok {
		c.violate("ReadIndex: %s confirmed unknown read %d", node, rs.RequestID)
		return
	}
	if rs.Index < floor {
		c.violate("ReadIndex: %s confirmed read %d at index %d, below the commit index %d when it started", node, rs.RequestID, rs.Index, floor)
	}
	delete(c.readFloor, rs.RequestID)
}

// LogMatching checks the Log Matching property over all pairs of logs,
// on the indexes both still hold.
func (c *Checker) LogMatching(logs map[string]LogView) {
	ids := slices.Sorted(maps.Keys(logs))
	for i, a := range ids {
		for _, b := range ids[i+1:] {
			la, lb := logs[a], logs[b]
			lo := max(la.First, lb.First)
			hi := min(la.First+uint64(len(la.Entries)), lb.First+uint64(len(lb.Entries))) // exclusive
			if lo >= hi {
				continue
			}
			at := func(v LogView, i uint64) raft.Entry { return v.Entries[i-v.First] }
			// The highest index where both logs have the same term.
			k := uint64(0)
			for j := hi - 1; j >= lo; j-- {
				if at(la, j).Term == at(lb, j).Term {
					k = j
					break
				}
				if j == lo {
					break
				}
			}
			if k == 0 {
				continue
			}
			for j := lo; j <= k; j++ {
				if !sameEntry(at(la, j), at(lb, j)) {
					c.violate("Log Matching: %s and %s agree at index %d (term %d) but differ at index %d", a, b, k, at(la, k).Term, j)
					return
				}
			}
		}
	}
}
