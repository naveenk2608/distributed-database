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
// It also checks ReadIndex safety: a confirmed read index covers every
// entry committed before the read started.
type Checker struct {
	leaders    map[uint64]string     // term -> leader
	committed  map[uint64]raft.Entry // index -> entry, from any node's commit index
	applied    map[uint64]raft.Entry // index -> entry, from any node's applied entries
	maxCommit  uint64
	readFloor  map[uint64]uint64 // read id -> highest commit index when it started
	appliedBy  map[string]map[string]bool
	violations []string
}

// NewChecker returns an empty Checker.
func NewChecker() *Checker {
	return &Checker{
		leaders:   map[uint64]string{},
		committed: map[uint64]raft.Entry{},
		applied:   map[uint64]raft.Entry{},
		readFloor: map[uint64]uint64{},
		appliedBy: map[string]map[string]bool{},
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

// LeaderElected records that id leads term with log, and checks Election
// Safety and Leader Completeness.
func (c *Checker) LeaderElected(term uint64, id string, log []raft.Entry) {
	if prev, ok := c.leaders[term]; ok && prev != id {
		c.violate("Election Safety: %s and %s both lead term %d", prev, id, term)
	}
	c.leaders[term] = id
	for _, i := range slices.Sorted(maps.Keys(c.committed)) {
		e := c.committed[i]
		if e.Term >= term {
			continue
		}
		if i > uint64(len(log)) || !sameEntry(log[i-1], e) {
			c.violate("Leader Completeness: leader %s of term %d lacks committed entry %d (term %d)", id, term, i, e.Term)
			return
		}
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
	}
}

// AppliedEverywhere reports whether every node in ids applied the entry
// carrying data.
func (c *Checker) AppliedEverywhere(data []byte, ids []string) bool {
	for _, id := range ids {
		if !c.appliedBy[string(data)][id] {
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

// LogMatching checks the Log Matching property over all pairs of logs.
func (c *Checker) LogMatching(logs map[string][]raft.Entry) {
	ids := slices.Sorted(maps.Keys(logs))
	for i, a := range ids {
		for _, b := range ids[i+1:] {
			la, lb := logs[a], logs[b]
			n := min(len(la), len(lb))
			// The highest index where both logs have the same term.
			k := -1
			for j := n - 1; j >= 0; j-- {
				if la[j].Term == lb[j].Term {
					k = j
					break
				}
			}
			for j := 0; j <= k; j++ {
				if !sameEntry(la[j], lb[j]) {
					c.violate("Log Matching: %s and %s agree at index %d (term %d) but differ at index %d", a, b, k+1, la[k].Term, j+1)
					return
				}
			}
		}
	}
}
