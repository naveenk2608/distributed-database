package raft

import "fmt"

// raftLog is the in-memory log. entries[0] is a sentinel holding the index
// and term of the entry just before the first real one: the last entry
// covered by the latest snapshot (0 and 0 without one), so term lookups
// need no special cases.
type raftLog struct {
	entries   []Entry
	committed uint64
	applied   uint64
	// stable is the highest index known to be persisted; entries after it
	// are handed out in Ready.
	stable uint64
}

func newLog(snap SnapshotMeta, persisted []Entry, committed, applied uint64) (*raftLog, error) {
	l := &raftLog{entries: []Entry{{Index: snap.Index, Term: snap.Term}}}
	for i, e := range persisted {
		if e.Index != snap.Index+uint64(i+1) {
			return nil, fmt.Errorf("raft: persisted entry %d has index %d", snap.Index+uint64(i+1), e.Index)
		}
		prevTerm := snap.Term
		if i > 0 {
			prevTerm = persisted[i-1].Term
		}
		if e.Term < prevTerm {
			return nil, fmt.Errorf("raft: persisted entry %d has term %d after %d", e.Index, e.Term, prevTerm)
		}
	}
	l.entries = append(l.entries, persisted...)
	if committed > l.lastIndex() {
		return nil, fmt.Errorf("raft: commit index %d beyond last persisted entry %d", committed, l.lastIndex())
	}
	if applied > l.lastIndex() {
		return nil, fmt.Errorf("raft: state machine applied %d, beyond last persisted entry %d", applied, l.lastIndex())
	}
	if applied < snap.Index {
		return nil, fmt.Errorf("raft: state machine applied %d, behind the snapshot at %d", applied, snap.Index)
	}
	// The commit index is persisted without fsync (Ready.MustSync), so it
	// can lag what the state machine applied. Everything applied was
	// committed.
	l.committed = max(committed, applied)
	l.applied = applied
	l.stable = l.lastIndex()
	return l, nil
}

// compact discards entries up to and including i, which becomes the
// sentinel. The caller guarantees i <= applied.
func (l *raftLog) compact(i uint64) {
	off := l.entries[0].Index
	if i <= off {
		return
	}
	rest := l.entries[i-off:]
	ents := make([]Entry, len(rest))
	copy(ents, rest)
	ents[0] = Entry{Index: rest[0].Index, Term: rest[0].Term}
	l.entries = ents
}

// restore replaces the log with an empty one after snapshot s.
func (l *raftLog) restore(s SnapshotMeta) {
	l.entries = []Entry{{Index: s.Index, Term: s.Term}}
	l.committed, l.applied, l.stable = s.Index, s.Index, s.Index
}

func (l *raftLog) firstIndex() uint64 { return l.entries[0].Index + 1 }
func (l *raftLog) lastIndex() uint64  { return l.entries[len(l.entries)-1].Index }
func (l *raftLog) lastTerm() uint64   { return l.entries[len(l.entries)-1].Term }

// term returns the term of the entry at i, or false if the log does not
// cover i.
func (l *raftLog) term(i uint64) (uint64, bool) {
	off := l.entries[0].Index
	if i < off || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-off].Term, true
}

func (l *raftLog) matchTerm(i, t uint64) bool {
	got, ok := l.term(i)
	return ok && got == t
}

// isUpToDate reports whether a log ending at (index, term) is at least as
// up to date as this one (Raft §5.4.1).
func (l *raftLog) isUpToDate(index, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && index >= l.lastIndex())
}

// slice returns entries [lo, hi) with at most max entries; the caller
// guarantees firstIndex <= lo <= hi <= lastIndex+1.
func (l *raftLog) slice(lo, hi uint64, max int) []Entry {
	off := l.entries[0].Index
	if hi-lo > uint64(max) {
		hi = lo + uint64(max)
	}
	out := make([]Entry, hi-lo)
	copy(out, l.entries[lo-off:hi-off])
	return out
}

// append adds entries that follow index prev, truncating any conflicting
// suffix first. It returns the index of the first entry it wrote, or 0 if
// it already had them all. The caller has checked that the entry at prev
// matches.
func (l *raftLog) append(ents []Entry) uint64 {
	for i, e := range ents {
		if e.Index <= l.entries[0].Index {
			continue // covered by the snapshot, so committed and matching
		}
		t, ok := l.term(e.Index)
		if ok && t == e.Term {
			continue // already have it
		}
		// First new or conflicting entry: drop everything from here on. A
		// committed entry never conflicts (Log Matching plus the commit
		// rule), so this never truncates below the commit index.
		if e.Index <= l.committed {
			panic(fmt.Sprintf("raft: entry %d (term %d) conflicts with committed entry (term %d)", e.Index, e.Term, t))
		}
		off := l.entries[0].Index
		l.entries = append(l.entries[:e.Index-off], ents[i:]...)
		if e.Index <= l.stable {
			l.stable = e.Index - 1 // the persisted suffix must be rewritten
		}
		return e.Index
	}
	return 0
}

// findConflictByTerm returns the largest index <= index whose term is <=
// term, and that term: the furthest point where a log ending in term at
// index could agree with this one.
func (l *raftLog) findConflictByTerm(index, term uint64) (uint64, uint64) {
	index = min(index, l.lastIndex())
	for {
		t, ok := l.term(index)
		if !ok {
			return index, 0
		}
		if t <= term {
			return index, t
		}
		index--
	}
}

func (l *raftLog) commitTo(i uint64) bool {
	if i > l.committed {
		if i > l.lastIndex() {
			panic(fmt.Sprintf("raft: commit %d beyond last index %d", i, l.lastIndex()))
		}
		l.committed = i
		return true
	}
	return false
}

func (l *raftLog) unstable() []Entry {
	if l.stable >= l.lastIndex() {
		return nil
	}
	return l.slice(l.stable+1, l.lastIndex()+1, len(l.entries))
}

// nextCommitted returns committed entries not yet handed out for applying.
// Only persisted entries are applied: the caller persists a Ready's entries
// before applying its committed ones.
func (l *raftLog) nextCommitted() []Entry {
	hi := l.committed
	if hi <= l.applied {
		return nil
	}
	return l.slice(l.applied+1, hi+1, len(l.entries))
}
