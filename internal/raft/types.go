// Package raft is dkv's Raft consensus core: a deterministic state machine
// with no goroutines, timers, network or disk I/O.
//
// The caller (internal/replica, or the simulator in tests) drives it:
// Tick advances logical time, Step delivers a message from a peer, Propose
// and ReadIndex start client operations. After any of these, Ready returns
// what the outside world must do, in this order:
//
//  1. persist HardState and Entries (fsync) — before anything below, so a
//     vote or an acknowledgment is never sent and then forgotten;
//  2. send Messages;
//  3. apply CommittedEntries to the state machine and answer ReadStates;
//
// and then call Advance. The design follows etcd/raft (docs/adr/0003):
// leader election with randomized timeouts, PreVote, CheckQuorum, log
// replication with conflict hints, committing only entries of the
// leader's current term by counting, and ReadIndex.
package raft

import "fmt"

// State is a node's role.
type State uint8

const (
	StateFollower State = iota
	StatePreCandidate
	StateCandidate
	StateLeader
)

func (s State) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StatePreCandidate:
		return "pre-candidate"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return fmt.Sprintf("State(%d)", uint8(s))
	}
}

// EntryType distinguishes log entries.
type EntryType uint8

const (
	// EntryNormal carries application data; an empty one is the no-op a
	// new leader appends to commit entries of earlier terms.
	EntryNormal EntryType = iota
	// EntryConfChange carries a membership change (Phase 7).
	EntryConfChange
)

// Entry is one log entry.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// HardState is what must be persisted before a node communicates: its
// current term, its vote in that term, and the commit index.
type HardState struct {
	Term   uint64
	Vote   string
	Commit uint64
}

// IsEmpty reports whether h is the zero HardState.
func (h HardState) IsEmpty() bool { return h == HardState{} }

// MsgType is the type of a Message.
type MsgType uint8

const (
	MsgPreVote MsgType = iota + 1
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
)

func (t MsgType) String() string {
	names := [...]string{"", "PreVote", "PreVoteResp", "Vote", "VoteResp", "App", "AppResp", "Heartbeat", "HeartbeatResp"}
	if int(t) < len(names) && t != 0 {
		return names[t]
	}
	return fmt.Sprintf("MsgType(%d)", uint8(t))
}

// Message is exchanged between nodes of one group.
type Message struct {
	Type MsgType
	From string
	To   string
	Term uint64
	// LogTerm and Index: for MsgApp, the entry preceding Entries; for
	// (pre)votes, the candidate's last entry; for MsgAppResp, the index
	// the follower now matches (or, when rejecting, the MsgApp's Index).
	LogTerm uint64
	Index   uint64
	Entries []Entry
	Commit  uint64
	Reject  bool
	// RejectHint and HintTerm let the leader skip a whole conflicting term
	// at once: the follower's best guess of where the logs agree.
	RejectHint uint64
	HintTerm   uint64
	// Context identifies the ReadIndex round a heartbeat belongs to.
	Context uint64
}

// ReadState confirms a ReadIndex request: once the state machine has
// applied Index, a read reflects every write committed before the request.
type ReadState struct {
	Index     uint64
	RequestID uint64
}

// SoftState is volatile state worth reporting: who leads, and our role.
type SoftState struct {
	Lead  string
	State State
}

// Ready is the work the caller must do; see the package comment.
type Ready struct {
	// SoftState is non-nil if it changed.
	SoftState *SoftState
	// HardState is non-empty if it must be persisted.
	HardState HardState
	// Entries must be persisted, replacing any persisted entries at or
	// after Entries[0].Index.
	Entries []Entry
	// MustSync is true if HardState and Entries must be durable before
	// any Message is sent: new entries, or a new term or vote. A change of
	// the commit index alone need not be: a node that forgets it learns
	// it again from the leader, and never applied anything it had not
	// already persisted.
	MustSync         bool
	CommittedEntries []Entry
	Messages         []Message
	ReadStates       []ReadState
}
