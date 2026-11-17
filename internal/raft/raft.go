package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
)

// Errors returned by the operations that start work.
var (
	// ErrNotLeader means this node cannot accept the request; Leader()
	// names the leader it knows of, if any.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrTransferring means the leader is handing leadership over and
	// accepts no proposals until the transfer ends.
	ErrTransferring = errors.New("raft: leadership transfer in progress")
	// ErrConfChangePending means a membership change cannot start yet:
	// the previous one has not committed, or the leader has not yet
	// committed an entry of its own term. Retry later.
	ErrConfChangePending = errors.New("raft: a membership change is pending")
)

// Config configures a node.
type Config struct {
	// ID identifies this node within the group.
	ID string
	// Peers bootstraps a new group: when the node has no persisted state,
	// it starts with a first entry making Peers (which must include ID)
	// the voters. With no persisted state and no Peers the node is
	// uninitialized: it waits for a leader to send it the log or a
	// snapshot, and never campaigns before it is a voter.
	Peers []string
	// ElectionTick is the election timeout in ticks; a follower that hears
	// nothing from a leader for a random time in [ElectionTick,
	// 2*ElectionTick) starts an election.
	ElectionTick int
	// HeartbeatTick is the leader's heartbeat interval in ticks.
	HeartbeatTick int
	// PreVote makes a would-be candidate first check that it could win,
	// without disturbing the term (Raft thesis §9.6).
	PreVote bool
	// CheckQuorum makes a leader step down when it has not heard from a
	// quorum within an election timeout, and lets nodes that heard from a
	// leader recently ignore vote requests.
	CheckQuorum bool
	// Rand drives election timeout randomization. Required; tests seed it.
	Rand *rand.Rand

	// Snapshot, HardState, Entries and Applied restore a node after a
	// restart: the latest snapshot or compaction point, the persisted
	// HardState and the entries after the snapshot, and the last applied
	// index.
	Snapshot  SnapshotMeta
	HardState HardState
	Entries   []Entry
	Applied   uint64

	// MaxEntriesPerMsg bounds the entries in one append message.
	// 0 means 64.
	MaxEntriesPerMsg int
	// MaxInflight bounds the append messages in flight to one follower
	// while replicating. 0 means 8.
	MaxInflight int
}

type progress struct {
	match, next uint64
	// probing: the leader does not know where the follower's log matches
	// and sends one append at a time; paused: that append is in flight.
	// Otherwise it pipelines up to MaxInflight appends.
	probing, paused bool
	// snapshotting: a snapshot is being streamed to the follower; nothing
	// else is sent until the caller reports the outcome.
	snapshotting bool
	inflight     int
	// stallMatch is match at the previous heartbeat response; no progress
	// between two responses means appends were lost.
	stallMatch uint64
	// recentActive: heard from in the current CheckQuorum window.
	recentActive bool
}

type readRequest struct {
	id    uint64
	index uint64
	acks  map[string]bool
}

// Raft is one node's consensus state. It is not safe for concurrent use.
type Raft struct {
	id  string
	cfg Config

	term  uint64
	vote  string
	log   *raftLog
	state State
	lead  string

	// conf is the membership in effect: that of the last membership entry
	// in the log, else of the snapshot. confIndex is that entry's index.
	conf      ConfState
	confIndex uint64
	snapConf  ConfState // membership at the log's sentinel (snapshot) index

	prs   map[string]*progress
	votes map[string]bool

	electionElapsed int
	// ticks counts every Tick since New. Leaders heartbeat when it is a
	// multiple of HeartbeatTick rather than HeartbeatTick after their
	// election, so all groups ticked by one clock heartbeat on the same
	// ticks and a runner can send their heartbeats in one batch.
	ticks         int
	randomTimeout int

	// leadTransferee is the voter a leader is handing leadership to.
	leadTransferee  string
	transferElapsed int

	msgs       []Message
	readStates []ReadState
	reads      []*readRequest // leader: reads awaiting a quorum of heartbeat acks
	waiting    []uint64       // leader: reads waiting for the first commit of the term

	pendingSnapshot *SnapshotMeta // accepted, not yet handed out in Ready

	prevSoft SoftState
	prevHard HardState
}

// New returns a node restored from cfg.
func New(cfg Config) (*Raft, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft: empty node ID")
	}
	if cfg.ElectionTick <= cfg.HeartbeatTick || cfg.HeartbeatTick < 1 {
		return nil, fmt.Errorf("raft: election tick %d must exceed heartbeat tick %d >= 1", cfg.ElectionTick, cfg.HeartbeatTick)
	}
	if cfg.Rand == nil {
		return nil, errors.New("raft: Config.Rand is required")
	}
	if cfg.MaxEntriesPerMsg <= 0 {
		cfg.MaxEntriesPerMsg = 64
	}
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 8
	}
	hs, ents := cfg.HardState, cfg.Entries
	bootstrap := cfg.Snapshot.IsEmpty() && len(ents) == 0 && hs.IsEmpty() && len(cfg.Peers) > 0
	if bootstrap {
		if !slices.Contains(cfg.Peers, cfg.ID) {
			return nil, fmt.Errorf("raft: ID %q must be one of the bootstrap peers %v", cfg.ID, cfg.Peers)
		}
		voters := slices.Clone(cfg.Peers)
		slices.Sort(voters)
		voters = slices.Compact(voters)
		// Every bootstrap member writes this same entry, committed by
		// construction, so a node that joins later learns the initial
		// voters by replaying the log.
		ents = []Entry{{Index: 1, Term: 1, Type: EntryConfChange,
			Data: EncodeConfChange(ConfChange{Type: ConfBootstrap, Conf: ConfState{Voters: voters}})}}
		hs = HardState{Term: 1, Commit: 1}
	}
	l, err := newLog(cfg.Snapshot, ents, hs.Commit, cfg.Applied)
	if err != nil {
		return nil, err
	}
	r := &Raft{id: cfg.ID, cfg: cfg, log: l, term: hs.Term, vote: hs.Vote,
		snapConf: cfg.Snapshot.Conf.Clone()}
	if err := r.rescanConf(); err != nil {
		return nil, err
	}
	r.becomeFollower(r.term, "")
	r.prevHard = r.hardState()
	if bootstrap {
		// The bootstrap entry and HardState are new: hand them out.
		l.stable = 0
		r.prevHard = HardState{}
	}
	r.prevSoft = r.softState()
	return r, nil
}

// ID returns this node's ID.
func (r *Raft) ID() string { return r.id }

// Term returns the current term.
func (r *Raft) Term() uint64 { return r.term }

// State returns this node's role.
func (r *Raft) State() State { return r.state }

// Leader returns the leader this node knows of, or "".
func (r *Raft) Leader() string { return r.lead }

// Committed returns the commit index.
func (r *Raft) Committed() uint64 { return r.log.committed }

// Applied returns the index of the last entry handed out for applying.
func (r *Raft) Applied() uint64 { return r.log.applied }

// LastIndex returns the index of the last log entry.
func (r *Raft) LastIndex() uint64 { return r.log.lastIndex() }

// FirstIndex returns the index of the first entry still in the log.
func (r *Raft) FirstIndex() uint64 { return r.log.firstIndex() }

// Conf returns the membership in effect.
func (r *Raft) Conf() ConfState { return r.conf.Clone() }

// Initialized reports whether this node knows its group's membership.
func (r *Raft) Initialized() bool { return len(r.conf.Voters) > 0 }

// Progress returns, on a leader, each member's known match index.
func (r *Raft) Progress() map[string]uint64 {
	if r.state != StateLeader {
		return nil
	}
	out := make(map[string]uint64, len(r.prs))
	for id, pr := range r.prs {
		out[id] = pr.match
	}
	return out
}

// Entries returns a copy of the in-memory log after the snapshot (for
// tests and the simulator's invariant checks).
func (r *Raft) Entries() []Entry {
	return r.log.slice(r.log.firstIndex(), r.log.lastIndex()+1, len(r.log.entries))
}

// Snapshot returns the metadata of the log's compaction point.
func (r *Raft) Snapshot() SnapshotMeta {
	return SnapshotMeta{Index: r.log.entries[0].Index, Term: r.log.entries[0].Term, Conf: r.snapConf.Clone()}
}

func (r *Raft) quorum() int { return len(r.conf.Voters)/2 + 1 }

func (r *Raft) hardState() HardState {
	return HardState{Term: r.term, Vote: r.vote, Commit: r.log.committed}
}

func (r *Raft) softState() SoftState { return SoftState{Lead: r.lead, State: r.state} }

func (r *Raft) send(m Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

// confAt returns the membership in effect at index i, which the log must
// cover (firstIndex-1 <= i <= lastIndex), and the index that set it.
func (r *Raft) confAt(i uint64) (ConfState, uint64, error) {
	off := r.log.entries[0].Index
	for j := i; j > off; j-- {
		e := r.log.entries[j-off]
		if e.Type != EntryConfChange {
			continue
		}
		cc, err := DecodeConfChange(e.Data)
		if err != nil {
			return ConfState{}, 0, fmt.Errorf("raft: membership entry %d: %w", j, err)
		}
		return cc.Conf, j, nil
	}
	return r.snapConf.Clone(), off, nil
}

// rescanConf recomputes the membership from the log.
func (r *Raft) rescanConf() error {
	conf, idx, err := r.confAt(r.log.lastIndex())
	if err != nil {
		return err
	}
	r.setConf(conf, idx)
	return nil
}

// noteAppended updates the membership after entries from first on were
// written to the log (first 0: nothing was written).
func (r *Raft) noteAppended(first uint64) {
	if first == 0 {
		return
	}
	if first <= r.confIndex {
		// The entry defining the membership was overwritten.
		if err := r.rescanConf(); err != nil {
			panic(err) // entries a leader produced are always decodable
		}
		return
	}
	off := r.log.entries[0].Index
	for j := r.log.lastIndex(); j >= first; j-- {
		e := r.log.entries[j-off]
		if e.Type == EntryConfChange {
			cc, err := DecodeConfChange(e.Data)
			if err != nil {
				panic(fmt.Sprintf("raft: membership entry %d: %v", j, err))
			}
			r.setConf(cc.Conf, j)
			return
		}
	}
}

// setConf puts a membership into effect. A leader starts or stops
// replicating to the members that came or went.
func (r *Raft) setConf(conf ConfState, index uint64) {
	r.conf, r.confIndex = conf.Clone(), index
	if r.state != StateLeader {
		return
	}
	for _, id := range conf.Members() {
		if r.prs[id] == nil {
			r.prs[id] = &progress{next: r.log.lastIndex() + 1, probing: true, recentActive: true}
		}
	}
	for id := range r.prs {
		if id != r.id && !conf.IsMember(id) {
			delete(r.prs, id)
		}
	}
	if r.leadTransferee != "" && !conf.IsVoter(r.leadTransferee) {
		r.leadTransferee = ""
	}
}

func (r *Raft) resetTimers() {
	r.electionElapsed = 0
	r.randomTimeout = r.cfg.ElectionTick + r.cfg.Rand.IntN(r.cfg.ElectionTick)
}

func (r *Raft) reset(term uint64) {
	if r.term != term {
		r.term = term
		r.vote = ""
	}
	r.lead = ""
	r.votes = nil
	r.prs = nil
	r.reads = nil
	r.waiting = nil
	r.leadTransferee = ""
	r.resetTimers()
}

func (r *Raft) becomeFollower(term uint64, lead string) {
	r.reset(term)
	r.lead = lead
	r.state = StateFollower
}

func (r *Raft) becomePreCandidate() {
	// The term does not change: a pre-candidate that cannot win must not
	// force everyone into a new term.
	r.lead = ""
	r.votes = map[string]bool{}
	r.resetTimers()
	r.state = StatePreCandidate
}

func (r *Raft) becomeCandidate() {
	r.reset(r.term + 1)
	r.vote = r.id
	r.votes = map[string]bool{}
	r.state = StateCandidate
}

func (r *Raft) becomeLeader() {
	r.reset(r.term)
	r.lead = r.id
	r.state = StateLeader
	r.prs = map[string]*progress{}
	for _, p := range append(r.conf.Members(), r.id) {
		r.prs[p] = &progress{next: r.log.lastIndex() + 1, probing: true, recentActive: true}
	}
	// A no-op entry of the new term: entries of earlier terms commit only
	// when an entry of the current term does (Raft §5.4.2), and ReadIndex
	// and membership changes wait for it.
	r.appendEntries(Entry{Type: EntryNormal})
	r.bcastAppend()
}

// appendEntries adds entries to the leader's log and counts them as
// matched by the leader itself.
func (r *Raft) appendEntries(ents ...Entry) {
	li := r.log.lastIndex()
	for i := range ents {
		ents[i].Term = r.term
		ents[i].Index = li + 1 + uint64(i)
	}
	r.noteAppended(r.log.append(ents))
	self := r.prs[r.id]
	self.match = r.log.lastIndex()
	self.next = self.match + 1
	if r.maybeCommit() {
		r.afterCommit()
	}
}

// maybeCommit advances the commit index to the highest index a quorum of
// voters matches, but only if that entry is from the current term.
// Committing an older-term entry by counting replicas is the bug of
// Figure 8 in the Raft paper: it could still be overwritten.
func (r *Raft) maybeCommit() bool {
	if len(r.conf.Voters) == 0 {
		return false
	}
	matches := make([]uint64, 0, len(r.conf.Voters))
	for _, v := range r.conf.Voters {
		if pr := r.prs[v]; pr != nil {
			matches = append(matches, pr.match)
		} else {
			matches = append(matches, 0)
		}
	}
	slices.Sort(matches)
	mci := matches[len(matches)-r.quorum()]
	if t, ok := r.log.term(mci); ok && t == r.term && r.log.commitTo(mci) {
		r.releaseWaitingReads()
		return true
	}
	return false
}

// afterCommit tells followers about a new commit index and, if this leader
// was removed from the voters by an entry that has now committed, steps
// down (Raft thesis §4.2.2).
func (r *Raft) afterCommit() {
	r.bcastAppend()
	if !r.conf.IsVoter(r.id) && r.log.committed >= r.confIndex {
		r.becomeFollower(r.term, "")
	}
}

// Tick advances logical time by one tick.
func (r *Raft) Tick() {
	r.ticks++
	switch r.state {
	case StateLeader:
		r.electionElapsed++
		if r.leadTransferee != "" {
			r.transferElapsed++
			if r.transferElapsed >= r.cfg.ElectionTick {
				r.leadTransferee = "" // the transferee did not take over in time
			}
		}
		if r.electionElapsed >= r.cfg.ElectionTick {
			r.electionElapsed = 0
			if r.cfg.CheckQuorum && !r.quorumActive() {
				r.becomeFollower(r.term, "") // cut off from a majority: step down
				return
			}
			for p, pr := range r.prs {
				pr.recentActive = p == r.id
			}
		}
		if r.ticks%r.cfg.HeartbeatTick == 0 {
			r.bcastHeartbeat(0)
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.randomTimeout {
			r.hup()
		}
	}
}

func (r *Raft) quorumActive() bool {
	n := 0
	for _, v := range r.conf.Voters {
		if pr := r.prs[v]; pr != nil && pr.recentActive {
			n++
		}
	}
	return n >= r.quorum()
}

// hup starts an election after an election timeout: a pre-vote round if
// PreVote is on (every timeout starts a new one), otherwise a real vote.
func (r *Raft) hup() {
	if r.state == StateLeader || !r.promotable() {
		// A node that may not campaign keeps counting time since it last
		// heard a leader, so its lease expires and it can vote for others.
		return
	}
	r.startElection(r.cfg.PreVote, false)
}

// promotable reports whether this node may campaign: it is a voter, or it
// was one before a membership change that has not committed yet. Such a
// node may still be needed to commit its own removal (Raft thesis
// §4.2.2); it campaigns among the new voters without counting itself.
func (r *Raft) promotable() bool {
	if r.conf.IsVoter(r.id) {
		return true
	}
	if r.confIndex <= r.log.committed || r.confIndex == 0 {
		return false
	}
	prev, _, err := r.confAt(r.confIndex - 1)
	return err == nil && prev.IsVoter(r.id)
}

func (r *Raft) startElection(pre, transfer bool) {
	voteType, term := MsgVote, uint64(0)
	if pre {
		r.becomePreCandidate()
		voteType, term = MsgPreVote, r.term+1
	} else {
		r.becomeCandidate()
		term = r.term
	}
	r.votes[r.id] = true
	if r.wonElection() {
		r.advanceElection()
		return
	}
	for _, p := range r.conf.Voters {
		if p != r.id {
			r.send(Message{Type: voteType, To: p, Term: term, Index: r.log.lastIndex(), LogTerm: r.log.lastTerm(), Transfer: transfer})
		}
	}
}

func (r *Raft) wonElection() bool {
	n := 0
	for _, v := range r.conf.Voters {
		if r.votes[v] {
			n++
		}
	}
	return n >= r.quorum()
}

func (r *Raft) lostElection() bool {
	n := 0
	for _, v := range r.conf.Voters {
		if granted, ok := r.votes[v]; ok && !granted {
			n++
		}
	}
	return n > len(r.conf.Voters)-r.quorum()
}

func (r *Raft) advanceElection() {
	if r.state == StatePreCandidate {
		r.startElection(false, false) // won the pre-vote: now the real election
	} else {
		r.becomeLeader()
	}
}

// Step processes a message from a peer. Messages from nodes outside the
// membership are accepted: a new member learns of its addition from the
// leader's messages, and a candidate's own membership decides its votes.
func (r *Raft) Step(m Message) error {
	if m.From == "" || m.From == r.id {
		return fmt.Errorf("raft: message from %q", m.From)
	}
	switch {
	case m.Term > r.term:
		switch {
		case m.Type == MsgPreVote:
			// Answered below without changing term.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// A granted pre-vote carries the future term; ours is unchanged.
		case m.Type == MsgVote && r.inLease() && !m.Transfer:
			// We heard from a leader within an election timeout: this
			// candidate is likely a node that was cut off; do not let it
			// depose a working leader (CheckQuorum lease). A leader
			// transfer is the leader's own request and overrides it.
			return nil
		default:
			lead := ""
			if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
				lead = m.From
			}
			r.becomeFollower(m.Term, lead)
		}
	case m.Term < r.term:
		switch m.Type {
		case MsgApp, MsgHeartbeat, MsgSnap:
			// A stale leader. Tell it the current term so it steps down;
			// with PreVote and CheckQuorum it would otherwise never learn.
			r.send(Message{Type: MsgAppResp, To: m.From})
		case MsgPreVote:
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Reject: true})
		}
		return nil
	}

	switch m.Type {
	case MsgPreVote, MsgVote:
		r.handleVote(m)
		return nil
	}
	switch r.state {
	case StateLeader:
		r.stepLeader(m)
	case StateCandidate, StatePreCandidate:
		r.stepCandidate(m)
	default:
		r.stepFollower(m)
	}
	return nil
}

// inLease reports whether this node heard from a leader within the
// minimum election timeout, so a vote request now cannot be legitimate.
func (r *Raft) inLease() bool {
	return r.cfg.CheckQuorum && r.lead != "" && r.electionElapsed < r.cfg.ElectionTick
}

// yieldsTo decides between this pre-candidate and a rival in the same
// pre-vote round: the rival wins if its log is more up to date, or as up
// to date and its ID is lower. Without a tie-break, two followers whose
// timers fire together grant each other's pre-votes, both start real
// elections, and split the vote, which costs another election timeout.
// Pre-votes change no state, so this affects only liveness.
func (r *Raft) yieldsTo(m Message) bool {
	if lt := r.log.lastTerm(); m.LogTerm != lt {
		return m.LogTerm > lt
	}
	if li := r.log.lastIndex(); m.Index != li {
		return m.Index > li
	}
	return m.From < r.id
}

func (r *Raft) handleVote(m Message) {
	respType := MsgVoteResp
	if m.Type == MsgPreVote {
		respType = MsgPreVoteResp
		if r.inLease() {
			r.send(Message{Type: respType, To: m.From, Reject: true})
			return
		}
	}
	canVote := r.vote == m.From || (r.vote == "" && r.lead == "") || (m.Type == MsgPreVote && m.Term > r.term)
	if m.Type == MsgPreVote && r.state == StatePreCandidate && m.Term == r.term+1 && !r.yieldsTo(m) {
		// The rival lost the tie-break. Being a pre-candidate it is out
		// of its lease now, so ask it again for the pre-vote it may have
		// refused earlier; it will grant it.
		r.send(Message{Type: MsgPreVoteResp, To: m.From, Reject: true})
		r.send(Message{Type: MsgPreVote, To: m.From, Term: r.term + 1, Index: r.log.lastIndex(), LogTerm: r.log.lastTerm()})
		return
	}
	if canVote && r.log.isUpToDate(m.Index, m.LogTerm) {
		if m.Type == MsgVote {
			r.electionElapsed = 0
			r.vote = m.From
			r.send(Message{Type: respType, To: m.From})
		} else {
			// Reply in the pre-candidate's future term so it can count us.
			r.send(Message{Type: respType, To: m.From, Term: m.Term})
		}
		return
	}
	r.send(Message{Type: respType, To: m.From, Reject: true})
}

func (r *Raft) stepCandidate(m Message) {
	want := MsgVoteResp
	if r.state == StatePreCandidate {
		want = MsgPreVoteResp
	}
	switch m.Type {
	case MsgApp, MsgHeartbeat, MsgSnap:
		r.becomeFollower(m.Term, m.From) // a leader exists in this term
		r.stepFollower(m)
	case want:
		r.votes[m.From] = !m.Reject
		switch {
		case r.wonElection():
			r.advanceElection()
		case r.lostElection():
			r.becomeFollower(r.term, "")
		}
	}
}

func (r *Raft) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppend(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		// The leader sends min(its commit, our match), which we hold.
		r.log.commitTo(min(m.Commit, r.log.lastIndex()))
		r.send(Message{Type: MsgHeartbeatResp, To: m.From, Context: m.Context})
	case MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case MsgTimeoutNow:
		if m.From == r.lead && r.promotable() {
			// The leader hands over: campaign at once, skipping the
			// pre-vote, with followers told to ignore their lease.
			r.startElection(false, true)
		}
	}
}

func (r *Raft) handleAppend(m Message) {
	if m.Index < r.log.committed {
		r.send(Message{Type: MsgAppResp, To: m.From, Index: r.log.committed})
		return
	}
	if r.log.matchTerm(m.Index, m.LogTerm) {
		last := m.Index + uint64(len(m.Entries))
		r.noteAppended(r.log.append(m.Entries))
		r.log.commitTo(min(m.Commit, last))
		r.send(Message{Type: MsgAppResp, To: m.From, Index: last})
		return
	}
	hint, hintTerm := r.log.findConflictByTerm(min(m.Index, r.log.lastIndex()), m.LogTerm)
	r.send(Message{Type: MsgAppResp, To: m.From, Index: m.Index, Reject: true, RejectHint: hint, HintTerm: hintTerm})
}

// handleSnapshot considers a snapshot the caller has staged. A snapshot at
// or below the commit index, or one the log already matches, adds
// nothing; otherwise the log is replaced by it and Ready hands it out for
// installing.
func (r *Raft) handleSnapshot(m Message) {
	s := m.Snapshot
	if s == nil {
		return
	}
	switch {
	case s.Index <= r.log.committed:
		r.send(Message{Type: MsgAppResp, To: m.From, Index: r.log.committed})
	case r.log.matchTerm(s.Index, s.Term):
		r.log.commitTo(s.Index)
		r.send(Message{Type: MsgAppResp, To: m.From, Index: s.Index})
	default:
		meta := SnapshotMeta{Index: s.Index, Term: s.Term, Conf: s.Conf.Clone()}
		r.log.restore(meta)
		r.snapConf = meta.Conf.Clone()
		r.setConf(meta.Conf, meta.Index)
		r.pendingSnapshot = &meta
		r.send(Message{Type: MsgAppResp, To: m.From, Index: s.Index})
	}
}

func (r *Raft) stepLeader(m Message) {
	pr := r.prs[m.From]
	if pr == nil {
		return
	}
	switch m.Type {
	case MsgAppResp:
		pr.recentActive = true
		if m.Reject {
			if pr.snapshotting || (pr.probing && m.Index != pr.next-1) {
				return // a reject of an append we no longer care about
			}
			next := m.RejectHint
			if m.HintTerm > 0 {
				next, _ = r.log.findConflictByTerm(m.RejectHint, m.HintTerm)
			}
			pr.next = max(min(next+1, pr.next-1), pr.match+1, 1)
			pr.probing, pr.paused = true, false
			r.sendAppend(m.From)
			return
		}
		if m.Index > pr.match {
			pr.match = m.Index
			pr.next = max(pr.next, m.Index+1)
			pr.probing, pr.paused, pr.snapshotting = false, false, false
			if m.Index+1 >= pr.next {
				pr.inflight = 0 // everything sent is acknowledged
			} else {
				pr.inflight = max(pr.inflight-1, 0)
			}
			if r.maybeCommit() {
				r.afterCommit()
				if r.state != StateLeader {
					return
				}
			} else if pr.next <= r.log.lastIndex() {
				r.sendAppend(m.From)
			}
			if m.From == r.leadTransferee && pr.match == r.log.lastIndex() {
				r.send(Message{Type: MsgTimeoutNow, To: m.From})
			}
		} else if pr.paused {
			pr.paused = false
			r.sendAppend(m.From)
		}
	case MsgHeartbeatResp:
		pr.recentActive = true
		if pr.match < r.log.lastIndex() && !pr.snapshotting {
			if pr.match == pr.stallMatch || pr.paused {
				// No progress since the last heartbeat: appends were lost.
				// Resend from its match; duplicates are harmless.
				pr.next = pr.match + 1
				pr.probing, pr.paused, pr.inflight = true, false, 0
			}
			pr.stallMatch = pr.match
			r.sendAppend(m.From)
		}
		if m.Context != 0 {
			r.ackRead(m.Context, m.From)
		}
	}
}

// sendAppend sends entries from pr.next: one append while probing, or up
// to MaxInflight pipelined appends while replicating. With nothing new to
// send it sends one empty append, which carries the commit index. If the
// follower needs entries the log no longer has, it asks for a snapshot.
func (r *Raft) sendAppend(to string) {
	pr := r.prs[to]
	if to == r.id || pr == nil {
		return
	}
	for {
		if pr.paused || pr.snapshotting || (!pr.probing && pr.inflight >= r.cfg.MaxInflight) {
			return
		}
		prevIndex := pr.next - 1
		prevTerm, ok := r.log.term(prevIndex)
		if !ok {
			pr.snapshotting = true
			r.send(Message{Type: MsgSnap, To: to})
			return
		}
		ents := r.log.slice(pr.next, r.log.lastIndex()+1, r.cfg.MaxEntriesPerMsg)
		r.send(Message{Type: MsgApp, To: to, Index: prevIndex, LogTerm: prevTerm, Entries: ents, Commit: r.log.committed})
		if pr.probing {
			pr.paused = true
			return
		}
		if len(ents) == 0 {
			return
		}
		pr.next = ents[len(ents)-1].Index + 1 // pipeline
		pr.inflight++
		if pr.next > r.log.lastIndex() {
			return
		}
	}
}

// members returns the IDs the leader replicates to, sorted so that
// message order does not depend on map iteration (the simulator replays
// schedules exactly).
func (r *Raft) members() []string {
	ids := make([]string, 0, len(r.prs))
	for id := range r.prs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (r *Raft) bcastAppend() {
	for _, id := range r.members() {
		r.sendAppend(id)
	}
}

func (r *Raft) bcastHeartbeat(ctx uint64) {
	for _, id := range r.members() {
		if id == r.id {
			continue
		}
		commit := min(r.prs[id].match, r.log.committed)
		r.send(Message{Type: MsgHeartbeat, To: id, Commit: commit, Context: ctx})
	}
}

// Propose appends data to the log if this node is the leader.
func (r *Raft) Propose(data []byte) error {
	if r.state != StateLeader {
		return ErrNotLeader
	}
	if r.leadTransferee != "" {
		return ErrTransferring
	}
	r.appendEntries(Entry{Type: EntryNormal, Data: data})
	r.bcastAppend()
	return nil
}

// ProposeConfChange appends a single-server membership change if this
// node is the leader and returns its index. The change takes effect on
// every node as soon as the node appends it. Only one change may be
// uncommitted at a time, and only after the leader has committed an entry
// of its own term (the fix to single-server changes in the Raft
// dissertation's errata).
func (r *Raft) ProposeConfChange(t ConfChangeType, node string) (uint64, error) {
	if r.state != StateLeader {
		return 0, ErrNotLeader
	}
	if r.leadTransferee != "" {
		return 0, ErrTransferring
	}
	if ct, _ := r.log.term(r.log.committed); ct != r.term || r.confIndex > r.log.committed {
		return 0, ErrConfChangePending
	}
	conf, err := r.conf.apply(t, node)
	if err != nil {
		return 0, err
	}
	r.appendEntries(Entry{Type: EntryConfChange, Data: EncodeConfChange(ConfChange{Type: t, Node: node, Conf: conf})})
	idx := r.log.lastIndex()
	if r.state == StateLeader {
		r.bcastAppend()
	}
	return idx, nil
}

// TransferLeader hands leadership to voter to: once to's log is complete,
// it is told to campaign at once. Proposals are refused meanwhile; the
// attempt ends after an election timeout either way.
func (r *Raft) TransferLeader(to string) error {
	if r.state != StateLeader {
		return ErrNotLeader
	}
	if to == r.id {
		return nil
	}
	if !r.conf.IsVoter(to) || r.prs[to] == nil {
		return fmt.Errorf("raft: cannot transfer leadership to %q, which is not a voter", to)
	}
	r.leadTransferee, r.transferElapsed = to, 0
	if r.prs[to].match == r.log.lastIndex() {
		r.send(Message{Type: MsgTimeoutNow, To: to})
	} else {
		r.sendAppend(to)
	}
	return nil
}

// Transferee returns the voter a leadership transfer is handing over to.
func (r *Raft) Transferee() string { return r.leadTransferee }

// Compact discards log entries up to index i, which the state machine
// must have applied and made durable; a follower that needs them later
// gets a snapshot instead.
func (r *Raft) Compact(i uint64) error {
	if i > r.log.applied {
		return fmt.Errorf("raft: cannot compact to %d beyond the applied index %d", i, r.log.applied)
	}
	if i < r.log.firstIndex() {
		return nil
	}
	conf, _, err := r.confAt(i)
	if err != nil {
		return err
	}
	r.log.compact(i)
	r.snapConf = conf
	return nil
}

// SnapshotAt describes a snapshot of the state machine at index i, which
// the log must cover (FirstIndex()-1 <= i <= LastIndex()); the caller takes
// one at its applied index to answer a MsgSnap.
func (r *Raft) SnapshotAt(i uint64) (SnapshotMeta, error) {
	t, ok := r.log.term(i)
	if !ok {
		return SnapshotMeta{}, fmt.Errorf("raft: index %d is outside the log [%d, %d]", i, r.log.firstIndex()-1, r.log.lastIndex())
	}
	conf, _, err := r.confAt(i)
	if err != nil {
		return SnapshotMeta{}, err
	}
	return SnapshotMeta{Index: i, Term: t, Conf: conf}, nil
}

// ReportSnapshot tells the leader how streaming a snapshot to a follower
// ended. Either way it goes back to probing; a follower that installed the
// snapshot acknowledges its index, and after a failure the next heartbeat
// response asks for another snapshot.
func (r *Raft) ReportSnapshot(to string, ok bool) {
	pr := r.prs[to]
	if r.state != StateLeader || pr == nil || !pr.snapshotting {
		return
	}
	pr.snapshotting = false
	pr.probing, pr.paused = true, false
	if !ok {
		pr.stallMatch = pr.match
	}
}

// ReadIndex starts a linearizable read identified by id (nonzero). When the
// leader has confirmed it still leads, a ReadState with id appears in Ready.
func (r *Raft) ReadIndex(id uint64) error {
	if r.state != StateLeader {
		return ErrNotLeader
	}
	if id == 0 {
		return errors.New("raft: ReadIndex id must be nonzero")
	}
	if t, _ := r.log.term(r.log.committed); t != r.term {
		// Until an entry of this term commits, the commit index may lag
		// what earlier leaders committed; wait (§6.4 of the thesis).
		r.waiting = append(r.waiting, id)
		return nil
	}
	r.startRead(id)
	return nil
}

func (r *Raft) startRead(id uint64) {
	if r.quorum() == 1 && r.conf.IsVoter(r.id) {
		r.readStates = append(r.readStates, ReadState{Index: r.log.committed, RequestID: id})
		return
	}
	r.reads = append(r.reads, &readRequest{id: id, index: r.log.committed, acks: map[string]bool{r.id: true}})
	r.bcastHeartbeat(id)
}

func (r *Raft) releaseWaitingReads() {
	if r.state != StateLeader || len(r.waiting) == 0 {
		return
	}
	if t, _ := r.log.term(r.log.committed); t != r.term {
		return
	}
	waiting := r.waiting
	r.waiting = nil
	for _, id := range waiting {
		r.startRead(id)
	}
}

// ackRead records a heartbeat ack for read id. A quorum of voters' acks
// for a read also confirms every read started before it.
func (r *Raft) ackRead(id uint64, from string) {
	at := -1
	for i, rr := range r.reads {
		if rr.id == id {
			rr.acks[from] = true
			n := 0
			for _, v := range r.conf.Voters {
				if rr.acks[v] {
					n++
				}
			}
			if n >= r.quorum() {
				at = i
			}
			break
		}
	}
	if at < 0 {
		return
	}
	for _, rr := range r.reads[:at+1] {
		r.readStates = append(r.readStates, ReadState{Index: rr.index, RequestID: rr.id})
	}
	r.reads = r.reads[at+1:]
}

// HasReady reports whether Ready would return any work.
func (r *Raft) HasReady() bool {
	return r.softState() != r.prevSoft || r.hardState() != r.prevHard || len(r.msgs) > 0 ||
		r.log.stable < r.log.lastIndex() || r.log.committed > r.log.applied || len(r.readStates) > 0 ||
		r.pendingSnapshot != nil
}

// Ready returns the work accumulated since the last Advance.
func (r *Raft) Ready() Ready {
	rd := Ready{
		Snapshot:         r.pendingSnapshot,
		Entries:          r.log.unstable(),
		CommittedEntries: r.log.nextCommitted(),
		Messages:         r.msgs,
		ReadStates:       r.readStates,
	}
	if ss := r.softState(); ss != r.prevSoft {
		rd.SoftState = &ss
	}
	if hs := r.hardState(); hs != r.prevHard {
		rd.HardState = hs
		rd.MustSync = hs.Term != r.prevHard.Term || hs.Vote != r.prevHard.Vote
	}
	rd.MustSync = rd.MustSync || len(rd.Entries) > 0 || rd.Snapshot != nil
	return rd
}

// Advance records that rd has been fully handled.
func (r *Raft) Advance(rd Ready) {
	if rd.SoftState != nil {
		r.prevSoft = *rd.SoftState
	}
	if !rd.HardState.IsEmpty() {
		r.prevHard = rd.HardState
	}
	if rd.Snapshot != nil && r.pendingSnapshot == rd.Snapshot {
		r.pendingSnapshot = nil
	}
	if n := len(rd.Entries); n > 0 {
		last := rd.Entries[n-1]
		// Only mark them stable if they are still in the log as handed out.
		if r.log.matchTerm(last.Index, last.Term) {
			r.log.stable = max(r.log.stable, last.Index)
		}
	}
	if n := len(rd.CommittedEntries); n > 0 {
		r.log.applied = max(r.log.applied, rd.CommittedEntries[n-1].Index)
	}
	r.msgs = r.msgs[len(rd.Messages):]
	r.readStates = r.readStates[len(rd.ReadStates):]
}
