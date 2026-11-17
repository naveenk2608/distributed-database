package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
)

// Errors returned by Propose and ReadIndex.
var (
	// ErrNotLeader means this node cannot accept the request; Leader()
	// names the leader it knows of, if any.
	ErrNotLeader = errors.New("raft: not the leader")
)

// Config configures a node.
type Config struct {
	// ID identifies this node within the group.
	ID string
	// Peers lists the group's voters, including ID.
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

	// HardState, Entries and Applied restore a node after a restart:
	// the persisted HardState and log, and the last applied index.
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
	inflight        int
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
	id    string
	peers []string
	cfg   Config

	term  uint64
	vote  string
	log   *raftLog
	state State
	lead  string

	prs   map[string]*progress
	votes map[string]bool

	electionElapsed int
	// ticks counts every Tick since New. Leaders heartbeat when it is a
	// multiple of HeartbeatTick rather than HeartbeatTick after their
	// election, so all groups ticked by one clock heartbeat on the same
	// ticks and a runner can send their heartbeats in one batch.
	ticks         int
	randomTimeout int

	msgs       []Message
	readStates []ReadState
	reads      []*readRequest // leader: reads awaiting a quorum of heartbeat acks
	waiting    []uint64       // leader: reads waiting for the first commit of the term

	prevSoft SoftState
	prevHard HardState
}

// New returns a node restored from cfg.
func New(cfg Config) (*Raft, error) {
	if cfg.ID == "" || !slices.Contains(cfg.Peers, cfg.ID) {
		return nil, fmt.Errorf("raft: ID %q must be one of the peers %v", cfg.ID, cfg.Peers)
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
	l, err := newLog(cfg.Entries, cfg.HardState.Commit, cfg.Applied)
	if err != nil {
		return nil, err
	}
	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)
	r := &Raft{id: cfg.ID, peers: slices.Compact(peers), cfg: cfg, log: l,
		term: cfg.HardState.Term, vote: cfg.HardState.Vote}
	r.becomeFollower(r.term, "")
	r.prevHard = r.hardState()
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

// LastIndex returns the index of the last log entry.
func (r *Raft) LastIndex() uint64 { return r.log.lastIndex() }

// Entries returns a copy of the whole in-memory log (for tests and the
// simulator's invariant checks).
func (r *Raft) Entries() []Entry {
	return r.log.slice(r.log.firstIndex(), r.log.lastIndex()+1, len(r.log.entries))
}

func (r *Raft) quorum() int { return len(r.peers)/2 + 1 }

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
	for _, p := range r.peers {
		r.prs[p] = &progress{next: r.log.lastIndex() + 1, probing: true, recentActive: true}
	}
	// A no-op entry of the new term: entries of earlier terms commit only
	// when an entry of the current term does (Raft §5.4.2), and ReadIndex
	// waits for it.
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
	r.log.append(ents)
	self := r.prs[r.id]
	self.match = r.log.lastIndex()
	self.next = self.match + 1
	r.maybeCommit()
}

// maybeCommit advances the commit index to the highest index a quorum
// matches, but only if that entry is from the current term. Committing an
// older-term entry by counting replicas is the bug of Figure 8 in the Raft
// paper: it could still be overwritten.
func (r *Raft) maybeCommit() bool {
	matches := make([]uint64, 0, len(r.peers))
	for _, p := range r.peers {
		matches = append(matches, r.prs[p].match)
	}
	slices.Sort(matches)
	mci := matches[len(matches)-r.quorum()]
	if t, ok := r.log.term(mci); ok && t == r.term && r.log.commitTo(mci) {
		r.releaseWaitingReads()
		return true
	}
	return false
}

// Tick advances logical time by one tick.
func (r *Raft) Tick() {
	r.ticks++
	switch r.state {
	case StateLeader:
		r.electionElapsed++
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
	for _, pr := range r.prs {
		if pr.recentActive {
			n++
		}
	}
	return n >= r.quorum()
}

// hup starts an election after an election timeout: a pre-vote round if
// PreVote is on (every timeout starts a new one), otherwise a real vote.
func (r *Raft) hup() {
	if r.state == StateLeader {
		return
	}
	r.startElection(r.cfg.PreVote)
}

func (r *Raft) startElection(pre bool) {
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
	for _, p := range r.peers {
		if p != r.id {
			r.send(Message{Type: voteType, To: p, Term: term, Index: r.log.lastIndex(), LogTerm: r.log.lastTerm()})
		}
	}
}

func (r *Raft) wonElection() bool {
	n := 0
	for _, granted := range r.votes {
		if granted {
			n++
		}
	}
	return n >= r.quorum()
}

func (r *Raft) lostElection() bool {
	n := 0
	for _, granted := range r.votes {
		if !granted {
			n++
		}
	}
	return n > len(r.peers)-r.quorum()
}

func (r *Raft) advanceElection() {
	if r.state == StatePreCandidate {
		r.startElection(false) // won the pre-vote: now the real election
	} else {
		r.becomeLeader()
	}
}

// Step processes a message from a peer.
func (r *Raft) Step(m Message) error {
	if !slices.Contains(r.peers, m.From) {
		return fmt.Errorf("raft: message from %q, which is not in the group", m.From)
	}
	switch {
	case m.Term > r.term:
		switch {
		case m.Type == MsgPreVote:
			// Answered below without changing term.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// A granted pre-vote carries the future term; ours is unchanged.
		case (m.Type == MsgVote) && r.inLease():
			// We heard from a leader within an election timeout: this
			// candidate is likely a node that was cut off; do not let it
			// depose a working leader (CheckQuorum lease).
			return nil
		default:
			lead := ""
			if m.Type == MsgApp || m.Type == MsgHeartbeat {
				lead = m.From
			}
			r.becomeFollower(m.Term, lead)
		}
	case m.Term < r.term:
		switch m.Type {
		case MsgApp, MsgHeartbeat:
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
	case MsgApp, MsgHeartbeat:
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
	}
}

func (r *Raft) handleAppend(m Message) {
	if m.Index < r.log.committed {
		r.send(Message{Type: MsgAppResp, To: m.From, Index: r.log.committed})
		return
	}
	if r.log.matchTerm(m.Index, m.LogTerm) {
		last := m.Index + uint64(len(m.Entries))
		r.log.append(m.Entries)
		r.log.commitTo(min(m.Commit, last))
		r.send(Message{Type: MsgAppResp, To: m.From, Index: last})
		return
	}
	hint, hintTerm := r.log.findConflictByTerm(min(m.Index, r.log.lastIndex()), m.LogTerm)
	r.send(Message{Type: MsgAppResp, To: m.From, Index: m.Index, Reject: true, RejectHint: hint, HintTerm: hintTerm})
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
			if pr.probing && m.Index != pr.next-1 {
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
			pr.probing, pr.paused = false, false
			if m.Index+1 >= pr.next {
				pr.inflight = 0 // everything sent is acknowledged
			} else {
				pr.inflight = max(pr.inflight-1, 0)
			}
			if r.maybeCommit() {
				r.bcastAppend() // tell everyone the new commit index
			} else if pr.next <= r.log.lastIndex() {
				r.sendAppend(m.From)
			}
		} else if pr.paused {
			pr.paused = false
			r.sendAppend(m.From)
		}
	case MsgHeartbeatResp:
		pr.recentActive = true
		if pr.match < r.log.lastIndex() {
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
// send it sends one empty append, which carries the commit index.
func (r *Raft) sendAppend(to string) {
	pr := r.prs[to]
	if to == r.id {
		return
	}
	for {
		if pr.paused || (!pr.probing && pr.inflight >= r.cfg.MaxInflight) {
			return
		}
		prevIndex := pr.next - 1
		prevTerm, ok := r.log.term(prevIndex)
		if !ok {
			return // needs entries this log no longer has: a snapshot (Phase 7)
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

func (r *Raft) bcastAppend() {
	for _, p := range r.peers {
		r.sendAppend(p)
	}
}

func (r *Raft) bcastHeartbeat(ctx uint64) {
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		commit := min(r.prs[p].match, r.log.committed)
		r.send(Message{Type: MsgHeartbeat, To: p, Commit: commit, Context: ctx})
	}
}

// Propose appends data to the log if this node is the leader.
func (r *Raft) Propose(data []byte) error {
	if r.state != StateLeader {
		return ErrNotLeader
	}
	r.appendEntries(Entry{Type: EntryNormal, Data: data})
	r.bcastAppend()
	return nil
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
	if r.quorum() == 1 {
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

// ackRead records a heartbeat ack for read id. A quorum of acks for a read
// also confirms every read started before it.
func (r *Raft) ackRead(id uint64, from string) {
	at := -1
	for i, rr := range r.reads {
		if rr.id == id {
			rr.acks[from] = true
			if len(rr.acks) >= r.quorum() {
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
		r.log.stable < r.log.lastIndex() || r.log.committed > r.log.applied || len(r.readStates) > 0
}

// Ready returns the work accumulated since the last Advance.
func (r *Raft) Ready() Ready {
	rd := Ready{
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
	rd.MustSync = rd.MustSync || len(rd.Entries) > 0
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
