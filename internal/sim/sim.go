// Package sim is a deterministic simulator for the Raft core. It runs a
// group of raft.Raft nodes over a simulated network that drops, delays,
// duplicates and reorders messages, partitions nodes, and crashes and
// restarts them (losing everything they had not persisted), while leaders
// change the membership one server at a time, transfer leadership, and
// nodes compact their logs and catch up from snapshots. Everything is
// driven by one seeded random generator, so any schedule replays exactly
// from its seed. A Checker verifies Raft's safety properties as it runs.
package sim

import (
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"slices"

	"github.com/naveenk2608/distributed-database/internal/raft"
)

// Options shape a schedule. Zero values are filled from the seed.
type Options struct {
	Nodes int // initial voters
	// Spares are extra nodes that start uninitialized; leaders may add
	// them (and any removed node) back as learners.
	Spares      int
	Steps       int
	DropRate    float64 // per message
	DupRate     float64 // per message
	MaxDelay    int     // steps a message may be delayed
	CrashRate   float64 // per step, for one random node
	RestartRate float64 // per step, for one crashed node
	SplitRate   float64 // per step: start a random partition
	HealRate    float64 // per step: heal the partition
	ProposeRate float64 // per step
	ReadRate    float64 // per step
	// ConfRate: per step, a leader proposes a random membership change.
	ConfRate float64
	// TransferRate: per step, a leader hands leadership to a random voter.
	TransferRate float64
	// CompactRate: per step, a node compacts its log up to a random
	// applied index, so lagging members need snapshots.
	CompactRate float64
	PreVote     bool
	CheckQuorum bool
	// MaxEntriesPerMsg bounds entries per append; small values make
	// partial replication, and so the Figure 8 situation, more likely.
	MaxEntriesPerMsg int
	// CalmSteps run after Steps with every node up, the network healed and
	// no message loss; a proposal made during them must reach every member
	// of the final membership, or the schedule fails the liveness check.
	CalmSteps int
	// Trace, if set, receives every event as a line of text.
	Trace io.Writer
}

// RandomOptions draws a schedule's shape from rng, without membership
// changes where liveness cannot be required with them (see
// limitMembershipChanges).
func RandomOptions(rng *rand.Rand) Options {
	o := randomShape(rng)
	o.limitMembershipChanges()
	return o
}

// randomShape draws every option from rng.
func randomShape(rng *rand.Rand) Options {
	return Options{
		Nodes:            []int{3, 3, 5}[rng.IntN(3)],
		Spares:           rng.IntN(3),
		Steps:            400,
		DropRate:         []float64{0, 0.02, 0.1, 0.3}[rng.IntN(4)],
		DupRate:          []float64{0, 0.02, 0.1}[rng.IntN(3)],
		MaxDelay:         rng.IntN(6),
		CrashRate:        []float64{0, 0.005, 0.02}[rng.IntN(3)],
		RestartRate:      0.05,
		SplitRate:        []float64{0, 0.005, 0.02}[rng.IntN(3)],
		HealRate:         0.03,
		ProposeRate:      0.3,
		ReadRate:         0.1,
		ConfRate:         []float64{0, 0.01, 0.03}[rng.IntN(3)],
		TransferRate:     []float64{0, 0.005, 0.02}[rng.IntN(3)],
		CompactRate:      []float64{0, 0.01, 0.05}[rng.IntN(3)],
		PreVote:          rng.IntN(4) != 0,
		CheckQuorum:      rng.IntN(4) != 0,
		MaxEntriesPerMsg: []int{1, 2, 8}[rng.IntN(3)],
		CalmSteps:        300,
	}
}

// limitMembershipChanges turns membership changes off unless both
// CheckQuorum and PreVote are on, as they always are in dkv
// (docs/adr/0026). Schedules without them still run every safety check.
//
//   - Without CheckQuorum there is no lease: a removed server that never
//     learns of its removal keeps winning pre-votes, or votes, and forcing
//     elections (Raft thesis §4.2.3).
//   - Without PreVote, a server that cannot win an election (a removed
//     one, or a voter whose log is behind) still raises everyone's term
//     each time it times out. Membership changes can leave two voters of
//     which only one can win; each term bump then cuts that one's election
//     short, and no calm phase of fixed length is sure to elect a leader
//     (seed 51674).
func (o *Options) limitMembershipChanges() {
	if !o.CheckQuorum || !o.PreVote {
		o.ConfRate = 0
	}
}

// seedRand is the generator a schedule's options and events come from.
func seedRand(seed uint64) *rand.Rand { return rand.New(rand.NewPCG(seed, 0x5ee1)) }

type envelope struct {
	at  int
	msg raft.Message
}

type simNode struct {
	id    string
	r     *raft.Raft
	rng   *rand.Rand
	up    bool
	side  int      // partition side; messages only flow within a side
	peers []string // bootstrap voters; nil for a spare
	// Persisted state: what survives a crash. The state machine is
	// durable only as of the snapshot; entries after it are re-applied.
	snap raft.SnapshotMeta
	hs   raft.HardState
	log  []raft.Entry // entries after snap
	// unsynced is a HardState written without fsync; a crash drops it.
	unsynced *raft.HardState
	// Commit index already reported to the checker.
	reportedCommit uint64
}

// Result is the outcome of one schedule.
type Result struct {
	Violations []string
	// Trace fingerprints every event, so replaying a seed can be checked
	// for identical behaviour.
	Trace uint64
	// Stats summarize what happened.
	Leaders, Commits, Crashes, Partitions, Reads, Delivered, Dropped int
	ConfChanges, Transfers, Compactions, Snapshots                   int
}

// Sim is one schedule in progress.
type Sim struct {
	rng      *rand.Rand
	opts     Options
	ids      []string
	nodes    map[string]*simNode
	inflight []envelope
	now      int
	split    bool
	check    *Checker
	trace    hashWriter
	res      Result
	nextProp int
	nextRead uint64
	hash     hash.Hash64
	// crashRng decides crashes between sending a leader's Ready.Appends
	// and persisting its Ready; a stream of its own, so schedules that
	// never crash draw from rng exactly as before.
	crashRng *rand.Rand
	// intercept, if set, sees every message a node sends; returning false
	// drops it. Targeted schedules use it to hold or lose chosen messages.
	intercept func(raft.Message) bool
}

type hashWriter struct {
	h   io.Writer
	out io.Writer // optional human-readable copy
}

func (w hashWriter) event(format string, args ...any) {
	_, _ = fmt.Fprintf(w.h, format, args...) // hashing into memory cannot fail
	_, _ = w.h.Write([]byte{'\n'})
	if w.out != nil {
		_, _ = fmt.Fprintf(w.out, format+"\n", args...) // best effort debugging aid
	}
}

// Run executes the schedule for seed with opts (RandomOptions if opts is
// nil) and returns its result. A schedule stops at its first violation:
// past it the cluster's state means nothing, and a broken core can then
// flood the simulated network until memory runs out instead of reporting.
func Run(seed uint64, opts *Options) Result {
	rng := seedRand(seed)
	o := RandomOptions(rng)
	if opts != nil {
		o = *opts
	}
	return newSim(seed, rng, o).run()
}

// run executes the schedule's faulty steps and its calm phase, stopping at
// the first violation.
func (s *Sim) run() Result {
	for s.now = 0; s.now < s.opts.Steps && !s.failed(); s.now++ {
		s.step()
		if s.now%10 == 0 {
			s.checkLogs()
		}
	}
	s.calm()
	return s.finish()
}

// newSim starts the nodes of a schedule with options o, events drawn from
// rng and node timers seeded from seed.
func newSim(seed uint64, rng *rand.Rand, o Options) *Sim {
	h := fnv.New64a()
	s := &Sim{rng: rng, opts: o, nodes: map[string]*simNode{}, check: NewChecker(), hash: h, trace: hashWriter{h: h, out: o.Trace},
		crashRng: rand.New(rand.NewPCG(seed, 0xc4a5))}
	var voters []string
	for i := range o.Nodes + o.Spares {
		id := fmt.Sprintf("n%d", i+1)
		s.ids = append(s.ids, id)
		if i < o.Nodes {
			voters = append(voters, id)
		}
	}
	for i, id := range s.ids {
		n := &simNode{id: id, rng: rand.New(rand.NewPCG(seed, uint64(i)+1)), up: true}
		if i < o.Nodes {
			n.peers = voters
		}
		s.nodes[id] = n
		s.start(n)
	}
	return s
}

func (s *Sim) failed() bool { return len(s.check.Violations()) > 0 }

// finish checks the logs one last time and returns the result.
func (s *Sim) finish() Result {
	s.checkLogs()
	s.res.Violations = s.check.Violations()
	s.res.Trace = s.hash.Sum64()
	return s.res
}

func (s *Sim) start(n *simNode) {
	r, err := raft.New(raft.Config{
		ID: n.id, Peers: n.peers, ElectionTick: 10, HeartbeatTick: 2,
		PreVote: s.opts.PreVote, CheckQuorum: s.opts.CheckQuorum, Rand: n.rng,
		Snapshot: n.snap, HardState: n.hs, Entries: slices.Clone(n.log), Applied: n.snap.Index,
		MaxEntriesPerMsg: max(s.opts.MaxEntriesPerMsg, 1),
	})
	if err != nil {
		s.check.violate("restart %s: %v", n.id, err)
		return
	}
	n.r = r
	n.reportedCommit = n.snap.Index
}

func (s *Sim) step() {
	// Faults.
	if s.rng.Float64() < s.opts.CrashRate {
		if n := s.pick(func(n *simNode) bool { return n.up }); n != nil {
			n.up = false // everything not persisted is lost with n.r
			n.unsynced = nil
			n.r = nil
			s.res.Crashes++
			s.trace.event("%d crash %s", s.now, n.id)
		}
	}
	if s.rng.Float64() < s.opts.RestartRate {
		if n := s.pick(func(n *simNode) bool { return !n.up }); n != nil {
			n.up = true
			s.start(n)
			s.trace.event("%d restart %s", s.now, n.id)
		}
	}
	if !s.split && s.rng.Float64() < s.opts.SplitRate {
		s.split = true
		for _, id := range s.ids {
			s.nodes[id].side = s.rng.IntN(2)
		}
		s.res.Partitions++
		s.trace.event("%d split", s.now)
	} else if s.split && s.rng.Float64() < s.opts.HealRate {
		s.split = false
		for _, id := range s.ids {
			s.nodes[id].side = 0
		}
		s.trace.event("%d heal", s.now)
	}
	if s.rng.Float64() < s.opts.CompactRate {
		if n := s.pick(func(n *simNode) bool { return n.up && n.r.Applied() >= n.r.FirstIndex() }); n != nil {
			s.compact(n)
		}
	}

	// Client and operator operations go to whoever believes it leads.
	leader := func(n *simNode) bool { return n.up && n.r.State() == raft.StateLeader }
	if s.rng.Float64() < s.opts.ProposeRate {
		if n := s.pick(leader); n != nil {
			s.nextProp++
			data := fmt.Appendf(nil, "p%d", s.nextProp)
			if n.r.Propose(data) == nil {
				s.trace.event("%d propose %s %s", s.now, n.id, data)
			}
		}
	}
	if s.rng.Float64() < s.opts.ReadRate {
		if n := s.pick(leader); n != nil {
			s.nextRead++
			if n.r.ReadIndex(s.nextRead) == nil {
				s.check.ReadStarted(s.nextRead)
				s.trace.event("%d read %s %d", s.now, n.id, s.nextRead)
			}
		}
	}
	if s.rng.Float64() < s.opts.ConfRate {
		if n := s.pick(leader); n != nil {
			s.changeMembership(n)
		}
	}
	if s.rng.Float64() < s.opts.TransferRate {
		if n := s.pick(leader); n != nil {
			if voters := n.r.Conf().Voters; len(voters) > 1 {
				to := voters[s.rng.IntN(len(voters))]
				if n.r.TransferLeader(to) == nil && to != n.id {
					s.res.Transfers++
					s.trace.event("%d transfer %s->%s", s.now, n.id, to)
				}
			}
		}
	}

	// One ticker drives every live node.
	for _, id := range s.ids {
		if n := s.nodes[id]; n.up {
			n.r.Tick()
		}
	}
	s.processReady()

	// Deliver due messages in random order.
	var due, later []envelope
	for _, e := range s.inflight {
		if e.at <= s.now {
			due = append(due, e)
		} else {
			later = append(later, e)
		}
	}
	s.inflight = later
	s.rng.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	for _, e := range due {
		to, from := s.nodes[e.msg.To], s.nodes[e.msg.From]
		ok := to.up && (!s.split || to.side == from.side)
		if e.msg.Type == raft.MsgSnap && from.up {
			// The leader's runner learns how the stream ended.
			from.r.ReportSnapshot(e.msg.To, ok)
		}
		if !ok {
			s.res.Dropped++
			continue
		}
		s.res.Delivered++
		s.trace.event("%d deliver %s->%s %s t%d i%d", s.now, e.msg.From, e.msg.To, e.msg.Type, e.msg.Term, e.msg.Index)
		if err := to.r.Step(e.msg); err != nil {
			s.check.violate("step %s: %v", to.id, err)
		}
		s.processReady()
	}
}

// changeMembership proposes a random single-server change on leader n:
// promote a learner, add a non-member as a learner, or remove a member.
func (s *Sim) changeMembership(n *simNode) {
	conf := n.r.Conf()
	var outsiders []string
	for _, id := range s.ids {
		if !conf.IsMember(id) {
			outsiders = append(outsiders, id)
		}
	}
	var t raft.ConfChangeType
	var target string
	switch k := s.rng.IntN(4); {
	case k == 0 && len(conf.Learners) > 0:
		t, target = raft.ConfPromote, conf.Learners[s.rng.IntN(len(conf.Learners))]
	case k == 1 && len(outsiders) > 0:
		t, target = raft.ConfAddLearner, outsiders[s.rng.IntN(len(outsiders))]
	case k == 2:
		members := conf.Members()
		t, target = raft.ConfRemove, members[s.rng.IntN(len(members))]
	default:
		return
	}
	if _, err := n.r.ProposeConfChange(t, target); err == nil {
		s.res.ConfChanges++
		s.trace.event("%d conf %s %v %s", s.now, n.id, t, target)
	}
}

// compact discards n's log up to a random applied index, as a runner does
// once its state machine is durable to that point.
func (s *Sim) compact(n *simNode) {
	first, applied := n.r.FirstIndex(), n.r.Applied()
	i := first + s.rng.Uint64N(applied-first+1)
	if err := n.r.Compact(i); err != nil {
		s.check.violate("compact %s to %d: %v", n.id, i, err)
		return
	}
	if i > n.snap.Index {
		n.log = slices.Clone(n.log[i-n.snap.Index:])
		n.snap = n.r.Snapshot()
	}
	s.res.Compactions++
	s.trace.event("%d compact %s %d", s.now, n.id, i)
}

// calm restarts every node, heals the network and stops faults, then
// requires a new proposal to be applied by every member of the leader's
// membership: the liveness check. Like a client, it proposes again if the
// proposal is lost to a leader change.
func (s *Sim) calm() {
	if s.opts.CalmSteps == 0 || s.failed() {
		return
	}
	s.split = false
	for _, id := range s.ids {
		n := s.nodes[id]
		n.side = 0
		if !n.up {
			n.up = true
			s.start(n)
		}
	}
	s.opts.CrashRate, s.opts.SplitRate, s.opts.DropRate, s.opts.DupRate = 0, 0, 0, 0
	s.opts.ProposeRate, s.opts.ReadRate, s.opts.ConfRate, s.opts.TransferRate = 0, 0, 0, 0
	end := s.now + s.opts.CalmSteps
	var final []byte
	var members []string
	proposedAt := 0
	for ; s.now < end && !s.failed(); s.now++ {
		if s.now >= end-s.opts.CalmSteps*2/3 && (final == nil || s.now-proposedAt > 50) {
			if n := s.pick(func(n *simNode) bool { return n.r.State() == raft.StateLeader && n.r.Transferee() == "" }); n != nil {
				data := fmt.Appendf(nil, "final@%d", s.now)
				if n.r.Propose(data) == nil {
					final, members, proposedAt = data, n.r.Conf().Members(), s.now
					s.trace.event("%d propose-final %s", s.now, n.id)
				}
			}
		}
		s.step()
		if final != nil && s.check.AppliedEverywhere(final, members, s.snapIndexes()) {
			return
		}
	}
	if s.failed() {
		return
	}
	if final == nil {
		s.check.violate("Liveness: no leader accepted a proposal in %d calm steps", s.opts.CalmSteps)
	} else {
		s.check.violate("Liveness: a proposal made during %d calm steps was not applied by every member of %v", s.opts.CalmSteps, members)
	}
}

func (s *Sim) snapIndexes() map[string]uint64 {
	out := map[string]uint64{}
	for _, id := range s.ids {
		out[id] = s.nodes[id].snap.Index
	}
	return out
}

func (s *Sim) pick(ok func(*simNode) bool) *simNode {
	var cand []*simNode
	for _, id := range s.ids {
		if n := s.nodes[id]; ok(n) {
			cand = append(cand, n)
		}
	}
	if len(cand) == 0 {
		return nil
	}
	return cand[s.rng.IntN(len(cand))]
}

// processReady handles every live node's Ready in ID order, the way the
// replica runner does: install a snapshot, persist, send, apply, advance.
func (s *Sim) processReady() {
	for _, id := range s.ids {
		n := s.nodes[id]
		for n.up && n.r.HasReady() {
			rd := n.r.Ready()
			if rd.Snapshot != nil {
				// Installing a snapshot replaces the state machine and the
				// log, durably, before anything is sent. The HardState
				// below carries the new commit index.
				n.snap, n.log, n.unsynced = *rd.Snapshot, nil, nil
				n.reportedCommit = max(n.reportedCommit, rd.Snapshot.Index)
				s.res.Snapshots++
				s.check.SnapshotInstalled(n.id, *rd.Snapshot)
				s.trace.event("%d install %s %d", s.now, n.id, rd.Snapshot.Index)
			}
			// A leader's appends may leave before its Ready is persisted;
			// it may then crash in between, and lose the entries its
			// followers just received.
			for _, m := range rd.Appends {
				s.send(m)
			}
			if len(rd.Appends) > 0 && len(rd.Entries) > 0 && s.crashRng.Float64() < s.opts.CrashRate {
				n.up, n.unsynced, n.r = false, nil, nil
				s.res.Crashes++
				s.trace.event("%d crash %s before persisting %d entries it sent", s.now, n.id, len(rd.Entries))
				break
			}
			// An unsynced write (a commit-only HardState update) is lost
			// in a crash unless a later synced write makes it durable, as
			// a later fsync of the same log file would.
			switch {
			case rd.MustSync:
				if n.unsynced != nil {
					n.hs = *n.unsynced
					n.unsynced = nil
				}
				if !rd.HardState.IsEmpty() {
					n.hs = rd.HardState
				}
			case !rd.HardState.IsEmpty():
				hs := rd.HardState
				n.unsynced = &hs
			}
			if len(rd.Entries) > 0 {
				first := rd.Entries[0].Index
				n.log = append(n.log[:first-1-n.snap.Index], rd.Entries...)
			}
			if rd.SoftState != nil && rd.SoftState.State == raft.StateLeader {
				s.res.Leaders++
				s.check.LeaderElected(n.r.Term(), n.id, n.r.FirstIndex(), n.r.Entries())
				s.trace.event("%d leader %s t%d", s.now, n.id, n.r.Term())
			}
			if c := n.r.Committed(); c > n.reportedCommit {
				ents, first := n.r.Entries(), n.r.FirstIndex()
				for i := max(n.reportedCommit+1, first); i <= c; i++ {
					s.check.Committed(n.id, ents[i-first])
				}
				n.reportedCommit = c
				s.res.Commits++
			}
			for _, m := range rd.Messages {
				if m.Type == raft.MsgSnap && m.Snapshot == nil {
					// Act as the leader's runner: describe a snapshot at
					// the applied index; it travels like any message.
					meta, err := n.r.SnapshotAt(n.r.Applied())
					if err != nil {
						s.check.violate("snapshot on %s: %v", n.id, err)
						continue
					}
					m.Snapshot = &meta
				}
				s.send(m)
			}
			for _, e := range rd.CommittedEntries {
				s.check.Applied(n.id, e)
			}
			for _, rs := range rd.ReadStates {
				s.res.Reads++
				s.check.ReadConfirmed(n.id, rs)
			}
			n.r.Advance(rd)
		}
	}
}

func (s *Sim) send(m raft.Message) {
	if (s.intercept != nil && !s.intercept(m)) || s.rng.Float64() < s.opts.DropRate {
		s.res.Dropped++
		if m.Type == raft.MsgSnap {
			s.nodes[m.From].r.ReportSnapshot(m.To, false)
		}
		return
	}
	copies := 1
	if s.rng.Float64() < s.opts.DupRate {
		copies = 2
	}
	for range copies {
		delay := 0
		if s.opts.MaxDelay > 0 {
			delay = s.rng.IntN(s.opts.MaxDelay + 1)
		}
		s.inflight = append(s.inflight, envelope{at: s.now + delay, msg: m})
	}
}

// checkLogs runs the Log Matching check over every node's log: the live
// one, or for a crashed node what it persisted.
func (s *Sim) checkLogs() {
	logs := map[string]LogView{}
	for _, id := range s.ids {
		if n := s.nodes[id]; n.up {
			logs[id] = LogView{First: n.r.FirstIndex(), Entries: n.r.Entries()}
		} else {
			logs[id] = LogView{First: n.snap.Index + 1, Entries: n.log}
		}
	}
	s.check.LogMatching(logs)
}
