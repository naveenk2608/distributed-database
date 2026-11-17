// Package sim is a deterministic simulator for the Raft core. It runs a
// group of raft.Raft nodes over a simulated network that drops, delays,
// duplicates and reorders messages, partitions nodes, and crashes and
// restarts them (losing everything they had not persisted), all driven by
// one seeded random generator, so any schedule replays exactly from its
// seed. A Checker verifies Raft's safety properties after every step.
package sim

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"

	"github.com/naveenk2608/distributed-database/internal/raft"
)

// Options shape a schedule. Zero values are filled from the seed.
type Options struct {
	Nodes       int
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
	PreVote     bool
	CheckQuorum bool
	// MaxEntriesPerMsg bounds entries per append; small values make
	// partial replication, and so the Figure 8 situation, more likely.
	MaxEntriesPerMsg int
	// CalmSteps run after Steps with every node up, the network healed and
	// no message loss; a proposal made during them must reach every node,
	// or the schedule fails the liveness check.
	CalmSteps int
}

// RandomOptions draws a schedule's shape from rng.
func RandomOptions(rng *rand.Rand) Options {
	return Options{
		Nodes:            []int{3, 3, 5}[rng.IntN(3)],
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
		PreVote:          rng.IntN(4) != 0,
		CheckQuorum:      rng.IntN(4) != 0,
		MaxEntriesPerMsg: []int{1, 2, 8}[rng.IntN(3)],
		CalmSteps:        300,
	}
}

type envelope struct {
	at  int
	msg raft.Message
}

type simNode struct {
	id   string
	r    *raft.Raft
	rng  *rand.Rand
	up   bool
	side int // partition side; messages only flow within a side
	// Persisted state: what survives a crash.
	hs  raft.HardState
	log []raft.Entry
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
}

type hashWriter struct {
	h interface{ Write([]byte) (int, error) }
}

func (w hashWriter) event(format string, args ...any) {
	_, _ = fmt.Fprintf(w.h, format, args...) // hashing into memory cannot fail
	_, _ = w.h.Write([]byte{'\n'})
}

// Run executes the schedule for seed with opts (RandomOptions if opts is
// the zero value) and returns its result.
func Run(seed uint64, opts *Options) Result {
	rng := rand.New(rand.NewPCG(seed, 0x5ee1))
	o := RandomOptions(rng)
	if opts != nil {
		o = *opts
	}
	h := fnv.New64a()
	s := &Sim{rng: rng, opts: o, nodes: map[string]*simNode{}, check: NewChecker(), trace: hashWriter{h}}
	for i := range o.Nodes {
		s.ids = append(s.ids, fmt.Sprintf("n%d", i+1))
	}
	for i, id := range s.ids {
		n := &simNode{id: id, rng: rand.New(rand.NewPCG(seed, uint64(i)+1)), up: true}
		s.nodes[id] = n
		s.start(n)
	}
	for s.now = 0; s.now < o.Steps; s.now++ {
		s.step()
		if s.now%10 == 0 {
			s.check.LogMatching(s.liveLogs())
		}
	}
	s.calm()
	s.check.LogMatching(s.liveLogs())
	s.res.Violations = s.check.Violations()
	s.res.Trace = h.Sum64()
	return s.res
}

func (s *Sim) start(n *simNode) {
	r, err := raft.New(raft.Config{
		ID: n.id, Peers: s.ids, ElectionTick: 10, HeartbeatTick: 2,
		PreVote: s.opts.PreVote, CheckQuorum: s.opts.CheckQuorum, Rand: n.rng,
		HardState: n.hs, Entries: slices.Clone(n.log),
		MaxEntriesPerMsg: max(s.opts.MaxEntriesPerMsg, 1),
	})
	if err != nil {
		s.check.violate("restart %s: %v", n.id, err)
		return
	}
	n.r = r
	n.reportedCommit = 0
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

	// Client operations go to whoever believes it leads.
	if s.rng.Float64() < s.opts.ProposeRate {
		if n := s.pick(func(n *simNode) bool { return n.up && n.r.State() == raft.StateLeader }); n != nil {
			s.nextProp++
			data := fmt.Appendf(nil, "p%d", s.nextProp)
			if n.r.Propose(data) == nil {
				s.trace.event("%d propose %s %s", s.now, n.id, data)
			}
		}
	}
	if s.rng.Float64() < s.opts.ReadRate {
		if n := s.pick(func(n *simNode) bool { return n.up && n.r.State() == raft.StateLeader }); n != nil {
			s.nextRead++
			if n.r.ReadIndex(s.nextRead) == nil {
				s.check.ReadStarted(s.nextRead)
				s.trace.event("%d read %s %d", s.now, n.id, s.nextRead)
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
		if !to.up || (s.split && to.side != from.side) {
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

// calm restarts every node, heals the network and stops faults, then
// requires a new proposal to be applied by every node: the liveness check.
func (s *Sim) calm() {
	if s.opts.CalmSteps == 0 {
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
	s.opts.ProposeRate, s.opts.ReadRate = 0, 0
	end := s.now + s.opts.CalmSteps
	var final []byte
	for ; s.now < end; s.now++ {
		if final == nil && s.now >= end-s.opts.CalmSteps*2/3 {
			if n := s.pick(func(n *simNode) bool { return n.r.State() == raft.StateLeader }); n != nil {
				data := []byte("final")
				if n.r.Propose(data) == nil {
					final = data
					s.trace.event("%d propose-final %s", s.now, n.id)
				}
			}
		}
		s.step()
		if final != nil && s.check.AppliedEverywhere(final, s.ids) {
			return
		}
	}
	if final == nil {
		s.check.violate("Liveness: no leader accepted a proposal in %d calm steps", s.opts.CalmSteps)
	} else {
		s.check.violate("Liveness: a proposal made during %d calm steps was not applied by every node", s.opts.CalmSteps)
	}
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
// replica runner does: persist, send, apply, advance.
func (s *Sim) processReady() {
	for _, id := range s.ids {
		n := s.nodes[id]
		for n.up && n.r.HasReady() {
			rd := n.r.Ready()
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
				n.log = append(n.log[:first-1], rd.Entries...)
			}
			if rd.SoftState != nil && rd.SoftState.State == raft.StateLeader {
				s.res.Leaders++
				s.check.LeaderElected(n.r.Term(), n.id, n.r.Entries())
				s.trace.event("%d leader %s t%d", s.now, n.id, n.r.Term())
			}
			if c := n.r.Committed(); c > n.reportedCommit {
				ents := n.r.Entries()
				for i := n.reportedCommit + 1; i <= c; i++ {
					s.check.Committed(n.id, ents[i-1])
				}
				n.reportedCommit = c
				s.res.Commits++
			}
			for _, m := range rd.Messages {
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
	if s.rng.Float64() < s.opts.DropRate {
		s.res.Dropped++
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

func (s *Sim) liveLogs() map[string][]raft.Entry {
	out := map[string][]raft.Entry{}
	for _, id := range s.ids {
		if n := s.nodes[id]; n.up {
			out[id] = n.r.Entries()
		} else {
			out[id] = n.log // a crashed node's log is what it persisted
		}
	}
	return out
}
