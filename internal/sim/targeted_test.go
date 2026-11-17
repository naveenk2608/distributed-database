package sim

import (
	"fmt"
	"testing"

	"github.com/naveenk2608/distributed-database/internal/raft"
)

// TestDelayedTimeoutNowMeetsTheLogCheck is a targeted schedule for the
// vote's log check (Raft paper §5.4.1), the guard on Leader Completeness
// that random schedules reach only when a lagging node happens to time
// out first. Here it is reached on every run.
//
// A leader starts a transfer to a caught-up follower, and its TimeoutNow
// is delayed past the transfer's timeout. The leader takes proposals
// again and commits an entry the follower does not get (its appends are
// lost; heartbeats still arrive, so it keeps following the leader). When
// the TimeoutNow finally arrives, the follower campaigns at once: no
// pre-vote, the lease bypassed, and a log without a committed entry.
// Every other node is inside its lease and would grant a transfer vote,
// so only the log check keeps it from winning.
func TestDelayedTimeoutNowMeetsTheLogCheck(t *testing.T) {
	for _, pre := range []bool{false, true} {
		t.Run(fmt.Sprintf("prevote=%v", pre), func(t *testing.T) {
			o := Options{Nodes: 3, PreVote: pre, CheckQuorum: true, MaxEntriesPerMsg: 8, CalmSteps: 300}
			s := newSim(1, seedRand(1), o)
			leader := func() *simNode {
				for _, id := range s.ids {
					if n := s.nodes[id]; n.r.State() == raft.StateLeader {
						return n
					}
				}
				return nil
			}
			runUntil := func(what string, done func() bool) {
				t.Helper()
				for limit := s.now + 200; !done(); s.now++ {
					if s.now == limit || s.failed() {
						t.Fatalf("%s: not reached by step %d; violations %v", what, s.now, s.check.Violations())
					}
					s.step()
				}
			}
			appliedEverywhere := func(i uint64) bool {
				for _, id := range s.ids {
					if s.nodes[id].r.Applied() < i {
						return false
					}
				}
				return true
			}

			runUntil("a leader with every node caught up", func() bool {
				l := leader()
				return l != nil && appliedEverywhere(l.r.LastIndex())
			})
			l := leader()
			var b *simNode // the transferee
			for _, id := range s.ids {
				if id != l.id {
					b = s.nodes[id]
					break
				}
			}

			var held []raft.Message
			s.intercept = func(m raft.Message) bool {
				if m.Type == raft.MsgTimeoutNow {
					held = append(held, m)
					return false
				}
				return true
			}
			if err := l.r.TransferLeader(b.id); err != nil {
				t.Fatal(err)
			}
			s.processReady()
			if len(held) != 1 {
				t.Fatalf("transfer to caught-up %s sent %d TimeoutNow messages; want 1 at once", b.id, len(held))
			}
			runUntil("the transfer to time out", func() bool { return l.r.Transferee() == "" })

			s.intercept = func(m raft.Message) bool { return m.Type != raft.MsgApp || m.To != b.id }
			if err := l.r.Propose([]byte("x")); err != nil {
				t.Fatal(err)
			}
			x := l.r.LastIndex()
			runUntil("x to commit", func() bool { return l.r.Committed() >= x })
			term := l.r.Term()
			if l.r.State() != raft.StateLeader || b.r.LastIndex() >= x || b.r.Leader() != l.id || b.r.Term() != held[0].Term {
				t.Fatalf("setup: %s is %v; %s has last index %d (x is at %d), follows %q at term %d (TimeoutNow term %d)",
					l.id, l.r.State(), b.id, b.r.LastIndex(), x, b.r.Leader(), b.r.Term(), held[0].Term)
			}

			// Record the campaign: b's requests and the answers to them.
			var votes, answers []raft.Message
			s.intercept = func(m raft.Message) bool {
				switch {
				case m.From == b.id && (m.Type == raft.MsgVote || m.Type == raft.MsgPreVote):
					votes = append(votes, m)
				case m.To == b.id && (m.Type == raft.MsgVoteResp || m.Type == raft.MsgPreVoteResp):
					answers = append(answers, m)
				}
				return true
			}
			s.inflight = append(s.inflight, envelope{at: s.now, msg: held[0]})
			s.step()
			s.now++
			if b.r.Term() != term+1 || b.r.State() == raft.StateFollower {
				t.Fatalf("%s did not campaign on the delayed TimeoutNow: %v at term %d", b.id, b.r.State(), b.r.Term())
			}
			s.step() // deliver the requests; the answers are sent
			s.now++
			s.intercept = nil
			if len(votes) != 2 {
				t.Fatalf("%s sent %d vote requests; want one to each other voter", b.id, len(votes))
			}
			for _, m := range votes {
				if m.Type != raft.MsgVote || !m.Transfer {
					t.Fatalf("%s sent %v with Transfer=%v; want transfer-marked MsgVote only, no pre-vote", b.id, m.Type, m.Transfer)
				}
			}
			// Both voters are inside their lease; an unmarked request would
			// go unanswered. A refusal means the lease was bypassed and the
			// log check said no.
			if len(answers) != 2 || !answers[0].Reject || !answers[1].Reject || answers[0].From == answers[1].From {
				t.Fatalf("answers to %s's transfer votes: %+v; want a refusal from each other voter", b.id, answers)
			}

			s.calm() // no fault: a leader with x must be elected and a new proposal applied everywhere
			if res := s.finish(); len(res.Violations) > 0 {
				t.Fatalf("%d violations; first: %s", len(res.Violations), res.Violations[0])
			}
		})
	}
}
