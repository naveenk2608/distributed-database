package node

import (
	"context"
	"log/slog"
	"sync"
	"time"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/raft"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

// maxQueued bounds the Raft messages waiting for one peer. Beyond it new
// messages are dropped: Raft retransmits whatever matters, and an
// unbounded queue would grow without limit while a peer is down.
const maxQueued = 8192

// heartbeatLinger is how long plain heartbeats (and their responses) may
// wait for other messages to the same node. Every replica on a node
// heartbeats on the same tick, each from its own goroutine; sending each
// one at once would cost one RPC per replica instead of one per tick.
// Heartbeats only need to arrive well within an election timeout.
// Everything else, including ReadIndex heartbeats, is sent at once and
// carries whatever is queued.
const heartbeatLinger = 2 * time.Millisecond

// raftNet carries Raft messages between nodes. Every replica on the node
// hands its outgoing messages to Send; one sender goroutine per peer node
// drains that peer's queue and delivers everything pending, for all
// shards, in one InternalService.Raft call. Batching per node rather than
// per group keeps the number of RPCs independent of the shard count.
type raftNet struct {
	pool    *transport.Pool
	log     *slog.Logger
	timeout time.Duration
	peers   map[string]*peerQueue // by advertise address; fixed at creation
}

type peerQueue struct {
	addr    string
	wake    chan struct{} // capacity 1
	mu      sync.Mutex
	msgs    []*dkvv1.RaftMessage
	dropped int64
	linger  bool // a delayed wake is scheduled
}

func (q *peerQueue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// lazy reports whether m can wait for heartbeatLinger.
func lazy(m raft.Message) bool {
	return (m.Type == raft.MsgHeartbeat || m.Type == raft.MsgHeartbeatResp) && m.Context == 0
}

func newRaftNet(pool *transport.Pool, self string, peers []string, timeout time.Duration, log *slog.Logger) *raftNet {
	n := &raftNet{pool: pool, log: log, timeout: timeout, peers: map[string]*peerQueue{}}
	for _, p := range peers {
		if p != self {
			n.peers[p] = &peerQueue{addr: p, wake: make(chan struct{}, 1)}
		}
	}
	return n
}

// Send queues msgs for delivery. It never blocks.
func (n *raftNet) Send(shard int, msgs []raft.Message) {
	for _, m := range msgs {
		q := n.peers[m.To]
		if q == nil {
			n.log.Warn("raft message for an unknown node dropped", "shard", shard, "to", m.To)
			continue
		}
		q.mu.Lock()
		if len(q.msgs) >= maxQueued {
			q.dropped++
		} else {
			q.msgs = append(q.msgs, toProto(shard, m))
		}
		startLinger := lazy(m) && !q.linger
		if startLinger {
			q.linger = true
		}
		q.mu.Unlock()
		switch {
		case !lazy(m):
			q.signal()
		case startLinger:
			time.AfterFunc(heartbeatLinger, q.signal)
		}
	}
}

// run starts one sender per peer and returns when ctx is cancelled and
// every sender has stopped.
func (n *raftNet) run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, q := range n.peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.sendLoop(ctx, q)
		}()
	}
	wg.Wait()
}

func (n *raftNet) sendLoop(ctx context.Context, q *peerQueue) {
	failing := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.wake:
		}
		q.mu.Lock()
		batch, dropped := q.msgs, q.dropped
		q.msgs, q.dropped, q.linger = nil, 0, false
		q.mu.Unlock()
		if dropped > 0 {
			n.log.Warn("raft messages dropped: peer queue full", "peer", q.addr, "dropped", dropped)
		}
		if len(batch) == 0 {
			continue
		}
		err := n.deliver(ctx, q.addr, batch)
		switch {
		case err != nil && !failing && ctx.Err() == nil:
			// Logged once per outage; Raft keeps retrying through
			// heartbeats and the pool reconnects with backoff.
			n.log.Warn("cannot deliver raft messages", "peer", q.addr, "error", err)
			failing = true
		case err == nil && failing:
			n.log.Info("raft messages are reaching the peer again", "peer", q.addr)
			failing = false
		}
	}
}

func (n *raftNet) deliver(ctx context.Context, addr string, batch []*dkvv1.RaftMessage) error {
	conn, err := n.pool.Conn(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, n.timeout)
	defer cancel()
	_, err = dkvv1.NewInternalServiceClient(conn).Raft(ctx, &dkvv1.RaftRequest{Messages: batch})
	return err
}

func toProto(shard int, m raft.Message) *dkvv1.RaftMessage {
	pm := &dkvv1.RaftMessage{
		Shard: uint32(shard), Type: uint32(m.Type), From: m.From, To: m.To, Term: m.Term,
		LogTerm: m.LogTerm, Index: m.Index, Commit: m.Commit, Reject: m.Reject,
		RejectHint: m.RejectHint, HintTerm: m.HintTerm, Context: m.Context,
	}
	if len(m.Entries) > 0 {
		pm.Entries = make([]*dkvv1.RaftEntry, len(m.Entries))
		for i, e := range m.Entries {
			pm.Entries[i] = &dkvv1.RaftEntry{Index: e.Index, Term: e.Term, Type: uint32(e.Type), Data: e.Data}
		}
	}
	return pm
}

func fromProto(pm *dkvv1.RaftMessage) raft.Message {
	m := raft.Message{
		Type: raft.MsgType(pm.GetType()), From: pm.GetFrom(), To: pm.GetTo(), Term: pm.GetTerm(),
		LogTerm: pm.GetLogTerm(), Index: pm.GetIndex(), Commit: pm.GetCommit(), Reject: pm.GetReject(),
		RejectHint: pm.GetRejectHint(), HintTerm: pm.GetHintTerm(), Context: pm.GetContext(),
	}
	if es := pm.GetEntries(); len(es) > 0 {
		m.Entries = make([]raft.Entry, len(es))
		for i, e := range es {
			m.Entries[i] = raft.Entry{Index: e.GetIndex(), Term: e.GetTerm(), Type: raft.EntryType(e.GetType()), Data: e.GetData()}
		}
	}
	return m
}
