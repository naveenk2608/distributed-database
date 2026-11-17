package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/raft"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
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

// snapshotChunk is the size of the pieces a snapshot file is streamed in.
const snapshotChunk = 1 << 20

// raftNet carries Raft messages between nodes. Every replica on the node
// hands its outgoing messages to Send; one sender goroutine per peer node
// drains that peer's queue and delivers everything pending, for all
// groups, in one InternalService.Raft call. Batching per node rather than
// per group keeps the number of RPCs independent of the shard count.
// Members are named by node ID and resolved through the directory.
type raftNet struct {
	pool    *transport.Pool
	dir     *directory
	log     *slog.Logger
	timeout time.Duration

	// drop, if set, makes the transport discard messages of the groups
	// it selects, in both directions (fault injection for tests).
	drop atomic.Pointer[func(shard int) bool]

	mu      sync.Mutex
	peers   map[string]*peerQueue // by address
	ctx     context.Context       // set by run; nil before
	wg      sync.WaitGroup
	unknown map[string]time.Time // unresolvable IDs, last warned
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

func newRaftNet(pool *transport.Pool, dir *directory, timeout time.Duration, log *slog.Logger) *raftNet {
	return &raftNet{pool: pool, dir: dir, log: log, timeout: timeout, peers: map[string]*peerQueue{}, unknown: map[string]time.Time{}}
}

// queue returns addr's queue, starting its sender if the transport runs.
func (n *raftNet) queue(addr string) *peerQueue {
	n.mu.Lock()
	defer n.mu.Unlock()
	q := n.peers[addr]
	if q == nil {
		q = &peerQueue{addr: addr, wake: make(chan struct{}, 1)}
		n.peers[addr] = q
		if n.ctx != nil {
			n.startLocked(q)
		}
	}
	return q
}

func (n *raftNet) startLocked(q *peerQueue) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.sendLoop(n.ctx, q)
	}()
}

func (n *raftNet) dropped(shard int) bool {
	f := n.drop.Load()
	return f != nil && (*f)(shard)
}

// Send queues msgs for delivery. It never blocks.
func (n *raftNet) Send(shard int, msgs []raft.Message) {
	if n.dropped(shard) {
		return
	}
	for _, m := range msgs {
		addr := n.dir.Addr(m.To)
		if addr == "" {
			n.warnUnknown(shard, m.To)
			continue
		}
		q := n.queue(addr)
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

func (n *raftNet) warnUnknown(shard int, id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if time.Since(n.unknown[id]) < 10*time.Second {
		return
	}
	n.unknown[id] = time.Now()
	n.log.Warn("raft message for a node with no known address dropped", "shard", shard, "to", id)
}

// run starts the senders and returns when ctx is cancelled and every
// sender has stopped.
func (n *raftNet) run(ctx context.Context) {
	n.mu.Lock()
	n.ctx = ctx
	for _, q := range n.peers {
		n.startLocked(q)
	}
	n.mu.Unlock()
	<-ctx.Done()
	n.wg.Wait()
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

// SendSnapshot streams the checkpoint in dir to m.To with
// InternalService.InstallSnapshot.
func (n *raftNet) SendSnapshot(ctx context.Context, shard int, m raft.Message, fsys vfs.FS, dir string) error {
	addr := n.dir.Addr(m.To)
	if addr == "" {
		return fmt.Errorf("no address known for node %s", m.To)
	}
	conn, err := n.pool.Conn(addr)
	if err != nil {
		return err
	}
	stream, err := dkvv1.NewInternalServiceClient(conn).InstallSnapshot(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&dkvv1.InstallSnapshotRequest{Part: &dkvv1.InstallSnapshotRequest_Header{Header: toProto(shard, m)}}); err != nil {
		return streamErr(stream, err)
	}
	names, err := fsys.List(dir)
	if err != nil {
		return err
	}
	buf := make([]byte, snapshotChunk)
	for _, name := range names {
		f, err := fsys.Open(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		size, err := f.Size()
		if err != nil {
			_ = f.Close()
			return err
		}
		for off := int64(0); off == 0 || off < size; {
			k, rerr := f.ReadAt(buf[:min(int64(len(buf)), size-off)], off)
			if rerr != nil && !errors.Is(rerr, io.EOF) {
				_ = f.Close()
				return rerr
			}
			off += int64(k)
			chunk := &dkvv1.SnapshotFileChunk{Name: name, Data: buf[:k], Last: off >= size}
			if err := stream.Send(&dkvv1.InstallSnapshotRequest{Part: &dkvv1.InstallSnapshotRequest_File{File: chunk}}); err != nil {
				_ = f.Close()
				return streamErr(stream, err)
			}
			if size == 0 {
				break
			}
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

// streamErr returns the server's status when a send fails because the
// stream ended.
func streamErr(stream dkvv1.InternalService_InstallSnapshotClient, err error) error {
	if errors.Is(err, io.EOF) {
		if _, rerr := stream.CloseAndRecv(); rerr != nil {
			return rerr
		}
	}
	return err
}

// receiveSnapshotFiles writes the files of a snapshot stream into dir,
// fsyncing each file and then dir.
func receiveSnapshotFiles(stream dkvv1.InternalService_InstallSnapshotServer, fsys vfs.FS, dir string) error {
	var f vfs.File
	var name string
	closeFile := func() error {
		if f == nil {
			return nil
		}
		// Fsync each file before the snapshot can be marked complete.
		err := errors.Join(f.Sync(), f.Close())
		f = nil
		return err
	}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if err := closeFile(); err != nil {
				return err
			}
			// Fsync the directory so the files' entries are durable.
			return fsys.SyncDir(dir)
		}
		if err != nil {
			_ = closeFile()
			return err
		}
		c := req.GetFile()
		if c == nil {
			_ = closeFile()
			return errors.New("snapshot stream: a second header")
		}
		if c.GetName() == "" || strings.ContainsAny(c.GetName(), `/\`) || c.GetName() == "." || c.GetName() == ".." {
			_ = closeFile()
			return fmt.Errorf("snapshot stream: bad file name %q", c.GetName())
		}
		if f == nil || c.GetName() != name {
			if err := closeFile(); err != nil {
				return err
			}
			name = c.GetName()
			if f, err = fsys.Create(filepath.Join(dir, name)); err != nil {
				return err
			}
		}
		if _, err := f.Write(c.GetData()); err != nil {
			_ = closeFile()
			return err
		}
		if c.GetLast() {
			if err := closeFile(); err != nil {
				return err
			}
		}
	}
}

func toProto(shard int, m raft.Message) *dkvv1.RaftMessage {
	pm := &dkvv1.RaftMessage{
		Shard: uint32(int32(shard)), Type: uint32(m.Type), From: m.From, To: m.To, Term: m.Term,
		LogTerm: m.LogTerm, Index: m.Index, Commit: m.Commit, Reject: m.Reject,
		RejectHint: m.RejectHint, HintTerm: m.HintTerm, Context: m.Context, Transfer: m.Transfer,
	}
	if len(m.Entries) > 0 {
		pm.Entries = make([]*dkvv1.RaftEntry, len(m.Entries))
		for i, e := range m.Entries {
			pm.Entries[i] = &dkvv1.RaftEntry{Index: e.Index, Term: e.Term, Type: uint32(e.Type), Data: e.Data}
		}
	}
	if s := m.Snapshot; s != nil {
		pm.Snapshot = &dkvv1.RaftSnapshot{Index: s.Index, Term: s.Term, Voters: s.Conf.Voters, Learners: s.Conf.Learners}
	}
	return pm
}

func fromProto(pm *dkvv1.RaftMessage) raft.Message {
	m := raft.Message{
		Type: raft.MsgType(pm.GetType()), From: pm.GetFrom(), To: pm.GetTo(), Term: pm.GetTerm(),
		LogTerm: pm.GetLogTerm(), Index: pm.GetIndex(), Commit: pm.GetCommit(), Reject: pm.GetReject(),
		RejectHint: pm.GetRejectHint(), HintTerm: pm.GetHintTerm(), Context: pm.GetContext(), Transfer: pm.GetTransfer(),
	}
	if es := pm.GetEntries(); len(es) > 0 {
		m.Entries = make([]raft.Entry, len(es))
		for i, e := range es {
			m.Entries[i] = raft.Entry{Index: e.GetIndex(), Term: e.GetTerm(), Type: raft.EntryType(e.GetType()), Data: e.GetData()}
		}
	}
	if s := pm.GetSnapshot(); s != nil {
		m.Snapshot = &raft.SnapshotMeta{Index: s.GetIndex(), Term: s.GetTerm(),
			Conf: raft.ConfState{Voters: s.GetVoters(), Learners: s.GetLearners()}}
	}
	return m
}

func validMessage(pm *dkvv1.RaftMessage) bool {
	if pm.GetType() < uint32(raft.MsgPreVote) || pm.GetType() > uint32(raft.MsgTimeoutNow) {
		return false
	}
	for _, e := range pm.GetEntries() {
		if e.GetType() > uint32(raft.EntryConfChange) {
			return false
		}
	}
	return true
}
