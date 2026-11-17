// Package replica runs one shard replica: a member of the shard's Raft
// group that persists the Raft log (raftlog), applies committed commands to
// its LSM engine, keeps the client session table, and serves reads.
//
// One goroutine owns the raft.Raft and drives the Ready loop:
//
//  1. persist HardState and entries with an fsync;
//  2. hand messages to the transport (passed in as a Sender);
//  3. apply committed entries to the engine, each at sequence number = its
//     Raft index, and answer the proposals waiting for them;
//  4. release linearizable reads whose read index has been applied.
//
// Engine keys are prefixed: user data under 'u', client sessions under 's',
// and an applied-index marker that every entry rewrites, so the engine's
// LastSeq is exactly the applied index and survives restarts. Sessions are
// part of the replicated state and expire by the leader's clock stamped
// into each command, never by a replica's own clock, so every replica
// decides alike. See docs/adr/0003, 0007, 0008, 0009 and 0010.
package replica

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/raft"
	"github.com/naveenk2608/distributed-database/internal/raft/raftlog"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

// Sender hands outgoing Raft messages to the transport. It must not block
// for long; delivery is best effort.
type Sender interface {
	Send(shard int, msgs []raft.Message)
}

// Config configures a Replica.
type Config struct {
	Shard int
	// ID is this node's member ID in the group; Peers lists all members.
	ID    string
	Peers []string
	// Dir holds the replica: the engine in Dir/kv, the Raft log in
	// Dir/raft.
	Dir string
	// FS is the filesystem; nil means the operating system's.
	FS vfs.FS
	// Engine configures the LSM engine (its FS is set from FS).
	Engine        lsm.Options
	ElectionTick  int
	HeartbeatTick int
	// SessionTTL is how long a client session lives without a write,
	// measured in leader-stamped time.
	SessionTTL time.Duration
	Sender     Sender
	Log        *slog.Logger
}

// NotLeaderError is returned when this replica cannot serve a request
// because it is not the shard's leader.
type NotLeaderError struct {
	Shard  int
	Leader string // the leader this replica knows of, or ""
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return fmt.Sprintf("replica: shard %d has no known leader", e.Shard)
	}
	return fmt.Sprintf("replica: not the leader of shard %d; try %s", e.Shard, e.Leader)
}

// LeaderHint returns the leader this replica knows of.
func (e *NotLeaderError) LeaderHint() string { return e.Leader }

// Errors returned to clients.
var (
	ErrStopped = status.Error(codes.Unavailable, "replica: stopped")
	// errLost: the entry at a proposal's index was not the proposal; a
	// leadership change replaced it. The write did not happen.
	errLost = status.Error(codes.Unavailable, "replica: leadership changed before the write committed; retry")
	// errUnknown: leadership was lost while the write was in flight; it may
	// still commit. Retrying with the same request ID is safe.
	errUnknown = status.Error(codes.Unavailable, "replica: leadership lost with the write in flight; retry with the same request id")
)

const (
	prefixUser    = 'u'
	prefixSession = 's'
)

var appliedKey = []byte("m/applied")

func userKey(k []byte) []byte                          { return append([]byte{prefixUser}, k...) }
func sessionKey(id string) []byte                      { return append([]byte{prefixSession}, id...) }
func indexBytes(i uint64) []byte                       { return binary.BigEndian.AppendUint64(nil, i) }
func errStatus(c codes.Code, f string, a ...any) error { return status.Errorf(c, f, a...) }

// result is the outcome of one applied command.
type result struct {
	version   uint64
	found     bool
	succeeded bool
	err       error
}

type proposal struct {
	cmd         *dkvv1.Command
	done        chan result
	index, term uint64
}

type readWaiter struct{ done chan error }

type pendingRead struct {
	index   uint64
	waiters []*readWaiter
}

// Replica is one shard replica. Its methods are safe for concurrent use.
type Replica struct {
	cfg  Config
	log  *slog.Logger
	db   *lsm.DB
	rlog *raftlog.Log
	r    *raft.Raft // owned by the run goroutine

	inbox chan raft.Message
	props chan *proposal
	readq chan *readWaiter
	ticks chan struct{}
	stop  chan struct{}
	done  chan struct{}

	applied atomic.Uint64

	mu        sync.Mutex // guards the fields below
	appliedCh chan struct{}
	leader    string
	term      uint64
	role      raft.State
	commit    uint64
	failed    error

	// Owned by the run goroutine.
	waiters  map[uint64]*proposal
	inflight map[uint64][]*readWaiter
	pending  []pendingRead
	nextRead uint64
}

// Open opens (and recovers) the replica's engine and Raft log and starts
// its goroutine.
func Open(cfg Config) (*Replica, error) {
	if cfg.FS == nil {
		cfg.FS = vfs.Default
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = time.Hour
	}
	eng := cfg.Engine
	eng.FS = cfg.FS
	db, err := lsm.Open(filepath.Join(cfg.Dir, "kv"), eng)
	if err != nil {
		return nil, fmt.Errorf("replica %d: %w", cfg.Shard, err)
	}
	rlog, err := raftlog.Open(cfg.FS, filepath.Join(cfg.Dir, "raft"))
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("replica %d: %w", cfg.Shard, err)
	}
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%s/%d", cfg.ID, cfg.Shard) // hashing into memory cannot fail
	r, err := raft.New(raft.Config{
		ID: cfg.ID, Peers: cfg.Peers,
		ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		PreVote: true, CheckQuorum: true,
		Rand:      rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), h.Sum64())),
		HardState: rlog.HardState(), Entries: rlog.Entries(),
		Applied: db.LastSeq(),
	})
	if err != nil {
		_ = rlog.Close()
		_ = db.Close()
		return nil, fmt.Errorf("replica %d: %w", cfg.Shard, err)
	}
	rp := &Replica{
		cfg: cfg, log: cfg.Log.With("shard", cfg.Shard), db: db, rlog: rlog, r: r,
		inbox: make(chan raft.Message, 4096), props: make(chan *proposal, 1024),
		readq: make(chan *readWaiter, 1024), ticks: make(chan struct{}, 1),
		stop: make(chan struct{}), done: make(chan struct{}),
		appliedCh: make(chan struct{}),
		waiters:   map[uint64]*proposal{}, inflight: map[uint64][]*readWaiter{},
	}
	rp.applied.Store(db.LastSeq())
	go rp.run()
	return rp, nil
}

// Close stops the replica and closes its log and engine.
func (rp *Replica) Close() error {
	select {
	case <-rp.stop:
		return nil
	default:
	}
	close(rp.stop)
	<-rp.done
	return errors.Join(rp.rlog.Close(), rp.db.Close())
}

// Tick advances the replica's logical clock; the node's ticker calls it.
func (rp *Replica) Tick() {
	select {
	case rp.ticks <- struct{}{}:
	default: // a tick is already pending; ticks coalesce
	}
}

// Step delivers a Raft message from a peer. Messages are dropped if the
// replica is backed up; Raft recovers from lost messages.
func (rp *Replica) Step(m raft.Message) {
	select {
	case rp.inbox <- m:
	default:
	}
}

func (rp *Replica) run() {
	defer close(rp.done)
	for {
		select {
		case <-rp.stop:
			rp.failAll(ErrStopped)
			return
		case <-rp.ticks:
			rp.r.Tick()
		case m := <-rp.inbox:
			rp.step(m)
			for drained := false; !drained; {
				select {
				case m := <-rp.inbox:
					rp.step(m)
				default:
					drained = true
				}
			}
		case p := <-rp.props:
			rp.propose(p)
			for drained := false; !drained; {
				select {
				case p := <-rp.props:
					rp.propose(p)
				default:
					drained = true
				}
			}
		case w := <-rp.readq:
			batch := []*readWaiter{w}
			for drained := false; !drained; {
				select {
				case w := <-rp.readq:
					batch = append(batch, w)
				default:
					drained = true
				}
			}
			rp.startRead(batch)
		}
		if err := rp.handleReady(); err != nil {
			rp.fail(err)
			rp.drainFailed()
			return
		}
	}
}

// drainFailed answers every request with the failure until Close.
func (rp *Replica) drainFailed() {
	rp.mu.Lock()
	err := status.Errorf(codes.Unavailable, "replica %d failed: %v", rp.cfg.Shard, rp.failed)
	rp.mu.Unlock()
	for {
		select {
		case <-rp.stop:
			return
		case p := <-rp.props:
			p.done <- result{err: err}
		case w := <-rp.readq:
			w.done <- err
		case <-rp.inbox:
		case <-rp.ticks:
		}
	}
}

func (rp *Replica) step(m raft.Message) {
	if err := rp.r.Step(m); err != nil {
		rp.log.Debug("dropping raft message", "error", err)
	}
}

// fail records a fatal error: the log or the engine can no longer be
// written, so the replica stops serving.
func (rp *Replica) fail(err error) {
	rp.log.Error("replica failed; it stops serving", "error", err)
	rp.mu.Lock()
	rp.failed = err
	rp.mu.Unlock()
	rp.failAll(status.Errorf(codes.Unavailable, "replica %d failed: %v", rp.cfg.Shard, err))
}

func (rp *Replica) failAll(err error) {
	for i, p := range rp.waiters {
		p.done <- result{err: err}
		delete(rp.waiters, i)
	}
	for id, ws := range rp.inflight {
		for _, w := range ws {
			w.done <- err
		}
		delete(rp.inflight, id)
	}
	for _, pr := range rp.pending {
		for _, w := range pr.waiters {
			w.done <- err
		}
	}
	rp.pending = nil
	for {
		select {
		case p := <-rp.props:
			p.done <- result{err: err}
		case w := <-rp.readq:
			w.done <- err
		default:
			return
		}
	}
}

func (rp *Replica) notLeader() error {
	return &NotLeaderError{Shard: rp.cfg.Shard, Leader: rp.r.Leader()}
}

func (rp *Replica) propose(p *proposal) {
	if rp.r.State() != raft.StateLeader {
		p.done <- result{err: rp.notLeader()}
		return
	}
	p.cmd.StampUnixNano = time.Now().UnixNano()
	data, err := proto.Marshal(p.cmd)
	if err != nil {
		p.done <- result{err: err}
		return
	}
	if err := rp.r.Propose(data); err != nil {
		p.done <- result{err: rp.notLeader()}
		return
	}
	p.index, p.term = rp.r.LastIndex(), rp.r.Term()
	rp.waiters[p.index] = p
}

func (rp *Replica) startRead(batch []*readWaiter) {
	if rp.r.State() != raft.StateLeader {
		for _, w := range batch {
			w.done <- rp.notLeader()
		}
		return
	}
	rp.nextRead++
	id := rp.nextRead
	if err := rp.r.ReadIndex(id); err != nil {
		for _, w := range batch {
			w.done <- rp.notLeader()
		}
		return
	}
	rp.inflight[id] = batch
}

func (rp *Replica) handleReady() error {
	for rp.r.HasReady() {
		rd := rp.r.Ready()
		// 1. Persist before anything leaves this node.
		if err := rp.rlog.Save(rd.HardState, rd.Entries, rd.MustSync); err != nil {
			return fmt.Errorf("persist raft log: %w", err)
		}
		// 2. Send.
		if len(rd.Messages) > 0 {
			rp.cfg.Sender.Send(rp.cfg.Shard, rd.Messages)
		}
		// 3. Apply.
		for _, e := range rd.CommittedEntries {
			if err := rp.apply(e); err != nil {
				return err
			}
		}
		for _, rs := range rd.ReadStates {
			if ws, ok := rp.inflight[rs.RequestID]; ok {
				delete(rp.inflight, rs.RequestID)
				rp.pending = append(rp.pending, pendingRead{index: rs.Index, waiters: ws})
			}
		}
		if rd.SoftState != nil && rd.SoftState.State != raft.StateLeader {
			// Lost (or never had) leadership: writes in flight may or may
			// not commit; unconfirmed reads cannot be confirmed here.
			for i, p := range rp.waiters {
				p.done <- result{err: errUnknown}
				delete(rp.waiters, i)
			}
			for id, ws := range rp.inflight {
				for _, w := range ws {
					w.done <- rp.notLeader()
				}
				delete(rp.inflight, id)
			}
		}
		rp.r.Advance(rd)
		rp.mu.Lock()
		rp.leader, rp.term, rp.role, rp.commit = rp.r.Leader(), rp.r.Term(), rp.r.State(), rp.r.Committed()
		rp.mu.Unlock()
		if rd.SoftState != nil {
			rp.log.Info("raft role changed", "role", rd.SoftState.State.String(), "leader", rd.SoftState.Lead, "term", rp.r.Term())
		}
		rp.releaseReads()
	}
	return nil
}

// releaseReads answers confirmed reads whose index has been applied.
func (rp *Replica) releaseReads() {
	applied := rp.applied.Load()
	kept := rp.pending[:0]
	for _, pr := range rp.pending {
		if pr.index <= applied {
			for _, w := range pr.waiters {
				w.done <- nil
			}
			continue
		}
		kept = append(kept, pr)
	}
	rp.pending = kept
}

// apply applies one committed entry at sequence number = its index.
func (rp *Replica) apply(e raft.Entry) error {
	var b lsm.Batch
	b.Set(appliedKey, indexBytes(e.Index))
	res := result{}
	if e.Type == raft.EntryNormal && len(e.Data) > 0 {
		var cmd dkvv1.Command
		if err := proto.Unmarshal(e.Data, &cmd); err != nil {
			// Every replica fails alike on the same bytes, so treating the
			// entry as a no-op keeps them identical.
			rp.log.Error("undecodable command; applying as a no-op", "index", e.Index, "error", err)
			res.err = errStatus(codes.Internal, "undecodable command at index %d", e.Index)
		} else {
			res = rp.execute(&cmd, e.Index, &b)
		}
	}
	if _, err := rp.db.Apply(&b, e.Index); err != nil {
		return fmt.Errorf("apply index %d: %w", e.Index, err)
	}
	rp.applied.Store(e.Index)
	rp.mu.Lock()
	close(rp.appliedCh)
	rp.appliedCh = make(chan struct{})
	rp.mu.Unlock()
	if p, ok := rp.waiters[e.Index]; ok {
		delete(rp.waiters, e.Index)
		if p.term == e.Term {
			p.done <- res
		} else {
			p.done <- result{err: errLost}
		}
	}
	return nil
}

// execute decides a command's outcome against the applied state and adds
// its writes to b. It reads only the engine and the command, so every
// replica reaches the same outcome.
func (rp *Replica) execute(cmd *dkvv1.Command, index uint64, b *lsm.Batch) result {
	var sess *session
	if cmd.GetClientId() != "" {
		sess = rp.loadSession(cmd.GetClientId())
		if sess != nil && cmd.GetStampUnixNano()-sess.stamp > int64(rp.cfg.SessionTTL) {
			sess = nil // expired by leader-stamped time
		}
		if sess != nil {
			switch {
			case cmd.GetRequestId() == sess.requestID:
				return sess.result // a retry: answer as before, change nothing
			case cmd.GetRequestId() < sess.requestID:
				return result{err: errStatus(codes.FailedPrecondition,
					"request %d of client %s is older than its latest request %d", cmd.GetRequestId(), cmd.GetClientId(), sess.requestID)}
			}
		}
	}
	uk := userKey(cmd.GetKey())
	var res result
	switch cmd.GetOp() {
	case dkvv1.Command_OP_PUT:
		b.Set(uk, cmd.GetValue())
		res.version = index
	case dkvv1.Command_OP_DELETE:
		_, _, err := rp.db.Get(uk, nil)
		res.found = err == nil
		b.Delete(uk)
		res.version = index
	case dkvv1.Command_OP_CAS:
		cur := uint64(0)
		if _, ver, err := rp.db.Get(uk, nil); err == nil {
			cur = ver
		}
		if cur == cmd.GetExpectedVersion() {
			b.Set(uk, cmd.GetValue())
			res.succeeded, res.version = true, index
		} else {
			res.version = cur
		}
	default:
		res.err = errStatus(codes.InvalidArgument, "unknown operation %v", cmd.GetOp())
	}
	if cmd.GetClientId() != "" && res.err == nil {
		b.Set(sessionKey(cmd.GetClientId()), encodeSession(session{requestID: cmd.GetRequestId(), stamp: cmd.GetStampUnixNano(), result: res}))
	}
	return res
}

type session struct {
	requestID uint64
	stamp     int64
	result    result
}

func encodeSession(s session) []byte {
	b := binary.AppendUvarint(nil, s.requestID)
	b = binary.AppendVarint(b, s.stamp)
	b = binary.AppendUvarint(b, s.result.version)
	var flags byte
	if s.result.found {
		flags |= 1
	}
	if s.result.succeeded {
		flags |= 2
	}
	return append(b, flags)
}

func (rp *Replica) loadSession(clientID string) *session {
	v, _, err := rp.db.Get(sessionKey(clientID), nil)
	if err != nil {
		return nil
	}
	var s session
	var k int
	if s.requestID, k = binary.Uvarint(v); k <= 0 {
		return nil
	}
	v = v[k:]
	if s.stamp, k = binary.Varint(v); k <= 0 {
		return nil
	}
	v = v[k:]
	if s.result.version, k = binary.Uvarint(v); k <= 0 || len(v) != k+1 {
		return nil
	}
	s.result.found, s.result.succeeded = v[k]&1 != 0, v[k]&2 != 0
	return &s
}

func (rp *Replica) submit(ctx context.Context, cmd *dkvv1.Command) (result, error) {
	p := &proposal{cmd: cmd, done: make(chan result, 1)}
	select {
	case rp.props <- p:
	case <-ctx.Done():
		return result{}, ctx.Err()
	case <-rp.done:
		return result{}, ErrStopped
	}
	select {
	case res := <-p.done:
		return res, res.err
	case <-ctx.Done():
		// The write may still commit; a retry with the same request ID is
		// answered from the session table.
		return result{}, ctx.Err()
	}
}

// Put proposes a write and waits until it is applied.
func (rp *Replica) Put(ctx context.Context, key, value []byte, clientID string, requestID uint64) (uint64, error) {
	res, err := rp.submit(ctx, &dkvv1.Command{Op: dkvv1.Command_OP_PUT, Key: key, Value: value, ClientId: clientID, RequestId: requestID})
	return res.version, err
}

// Delete proposes a delete and waits until it is applied.
func (rp *Replica) Delete(ctx context.Context, key []byte, clientID string, requestID uint64) (bool, uint64, error) {
	res, err := rp.submit(ctx, &dkvv1.Command{Op: dkvv1.Command_OP_DELETE, Key: key, ClientId: clientID, RequestId: requestID})
	return res.found, res.version, err
}

// CompareAndSet proposes a conditional write; whether it happens is decided
// when it is applied, identically on every replica.
func (rp *Replica) CompareAndSet(ctx context.Context, key, value []byte, expected uint64, clientID string, requestID uint64) (bool, uint64, error) {
	res, err := rp.submit(ctx, &dkvv1.Command{Op: dkvv1.Command_OP_CAS, Key: key, Value: value, ExpectedVersion: expected,
		ClientId: clientID, RequestId: requestID})
	return res.succeeded, res.version, err
}

// Get reads key. A linearizable read (stale false) confirms leadership with
// a ReadIndex round shared by every read in flight and waits until that
// index is applied. A stale read answers from local applied state, after
// waiting for minVersion if it is nonzero. It also returns the applied
// index the answer reflects.
func (rp *Replica) Get(ctx context.Context, key []byte, stale bool, minVersion uint64) (value []byte, version uint64, found bool, applied uint64, err error) {
	if stale {
		if minVersion > 0 {
			if err := rp.waitApplied(ctx, minVersion); err != nil {
				return nil, 0, false, 0, err
			}
		}
	} else {
		w := &readWaiter{done: make(chan error, 1)}
		select {
		case rp.readq <- w:
		case <-ctx.Done():
			return nil, 0, false, 0, ctx.Err()
		case <-rp.done:
			return nil, 0, false, 0, ErrStopped
		}
		select {
		case err := <-w.done:
			if err != nil {
				return nil, 0, false, 0, err
			}
		case <-ctx.Done():
			return nil, 0, false, 0, ctx.Err()
		}
	}
	applied = rp.applied.Load()
	v, ver, err := rp.db.Get(userKey(key), nil)
	if errors.Is(err, lsm.ErrNotFound) {
		return nil, 0, false, applied, nil
	}
	if err != nil {
		return nil, 0, false, applied, err
	}
	return v, ver, true, applied, nil
}

func (rp *Replica) waitApplied(ctx context.Context, idx uint64) error {
	for {
		rp.mu.Lock()
		ch := rp.appliedCh
		rp.mu.Unlock()
		if rp.applied.Load() >= idx {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-rp.done:
			return ErrStopped
		}
	}
}

// Status describes the replica.
type Status struct {
	Leader  string
	Term    uint64
	Role    raft.State
	Commit  uint64
	Applied uint64
	Failed  error
}

// Status returns the replica's current status.
func (rp *Replica) Status() Status {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return Status{Leader: rp.leader, Term: rp.term, Role: rp.role, Commit: rp.commit, Applied: rp.applied.Load(), Failed: rp.failed}
}

// Leader returns the leader this replica knows of, or "".
func (rp *Replica) Leader() string {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.leader
}

// DB returns the replica's engine (for tests and status).
func (rp *Replica) DB() *lsm.DB { return rp.db }
