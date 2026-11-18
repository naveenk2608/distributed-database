// Package replica runs one shard replica: a member of the shard's Raft
// group that persists the Raft log (raftlog), applies committed commands to
// its LSM engine, keeps the client session table, and serves reads.
//
// One goroutine owns the raft.Raft and drives the Ready loop:
//
//  1. install an accepted snapshot, then persist HardState and entries
//     (with an fsync when Raft asks for one);
//  2. hand messages to the transport (passed in as a Sender); a request
//     for a snapshot is answered by checkpointing the engine and streaming
//     the checkpoint (SnapshotSender) from a background goroutine;
//  3. apply committed entries to the engine, each at sequence number = its
//     Raft index, and answer the proposals waiting for them;
//  4. release linearizable reads whose read index has been applied;
//  5. compact the Raft log once it has grown past a threshold.
//
// Engine keys are prefixed: user data under 'u', client sessions under 's',
// and an applied-index marker that every entry rewrites, so the engine's
// LastSeq is exactly the applied index and survives restarts. Sessions are
// part of the replicated state and expire by the leader's clock stamped
// into each command, never by a replica's own clock, so every replica
// decides alike. See docs/adr/0003, 0007, 0008, 0009, 0010 and 0012.
//
// Directory layout: raft/ holds the Raft log; ENGINE names the engine's
// directory, kv/ at first and snap-N/ after installing a snapshot at index
// N. A received snapshot is moved into snap-N/ and marked complete by
// snap-N.done before Raft hears of it; installing it records it in the
// Raft log first and then repoints ENGINE, so a crash in between is
// finished on the next start.
package replica

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/obs"
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

// SnapshotSender streams a snapshot to another member: m is the MsgSnap
// (with Snapshot set) and dir holds the engine checkpoint to send, read
// through fs. It returns once the receiver has staged it, or failed.
type SnapshotSender interface {
	SendSnapshot(ctx context.Context, shard int, m raft.Message, fs vfs.FS, dir string) error
}

// Config configures a Replica.
type Config struct {
	Shard int
	// ID is this node's member ID in the group. Peers lists the initial
	// voters when the group is bootstrapped; it is empty for a replica
	// created to join an existing group, which waits for the leader.
	ID    string
	Peers []string
	// Dir holds the replica (see the package comment).
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
	// CompactEntries: once the Raft log holds this many applied entries,
	// it is compacted, keeping the last quarter for followers that lag a
	// little. 0 means 10,000.
	CompactEntries int
	Sender         Sender
	// Snapshots streams snapshots to followers that need them; nil means
	// none can be sent.
	Snapshots SnapshotSender
	// UnsafeNoSync skips the fsync of every Raft log write (new entries,
	// the term and the vote); log compaction, snapshot installs and segment
	// rotation still sync. A power loss can then lose votes and
	// acknowledged entries, breaking Raft's guarantees. Only for measuring
	// what fsync costs (storage.sync = never).
	UnsafeNoSync bool
	// OnEvent, if set, is told of elections, applied membership changes,
	// snapshot installs and leadership transfers, from the replica's own
	// goroutine; it must not block.
	OnEvent func(obs.Event)
	// OnLatency, if set, is told how long internal steps take:
	// "raft_log_sync" (persisting a Ready that must be durable) and
	// "commit" (a write from proposal until applied here, as leader).
	OnLatency func(step string, d time.Duration)
	Log       *slog.Logger
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
	// errTransferring: the leader is handing leadership over.
	errTransferring = status.Error(codes.Unavailable, "replica: leadership transfer in progress; retry")
)

const (
	prefixUser    = 'u'
	prefixSession = 's'

	engineFile    = "ENGINE"
	initialEngine = "kv"
)

var appliedKey = []byte("m/applied")

func userKey(k []byte) []byte                          { return append([]byte{prefixUser}, k...) }
func sessionKey(id string) []byte                      { return append([]byte{prefixSession}, id...) }
func indexBytes(i uint64) []byte                       { return binary.BigEndian.AppendUint64(nil, i) }
func errStatus(c codes.Code, f string, a ...any) error { return status.Errorf(c, f, a...) }
func snapDirName(index uint64) string                  { return fmt.Sprintf("snap-%d", index) }

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

type staged struct {
	m    raft.Message // MsgSnap with Snapshot set
	dir  string
	done chan error
}

type snapReport struct {
	to string
	ok bool
}

// op runs in the replica's goroutine; it gets nil if the replica failed.
type op func(rp *Replica)

// Replica is one shard replica. Its methods are safe for concurrent use.
type Replica struct {
	cfg  Config
	log  *slog.Logger
	rlog *raftlog.Log
	r    *raft.Raft // owned by the run goroutine

	dbMu      sync.RWMutex // guards db against a snapshot install; only the run goroutine changes it
	db        *lsm.DB
	engineDir string // owned by the run goroutine

	inbox    chan raft.Message
	props    chan *proposal
	readq    chan *readWaiter
	ticks    chan struct{}
	snapIn   chan staged
	snapDone chan snapReport
	ops      chan op
	stop     chan struct{}
	done     chan struct{}
	bgCtx    context.Context
	bgCancel context.CancelFunc
	bg       sync.WaitGroup // snapshot sends

	applied     atomic.Uint64
	snapsSent   atomic.Int64
	snapsRecv   atomic.Int64
	compactions atomic.Int64

	mu        sync.Mutex // guards the fields below
	appliedCh chan struct{}
	leader    string
	term      uint64
	role      raft.State
	commit    uint64
	conf      raft.ConfState
	progress  map[string]uint64
	firstIdx  uint64
	lastIdx   uint64
	failed    error

	// Owned by the run goroutine.
	waiters  map[uint64]*proposal
	inflight map[uint64][]*readWaiter
	pending  []pendingRead
	nextRead uint64
	sending  map[string]bool // followers a snapshot is streaming to
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
	if cfg.CompactEntries <= 0 {
		cfg.CompactEntries = 10000
	}
	if err := cfg.FS.MkdirAll(cfg.Dir); err != nil {
		return nil, fmt.Errorf("replica %d: %w", cfg.Shard, err)
	}
	rlog, err := raftlog.Open(cfg.FS, filepath.Join(cfg.Dir, "raft"))
	if err != nil {
		return nil, fmt.Errorf("replica %d: %w", cfg.Shard, err)
	}
	rp := &Replica{
		cfg: cfg, log: cfg.Log.With("shard", cfg.Shard), rlog: rlog,
		inbox: make(chan raft.Message, 4096), props: make(chan *proposal, 1024),
		readq: make(chan *readWaiter, 1024), ticks: make(chan struct{}, 1),
		snapIn: make(chan staged), snapDone: make(chan snapReport, 16), ops: make(chan op),
		stop: make(chan struct{}), done: make(chan struct{}),
		appliedCh: make(chan struct{}),
		waiters:   map[uint64]*proposal{}, inflight: map[uint64][]*readWaiter{}, sending: map[string]bool{},
	}
	if err := rp.openEngine(); err != nil {
		_ = rlog.Close()
		return nil, fmt.Errorf("replica %d: %w", cfg.Shard, err)
	}
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%s/%d", cfg.ID, cfg.Shard) // hashing into memory cannot fail
	r, err := raft.New(raft.Config{
		ID: cfg.ID, Peers: cfg.Peers,
		ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		PreVote: true, CheckQuorum: true,
		Rand:     rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), h.Sum64())),
		Snapshot: rlog.Snapshot(), HardState: rlog.HardState(), Entries: rlog.Entries(),
		Applied: rp.db.LastSeq(),
	})
	if err != nil {
		_ = rp.db.Close()
		_ = rlog.Close()
		return nil, fmt.Errorf("replica %d: %w", cfg.Shard, err)
	}
	rp.r = r
	rp.applied.Store(rp.db.LastSeq())
	rp.publish()
	rp.bgCtx, rp.bgCancel = context.WithCancel(context.Background())
	go rp.run()
	return rp, nil
}

// openEngine opens the engine ENGINE names, first finishing a snapshot
// install a crash interrupted, and removes data no longer needed.
func (rp *Replica) openEngine() error {
	fsys, dir := rp.cfg.FS, rp.cfg.Dir
	current := initialEngine
	if b, err := vfs.ReadFile(fsys, filepath.Join(dir, engineFile)); err == nil {
		current = strings.TrimSpace(string(b))
	}
	snapIdx := rp.rlog.Snapshot().Index
	open := func(name string) (*lsm.DB, error) {
		eng := rp.cfg.Engine
		eng.FS = fsys
		return lsm.Open(filepath.Join(dir, name), eng)
	}
	db, err := open(current)
	if err != nil {
		return err
	}
	if db.LastSeq() < snapIdx {
		// The Raft log already records an installed snapshot the engine
		// does not hold: a crash came between the two steps of an install.
		name := snapDirName(snapIdx)
		if _, err := fsys.Open(filepath.Join(dir, name+".done")); err != nil {
			_ = db.Close()
			return fmt.Errorf("engine at %d is behind the snapshot at %d, whose data is missing", db.LastSeq(), snapIdx)
		}
		if err := db.Close(); err != nil {
			return err
		}
		if err := vfs.WriteFileDurably(fsys, filepath.Join(dir, engineFile), []byte(name)); err != nil {
			return err
		}
		current = name
		if db, err = open(current); err != nil {
			return err
		}
		if db.LastSeq() < snapIdx {
			_ = db.Close()
			return fmt.Errorf("snapshot engine %s ends at %d, before %d", name, db.LastSeq(), snapIdx)
		}
	}
	rp.db, rp.engineDir = db, current
	rp.removeStale(true)
	return nil
}

// removeStale deletes engine directories and staged snapshots other than
// the current engine; at startup also the leftovers of interrupted
// transfers. Failures only leave garbage behind.
func (rp *Replica) removeStale(startup bool) {
	names, err := rp.cfg.FS.List(rp.cfg.Dir)
	if err != nil {
		return
	}
	for _, n := range names {
		base := strings.TrimSuffix(n, ".done")
		stale := (n == initialEngine || strings.HasPrefix(n, "snap-")) && base != rp.engineDir
		stale = stale || startup && (strings.HasPrefix(n, "out-") || strings.HasPrefix(n, "recv-"))
		if stale {
			if err := vfs.RemoveAll(rp.cfg.FS, filepath.Join(rp.cfg.Dir, n)); err != nil {
				rp.log.Warn("cannot remove stale replica data", "path", n, "error", err)
			}
		}
	}
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
	rp.bgCancel()
	rp.bg.Wait()
	rp.dbMu.Lock()
	defer rp.dbMu.Unlock()
	return errors.Join(rp.rlog.Close(), rp.db.Close())
}

// Destroy closes the replica and deletes its data: it is no longer a
// member of its group.
func (rp *Replica) Destroy() error {
	err := rp.Close()
	return errors.Join(err, vfs.RemoveAll(rp.cfg.FS, rp.cfg.Dir), rp.cfg.FS.SyncDir(filepath.Dir(rp.cfg.Dir)))
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
		case s := <-rp.snapIn:
			rp.receiveStaged(s)
		case rep := <-rp.snapDone:
			delete(rp.sending, rep.to)
			rp.r.ReportSnapshot(rep.to, rep.ok)
		case f := <-rp.ops:
			f(rp)
		}
		if err := rp.handleReady(); err != nil {
			rp.fail(err)
			rp.drainFailed()
			return
		}
		rp.maybeCompact()
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
		case s := <-rp.snapIn:
			s.done <- err
		case <-rp.inbox:
		case <-rp.ticks:
		case <-rp.snapDone:
		case f := <-rp.ops:
			f(nil)
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
	switch err := rp.r.Propose(data); {
	case errors.Is(err, raft.ErrTransferring):
		p.done <- result{err: errTransferring}
		return
	case err != nil:
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
		// 1. Install an accepted snapshot, then persist, before anything
		// leaves this node.
		if rd.Snapshot != nil {
			if err := rp.install(*rd.Snapshot, rd.HardState); err != nil {
				return fmt.Errorf("install snapshot at %d: %w", rd.Snapshot.Index, err)
			}
		}
		// A leader's appends go out before its own fsync, so followers
		// write to their disks while it writes to its own (Raft thesis
		// §10.2.1); it counts its entries toward a commit only once they
		// are persisted, at Advance.
		if len(rd.Appends) > 0 {
			rp.cfg.Sender.Send(rp.cfg.Shard, rd.Appends)
		}
		sync := rd.MustSync && !rp.cfg.UnsafeNoSync
		start := time.Now()
		if err := rp.rlog.Save(rd.HardState, rd.Entries, sync); err != nil {
			return fmt.Errorf("persist raft log: %w", err)
		}
		if sync {
			rp.latency("raft_log_sync", time.Since(start))
		}
		// 2. Send the rest; a snapshot request is served from a checkpoint.
		msgs := make([]raft.Message, 0, len(rd.Messages))
		for _, m := range rd.Messages {
			if m.Type == raft.MsgSnap && m.Snapshot == nil {
				rp.sendSnapshot(m)
				continue
			}
			msgs = append(msgs, m)
		}
		if len(msgs) > 0 {
			rp.cfg.Sender.Send(rp.cfg.Shard, msgs)
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
		wasLeader := rp.role == raft.StateLeader // published state, written only by this goroutine
		rp.r.Advance(rd)
		rp.publish()
		if rd.SoftState != nil {
			rp.log.Info("raft role changed", "role", rd.SoftState.State.String(), "leader", rd.SoftState.Lead, "term", rp.r.Term())
			switch isLeader := rd.SoftState.State == raft.StateLeader; {
			case isLeader && !wasLeader:
				rp.event(obs.Event{Kind: obs.EventLeaderElected, Node: rp.cfg.ID, Term: rp.r.Term()})
			case !isLeader && wasLeader:
				rp.event(obs.Event{Kind: obs.EventLeaderSteppedDown, Node: rd.SoftState.Lead, Term: rp.r.Term()})
			}
		}
		rp.releaseReads()
	}
	return nil
}

// publish copies the Raft state that other goroutines report.
func (rp *Replica) publish() {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	rp.leader, rp.term, rp.role, rp.commit = rp.r.Leader(), rp.r.Term(), rp.r.State(), rp.r.Committed()
	rp.conf, rp.progress = rp.r.Conf(), rp.r.Progress()
	rp.firstIdx, rp.lastIdx = rp.r.FirstIndex(), rp.r.LastIndex()
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

// noteApplied publishes a new applied index and wakes waiters.
func (rp *Replica) noteApplied(i uint64) {
	rp.applied.Store(i)
	rp.mu.Lock()
	close(rp.appliedCh)
	rp.appliedCh = make(chan struct{})
	rp.mu.Unlock()
}

// apply applies one committed entry at sequence number = its index.
func (rp *Replica) apply(e raft.Entry) error {
	var b lsm.Batch
	b.Set(appliedKey, indexBytes(e.Index))
	res := result{}
	switch {
	case e.Type == raft.EntryNormal && len(e.Data) > 0:
		var cmd dkvv1.Command
		if err := proto.Unmarshal(e.Data, &cmd); err != nil {
			// Every replica fails alike on the same bytes, so treating the
			// entry as a no-op keeps them identical.
			rp.log.Error("undecodable command; applying as a no-op", "index", e.Index, "error", err)
			res.err = errStatus(codes.Internal, "undecodable command at index %d", e.Index)
		} else {
			res = rp.execute(&cmd, e.Index, &b)
		}
	case e.Type == raft.EntryConfChange:
		if cc, err := raft.DecodeConfChange(e.Data); err == nil {
			rp.log.Info("membership change applied", "change", cc.Type.String(), "node", cc.Node,
				"voters", cc.Conf.Voters, "learners", cc.Conf.Learners, "index", e.Index)
			kind := map[raft.ConfChangeType]string{raft.ConfAddLearner: obs.EventReplicaAdded,
				raft.ConfPromote: obs.EventReplicaPromoted, raft.ConfRemove: obs.EventReplicaRemoved}[cc.Type]
			if kind != "" {
				rp.event(obs.Event{Kind: kind, Node: cc.Node, Term: e.Term, Index: e.Index,
					Detail: fmt.Sprintf("voters %v, learners %v", cc.Conf.Voters, cc.Conf.Learners)})
			}
		}
	}
	if _, err := rp.db.Apply(&b, e.Index); err != nil {
		return fmt.Errorf("apply index %d: %w", e.Index, err)
	}
	rp.noteApplied(e.Index)
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
	case dkvv1.Command_OP_TXN:
		res.succeeded, res.version = true, index
		for _, c := range cmd.GetTxn().GetConditions() {
			cur := uint64(0)
			if _, ver, err := rp.db.Get(userKey(c.GetKey()), nil); err == nil {
				cur = ver
			}
			if cur != c.GetVersion() {
				res.succeeded, res.version = false, 0
				break
			}
		}
		if res.succeeded {
			for _, w := range cmd.GetTxn().GetWrites() {
				if w.GetDelete() {
					b.Delete(userKey(w.GetKey()))
				} else {
					b.Set(userKey(w.GetKey()), w.GetValue())
				}
			}
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

// maybeCompact compacts the Raft log once it holds CompactEntries applied
// entries, keeping the last quarter. The engine is flushed first, so
// everything compacted away is durable in its tables.
func (rp *Replica) maybeCompact() {
	first, applied := rp.r.FirstIndex(), rp.r.Applied()
	n := uint64(rp.cfg.CompactEntries)
	if applied < first || applied-first+1 < n {
		return
	}
	to := applied - n/4
	meta, err := rp.r.SnapshotAt(to)
	if err != nil {
		rp.log.Warn("cannot compact the raft log", "error", err)
		return
	}
	// Make the state machine durable through `to` before forgetting how
	// to rebuild it.
	if err := rp.db.Flush(); err != nil {
		rp.log.Warn("cannot compact the raft log: flush failed", "error", err)
		return
	}
	if err := rp.rlog.Compact(meta); err != nil {
		rp.fail(fmt.Errorf("compact raft log: %w", err))
		return
	}
	if err := rp.r.Compact(to); err != nil {
		rp.fail(fmt.Errorf("compact raft log: %w", err))
		return
	}
	rp.compactions.Add(1)
	rp.publish()
}

// sendSnapshot answers a MsgSnap: it checkpoints the engine at the applied
// index and streams the checkpoint from a background goroutine, which
// reports the outcome back to Raft.
func (rp *Replica) sendSnapshot(m raft.Message) {
	to := m.To
	if rp.sending[to] {
		return // the stream in flight will report
	}
	if rp.cfg.Snapshots == nil {
		rp.r.ReportSnapshot(to, false)
		return
	}
	applied := rp.r.Applied()
	dir := filepath.Join(rp.cfg.Dir, "out-"+strconv.FormatUint(applied, 10)+"-"+sanitize(to))
	_ = vfs.RemoveAll(rp.cfg.FS, dir)
	seq, err := rp.db.Checkpoint(dir)
	if err == nil && seq != applied {
		err = fmt.Errorf("checkpoint holds %d, not the applied index %d", seq, applied)
	}
	var meta raft.SnapshotMeta
	if err == nil {
		meta, err = rp.r.SnapshotAt(seq)
	}
	if err != nil {
		rp.log.Warn("cannot checkpoint for a snapshot", "to", to, "error", err)
		_ = vfs.RemoveAll(rp.cfg.FS, dir)
		rp.r.ReportSnapshot(to, false)
		return
	}
	m.Snapshot = &meta
	m.From, m.Term = rp.cfg.ID, rp.r.Term()
	rp.sending[to] = true
	rp.log.Info("sending snapshot", "to", to, "index", meta.Index)
	rp.bg.Add(1)
	go func() {
		defer rp.bg.Done()
		err := rp.cfg.Snapshots.SendSnapshot(rp.bgCtx, rp.cfg.Shard, m, rp.cfg.FS, dir)
		if rerr := vfs.RemoveAll(rp.cfg.FS, dir); rerr != nil {
			rp.log.Warn("cannot remove a sent checkpoint", "dir", dir, "error", rerr)
		}
		if err != nil {
			rp.log.Warn("snapshot send failed", "to", to, "index", meta.Index, "error", err)
		} else {
			rp.snapsSent.Add(1)
		}
		select {
		case rp.snapDone <- snapReport{to: to, ok: err == nil}:
		case <-rp.done:
		}
	}()
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, s)
}

// ReceiveSnapshot stages a snapshot sent by the leader: write fills dir
// with the checkpoint's files, fsyncing each. Once staged it is handed to
// Raft, which installs it if it is still useful.
func (rp *Replica) ReceiveSnapshot(ctx context.Context, m raft.Message, write func(dir string) error) error {
	if m.Snapshot == nil || m.Type != raft.MsgSnap {
		return errors.New("replica: not a snapshot message")
	}
	fsys := rp.cfg.FS
	dir := filepath.Join(rp.cfg.Dir, fmt.Sprintf("recv-%d-%s", m.Snapshot.Index, strconv.FormatInt(time.Now().UnixNano(), 36)))
	if err := fsys.MkdirAll(dir); err != nil {
		return err
	}
	cleanup := func() { _ = vfs.RemoveAll(fsys, dir) }
	if err := write(dir); err != nil {
		cleanup()
		return err
	}
	s := staged{m: m, dir: dir, done: make(chan error, 1)}
	select {
	case rp.snapIn <- s:
	case <-ctx.Done():
		cleanup()
		return ctx.Err()
	case <-rp.done:
		cleanup()
		return ErrStopped
	}
	select {
	case err := <-s.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// receiveStaged moves a received snapshot to snap-N, marks it complete and
// hands it to Raft; install picks it up if Raft accepts it, and otherwise
// the next install or restart deletes it.
func (rp *Replica) receiveStaged(s staged) {
	idx := s.m.Snapshot.Index
	name := snapDirName(idx)
	if rp.engineDir == name || idx <= rp.r.Committed() {
		_ = vfs.RemoveAll(rp.cfg.FS, s.dir)
		rp.step(s.m) // Raft answers with what it has
		s.done <- nil
		return
	}
	final := filepath.Join(rp.cfg.Dir, name)
	_ = vfs.RemoveAll(rp.cfg.FS, final)
	_ = rp.cfg.FS.Remove(final + ".done")
	err := rp.moveDir(s.dir, final)
	if err == nil {
		// The marker says the data is complete and durable; only then may
		// the Raft log refer to it.
		err = vfs.WriteFileDurably(rp.cfg.FS, final+".done", nil)
	}
	if err != nil {
		_ = vfs.RemoveAll(rp.cfg.FS, s.dir)
		_ = vfs.RemoveAll(rp.cfg.FS, final)
		s.done <- err
		return
	}
	rp.snapsRecv.Add(1)
	rp.step(s.m)
	s.done <- nil
}

// moveDir moves the files of src (a flat directory) into dst.
func (rp *Replica) moveDir(src, dst string) error {
	fsys := rp.cfg.FS
	if err := fsys.MkdirAll(dst); err != nil {
		return err
	}
	names, err := fsys.List(src)
	if err != nil {
		return err
	}
	for _, n := range names {
		if err := fsys.Rename(filepath.Join(src, n), filepath.Join(dst, n)); err != nil {
			return err
		}
	}
	// Fsync both directories so the moved entries are durable where they
	// now are, then the parent for the new directory itself.
	if err := fsys.SyncDir(dst); err != nil {
		return err
	}
	if err := fsys.SyncDir(src); err != nil {
		return err
	}
	if err := fsys.Remove(src); err != nil {
		return err
	}
	return fsys.SyncDir(filepath.Dir(dst))
}

// install makes the staged snapshot at s.Index the state machine: the
// Raft log records it first, then ENGINE points at its directory.
func (rp *Replica) install(s raft.SnapshotMeta, hs raft.HardState) error {
	fsys := rp.cfg.FS
	name := snapDirName(s.Index)
	if _, err := fsys.Open(filepath.Join(rp.cfg.Dir, name+".done")); err != nil {
		return fmt.Errorf("staged data for %s is missing: %w", name, err)
	}
	if err := rp.rlog.Restore(s, hs); err != nil {
		return err
	}
	eng := rp.cfg.Engine
	eng.FS = fsys
	db, err := lsm.Open(filepath.Join(rp.cfg.Dir, name), eng)
	if err != nil {
		return err
	}
	if db.LastSeq() != s.Index {
		_ = db.Close()
		return fmt.Errorf("snapshot data ends at %d, not %d", db.LastSeq(), s.Index)
	}
	if err := vfs.WriteFileDurably(fsys, filepath.Join(rp.cfg.Dir, engineFile), []byte(name)); err != nil {
		_ = db.Close()
		return err
	}
	rp.dbMu.Lock()
	old := rp.db
	rp.db, rp.engineDir = db, name
	rp.dbMu.Unlock()
	if err := old.Close(); err != nil {
		rp.log.Warn("closing the replaced engine", "error", err)
	}
	rp.removeStale(false)
	// Proposals waiting on entries the snapshot replaced will not see them.
	for i, p := range rp.waiters {
		p.done <- result{err: errUnknown}
		delete(rp.waiters, i)
	}
	rp.noteApplied(s.Index)
	rp.log.Info("snapshot installed", "index", s.Index, "voters", s.Conf.Voters, "learners", s.Conf.Learners)
	rp.event(obs.Event{Kind: obs.EventSnapshotInstalled, Node: rp.cfg.ID, Term: s.Term, Index: s.Index})
	return nil
}

// event reports e, about this replica's group, to OnEvent.
func (rp *Replica) event(e obs.Event) {
	if rp.cfg.OnEvent != nil {
		e.Shard = rp.cfg.Shard
		rp.cfg.OnEvent(e)
	}
}

// RaftLogBytes returns the bytes the replica has written to its Raft log
// since it was opened.
func (rp *Replica) RaftLogBytes() int64 { return rp.rlog.BytesWritten() }

// EngineMetrics returns the metrics of the replica's storage engine.
func (rp *Replica) EngineMetrics() lsm.Metrics {
	rp.dbMu.RLock()
	defer rp.dbMu.RUnlock()
	return rp.db.Metrics()
}

func (rp *Replica) submit(ctx context.Context, cmd *dkvv1.Command) (result, error) {
	start := time.Now()
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
		if res.err == nil {
			rp.latency("commit", time.Since(start))
		}
		return res, res.err
	case <-ctx.Done():
		// The write may still commit; a retry with the same request ID is
		// answered from the session table.
		return result{}, ctx.Err()
	}
}

func (rp *Replica) latency(step string, d time.Duration) {
	if rp.cfg.OnLatency != nil {
		rp.cfg.OnLatency(step, d)
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

// Txn proposes an atomic conditional multi-key write: every write happens,
// at one new version, only if each condition's key is at its version.
func (rp *Replica) Txn(ctx context.Context, txn *dkvv1.Txn, clientID string, requestID uint64) (bool, uint64, error) {
	res, err := rp.submit(ctx, &dkvv1.Command{Op: dkvv1.Command_OP_TXN, Txn: txn, ClientId: clientID, RequestId: requestID})
	return res.succeeded, res.version, err
}

// confirmRead waits until a read here reflects every write committed
// before the call: a ReadIndex round on the leader.
func (rp *Replica) confirmRead(ctx context.Context) error {
	w := &readWaiter{done: make(chan error, 1)}
	select {
	case rp.readq <- w:
	case <-ctx.Done():
		return ctx.Err()
	case <-rp.done:
		return ErrStopped
	}
	select {
	case err := <-w.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
	} else if err := rp.confirmRead(ctx); err != nil {
		return nil, 0, false, 0, err
	}
	rp.dbMu.RLock()
	defer rp.dbMu.RUnlock()
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

// KV is a key, its value and its version.
type KV struct {
	Key, Value []byte
	Version    uint64
}

// Scan returns every key with prefix, linearizably unless stale.
func (rp *Replica) Scan(ctx context.Context, prefix []byte, stale bool) ([]KV, error) {
	if !stale {
		if err := rp.confirmRead(ctx); err != nil {
			return nil, err
		}
	}
	rp.dbMu.RLock()
	defer rp.dbMu.RUnlock()
	it, err := rp.db.NewIterator(lsm.IterOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	start := userKey(prefix)
	var out []KV
	for it.SeekGE(start); it.Valid() && bytes.HasPrefix(it.Key(), start); it.Next() {
		out = append(out, KV{Key: bytes.Clone(it.Key()[1:]), Value: bytes.Clone(it.Value()), Version: it.Version()})
	}
	return out, it.Err()
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

// do runs f in the replica's goroutine.
func (rp *Replica) do(ctx context.Context, f op) error {
	select {
	case rp.ops <- f:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-rp.done:
		return ErrStopped
	}
}

// ChangeMembership proposes a single-server membership change on the
// leader and waits until it is applied here. raft.ErrConfChangePending
// means another change is in progress; retry later.
func (rp *Replica) ChangeMembership(ctx context.Context, t raft.ConfChangeType, node string) error {
	p := &proposal{done: make(chan result, 1)}
	err := rp.do(ctx, func(rp *Replica) {
		if rp == nil {
			p.done <- result{err: ErrStopped}
			return
		}
		idx, err := rp.r.ProposeConfChange(t, node)
		switch {
		case errors.Is(err, raft.ErrNotLeader):
			p.done <- result{err: rp.notLeader()}
		case errors.Is(err, raft.ErrTransferring):
			p.done <- result{err: errTransferring}
		case err != nil:
			p.done <- result{err: err}
		default:
			p.index, p.term = idx, rp.r.Term()
			rp.waiters[idx] = p
		}
	})
	if err != nil {
		return err
	}
	select {
	case res := <-p.done:
		return res.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TransferLeader hands leadership to member to and waits until this
// replica sees to lead, or ctx ends.
func (rp *Replica) TransferLeader(ctx context.Context, to string) error {
	errc := make(chan error, 1)
	if err := rp.do(ctx, func(rp *Replica) {
		if rp == nil {
			errc <- ErrStopped
			return
		}
		err := rp.r.TransferLeader(to)
		switch {
		case errors.Is(err, raft.ErrNotLeader):
			errc <- rp.notLeader()
		case err == nil && to != rp.cfg.ID:
			rp.event(obs.Event{Kind: obs.EventLeaderTransfer, Node: to, Term: rp.r.Term()})
			errc <- nil
		default:
			errc <- err
		}
	}); err != nil {
		return err
	}
	select {
	case err := <-errc:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for rp.Leader() != to {
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-rp.done:
			return ErrStopped
		}
	}
	return nil
}

// Status describes the replica.
type Status struct {
	Leader  string
	Term    uint64
	Role    raft.State
	Commit  uint64
	Applied uint64
	// Conf is the membership in effect here; Progress is each member's
	// match index, on the leader only.
	Conf     raft.ConfState
	Progress map[string]uint64
	// FirstIndex and LastIndex bound the Raft log; entries before
	// FirstIndex were compacted into the state machine.
	FirstIndex, LastIndex uint64
	// SnapshotsSent and SnapshotsInstalled count snapshot transfers, and
	// Compactions log compactions, since the replica opened.
	SnapshotsSent, SnapshotsInstalled, Compactions int64
	Failed                                         error
}

// Status returns the replica's current status.
func (rp *Replica) Status() Status {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	prog := make(map[string]uint64, len(rp.progress))
	for k, v := range rp.progress {
		prog[k] = v
	}
	return Status{Leader: rp.leader, Term: rp.term, Role: rp.role, Commit: rp.commit, Applied: rp.applied.Load(),
		Conf: rp.conf.Clone(), Progress: prog, FirstIndex: rp.firstIdx, LastIndex: rp.lastIdx,
		SnapshotsSent: rp.snapsSent.Load(), SnapshotsInstalled: rp.snapsRecv.Load(), Compactions: rp.compactions.Load(),
		Failed: rp.failed}
}

// Leader returns the leader this replica knows of, or "".
func (rp *Replica) Leader() string {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.leader
}

// LastSeq returns the engine's last sequence number (its applied index).
func (rp *Replica) LastSeq() uint64 {
	rp.dbMu.RLock()
	defer rp.dbMu.RUnlock()
	return rp.db.LastSeq()
}
