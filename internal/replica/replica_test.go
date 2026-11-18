package replica

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/obs"
	"github.com/naveenk2608/distributed-database/internal/raft"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// loopback delivers messages straight to the destination replica.
type loopback struct {
	mu   sync.Mutex
	reps map[string]*Replica
	down map[string]bool
}

func (l *loopback) Send(_ int, msgs []raft.Message) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range msgs {
		if r := l.reps[m.To]; r != nil && !l.down[m.To] && !l.down[m.From] {
			r.Step(m)
		}
	}
}

type group struct {
	t       *testing.T
	fs      *vfs.MemFS
	fss     map[string]*vfs.MemFS // replicas with a filesystem of their own
	ids     []string              // every replica ever opened
	peers   []string              // the bootstrap voters
	compact int
	net     *loopback
	stop    chan struct{}
	wg      sync.WaitGroup

	evMu   sync.Mutex
	events []seenEvent // every replica's OnEvent calls
}

// seenEvent is an event and the replica that reported it.
type seenEvent struct {
	rep string
	obs.Event
}

// eventsOf returns the recorded events of one kind.
func (g *group) eventsOf(kind string) []seenEvent {
	g.evMu.Lock()
	defer g.evMu.Unlock()
	var out []seenEvent
	for _, e := range g.events {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

func newGroup(t *testing.T, n int) *group { return newGroupCompacting(t, n, 0) }

// newGroupCompacting starts n bootstrap replicas that compact their Raft
// logs every compact entries (0: the default).
func newGroupCompacting(t *testing.T, n, compact int) *group {
	t.Helper()
	g := &group{t: t, fs: vfs.NewMemFS(), fss: map[string]*vfs.MemFS{}, compact: compact,
		net: &loopback{reps: map[string]*Replica{}, down: map[string]bool{}}, stop: make(chan struct{})}
	for i := range n {
		g.ids = append(g.ids, fmt.Sprintf("n%d", i+1))
	}
	g.peers = slices.Clone(g.ids)
	for _, id := range g.ids {
		g.open(id)
	}
	g.wg.Add(1)
	go func() { // one ticker drives every replica
		defer g.wg.Done()
		tk := time.NewTicker(2 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-g.stop:
				return
			case <-tk.C:
				g.net.mu.Lock()
				for _, r := range g.net.reps {
					r.Tick()
				}
				g.net.mu.Unlock()
			}
		}
	}()
	t.Cleanup(func() {
		close(g.stop)
		g.wg.Wait()
		for _, id := range g.ids {
			g.close(id)
		}
	})
	return g
}

func (g *group) fsOf(id string) *vfs.MemFS {
	if fs := g.fss[id]; fs != nil {
		return fs
	}
	return g.fs
}

func (g *group) open(id string) {
	g.t.Helper()
	if err := g.tryOpen(id); err != nil {
		g.t.Fatal(err)
	}
}

func (g *group) tryOpen(id string) error {
	var peers []string
	if slices.Contains(g.peers, id) {
		peers = g.peers
	}
	if !slices.Contains(g.ids, id) {
		g.ids = append(g.ids, id)
	}
	r, err := Open(Config{Shard: 7, ID: id, Peers: peers, Dir: "/data/" + id, FS: g.fsOf(id),
		// No engine log, as in production: the Raft log is the only one.
		Engine: lsm.Options{DisableWAL: true}, ElectionTick: 10, HeartbeatTick: 2,
		SessionTTL: time.Hour, CompactEntries: g.compact, Sender: g.net, Snapshots: g,
		OnEvent: func(e obs.Event) {
			g.evMu.Lock()
			g.events = append(g.events, seenEvent{id, e})
			g.evMu.Unlock()
		},
		Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		return err
	}
	g.net.mu.Lock()
	g.net.reps[id] = r
	g.net.mu.Unlock()
	return nil
}

// SendSnapshot streams a checkpoint in process: it copies the files from
// the sender's filesystem into the receiver's.
func (g *group) SendSnapshot(ctx context.Context, _ int, m raft.Message, fs vfs.FS, dir string) error {
	to := g.rep(m.To)
	if to == nil {
		return fmt.Errorf("%s is not running", m.To)
	}
	dstFS := g.fsOf(m.To)
	return to.ReceiveSnapshot(ctx, m, func(dst string) error {
		names, err := fs.List(dir)
		if err != nil {
			return err
		}
		for _, n := range names {
			data, err := vfs.ReadFile(fs, filepath.Join(dir, n))
			if err != nil {
				return err
			}
			f, err := dstFS.Create(filepath.Join(dst, n))
			if err != nil {
				return err
			}
			if _, err := f.Write(data); err != nil {
				return err
			}
			if err := f.Sync(); err != nil {
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
		return dstFS.SyncDir(dst)
	})
}

func (g *group) close(id string) {
	g.net.mu.Lock()
	r := g.net.reps[id]
	delete(g.net.reps, id)
	g.net.mu.Unlock()
	if r != nil {
		if err := r.Close(); err != nil {
			g.t.Errorf("close %s: %v", id, err)
		}
	}
}

func (g *group) rep(id string) *Replica {
	g.net.mu.Lock()
	defer g.net.mu.Unlock()
	return g.net.reps[id]
}

func (g *group) leader() *Replica {
	g.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range g.ids {
			if r := g.rep(id); r != nil && r.Status().Role == raft.StateLeader {
				return r
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	g.t.Fatal("no leader elected")
	return nil
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestReplicatedOperations(t *testing.T) {
	g := newGroup(t, 3)
	l := g.leader()
	v1, err := l.Put(ctx(t), []byte("k"), []byte("a"), "c1", 1)
	if err != nil {
		t.Fatal(err)
	}
	v, ver, found, _, err := l.Get(ctx(t), []byte("k"), false, 0)
	if err != nil || !found || string(v) != "a" || ver != v1 {
		t.Fatalf("linearizable get = %q@%d %v %v", v, ver, found, err)
	}
	// CAS with a stale expected version fails and reports the current one.
	ok, cur, err := l.CompareAndSet(ctx(t), []byte("k"), []byte("b"), v1+100, "c1", 2)
	if err != nil || ok || cur != v1 {
		t.Fatalf("CAS with wrong version = %v %d %v", ok, cur, err)
	}
	ok, v2, err := l.CompareAndSet(ctx(t), []byte("k"), []byte("b"), v1, "c1", 3)
	if err != nil || !ok || v2 <= v1 {
		t.Fatalf("CAS with right version = %v %d %v", ok, v2, err)
	}
	ok, vNew, err := l.CompareAndSet(ctx(t), []byte("new"), []byte("x"), 0, "c1", 4)
	if err != nil || !ok {
		t.Fatalf("CAS creating a key = %v %v", ok, err)
	}
	found, _, err = l.Delete(ctx(t), []byte("k"), "c1", 5)
	if err != nil || !found {
		t.Fatalf("delete = %v %v", found, err)
	}
	// A follower serves stale reads once it has applied the write.
	for _, id := range g.ids {
		r := g.rep(id)
		if r == l {
			if _, _, _, _, err := r.Get(ctx(t), []byte("new"), false, 0); err != nil {
				t.Fatal(err)
			}
			continue
		}
		v, _, found, applied, err := r.Get(ctx(t), []byte("new"), true, vNew)
		if err != nil || !found || string(v) != "x" || applied < vNew {
			t.Fatalf("stale read on %s = %q %v applied %d %v", id, v, found, applied, err)
		}
		var nl *NotLeaderError
		if _, _, _, _, err := r.Get(ctx(t), []byte("new"), false, 0); !errors.As(err, &nl) || nl.LeaderHint() == "" {
			t.Fatalf("linearizable read on follower %s: %v", id, err)
		}
	}
}

// TestRetriedWriteAppliedOnce: the same (client, request) twice gives the
// same result and changes nothing the second time.
func TestRetriedWriteAppliedOnce(t *testing.T) {
	g := newGroup(t, 3)
	l := g.leader()
	v1, err := l.Put(ctx(t), []byte("k"), []byte("first"), "c1", 7)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := l.Put(ctx(t), []byte("k"), []byte("second"), "c1", 7) // a retry, even with other bytes
	if err != nil || v2 != v1 {
		t.Fatalf("retry returned %d, %v; want the original version %d", v2, err, v1)
	}
	if v, ver, _, _, _ := l.Get(ctx(t), []byte("k"), false, 0); string(v) != "first" || ver != v1 {
		t.Fatalf("after the retry k = %q@%d; the retry must not apply", v, ver)
	}
	if _, err := l.Put(ctx(t), []byte("k"), []byte("old"), "c1", 6); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("an older request id: %v", err)
	}
	// Another client is unaffected.
	if v3, err := l.Put(ctx(t), []byte("k"), []byte("other"), "c2", 7); err != nil || v3 <= v1 {
		t.Fatalf("other client: %d %v", v3, err)
	}
}

func TestStateSurvivesRestart(t *testing.T) {
	g := newGroup(t, 3)
	l := g.leader()
	var last uint64
	for i := range 20 {
		var err error
		if last, err = l.Put(ctx(t), []byte(fmt.Sprint("k", i)), []byte("v"), "c1", uint64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range g.ids { // restart every replica
		g.close(id)
	}
	for _, id := range g.ids {
		g.open(id)
	}
	l = g.leader()
	if v, ver, found, _, err := l.Get(ctx(t), []byte("k19"), false, 0); err != nil || !found || string(v) != "v" || ver != last {
		t.Fatalf("after restart: %q@%d %v %v", v, ver, found, err)
	}
	// The session table survived too: replaying request 20 is a duplicate.
	if v, err := l.Put(ctx(t), []byte("k19"), []byte("again"), "c1", 20); err != nil || v != last {
		t.Fatalf("duplicate after restart: %d %v", v, err)
	}
}

// TestPowerLossReplaysTheRaftLog: a whole group loses power right after
// its writes were acknowledged. The engines keep no log, so whatever they
// had not flushed comes back from the Raft logs: every write at its
// version, and the session table that answers a retry. With log
// compaction, the Raft log no longer holds the start of history; the
// engines must have flushed it before it was compacted away.
func TestPowerLossReplaysTheRaftLog(t *testing.T) {
	for _, tc := range []struct {
		compact, writes int
	}{{0, 20}, {50, 120}} {
		t.Run(fmt.Sprintf("compact=%d", tc.compact), func(t *testing.T) {
			g := newGroupCompacting(t, 3, tc.compact)
			l := g.leader()
			var last uint64
			for i := range tc.writes {
				var err error
				if last, err = l.Put(ctx(t), []byte(fmt.Sprint("k", i)), []byte(fmt.Sprint("v", i)), "c1", uint64(i+1)); err != nil {
					t.Fatal(err)
				}
			}
			if tc.compact == 0 {
				for _, id := range g.ids {
					if st := g.rep(id).EngineMetrics(); st.FlushedSeq != 0 {
						t.Fatalf("setup: %s's engine flushed through %d; want nothing flushed", id, st.FlushedSeq)
					}
				}
			} else {
				g.waitFor("the leader to compact", func() bool { return l.Status().FirstIndex > 2 })
			}
			img := g.fs.CrashImage(nil) // power cut: only what was fsynced
			for _, id := range g.ids {
				g.discard(id) // its filesystem is gone; closing it can only fail
			}
			g.fs = img
			for _, id := range g.ids {
				g.open(id)
			}
			l = g.leader()
			for i := range tc.writes {
				key := fmt.Sprint("k", i)
				if v, _, found, _, err := l.Get(ctx(t), []byte(key), false, 0); err != nil || !found || string(v) != fmt.Sprint("v", i) {
					t.Fatalf("after the power loss %s = %q %v %v", key, v, found, err)
				}
			}
			if v, err := l.Put(ctx(t), []byte(fmt.Sprint("k", tc.writes-1)), []byte("again"), "c1", uint64(tc.writes)); err != nil || v != last {
				t.Fatalf("retry of the last write after the power loss: version %d, %v; want the original %d", v, err, last)
			}
		})
	}
}

func TestWritesContinueAfterLeaderStops(t *testing.T) {
	g := newGroup(t, 3)
	l := g.leader()
	if _, err := l.Put(ctx(t), []byte("before"), []byte("v"), "", 0); err != nil {
		t.Fatal(err)
	}
	var old string
	for _, id := range g.ids {
		if g.rep(id) == l {
			old = id
		}
	}
	g.close(old)
	start := time.Now()
	nl := g.leader()
	t.Logf("new leader after %v", time.Since(start).Round(time.Millisecond))
	if _, err := nl.Put(ctx(t), []byte("after"), []byte("v"), "", 0); err != nil {
		t.Fatal(err)
	}
	if v, _, found, _, err := nl.Get(ctx(t), []byte("before"), false, 0); err != nil || !found || string(v) != "v" {
		t.Fatalf("write from the old leader's term: %q %v %v", v, found, err)
	}
}

// TestTxnIsAtomicAndConditional: a transaction's writes all happen, at one
// version, only when every condition holds; Scan sees them by prefix.
func TestTxnIsAtomicAndConditional(t *testing.T) {
	g := newGroup(t, 3)
	l := g.leader()
	w := func(k, v string) *dkvv1.TxnWrite { return &dkvv1.TxnWrite{Key: []byte(k), Value: []byte(v)} }
	ok, ver, err := l.Txn(ctx(t), &dkvv1.Txn{
		Conditions: []*dkvv1.TxnCondition{{Key: []byte("n/a"), Version: 0}},
		Writes:     []*dkvv1.TxnWrite{w("n/a", "1"), w("n/b", "2"), w("d/1", "x")},
	}, "", 0)
	if err != nil || !ok {
		t.Fatalf("first txn: %v %v", ok, err)
	}
	// The condition no longer holds: nothing is written.
	ok, _, err = l.Txn(ctx(t), &dkvv1.Txn{
		Conditions: []*dkvv1.TxnCondition{{Key: []byte("n/a"), Version: 0}},
		Writes:     []*dkvv1.TxnWrite{w("n/c", "3")},
	}, "", 0)
	if err != nil || ok {
		t.Fatalf("txn with a stale condition: %v %v", ok, err)
	}
	ok, _, err = l.Txn(ctx(t), &dkvv1.Txn{
		Conditions: []*dkvv1.TxnCondition{{Key: []byte("n/a"), Version: ver}, {Key: []byte("n/b"), Version: ver}},
		Writes:     []*dkvv1.TxnWrite{{Key: []byte("n/b"), Delete: true}, w("n/a", "9")},
	}, "", 0)
	if err != nil || !ok {
		t.Fatalf("txn with current conditions: %v %v", ok, err)
	}
	kvs, err := l.Scan(ctx(t), []byte("n/"), false)
	if err != nil || len(kvs) != 1 || string(kvs[0].Key) != "n/a" || string(kvs[0].Value) != "9" {
		t.Fatalf("scan n/: %+v %v", kvs, err)
	}
}

// TestUnsafeNoSyncSkipsWriteSyncs: with UnsafeNoSync (storage.sync =
// never) a replica's writes reach the Raft log without a single fsync;
// without it every write is fsynced before it is acknowledged. The engine
// runs with SyncNever in both, so every counted sync is the Raft log's.
func TestUnsafeNoSyncSkipsWriteSyncs(t *testing.T) {
	for _, unsafe := range []bool{false, true} {
		t.Run(fmt.Sprintf("unsafe=%v", unsafe), func(t *testing.T) {
			var syncs atomic.Int64
			net := &loopback{reps: map[string]*Replica{}, down: map[string]bool{}}
			r, err := Open(Config{Shard: 7, ID: "n1", Peers: []string{"n1"}, Dir: "/data/n1",
				FS: vfs.CountSyncs(vfs.NewMemFS(), &syncs), Engine: lsm.Options{Sync: lsm.SyncNever},
				ElectionTick: 10, HeartbeatTick: 2, SessionTTL: time.Hour, Sender: net,
				UnsafeNoSync: unsafe, Log: slog.New(slog.DiscardHandler)})
			if err != nil {
				t.Fatal(err)
			}
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				tk := time.NewTicker(2 * time.Millisecond)
				defer tk.Stop()
				for {
					select {
					case <-stop:
						return
					case <-tk.C:
						r.Tick()
					}
				}
			}()
			defer func() {
				close(stop)
				wg.Wait()
				if err := r.Close(); err != nil {
					t.Error(err)
				}
			}()
			for deadline := time.Now().Add(10 * time.Second); r.Status().Role != raft.StateLeader; time.Sleep(5 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatal("the single replica did not elect itself")
				}
			}
			if _, err := r.Put(ctx(t), []byte("k"), []byte("v0"), "c1", 1); err != nil {
				t.Fatal(err)
			}
			before := syncs.Load()
			const writes = 20
			for i := range writes {
				if _, err := r.Put(ctx(t), []byte("k"), fmt.Appendf(nil, "v%d", i+1), "c1", uint64(i+2)); err != nil {
					t.Fatal(err)
				}
			}
			n := syncs.Load() - before
			switch {
			case unsafe && n != 0:
				t.Fatalf("%d syncs for %d acknowledged writes with UnsafeNoSync; want none", n, writes)
			case !unsafe && n < writes:
				t.Fatalf("%d syncs for %d acknowledged writes; want one at least per write", n, writes)
			}
		})
	}
}
