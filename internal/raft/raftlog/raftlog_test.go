package raftlog

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/naveenk2608/distributed-database/internal/raft"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
	"github.com/naveenk2608/distributed-database/internal/storage/wal"
)

const dir = "/shard/raft"

func ent(i, t uint64) raft.Entry {
	return raft.Entry{Index: i, Term: t, Data: []byte(fmt.Sprintf("e%d.%d", i, t))}
}

func open(t *testing.T, fs vfs.FS) *Log {
	t.Helper()
	l, err := Open(fs, dir)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func equalEntries(a, b []raft.Entry) bool {
	return slices.EqualFunc(a, b, func(x, y raft.Entry) bool {
		return x.Index == y.Index && x.Term == y.Term && x.Type == y.Type && string(x.Data) == string(y.Data)
	})
}

func TestSaveReplayAndTruncation(t *testing.T) {
	fs := vfs.NewMemFS()
	l := open(t, fs)
	hs := raft.HardState{Term: 2, Vote: "n1", Commit: 2}
	if err := l.Save(hs, []raft.Entry{ent(1, 1), ent(2, 1), ent(3, 2), ent(4, 2)}, true); err != nil {
		t.Fatal(err)
	}
	// A new leader overwrites the uncommitted suffix from index 3.
	if err := l.Save(raft.HardState{Term: 3, Commit: 3}, []raft.Entry{ent(3, 3)}, true); err != nil {
		t.Fatal(err)
	}
	want := []raft.Entry{ent(1, 1), ent(2, 1), ent(3, 3)}
	if !equalEntries(l.Entries(), want) {
		t.Fatalf("in memory: %v", l.Entries())
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = open(t, fs)
	if !equalEntries(l.Entries(), want) || l.HardState() != (raft.HardState{Term: 3, Commit: 3}) {
		t.Fatalf("after reopen: %v %+v", l.Entries(), l.HardState())
	}
	if err := l.Save(raft.HardState{}, []raft.Entry{ent(9, 3)}, true); err == nil {
		t.Error("a gap in the log was accepted")
	}
}

// TestPowerLossKeepsSavedState: whatever a synced Save returned for
// survives a power loss, including a torn unsynced tail. Unsynced
// commit-index updates may or may not survive, in order.
func TestPowerLossKeepsSavedState(t *testing.T) {
	for seed := range uint64(200) {
		r := rand.New(rand.NewPCG(seed, 1))
		fs := vfs.NewMemFS()
		l := open(t, fs)
		var model []raft.Entry
		var hs raft.HardState
		var latestCommit uint64 // of the unsynced updates since hs
		term := uint64(1)
		for range 1 + r.IntN(30) {
			if len(model) > 0 && r.IntN(3) == 0 {
				latestCommit = max(latestCommit, hs.Commit) + r.Uint64N(uint64(len(model))-max(latestCommit, hs.Commit)+1)
				if err := l.Save(raft.HardState{Term: hs.Term, Vote: hs.Vote, Commit: latestCommit}, nil, false); err != nil {
					t.Fatal(err)
				}
				continue
			}
			if r.IntN(4) == 0 {
				term++
			}
			first := uint64(len(model)) + 1
			if len(model) > 0 && r.IntN(5) == 0 {
				first = 1 + r.Uint64N(uint64(len(model))) // overwrite a suffix
			}
			var ents []raft.Entry
			for i := range 1 + r.IntN(4) {
				ents = append(ents, ent(first+uint64(i), term))
			}
			hs = raft.HardState{Term: term, Vote: fmt.Sprint("n", r.IntN(3)), Commit: first - 1}
			if err := l.Save(hs, ents, true); err != nil {
				t.Fatal(err)
			}
			latestCommit = 0
			model = append(model[:first-1], ents...)
		}
		img := fs.CrashImage(rand.New(rand.NewPCG(seed, 2)))
		got := open(t, img)
		g := got.HardState()
		if !equalEntries(got.Entries(), model) || g.Term != hs.Term || g.Vote != hs.Vote ||
			g.Commit < hs.Commit || g.Commit > max(hs.Commit, latestCommit) {
			t.Fatalf("seed %d: recovered %d entries %+v; want %d entries %+v (commit up to %d)",
				seed, len(got.Entries()), g, len(model), hs, latestCommit)
		}
	}
}

func TestDamageIsCorruption(t *testing.T) {
	fs := vfs.NewMemFS()
	l := open(t, fs)
	for i := range uint64(20) {
		if err := l.Save(raft.HardState{Term: 1, Commit: i}, []raft.Entry{ent(i+1, 1)}, true); err != nil {
			t.Fatal(err)
		}
	}
	_ = l.Close()
	path := wal.SegmentPath(dir, 1)
	data, _ := fs.ReadFile(path)
	data[len(data)/3] ^= 0xff
	_ = fs.WriteFile(path, data)
	if _, err := Open(fs, dir); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("Open on a damaged log: %v", err)
	}
}

func conf3() raft.ConfState {
	return raft.ConfState{Voters: []string{"a", "b", "c"}, Learners: []string{"d"}}
}

func TestCompactAndRestore(t *testing.T) {
	fs := vfs.NewMemFS()
	l := open(t, fs)
	var ents []raft.Entry
	for i := uint64(1); i <= 10; i++ {
		ents = append(ents, ent(i, 1+i/5))
	}
	hs := raft.HardState{Term: 3, Vote: "a", Commit: 8}
	if err := l.Save(hs, ents, true); err != nil {
		t.Fatal(err)
	}
	snap := raft.SnapshotMeta{Index: 6, Term: ents[5].Term, Conf: conf3()}
	if err := l.Compact(raft.SnapshotMeta{Index: 6, Term: 99}); err == nil {
		t.Fatal("compaction with the wrong term accepted")
	}
	if err := l.Compact(snap); err != nil {
		t.Fatal(err)
	}
	if err := l.Save(raft.HardState{}, []raft.Entry{ent(11, 3)}, true); err != nil {
		t.Fatal(err)
	}
	if err := l.Save(raft.HardState{}, []raft.Entry{ent(5, 3)}, true); err == nil {
		t.Fatal("entries below the compaction point accepted")
	}
	_ = l.Close()
	if segs, _ := wal.ListSegments(fs, dir); len(segs) != 1 {
		t.Fatalf("segments after compaction: %v; want only the checkpoint's, which the writer continues", segs)
	}
	l = open(t, fs)
	got := l.Snapshot()
	if got.Index != 6 || got.Term != snap.Term || !got.Conf.Equal(conf3()) || l.HardState() != hs ||
		!equalEntries(l.Entries(), append(ents[6:], ent(11, 3))) || l.LastIndex() != 11 {
		t.Fatalf("after compaction and reopen: snap %+v hs %+v entries %v", got, l.HardState(), l.Entries())
	}
	restored := raft.SnapshotMeta{Index: 40, Term: 7, Conf: raft.ConfState{Voters: []string{"x"}}}
	if err := l.Restore(restored, raft.HardState{Term: 7}); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	l = open(t, fs)
	if l.Snapshot().Index != 40 || len(l.Entries()) != 0 || l.HardState().Commit != 40 || l.HardState().Term != 7 {
		t.Fatalf("after restore: %+v %+v %v", l.Snapshot(), l.HardState(), l.Entries())
	}
	if err := l.Save(raft.HardState{}, []raft.Entry{ent(41, 7)}, true); err != nil {
		t.Fatal(err)
	}
}

// TestPowerLossDuringCompaction crashes at every filesystem operation of a
// compaction and of a restore: the recovered log is the state before or
// after, never one that lost entries.
func TestPowerLossDuringCompaction(t *testing.T) {
	var ents []raft.Entry
	for i := uint64(1); i <= 12; i++ {
		ents = append(ents, ent(i, 2))
	}
	hs := raft.HardState{Term: 2, Commit: 12}
	snap := raft.SnapshotMeta{Index: 7, Term: 2, Conf: conf3()}
	restored := raft.SnapshotMeta{Index: 30, Term: 4, Conf: conf3()}
	for _, op := range []string{"compact", "restore"} {
		for n := int64(0); ; n++ {
			fs := vfs.NewMemFS()
			l := open(t, fs)
			if err := l.Save(hs, ents, true); err != nil {
				t.Fatal(err)
			}
			fs.CrashAfter(n)
			var err error
			if op == "compact" {
				err = l.Compact(snap)
			} else {
				err = l.Restore(restored, raft.HardState{Term: 4})
			}
			crashed := fs.Crashed()
			got := open(t, fs.CrashImage(rand.New(rand.NewPCG(uint64(n), 3))))
			before := got.Snapshot().Index == 0 && equalEntries(got.Entries(), ents) && got.HardState() == hs
			var after bool
			if op == "compact" {
				after = got.Snapshot().Index == 7 && equalEntries(got.Entries(), ents[7:]) && got.HardState() == hs
			} else {
				after = got.Snapshot().Index == 30 && len(got.Entries()) == 0 && got.HardState().Commit == 30
			}
			if !before && !after {
				t.Fatalf("%s, crash after %d operations: recovered snap %+v hs %+v entries %v", op, n, got.Snapshot(), got.HardState(), got.Entries())
			}
			if !crashed {
				if err != nil || !after {
					t.Fatalf("%s without a crash: %v, after=%v", op, err, after)
				}
				t.Logf("%s: crashed at each of %d operations", op, n)
				break
			}
		}
	}
}
