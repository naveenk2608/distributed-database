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
