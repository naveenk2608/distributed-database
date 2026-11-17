// Package raftlog persists one Raft group's HardState and log entries on
// the WAL package.
//
// Two record types: an entries record holds consecutive entries starting at
// some index i and replaces everything persisted at or after i (that is how
// a follower's conflicting suffix is truncated), and a hard-state record
// holds term, vote and commit index. Recovery replays them in order. Save
// writes a Ready's entries and HardState with one write and one fsync, and
// the replica sends no message before Save returns, so a vote or an
// acknowledgment is never forgotten after a crash.
package raftlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/naveenk2608/distributed-database/internal/raft"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
	"github.com/naveenk2608/distributed-database/internal/storage/wal"
)

const (
	recEntries   uint8 = 1
	recHardState uint8 = 2
)

// ErrCorrupt is returned by Open for a log that cannot be replayed.
var ErrCorrupt = errors.New("raftlog: corrupt log")

// Log is a durable Raft log. It is not safe for concurrent use; one
// replica goroutine owns it.
type Log struct {
	fs      vfs.FS
	dir     string
	w       *wal.Writer
	hs      raft.HardState
	entries []raft.Entry // index 1..len
}

// Open opens or creates the log in dir and replays it.
func Open(fsys vfs.FS, dir string) (*Log, error) {
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, fmt.Errorf("raftlog: create %s: %w", dir, err)
	}
	// Fsync the parent so the log directory itself survives a power loss.
	if err := fsys.SyncDir(filepath.Dir(filepath.Clean(dir))); err != nil {
		return nil, fmt.Errorf("raftlog: sync parent of %s: %w", dir, err)
	}
	l := &Log{fs: fsys, dir: dir}
	res, err := wal.Recover(fsys, dir, func(_ uint64, r wal.Record) error {
		switch r.Type {
		case recEntries:
			ents, err := decodeEntries(r.Payload)
			if err != nil {
				return err
			}
			if len(ents) == 0 {
				return nil
			}
			first := ents[0].Index
			if first == 0 || first > uint64(len(l.entries))+1 {
				return fmt.Errorf("%w: entries start at %d after %d persisted", ErrCorrupt, first, len(l.entries))
			}
			l.entries = append(l.entries[:first-1], ents...)
		case recHardState:
			hs, err := decodeHardState(r.Payload)
			if err != nil {
				return err
			}
			l.hs = hs
		default:
			return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, r.Type)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("raftlog: open %s: %w", dir, err)
	}
	if l.hs.Commit > uint64(len(l.entries)) {
		return nil, fmt.Errorf("%w: commit index %d beyond %d persisted entries", ErrCorrupt, l.hs.Commit, len(l.entries))
	}
	next := uint64(1)
	if n := len(res.Segments); n > 0 {
		next = res.Segments[n-1] + 1
	}
	if l.w, err = wal.NewWriter(fsys, dir, next); err != nil {
		return nil, fmt.Errorf("raftlog: open %s: %w", dir, err)
	}
	return l, nil
}

// HardState returns the persisted HardState.
func (l *Log) HardState() raft.HardState { return l.hs }

// Entries returns the persisted entries (index 1 first). The slice must not
// be modified.
func (l *Log) Entries() []raft.Entry { return l.entries }

// LastIndex returns the index of the last persisted entry.
func (l *Log) LastIndex() uint64 { return uint64(len(l.entries)) }

// Save persists hs (if non-empty) and ents, which replace any persisted
// entries at or after ents[0].Index, with one write and, if sync is set,
// one fsync. Without sync the write survives a process crash but may be
// lost on power loss; Raft asks for that only for commit-index updates.
func (l *Log) Save(hs raft.HardState, ents []raft.Entry, sync bool) error {
	var recs []wal.Record
	if len(ents) > 0 {
		first := ents[0].Index
		if first == 0 || first > uint64(len(l.entries))+1 {
			return fmt.Errorf("raftlog: entries start at %d but %d are persisted", first, len(l.entries))
		}
		recs = append(recs, wal.Record{Type: recEntries, Payload: encodeEntries(ents)})
	}
	if !hs.IsEmpty() {
		recs = append(recs, wal.Record{Type: recHardState, Payload: encodeHardState(hs)})
	}
	if len(recs) == 0 {
		return nil
	}
	if err := l.w.Append(recs...); err != nil {
		return err
	}
	// Fsync before the caller sends any message: Raft's safety depends on
	// a vote or an acknowledged entry never being forgotten.
	if sync {
		if err := l.w.Sync(); err != nil {
			return err
		}
	}
	if len(ents) > 0 {
		l.entries = append(l.entries[:ents[0].Index-1], ents...)
	}
	if !hs.IsEmpty() {
		l.hs = hs
	}
	return nil
}

// Close syncs and closes the log.
func (l *Log) Close() error { return l.w.Close() }

func encodeEntries(ents []raft.Entry) []byte {
	b := binary.AppendUvarint(nil, uint64(len(ents)))
	for _, e := range ents {
		b = binary.LittleEndian.AppendUint64(b, e.Index)
		b = binary.LittleEndian.AppendUint64(b, e.Term)
		b = append(b, byte(e.Type))
		b = binary.AppendUvarint(b, uint64(len(e.Data)))
		b = append(b, e.Data...)
	}
	return b
}

func decodeEntries(b []byte) ([]raft.Entry, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)) {
		return nil, fmt.Errorf("%w: bad entry count", ErrCorrupt)
	}
	b = b[k:]
	ents := make([]raft.Entry, 0, n)
	for i := range n {
		if len(b) < 17 {
			return nil, fmt.Errorf("%w: entry %d truncated", ErrCorrupt, i)
		}
		e := raft.Entry{
			Index: binary.LittleEndian.Uint64(b),
			Term:  binary.LittleEndian.Uint64(b[8:]),
			Type:  raft.EntryType(b[16]),
		}
		b = b[17:]
		dl, k := binary.Uvarint(b)
		if k <= 0 || dl > uint64(len(b)-k) {
			return nil, fmt.Errorf("%w: entry %d data length", ErrCorrupt, i)
		}
		e.Data = append([]byte(nil), b[k:k+int(dl)]...)
		b = b[k+int(dl):]
		if len(ents) > 0 && e.Index != ents[len(ents)-1].Index+1 {
			return nil, fmt.Errorf("%w: entries not consecutive", ErrCorrupt)
		}
		ents = append(ents, e)
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrCorrupt, len(b))
	}
	return ents, nil
}

func encodeHardState(hs raft.HardState) []byte {
	b := binary.LittleEndian.AppendUint64(nil, hs.Term)
	b = binary.LittleEndian.AppendUint64(b, hs.Commit)
	b = binary.AppendUvarint(b, uint64(len(hs.Vote)))
	return append(b, hs.Vote...)
}

func decodeHardState(b []byte) (raft.HardState, error) {
	if len(b) < 17 {
		return raft.HardState{}, fmt.Errorf("%w: hard state truncated", ErrCorrupt)
	}
	hs := raft.HardState{Term: binary.LittleEndian.Uint64(b), Commit: binary.LittleEndian.Uint64(b[8:])}
	vl, k := binary.Uvarint(b[16:])
	if k <= 0 || vl != uint64(len(b)-16-k) {
		return raft.HardState{}, fmt.Errorf("%w: hard state vote", ErrCorrupt)
	}
	hs.Vote = string(b[16+k:])
	return hs, nil
}
