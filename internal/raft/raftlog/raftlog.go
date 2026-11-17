// Package raftlog persists one Raft group's snapshot metadata, HardState
// and log entries on the WAL package.
//
// Three record types: an entries record holds consecutive entries starting
// at some index i and replaces everything persisted at or after i (that is
// how a follower's conflicting suffix is truncated); a hard-state record
// holds term, vote and commit index; and a checkpoint record holds a
// snapshot's metadata, the HardState and every entry after the snapshot,
// replacing everything before it. Recovery replays them in order. Save
// writes a Ready's entries and HardState with one write and, when Raft asks
// for it, one fsync, and the replica sends no message before Save returns,
// so a vote or an acknowledgment is never forgotten after a crash.
//
// Compaction and snapshot installation write a checkpoint as the only
// record of a fresh segment, fsync it, and then delete the older segments.
// The checkpoint is one checksummed record, so a crash leaves either the
// old segments or the whole checkpoint, never a mix that loses entries.
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
	recEntries    uint8 = 1
	recHardState  uint8 = 2
	recCheckpoint uint8 = 3
)

// ErrCorrupt is returned by Open for a log that cannot be replayed.
var ErrCorrupt = errors.New("raftlog: corrupt log")

// Log is a durable Raft log. It is not safe for concurrent use; one
// replica goroutine owns it.
type Log struct {
	fs      vfs.FS
	dir     string
	w       *wal.Writer
	snap    raft.SnapshotMeta
	hs      raft.HardState
	entries []raft.Entry // indexes snap.Index+1 onward
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
			if err := l.place(ents); err != nil {
				return err
			}
		case recHardState:
			hs, err := decodeHardState(r.Payload)
			if err != nil {
				return err
			}
			l.hs = hs
		case recCheckpoint:
			snap, hs, ents, err := decodeCheckpoint(r.Payload)
			if err != nil {
				return err
			}
			l.snap, l.hs, l.entries = snap, hs, ents
		default:
			return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, r.Type)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("raftlog: open %s: %w", dir, err)
	}
	if l.hs.Commit > l.LastIndex() {
		return nil, fmt.Errorf("%w: commit index %d beyond last entry %d", ErrCorrupt, l.hs.Commit, l.LastIndex())
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

// place puts ents into the in-memory log, replacing any entries at or
// after the first of them.
func (l *Log) place(ents []raft.Entry) error {
	first := ents[0].Index
	if first <= l.snap.Index || first > l.LastIndex()+1 {
		return fmt.Errorf("%w: entries start at %d; the log covers %d..%d", ErrCorrupt, first, l.snap.Index+1, l.LastIndex())
	}
	l.entries = append(l.entries[:first-1-l.snap.Index], ents...)
	return nil
}

// Snapshot returns the metadata of the latest snapshot or compaction
// point; entries up to its index are gone from the log.
func (l *Log) Snapshot() raft.SnapshotMeta { return l.snap }

// HardState returns the persisted HardState.
func (l *Log) HardState() raft.HardState { return l.hs }

// Entries returns the persisted entries after the snapshot. The slice must
// not be modified.
func (l *Log) Entries() []raft.Entry { return l.entries }

// LastIndex returns the index of the last persisted entry.
func (l *Log) LastIndex() uint64 { return l.snap.Index + uint64(len(l.entries)) }

// Save persists hs (if non-empty) and ents, which replace any persisted
// entries at or after ents[0].Index, with one write and, if sync is set,
// one fsync. Without sync the write survives a process crash but may be
// lost on power loss; Raft asks for that only for commit-index updates.
func (l *Log) Save(hs raft.HardState, ents []raft.Entry, sync bool) error {
	var recs []wal.Record
	if len(ents) > 0 {
		first := ents[0].Index
		if first <= l.snap.Index || first > l.LastIndex()+1 {
			return fmt.Errorf("raftlog: entries start at %d; the log covers %d..%d", first, l.snap.Index+1, l.LastIndex())
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
		_ = l.place(ents) // checked above
	}
	if !hs.IsEmpty() {
		l.hs = hs
	}
	return nil
}

// Compact drops the entries up to snap.Index, which the state machine has
// made durable; snap describes that point (its term and membership).
func (l *Log) Compact(snap raft.SnapshotMeta) error {
	if snap.Index <= l.snap.Index {
		return nil
	}
	if snap.Index > l.LastIndex() {
		return fmt.Errorf("raftlog: cannot compact to %d beyond the last entry %d", snap.Index, l.LastIndex())
	}
	if t := l.entries[snap.Index-1-l.snap.Index].Term; t != snap.Term {
		return fmt.Errorf("raftlog: compaction point %d has term %d, not %d", snap.Index, t, snap.Term)
	}
	rest := append([]raft.Entry(nil), l.entries[snap.Index-l.snap.Index:]...)
	return l.checkpoint(snap, l.hs, rest)
}

// Restore replaces the whole log with an installed snapshot and hs.
func (l *Log) Restore(snap raft.SnapshotMeta, hs raft.HardState) error {
	if hs.IsEmpty() {
		hs = l.hs
	}
	hs.Commit = max(hs.Commit, snap.Index)
	return l.checkpoint(snap, hs, nil)
}

// checkpoint writes the log's whole state as the only record of a new
// segment and deletes the older segments.
func (l *Log) checkpoint(snap raft.SnapshotMeta, hs raft.HardState, ents []raft.Entry) error {
	seg, err := l.w.Rotate()
	if err != nil {
		return err
	}
	if err := l.w.Append(wal.Record{Type: recCheckpoint, Payload: encodeCheckpoint(snap, hs, ents)}); err != nil {
		return err
	}
	// Fsync the checkpoint before deleting the segments it supersedes
	// (Rotate already made the new segment's directory entry durable).
	if err := l.w.Sync(); err != nil {
		return err
	}
	l.snap, l.hs, l.entries = snap, hs, ents
	segs, err := wal.ListSegments(l.fs, l.dir)
	if err != nil {
		return err
	}
	for _, s := range segs {
		if s < seg {
			if err := l.fs.Remove(wal.SegmentPath(l.dir, s)); err != nil {
				return fmt.Errorf("raftlog: remove old segment: %w", err)
			}
		}
	}
	// Fsync the directory so the space is reclaimed for good; if the
	// removals were lost, replay would reach the same state anyway.
	if err := l.fs.SyncDir(l.dir); err != nil {
		return fmt.Errorf("raftlog: sync %s: %w", l.dir, err)
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

func encodeCheckpoint(snap raft.SnapshotMeta, hs raft.HardState, ents []raft.Entry) []byte {
	conf := raft.EncodeConfChange(raft.ConfChange{Conf: snap.Conf})
	b := binary.LittleEndian.AppendUint64(nil, snap.Index)
	b = binary.LittleEndian.AppendUint64(b, snap.Term)
	b = binary.AppendUvarint(b, uint64(len(conf)))
	b = append(b, conf...)
	h := encodeHardState(hs)
	b = binary.AppendUvarint(b, uint64(len(h)))
	b = append(b, h...)
	return append(b, encodeEntries(ents)...)
}

func decodeCheckpoint(b []byte) (raft.SnapshotMeta, raft.HardState, []raft.Entry, error) {
	var snap raft.SnapshotMeta
	bad := func(what string) (raft.SnapshotMeta, raft.HardState, []raft.Entry, error) {
		return snap, raft.HardState{}, nil, fmt.Errorf("%w: checkpoint %s", ErrCorrupt, what)
	}
	if len(b) < 16 {
		return bad("truncated")
	}
	snap.Index, snap.Term = binary.LittleEndian.Uint64(b), binary.LittleEndian.Uint64(b[8:])
	b = b[16:]
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)-k) {
		return bad("membership length")
	}
	cc, err := raft.DecodeConfChange(b[k : k+int(n)])
	if err != nil {
		return bad("membership")
	}
	snap.Conf = cc.Conf
	b = b[k+int(n):]
	n, k = binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)-k) {
		return bad("hard state length")
	}
	hs, err := decodeHardState(b[k : k+int(n)])
	if err != nil {
		return bad("hard state")
	}
	ents, err := decodeEntries(b[k+int(n):])
	if err != nil {
		return bad("entries")
	}
	if len(ents) > 0 && ents[0].Index != snap.Index+1 {
		return bad("entries do not follow the snapshot")
	}
	return snap, hs, ents, nil
}
