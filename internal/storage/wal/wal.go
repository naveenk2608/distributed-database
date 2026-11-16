// Package wal implements dkv's write-ahead log: a directory of numbered
// segment files, each a sequence of checksummed records.
//
//	segment 000042.wal := record*
//	record             := crc32c(4) | length(4) | type(1) | payload(length)
//
// Integers are little-endian. The CRC-32C (Castagnoli) covers the length,
// type and payload, so a torn or bit-flipped record is detected rather than
// read. Type 0 is reserved and never written, so a zero-filled region can
// never decode as a record. The payload is opaque here; the engine stores
// batches in it, and Phase 6 reuses this package for the Raft log.
//
// A Writer appends to one segment at a time. Append writes any number of
// records with a single write call and Sync makes them durable, which is
// what group commit needs. Recover replays every segment in order, truncates
// a torn tail at the end of the newest segment, and refuses to continue past
// damage anywhere else. See docs/adr/0019-wal-format-and-recovery.md.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

const (
	headerSize = 9 // crc(4) + length(4) + type(1)
	// MaxPayload bounds one record's payload. It keeps a corrupt length
	// field from looking plausible and fits the 4-byte length.
	MaxPayload = 1 << 31
	suffix     = ".wal"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Record is one log record. Type 0 is reserved.
type Record struct {
	Type    uint8
	Payload []byte
}

// Errors returned by this package. Check them with errors.Is.
var (
	// ErrCorrupt means the log is damaged somewhere a crash cannot explain.
	ErrCorrupt = errors.New("wal: corrupt log")
	// ErrClosed is returned by a Writer after Close.
	ErrClosed = errors.New("wal: writer closed")
)

// CorruptionError describes damage found by Recover. It matches ErrCorrupt
// with errors.Is.
type CorruptionError struct {
	Path   string // segment file
	Offset int64  // byte offset of the damaged record
	Reason string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("wal: corrupt record in %s at offset %d: %s", e.Path, e.Offset, e.Reason)
}

// Is makes errors.Is(err, ErrCorrupt) true.
func (e *CorruptionError) Is(target error) bool { return target == ErrCorrupt }

// SegmentPath returns the path of segment n in dir.
func SegmentPath(dir string, n uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", n, suffix))
}

// parseSegmentName returns the number of a segment file name, or false if
// name is not one. Only the canonical spelling counts, so "1.wal" and
// "000001.wal" can never both name segment 1.
func parseSegmentName(name string) (uint64, bool) {
	digits, ok := strings.CutSuffix(name, suffix)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || filepath.Base(SegmentPath("", n)) != name {
		return 0, false
	}
	return n, true
}

// ListSegments returns the segment numbers in dir in ascending order.
func ListSegments(fs vfs.FS, dir string) ([]uint64, error) {
	names, err := fs.List(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: list %s: %w", dir, err)
	}
	var segs []uint64
	for _, name := range names {
		if n, ok := parseSegmentName(name); ok {
			segs = append(segs, n)
		}
	}
	// Names sort numerically only while they have the same width, and
	// widths grow past 999999, so sort by number.
	slices.Sort(segs)
	return segs, nil
}

func appendRecord(dst []byte, r Record) []byte {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0) // CRC, filled in below
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(r.Payload)))
	dst = append(dst, r.Type)
	dst = append(dst, r.Payload...)
	binary.LittleEndian.PutUint32(dst[start:], crc32.Checksum(dst[start+4:], castagnoli))
	return dst
}

// Writer appends records to the newest segment of a log. It is safe for
// concurrent use, though the engine has a single writer at a time.
type Writer struct {
	fs  vfs.FS
	dir string

	mu     sync.Mutex
	seg    uint64
	f      vfs.File
	size   int64
	buf    []byte
	err    error // sticky: after a failed write or sync the file's state is unknown
	closed bool
}

// NewWriter creates segment seg in dir and returns a Writer appending to
// it. seg must be newer than every existing segment.
func NewWriter(fs vfs.FS, dir string, seg uint64) (*Writer, error) {
	w := &Writer{fs: fs, dir: dir}
	if err := w.openSegment(seg); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) openSegment(seg uint64) error {
	path := SegmentPath(w.dir, seg)
	f, err := w.fs.Create(path)
	if err != nil {
		return fmt.Errorf("wal: create %s: %w", path, err)
	}
	// Fsync the directory so the new segment's entry is durable before any
	// record in it is acknowledged. Without it a power loss could remove the
	// whole file, synced records included.
	if err := w.fs.SyncDir(w.dir); err != nil {
		_ = f.Close() // the sync error is the one worth reporting
		return fmt.Errorf("wal: sync directory %s: %w", w.dir, err)
	}
	w.seg, w.f, w.size = seg, f, 0
	return nil
}

func (w *Writer) usable() error {
	if w.closed {
		return ErrClosed
	}
	return w.err
}

// Append writes recs to the current segment with one write call. They are
// not durable until Sync returns. If Append fails, the Writer is unusable:
// a partial write may be in the file.
func (w *Writer) Append(recs ...Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usable(); err != nil {
		return err
	}
	w.buf = w.buf[:0]
	for _, r := range recs {
		if r.Type == 0 {
			return errors.New("wal: record type 0 is reserved")
		}
		if len(r.Payload) > MaxPayload {
			return fmt.Errorf("wal: record payload of %d bytes exceeds %d", len(r.Payload), MaxPayload)
		}
		w.buf = appendRecord(w.buf, r)
	}
	if len(w.buf) == 0 {
		return nil
	}
	if _, err := w.f.Write(w.buf); err != nil {
		w.err = fmt.Errorf("wal: write %s: %w", SegmentPath(w.dir, w.seg), err)
		return w.err
	}
	w.size += int64(len(w.buf))
	if cap(w.buf) > 4<<20 {
		w.buf = nil // do not pin a large buffer after one big batch
	}
	return nil
}

// Sync makes every appended record durable. A failed Sync makes the Writer
// unusable: after a failed fsync the kernel may have dropped the dirty
// pages, so retrying could report success for data that is gone.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usable(); err != nil {
		return err
	}
	// Fsync the segment: callers acknowledge records only after this.
	if err := w.f.Sync(); err != nil {
		w.err = fmt.Errorf("wal: sync %s: %w", SegmentPath(w.dir, w.seg), err)
		return w.err
	}
	return nil
}

// Rotate makes the current segment durable and closed and starts the next
// one, returning its number.
func (w *Writer) Rotate() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usable(); err != nil {
		return 0, err
	}
	// Fsync the old segment before anything goes into the new one, so a
	// torn write can only ever be at the end of the newest segment. Recover
	// treats damage anywhere else as corruption.
	if err := w.f.Sync(); err != nil {
		w.err = fmt.Errorf("wal: sync %s: %w", SegmentPath(w.dir, w.seg), err)
		return 0, w.err
	}
	if err := w.f.Close(); err != nil {
		w.err = fmt.Errorf("wal: close %s: %w", SegmentPath(w.dir, w.seg), err)
		return 0, w.err
	}
	if err := w.openSegment(w.seg + 1); err != nil {
		w.err = err
		return 0, err
	}
	return w.seg, nil
}

// Segment returns the number of the segment being written.
func (w *Writer) Segment() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seg
}

// Size returns the number of bytes written to the current segment.
func (w *Writer) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// Close syncs and closes the current segment. Later calls return ErrClosed.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	w.closed = true
	var syncErr error
	if w.err == nil {
		// Fsync on close so a clean shutdown is durable under every sync
		// policy, including the ones that do not sync each write.
		syncErr = w.f.Sync()
	}
	closeErr := w.f.Close()
	if w.err != nil {
		return w.err
	}
	if syncErr != nil {
		return fmt.Errorf("wal: sync %s: %w", SegmentPath(w.dir, w.seg), syncErr)
	}
	return closeErr
}
