package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

// RecoverResult reports what Recover found.
type RecoverResult struct {
	// Segments lists the segment numbers in the log, ascending.
	Segments []uint64
	// Records is the number of records passed to the callback.
	Records int
	// TornBytes is the number of bytes truncated from the end of the newest
	// segment, at TornOffset; 0 if the log ended cleanly.
	TornBytes  int64
	TornOffset int64
	TornReason string
}

// Recover reads every segment in dir in order and calls fn for each record.
// The record's payload is only valid during the call.
//
// Damage in the newest segment is a torn write from a crash if no valid
// record follows it: Recover truncates the segment there, fsyncs it, and
// reports the bytes dropped. Damage anywhere else, or damage followed by a
// valid record, cannot be explained by a crash, because older segments are
// synced before the next one is started and a torn write only damages the
// end of what was written. Recover then returns a *CorruptionError and
// changes nothing.
//
// If fn returns an error, Recover stops and returns it, annotated with the
// record's position.
func Recover(fs vfs.FS, dir string, fn func(seg uint64, r Record) error) (RecoverResult, error) {
	var res RecoverResult
	segs, err := ListSegments(fs, dir)
	if err != nil {
		return res, err
	}
	res.Segments = segs
	for i, seg := range segs {
		path := SegmentPath(dir, seg)
		data, err := readFile(fs, path)
		if err != nil {
			return res, fmt.Errorf("wal: read %s: %w", path, err)
		}
		newest := i == len(segs)-1
		for off := 0; off < len(data); {
			rec, n, problem := decodeAt(data, off)
			if problem != "" {
				if newest && !validRecordAfter(data, off) {
					if err := truncate(fs, path, int64(off)); err != nil {
						return res, err
					}
					res.TornBytes = int64(len(data) - off)
					res.TornOffset = int64(off)
					res.TornReason = problem
					break
				}
				reason := problem
				if newest {
					reason += " (a valid record follows it, so it is not a torn write)"
				} else {
					reason += " (not in the newest segment, so it is not a torn write)"
				}
				return res, &CorruptionError{Path: path, Offset: int64(off), Reason: reason}
			}
			if err := fn(seg, rec); err != nil {
				return res, fmt.Errorf("wal: record in %s at offset %d: %w", path, off, err)
			}
			res.Records++
			off += n
		}
	}
	return res, nil
}

func readFile(fs vfs.FS, path string) ([]byte, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }() // read-only: nothing to lose on close
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err := f.ReadAt(data, 0); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return data, nil
}

func truncate(fs vfs.FS, path string, size int64) error {
	f, err := fs.OpenReadWrite(path)
	if err != nil {
		return fmt.Errorf("wal: open %s to truncate torn tail: %w", path, err)
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close() // the truncate error is the one worth reporting
		return fmt.Errorf("wal: truncate torn tail of %s: %w", path, err)
	}
	// Fsync the truncation. Otherwise the garbage could reappear after
	// another crash, by which time newer segments exist and recovery would
	// have to call it corruption.
	if err := f.Sync(); err != nil {
		_ = f.Close() // the sync error is the one worth reporting
		return fmt.Errorf("wal: sync truncated %s: %w", path, err)
	}
	return f.Close()
}

// decodeAt decodes the record at data[off:]. It returns the record and its
// encoded size, or a description of the problem.
func decodeAt(data []byte, off int) (Record, int, string) {
	rest := data[off:]
	if len(rest) < headerSize {
		return Record{}, 0, fmt.Sprintf("truncated record header (%d bytes)", len(rest))
	}
	length := binary.LittleEndian.Uint32(rest[4:8])
	if uint64(length) > MaxPayload {
		return Record{}, 0, fmt.Sprintf("record length %d exceeds the maximum %d", length, MaxPayload)
	}
	end := headerSize + int(length)
	if end > len(rest) {
		return Record{}, 0, fmt.Sprintf("record of %d bytes extends past the end of the segment", end)
	}
	if crc32.Checksum(rest[4:end], castagnoli) != binary.LittleEndian.Uint32(rest[0:4]) {
		return Record{}, 0, "checksum mismatch"
	}
	if rest[8] == 0 {
		return Record{}, 0, "record type 0 is reserved"
	}
	return Record{Type: rest[8], Payload: rest[headerSize:end]}, end, ""
}

// validRecordAfter reports whether a valid record starts anywhere after off.
// The cheap checks reject almost every offset before a CRC is computed.
func validRecordAfter(data []byte, off int) bool {
	for p := off + 1; p+headerSize <= len(data); p++ {
		if data[p+8] == 0 {
			continue
		}
		length := binary.LittleEndian.Uint32(data[p+4 : p+8])
		end := uint64(p) + headerSize + uint64(length)
		if end > uint64(len(data)) {
			continue
		}
		if crc32.Checksum(data[p+4:end], castagnoli) == binary.LittleEndian.Uint32(data[p:p+4]) {
			return true
		}
	}
	return false
}
