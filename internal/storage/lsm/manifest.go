package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

// The manifest names the live tables and how much of the log they cover.
// It is rewritten in full on every change: written to MANIFEST.tmp, fsynced,
// renamed over MANIFEST, and the directory fsynced (ADR 0006).
//
//	manifest := magic(8) | body | crc32c(4)
//	body     := nextFileNum | logNum | flushedSeq | numTables | table*   (uvarints)
//	table    := fileNum | size | minSeq | maxSeq | entries | deletions |
//	            len | smallest | len | largest                        (uvarints, bytes)
//
// Tables are listed newest first; their sequence ranges do not overlap.
const (
	manifestName    = "MANIFEST"
	manifestTmpName = "MANIFEST.tmp"
	manifestMagic   = "DKVMANI1"
	tableSuffix     = ".sst"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type tableMeta struct {
	fileNum           uint64
	size              uint64
	minSeq, maxSeq    uint64
	entries           uint64
	deletions         uint64
	smallest, largest []byte // user keys
}

type manifest struct {
	nextFileNum uint64 // next number for a table file
	logNum      uint64 // log segments below this are obsolete
	flushedSeq  uint64 // every batch with seq <= flushedSeq is in the tables
	tables      []tableMeta
}

func tablePath(dir string, n uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", n, tableSuffix))
}

func parseTableName(name string) (uint64, bool) {
	digits, ok := strings.CutSuffix(name, tableSuffix)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || filepath.Base(tablePath("", n)) != name {
		return 0, false
	}
	return n, true
}

func (m *manifest) encode() []byte {
	b := []byte(manifestMagic)
	for _, v := range []uint64{m.nextFileNum, m.logNum, m.flushedSeq, uint64(len(m.tables))} {
		b = binary.AppendUvarint(b, v)
	}
	for _, t := range m.tables {
		for _, v := range []uint64{t.fileNum, t.size, t.minSeq, t.maxSeq, t.entries, t.deletions} {
			b = binary.AppendUvarint(b, v)
		}
		b = binary.AppendUvarint(b, uint64(len(t.smallest)))
		b = append(b, t.smallest...)
		b = binary.AppendUvarint(b, uint64(len(t.largest)))
		b = append(b, t.largest...)
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crcTable))
}

func decodeManifest(b []byte) (manifest, error) {
	var m manifest
	if len(b) < len(manifestMagic)+4 || string(b[:len(manifestMagic)]) != manifestMagic {
		return m, errors.New("bad magic")
	}
	n := len(b) - 4
	if crc32.Checksum(b[:n], crcTable) != binary.LittleEndian.Uint32(b[n:]) {
		return m, errors.New("checksum mismatch")
	}
	p := b[len(manifestMagic):n]
	uv := func() (uint64, error) {
		v, k := binary.Uvarint(p)
		if k <= 0 {
			return 0, errors.New("truncated")
		}
		p = p[k:]
		return v, nil
	}
	bs := func() ([]byte, error) {
		l, err := uv()
		if err != nil || l > uint64(len(p)) {
			return nil, errors.New("bad key length")
		}
		out := append([]byte(nil), p[:l]...)
		p = p[l:]
		return out, nil
	}
	var count uint64
	for _, v := range []*uint64{&m.nextFileNum, &m.logNum, &m.flushedSeq, &count} {
		var err error
		if *v, err = uv(); err != nil {
			return m, err
		}
	}
	if count > uint64(len(p)) {
		return m, errors.New("table count exceeds the manifest size")
	}
	for range count {
		var t tableMeta
		for _, v := range []*uint64{&t.fileNum, &t.size, &t.minSeq, &t.maxSeq, &t.entries, &t.deletions} {
			var err error
			if *v, err = uv(); err != nil {
				return m, err
			}
		}
		var err error
		if t.smallest, err = bs(); err != nil {
			return m, err
		}
		if t.largest, err = bs(); err != nil {
			return m, err
		}
		m.tables = append(m.tables, t)
	}
	if len(p) != 0 {
		return m, fmt.Errorf("%d trailing bytes", len(p))
	}
	return m, nil
}

// readManifest returns the manifest in dir; exists is false if there is
// none (a new engine, or one written before Phase 3).
func readManifest(fsys vfs.FS, dir string) (m manifest, exists bool, err error) {
	path := filepath.Join(dir, manifestName)
	f, err := fsys.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return manifest{}, false, nil
	}
	if err != nil {
		return manifest{}, false, fmt.Errorf("lsm: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }() // read-only
	data, err := io.ReadAll(f)
	if err != nil {
		return manifest{}, false, fmt.Errorf("lsm: read %s: %w", path, err)
	}
	m, err = decodeManifest(data)
	if err != nil {
		return manifest{}, false, fmt.Errorf("%w: %s: %w", ErrCorruption, path, err)
	}
	return m, true, nil
}

// writeManifest replaces the manifest in dir atomically and durably.
func writeManifest(fsys vfs.FS, dir string, m *manifest) error {
	tmp := filepath.Join(dir, manifestTmpName)
	f, err := fsys.Create(tmp)
	if err != nil {
		return fmt.Errorf("lsm: create %s: %w", tmp, err)
	}
	if _, err := f.Write(m.encode()); err != nil {
		_ = f.Close() // the write error is the one worth reporting
		return fmt.Errorf("lsm: write %s: %w", tmp, err)
	}
	// Fsync the new manifest before the rename publishes it; otherwise a
	// crash could leave MANIFEST naming an empty or partial file.
	if err := f.Sync(); err != nil {
		_ = f.Close() // the sync error is the one worth reporting
		return fmt.Errorf("lsm: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("lsm: close %s: %w", tmp, err)
	}
	if err := fsys.Rename(tmp, filepath.Join(dir, manifestName)); err != nil {
		return fmt.Errorf("lsm: install manifest: %w", err)
	}
	// Fsync the directory so the rename is durable. This also makes durable
	// the entries of any table files created before it, which the manifest
	// may now name.
	if err := fsys.SyncDir(dir); err != nil {
		return fmt.Errorf("lsm: sync directory %s: %w", dir, err)
	}
	return nil
}
