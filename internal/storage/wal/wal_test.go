package wal

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

const dir = "/log"

func newFS(t *testing.T) *vfs.MemFS {
	t.Helper()
	fs := vfs.NewMemFS()
	if err := fs.MkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	return fs
}

func rec(i int) Record {
	return Record{Type: uint8(1 + i%3), Payload: []byte(fmt.Sprintf("payload-%d-%s", i, bytes.Repeat([]byte{'x'}, i%17)))}
}

type got struct {
	seg uint64
	r   Record
}

func recoverAll(t *testing.T, fs vfs.FS) ([]got, RecoverResult, error) {
	t.Helper()
	var out []got
	res, err := Recover(fs, dir, func(seg uint64, r Record) error {
		out = append(out, got{seg, Record{r.Type, bytes.Clone(r.Payload)}})
		return nil
	})
	return out, res, err
}

func mustWriter(t *testing.T, fs vfs.FS, seg uint64) *Writer {
	t.Helper()
	w, err := NewWriter(fs, dir, seg)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestRoundTripAcrossSegments(t *testing.T) {
	fs := newFS(t)
	w := mustWriter(t, fs, 7)
	var want []got
	for i := range 50 {
		if i%12 == 11 {
			if _, err := w.Rotate(); err != nil {
				t.Fatal(err)
			}
		}
		group := []Record{rec(i), rec(i + 1000)}
		if err := w.Append(group...); err != nil {
			t.Fatal(err)
		}
		for _, r := range group {
			want = append(want, got{w.Segment(), r})
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("second Close: %v", err)
	}
	out, res, err := recoverAll(t, fs)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(want) || res.Records != len(want) || res.TornBytes != 0 {
		t.Fatalf("recovered %d records (%+v), want %d", len(out), res, len(want))
	}
	for i := range want {
		if out[i].seg != want[i].seg || out[i].r.Type != want[i].r.Type || !bytes.Equal(out[i].r.Payload, want[i].r.Payload) {
			t.Fatalf("record %d = %+v, want %+v", i, out[i], want[i])
		}
	}
	if fmt.Sprint(res.Segments) != "[7 8 9 10 11]" {
		t.Errorf("segments %v", res.Segments)
	}
}

func TestSegmentNames(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    uint64
		ok   bool
	}{
		{"000001.wal", 1, true},
		{"1234567.wal", 1234567, true},
		{"1.wal", 0, false},
		{"000001.sst", 0, false},
		{"00000x.wal", 0, false},
		{"0000001.wal", 0, false},
	} {
		n, ok := parseSegmentName(tc.name)
		if n != tc.n || ok != tc.ok {
			t.Errorf("parse %q = %d,%v; want %d,%v", tc.name, n, ok, tc.n, tc.ok)
		}
	}
}

func TestReservedTypeAndEmptyAppend(t *testing.T) {
	fs := newFS(t)
	w := mustWriter(t, fs, 1)
	defer func() { _ = w.Close() }()
	if err := w.Append(Record{Type: 0}); err == nil {
		t.Error("type 0 accepted")
	}
	if err := w.Append(); err != nil || w.Size() != 0 {
		t.Errorf("empty Append: %v, size %d", err, w.Size())
	}
}

// TestPowerLossKeepsSyncedRecords crashes after a mix of synced and unsynced
// appends, many times, with torn tails. Recovery must return every synced
// record, then a prefix of the unsynced ones, and never report corruption.
func TestPowerLossKeepsSyncedRecords(t *testing.T) {
	for seed := range uint64(400) {
		r := rand.New(rand.NewPCG(seed, 1))
		fs := newFS(t)
		w := mustWriter(t, fs, 1)
		var written []Record
		synced := 0
		for i := range 5 + r.IntN(40) {
			if r.IntN(10) == 0 {
				if _, err := w.Rotate(); err != nil {
					t.Fatal(err)
				}
				synced = len(written) // rotation syncs the old segment
			}
			group := []Record{rec(i)}
			for r.IntN(2) == 0 {
				group = append(group, rec(i*100+len(group)))
			}
			if err := w.Append(group...); err != nil {
				t.Fatal(err)
			}
			written = append(written, group...)
			if r.IntN(3) == 0 {
				if err := w.Sync(); err != nil {
					t.Fatal(err)
				}
				synced = len(written)
			}
		}
		img := fs.CrashImage(rand.New(rand.NewPCG(seed, 2)))
		out, res, err := recoverAll(t, img)
		if err != nil {
			t.Fatalf("seed %d: recovery after power loss: %v", seed, err)
		}
		if len(out) < synced || len(out) > len(written) {
			t.Fatalf("seed %d: recovered %d records; %d synced, %d written", seed, len(out), synced, len(written))
		}
		for i := range out {
			if !bytes.Equal(out[i].r.Payload, written[i].Payload) {
				t.Fatalf("seed %d: record %d differs: recovered log is not a prefix of what was written", seed, i)
			}
		}
		// The truncation must itself be durable: a second crash right away
		// must find a clean log with the same records.
		out2, res2, err := recoverAll(t, img.CrashImage(nil))
		if err != nil || res2.TornBytes != 0 || len(out2) != len(out) {
			t.Fatalf("seed %d: second recovery: %d records, torn %d, %v (first: %d, torn %d)",
				seed, len(out2), res2.TornBytes, err, len(out), res.TornBytes)
		}
	}
}

// writeLog writes n records in segments of per records each, synced, and
// returns the offsets of each record within its segment.
func writeLog(t *testing.T, fs vfs.FS, n, per int) map[uint64][]int64 {
	t.Helper()
	w := mustWriter(t, fs, 1)
	offsets := map[uint64][]int64{}
	for i := range n {
		if i > 0 && i%per == 0 {
			if _, err := w.Rotate(); err != nil {
				t.Fatal(err)
			}
		}
		offsets[w.Segment()] = append(offsets[w.Segment()], w.Size())
		if err := w.Append(rec(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return offsets
}

func corruptByte(t *testing.T, fs *vfs.MemFS, seg uint64, off int64) {
	t.Helper()
	path := SegmentPath(dir, seg)
	data, err := fs.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[off] ^= 0x40
	if err := fs.WriteFile(path, data); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptionIsReportedNotRepaired(t *testing.T) {
	for _, tc := range []struct {
		name    string
		seg     uint64
		record  int   // index of the record within the segment; -1 = last
		byteOff int64 // offset within the record
	}{
		{"payload in the middle of the newest segment", 3, 1, headerSize + 2},
		{"length field in the middle of the newest segment", 3, 0, 5},
		{"CRC in the middle of the newest segment", 3, 2, 0},
		{"last record of an older segment", 1, -1, headerSize},
		{"first record of an older segment", 2, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFS(t)
			offsets := writeLog(t, fs, 12, 4) // segments 1, 2, 3 with 4 records each
			i := tc.record
			if i < 0 {
				i = len(offsets[tc.seg]) - 1
			}
			off := offsets[tc.seg][i]
			corruptByte(t, fs, tc.seg, off+tc.byteOff)
			before, _ := fs.ReadFile(SegmentPath(dir, tc.seg))

			_, _, err := recoverAll(t, fs)
			var ce *CorruptionError
			if !errors.Is(err, ErrCorrupt) || !errors.As(err, &ce) {
				t.Fatalf("Recover = %v; want a CorruptionError", err)
			}
			if ce.Path != SegmentPath(dir, tc.seg) || ce.Offset != off {
				t.Errorf("error points at %s:%d, want %s:%d", ce.Path, ce.Offset, SegmentPath(dir, tc.seg), off)
			}
			t.Logf("error: %v", err)
			after, _ := fs.ReadFile(SegmentPath(dir, tc.seg))
			if !bytes.Equal(before, after) {
				t.Error("Recover modified a corrupt log")
			}
		})
	}
}

// TestDamagedLastRecordIsATornTail documents the rule's blind spot: damage
// to the very last record of the newest segment is indistinguishable from a
// torn write, so that record is dropped.
func TestDamagedLastRecordIsATornTail(t *testing.T) {
	fs := newFS(t)
	offsets := writeLog(t, fs, 8, 4)
	last := offsets[2][3]
	corruptByte(t, fs, 2, last+headerSize+1)
	out, res, err := recoverAll(t, fs)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 7 || res.TornOffset != last || res.TornBytes == 0 {
		t.Fatalf("recovered %d records, torn at %d (%d bytes); want 7 records torn at %d", len(out), res.TornOffset, res.TornBytes, last)
	}
}

func TestZeroFilledTailIsTorn(t *testing.T) {
	fs := newFS(t)
	writeLog(t, fs, 3, 10)
	path := SegmentPath(dir, 1)
	data, _ := fs.ReadFile(path)
	if err := fs.WriteFile(path, append(data, make([]byte, 4096)...)); err != nil {
		t.Fatal(err)
	}
	out, res, err := recoverAll(t, fs)
	if err != nil || len(out) != 3 || res.TornBytes != 4096 {
		t.Fatalf("got %d records, torn %d, %v", len(out), res.TornBytes, err)
	}
}

func TestCallbackErrorStopsRecovery(t *testing.T) {
	fs := newFS(t)
	writeLog(t, fs, 5, 10)
	boom := errors.New("boom")
	n := 0
	_, err := Recover(fs, dir, func(uint64, Record) error {
		n++
		if n == 3 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) || n != 3 {
		t.Fatalf("err %v after %d records", err, n)
	}
}

func TestWriterErrorsAreSticky(t *testing.T) {
	fs := newFS(t)
	w := mustWriter(t, fs, 1)
	fs.CrashAfter(0)
	if err := w.Append(rec(1)); !errors.Is(err, vfs.ErrCrashed) {
		t.Fatalf("Append on crashed fs: %v", err)
	}
	if err := w.Sync(); !errors.Is(err, vfs.ErrCrashed) {
		t.Errorf("Sync after failed Append: %v, want the sticky error", err)
	}
	if _, err := w.Rotate(); !errors.Is(err, vfs.ErrCrashed) {
		t.Errorf("Rotate after failure: %v", err)
	}
	if err := w.Close(); !errors.Is(err, vfs.ErrCrashed) {
		t.Errorf("Close after failure: %v", err)
	}
}

func TestRealFilesystem(t *testing.T) {
	d := t.TempDir()
	w, err := NewWriter(vfs.Default, d, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		if err := w.Append(rec(i)); err != nil {
			t.Fatal(err)
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	n := 0
	res, err := Recover(vfs.Default, d, func(uint64, Record) error { n++; return nil })
	if err != nil || n != 10 || res.TornBytes != 0 {
		t.Fatalf("recovered %d, %+v, %v", n, res, err)
	}
	if filepath.Dir(SegmentPath(d, 1)) != d {
		t.Error("SegmentPath not inside dir")
	}
}

// FuzzRecover feeds arbitrary bytes to recovery as the only segment. It must
// never panic, and whatever it accepts must be stable on a second pass.
func FuzzRecover(f *testing.F) {
	var seed []byte
	for i := range 4 {
		seed = appendRecord(seed, rec(i))
	}
	f.Add(seed)
	f.Add(seed[:len(seed)-3])
	f.Add(append(bytes.Clone(seed), 0, 0, 0))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		fs := vfs.NewMemFS()
		_ = fs.MkdirAll(dir)
		f, _ := fs.Create(SegmentPath(dir, 1))
		_, _ = f.Write(data)
		_ = fs.SyncDir(dir)
		_ = f.Sync()
		n1 := 0
		res, err := Recover(fs, dir, func(uint64, Record) error { n1++; return nil })
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("unexpected error kind: %v", err)
			}
			return
		}
		n2 := 0
		res2, err := Recover(fs, dir, func(uint64, Record) error { n2++; return nil })
		if err != nil || n2 != n1 || res2.TornBytes != 0 {
			t.Fatalf("second pass: %d records (first %d, torn %d), torn %d, %v", n2, n1, res.TornBytes, res2.TornBytes, err)
		}
	})
}
