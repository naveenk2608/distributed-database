package lsm

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

func applySeq(t *testing.T, d *DB, from, to uint64) {
	t.Helper()
	for s := from; s <= to; s++ {
		var b Batch
		b.Set(fmt.Appendf(nil, "k%04d", s), fmt.Appendf(nil, "v%d", s))
		if _, err := d.Apply(&b, s); err != nil {
			t.Fatal(err)
		}
	}
}

func logFiles(t *testing.T, fs vfs.FS, dir string) []string {
	t.Helper()
	names, err := fs.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, n := range names {
		if strings.HasSuffix(n, ".wal") {
			out = append(out, n)
		}
	}
	return out
}

// TestDisableWALKeepsOnlyFlushedWrites: without a log, what was flushed
// survives a close and a power loss, what was not is gone, the engine
// writes no log segment, and it reopens at the last flushed sequence
// number, ready for its owner to replay the rest.
func TestDisableWALKeepsOnlyFlushedWrites(t *testing.T) {
	for _, crash := range []bool{false, true} {
		t.Run(fmt.Sprintf("crash=%v", crash), func(t *testing.T) {
			fs := vfs.NewMemFS()
			opts := Options{FS: fs, DisableWAL: true}
			d, err := Open("/db", opts)
			if err != nil {
				t.Fatal(err)
			}
			applySeq(t, d, 1, 100)
			if err := d.Flush(); err != nil {
				t.Fatal(err)
			}
			applySeq(t, d, 101, 150)
			if logs := logFiles(t, fs, "/db"); len(logs) != 0 {
				t.Fatalf("log segments %v with DisableWAL", logs)
			}
			reopenFS := fs
			if crash {
				reopenFS = fs.CrashImage(nil) // only what was made durable
				_ = d.Close()
			} else if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			opts.FS = reopenFS
			d, err = Open("/db", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = d.Close() }()
			if got := d.LastSeq(); got != 100 {
				t.Fatalf("reopened at sequence %d; want 100, the last flushed", got)
			}
			if v, ver, err := d.Get([]byte("k0100"), nil); err != nil || string(v) != "v100" || ver != 100 {
				t.Fatalf("flushed k0100: %q@%d %v", v, ver, err)
			}
			if _, _, err := d.Get([]byte("k0101"), nil); !errors.Is(err, ErrNotFound) {
				t.Fatalf("unflushed k0101: %v; want it gone", err)
			}
			applySeq(t, d, 101, 101) // the owner replays from where the engine is
		})
	}
}

// TestSwitchingOffTheWALKeepsLoggedWrites: an engine that ran with a log
// and reopens without one replays and flushes the old log at Open, keeps
// every write it had acknowledged, and deletes the old segments.
func TestSwitchingOffTheWALKeepsLoggedWrites(t *testing.T) {
	fs := vfs.NewMemFS()
	d, err := Open("/db", Options{FS: fs, Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	applySeq(t, d, 1, 50)
	if len(logFiles(t, fs, "/db")) == 0 {
		t.Fatal("setup: no log segment with the log on")
	}
	crashed := fs.CrashImage(nil) // a power loss: only the synced log
	_ = d.Close()
	d, err = Open("/db", Options{FS: crashed, DisableWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if got := d.LastSeq(); got != 50 {
		t.Fatalf("reopened without a log at %d; want 50", got)
	}
	if v, _, err := d.Get([]byte("k0050"), nil); err != nil || string(v) != "v50" {
		t.Fatalf("k0050 after switching the log off: %q %v", v, err)
	}
	if logs := logFiles(t, crashed, "/db"); len(logs) != 0 {
		t.Fatalf("old log segments %v left after their writes were flushed", logs)
	}
}
