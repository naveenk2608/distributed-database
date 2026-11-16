// Package crash is the kill -9 harness for a single storage engine: a child
// process writes with SyncAlways and reports each write before and after it
// is acknowledged, the parent kills it with SIGKILL at a random moment,
// reopens the engine and checks every key it was told about.
package crash

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
)

var (
	cycles   = flag.Int("cycles", 0, "number of kill -9 cycles (0 = 100, or 20 under -race; 10 with -short)")
	seedFlag = flag.Uint64("seed", 0, "seed for kill timing (0 = derive from the clock)")
)

// The child is this test binary started with these variables set.
const (
	childDirEnv   = "CRASHTEST_CHILD_DIR"
	childCycleEnv = "CRASHTEST_CHILD_CYCLE"
	childWriters  = 4
)

func TestMain(m *testing.M) {
	if dir := os.Getenv(childDirEnv); dir != "" {
		os.Exit(runChild(dir, os.Getenv(childCycleEnv)))
	}
	goleak.VerifyTestMain(m)
}

func valueFor(key string, n int) []byte {
	return []byte(key + "=" + strings.Repeat("v", n))
}

// runChild writes until it is killed. Each line it prints is one write
// call on a pipe, which is atomic for lines this short, so the parent never
// sees half a line.
func runChild(dir, cycle string) int {
	db, err := lsm.Open(dir, lsm.Options{Sync: lsm.SyncAlways, MemtableBytes: 64 << 10})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: open: %v\n", err)
		return 3
	}
	var outMu sync.Mutex
	say := func(format string, args ...any) {
		line := fmt.Sprintf(format+"\n", args...)
		outMu.Lock()
		defer outMu.Unlock()
		if _, err := os.Stdout.WriteString(line); err != nil {
			os.Exit(4) // the parent is gone
		}
	}
	say("ready")
	var wg sync.WaitGroup
	for g := range childWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(g)))
			var live []string
			for i := 0; ; i++ {
				var b lsm.Batch
				if len(live) > 0 && r.IntN(5) == 0 {
					j := r.IntN(len(live))
					key := live[j]
					live = append(live[:j], live[j+1:]...)
					say("try del %s", key)
					b.Delete([]byte(key))
					seq, err := db.Apply(&b, 0)
					if err != nil {
						fmt.Fprintf(os.Stderr, "child: apply: %v\n", err)
						os.Exit(5)
					}
					say("ack del %s %d", key, seq)
					continue
				}
				key := fmt.Sprintf("c%s-g%d-%06d", cycle, g, i)
				n := r.IntN(300)
				say("try put %s %d", key, n)
				b.Set([]byte(key), valueFor(key, n))
				seq, err := db.Apply(&b, 0)
				if err != nil {
					fmt.Fprintf(os.Stderr, "child: apply: %v\n", err)
					os.Exit(5)
				}
				say("ack put %s %d", key, seq)
				live = append(live, key)
			}
		}()
	}
	wg.Wait() // never returns: the writers loop until the process is killed
	return 0
}

// keyState is what the parent knows about one key.
type keyState struct {
	n        int    // value length of the put
	putSeq   uint64 // version of the acknowledged put; 0 if not acknowledged
	putTried bool
	delTried bool
	delAcked bool
}

type model struct {
	keys  map[string]*keyState
	acked int
}

func (m *model) apply(line string) error {
	f := strings.Fields(line)
	switch {
	case len(f) == 1 && f[0] == "ready":
		return nil
	case len(f) == 4 && f[0] == "try" && f[1] == "put":
		n, err := strconv.Atoi(f[3])
		if err != nil {
			return err
		}
		m.keys[f[2]] = &keyState{n: n, putTried: true}
	case len(f) == 4 && f[0] == "ack" && f[1] == "put":
		seq, err := strconv.ParseUint(f[3], 10, 64)
		if err != nil {
			return err
		}
		m.keys[f[2]].putSeq = seq
		m.acked++
	case len(f) == 3 && f[0] == "try" && f[1] == "del":
		m.keys[f[2]].delTried = true
	case len(f) == 4 && f[0] == "ack" && f[1] == "del":
		m.keys[f[2]].delAcked = true
		m.acked++
	default:
		return fmt.Errorf("bad line %q", line)
	}
	return nil
}

// verify opens the engine and checks every key, then resolves writes whose
// outcome was unknown to what recovery actually kept.
func (m *model) verify(t *testing.T, dir string, log *slog.Logger) {
	t.Helper()
	db, err := lsm.Open(dir, lsm.Options{Logger: log})
	if err != nil {
		t.Fatalf("reopen after kill -9: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	for key, ks := range m.keys {
		v, ver, err := db.Get([]byte(key), nil)
		present := err == nil
		if err != nil && !errors.Is(err, lsm.ErrNotFound) {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if present && !bytes.Equal(v, valueFor(key, ks.n)) {
			t.Fatalf("%s has a wrong value (%d bytes)", key, len(v))
		}
		switch {
		case ks.delAcked:
			if present {
				t.Fatalf("%s: acknowledged delete was lost", key)
			}
		case ks.putSeq != 0 && !ks.delTried:
			if !present || ver != ks.putSeq {
				t.Fatalf("%s: acknowledged put at version %d lost (present=%v version=%d)", key, ks.putSeq, present, ver)
			}
		case ks.putSeq != 0 && ks.delTried: // the delete was in flight: either outcome is fine
			if present && ver != ks.putSeq {
				t.Fatalf("%s: version %d, want %d", key, ver, ks.putSeq)
			}
			ks.delTried = false
			ks.delAcked = !present // adopt what recovery kept
		default: // the put was in flight: either outcome is fine
			if present {
				ks.putSeq = ver
			} else {
				delete(m.keys, key)
			}
		}
	}
	// No key may exist that the child never tried to write.
	it, err := db.NewIterator(lsm.IterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	live := 0
	for it.First(); it.Valid(); it.Next() {
		ks, ok := m.keys[string(it.Key())]
		if !ok || ks.delAcked {
			t.Fatalf("phantom key %q", it.Key())
		}
		live++
	}
	want := 0
	for _, ks := range m.keys {
		if !ks.delAcked {
			want++
		}
	}
	if live != want {
		t.Fatalf("scan found %d live keys, model has %d", live, want)
	}
}

type tornCounter struct {
	mu sync.Mutex
	n  int
}

func (c *tornCounter) Enabled(context.Context, slog.Level) bool { return true }
func (c *tornCounter) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "torn write") {
		c.mu.Lock()
		c.n++
		c.mu.Unlock()
	}
	return nil
}
func (c *tornCounter) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *tornCounter) WithGroup(string) slog.Handler      { return c }

// TestKill9Cycles is the Phase 2 gate check for real process crashes: every
// cycle starts a writer process, kills it with SIGKILL after a random delay
// (sometimes before it has finished recovering), and checks the recovered
// engine against what the writer reported. The directory and the model carry
// over between cycles, so recovery runs over the whole accumulated log.
func TestKill9Cycles(t *testing.T) {
	n := *cycles
	switch {
	case n > 0:
	case testing.Short():
		n = 10
	case raceBuild:
		n = 20
	default:
		n = 100
	}
	seed := *seedFlag
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	t.Logf("seed %d (replay kill timing with -seed=%d), %d cycles", seed, seed, n)
	r := rand.New(rand.NewPCG(seed, 0))
	dir := t.TempDir()
	m := &model{keys: map[string]*keyState{}}
	torn := &tornCounter{}
	log := slog.New(torn)
	killedBeforeReady := 0

	for cycle := range n {
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), childDirEnv+"="+dir, childCycleEnv+"="+strconv.Itoa(cycle))
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		lines := make(chan string, 1024)
		readDone := make(chan error, 1)
		go func() {
			defer close(lines)
			sc := bufio.NewScanner(stdout)
			for sc.Scan() {
				lines <- sc.Text()
			}
			readDone <- sc.Err()
		}()

		ready := false
		var killAt <-chan time.Time
		if r.IntN(4) == 0 {
			// Kill while the child may still be starting or recovering.
			killAt = time.After(time.Duration(r.IntN(20000)) * time.Microsecond)
		}
	read:
		for {
			select {
			case line, ok := <-lines:
				if !ok {
					break read
				}
				if err := m.apply(line); err != nil {
					t.Fatal(err)
				}
				if line == "ready" {
					ready = true
					if killAt == nil {
						killAt = time.After(time.Duration(1+r.IntN(40)) * time.Millisecond)
					}
				}
			case <-killAt:
				if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
					t.Fatal(err)
				}
				killAt = nil
			}
		}
		if err := <-readDone; err != nil {
			t.Fatal(err)
		}
		err = cmd.Wait()
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Fatalf("cycle %d: child exited with %v, not SIGKILL\n%s", cycle, err, stderr.String())
		}
		if !ready {
			killedBeforeReady++
		}
		m.verify(t, dir, log)
	}
	torn.mu.Lock()
	defer torn.mu.Unlock()
	t.Logf("%d kill -9 cycles: %d acknowledged writes, %d keys tracked, all present; %d kills during recovery; %d torn tails truncated",
		n, m.acked, len(m.keys), killedBeforeReady, torn.n)
}
