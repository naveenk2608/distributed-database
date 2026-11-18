package kvstore

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// open returns a Store over an engine on an in-memory filesystem whose
// fsyncs take delay, with every write synced; syncs counts the fsyncs.
func open(t *testing.T, delay time.Duration, syncs *atomic.Int64) *Store {
	t.Helper()
	fsys := vfs.CountSyncs(vfs.Slow(vfs.NewMemFS(), delay), syncs)
	db, err := lsm.Open("db", lsm.Options{FS: fsys, Sync: lsm.SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return New(db)
}

// TestConcurrentPutsShareFsyncs: Puts from many clients at once are
// committed in groups, one fsync per group, not one per Put.
func TestConcurrentPutsShareFsyncs(t *testing.T) {
	var syncs atomic.Int64
	s := open(t, 2*time.Millisecond, &syncs)
	const workers, each = 32, 10
	before := syncs.Load()
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				key := fmt.Sprintf("w%02d/%02d", w, i)
				if _, err := s.Put(context.Background(), &dkvv1.PutRequest{Key: []byte(key), Value: []byte(key)}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	n := syncs.Load() - before
	t.Logf("%d puts from %d clients took %d fsyncs", workers*each, workers, n)
	if n > workers*each/4 {
		t.Fatalf("%d puts from %d clients took %d fsyncs: they were not committed together", workers*each, workers, n)
	}
}

// TestCompareAndSetIsAtomicWithPuts: while Puts to the same key run
// concurrently, a CompareAndSet that succeeds wrote over exactly the
// version it expected: no other write to the key falls between the two.
func TestCompareAndSetIsAtomicWithPuts(t *testing.T) {
	var syncs atomic.Int64
	s := open(t, time.Millisecond, &syncs)
	key := []byte("contended")
	var mu sync.Mutex
	var writes []uint64 // versions of every write to key
	type swap struct{ expected, version uint64 }
	var swaps []swap
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// Pauses leave CompareAndSet room to succeed; the Puts
				// still overlap its check and write often.
				select {
				case <-stop:
					return
				case <-time.After(time.Duration(rand.IntN(2000)) * time.Microsecond):
				}
				resp, err := s.Put(context.Background(), &dkvv1.PutRequest{Key: key, Value: []byte("put")})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				writes = append(writes, resp.GetVersion())
				mu.Unlock()
			}
		}()
	}
	var cas sync.WaitGroup
	for range 4 {
		cas.Add(1)
		go func() {
			defer cas.Done()
			for range 50 {
				got, err := s.Get(context.Background(), &dkvv1.GetRequest{Key: key})
				if err != nil {
					t.Error(err)
					return
				}
				resp, err := s.CompareAndSet(context.Background(), &dkvv1.CompareAndSetRequest{Key: key, Value: []byte("cas"),
					ExpectedVersion: got.GetVersion()})
				if err != nil {
					t.Error(err)
					return
				}
				if resp.GetSucceeded() {
					mu.Lock()
					writes = append(writes, resp.GetVersion())
					swaps = append(swaps, swap{got.GetVersion(), resp.GetVersion()})
					mu.Unlock()
				}
			}
		}()
	}
	cas.Wait()
	close(stop)
	wg.Wait()

	slices.Sort(writes)
	for _, sw := range swaps {
		i, _ := slices.BinarySearch(writes, sw.version)
		prev := uint64(0) // the key had no value
		if i > 0 {
			prev = writes[i-1]
		}
		if prev != sw.expected {
			t.Fatalf("a CompareAndSet expecting version %d wrote version %d over version %d", sw.expected, sw.version, prev)
		}
	}
	t.Logf("%d writes, %d successful CompareAndSets", len(writes), len(swaps))
	if len(swaps) == 0 {
		t.Fatal("no CompareAndSet succeeded: the test checked nothing")
	}
}
