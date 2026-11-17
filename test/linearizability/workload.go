package linearizability

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/naveenk2608/distributed-database/pkg/client"
)

// Workload is a set of clients issuing random puts, gets, deletes and
// compare-and-sets on a few keys, recording every operation in History.
// Values are unique, so a read names the write it saw.
type Workload struct {
	// Addrs are the nodes' public API addresses.
	Addrs []string
	// Workers is the number of concurrent clients; Keys the number of keys
	// they share (few, so that operations collide).
	Workers, Keys int
	// Dup makes clients send about a third of their writes twice, as a
	// client that lost a response and retried would; the first answer is
	// discarded.
	Dup bool
	// OpTimeout bounds one operation, retries included; 0 means 1.5 s.
	OpTimeout time.Duration
	History   *History
}

func (wl *Workload) opTimeout() time.Duration {
	if wl.OpTimeout > 0 {
		return wl.OpTimeout
	}
	return 1500 * time.Millisecond
}

// Start starts the clients; they run until ctx ends. Call the returned
// function to wait for them.
func (wl *Workload) Start(ctx context.Context, seed uint64) (wait func(), err error) {
	var wg sync.WaitGroup
	var clients []*client.Client
	defer func() {
		if err != nil {
			for _, c := range clients {
				_ = c.Close()
			}
		}
	}()
	for id := range wl.Workers {
		rng := rand.New(rand.NewPCG(seed, uint64(id)))
		// Most clients stay on one node each, so every node, an isolated
		// old leader included, keeps serving some clients directly; the
		// others move between nodes as a normal client does.
		addrs := []string{wl.Addrs[id%len(wl.Addrs)]}
		if id >= len(wl.Addrs) {
			addrs = slices.Clone(wl.Addrs)
		}
		opts := client.Options{Addrs: addrs, MaxAttempts: 4, InitialBackoff: 20 * time.Millisecond, MaxBackoff: 200 * time.Millisecond}
		if wl.Dup {
			var mu sync.Mutex
			opts.DialOptions = append(opts.DialOptions, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any,
				cc *grpc.ClientConn, invoker grpc.UnaryInvoker, o ...grpc.CallOption) error {
				mu.Lock()
				twice := method != "/dkv.v1.KVService/Get" && rng.IntN(3) == 0
				mu.Unlock()
				if twice {
					_ = invoker(ctx, method, req, reply, cc, o...) // its answer is lost
				}
				return invoker(ctx, method, req, reply, cc, o...)
			}))
		}
		cl, err := client.NewWithOptions(opts)
		if err != nil {
			return nil, fmt.Errorf("client %d: %w", id, err)
		}
		clients = append(clients, cl)
	}
	for id, cl := range clients {
		rng := rand.New(rand.NewPCG(seed, uint64(1000+id)))
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { _ = cl.Close() }()
			seen := map[string]uint64{}
			for n := 0; ctx.Err() == nil; n++ {
				key := fmt.Sprintf("k%d", rng.IntN(wl.Keys))
				wl.do(ctx, cl, id, key, fmt.Sprintf("c%d-%d", id, n), rng, seen)
			}
		}()
	}
	return wg.Wait, nil
}

// do runs one random operation and records it. seen holds the latest
// version this client saw of each key, which its compare-and-sets expect.
func (wl *Workload) do(ctx context.Context, cl *client.Client, id int, key, value string, rng *rand.Rand, seen map[string]uint64) {
	// An operation in flight when the load stops still completes, so its
	// outcome is known whenever possible.
	octx, cancel := context.WithTimeout(context.WithoutCancel(ctx), wl.opTimeout())
	defer cancel()
	h := wl.History
	switch p := rng.IntN(10); {
	case p < 4:
		done := h.Begin(id, Input{Kind: Put, Key: key, Value: value})
		ver, err := cl.Put(octx, []byte(key), []byte(value))
		done(Output{Unknown: err != nil, Version: ver})
		if err == nil {
			seen[key] = ver
		}
	case p < 8:
		if out := wl.get(octx, cl, id, key); out.Found {
			seen[key] = out.Version
		}
	case p < 9:
		done := h.Begin(id, Input{Kind: Delete, Key: key})
		found, ver, err := cl.Delete(octx, []byte(key))
		done(Output{Unknown: err != nil, Found: found, Version: ver})
	default:
		exp := seen[key]
		done := h.Begin(id, Input{Kind: CAS, Key: key, Value: value, Expected: exp})
		ok, ver, err := cl.CompareAndSet(octx, []byte(key), []byte(value), exp)
		done(Output{Unknown: err != nil, Succeeded: ok, Version: ver})
	}
}

// get runs one linearizable read and records it.
func (wl *Workload) get(ctx context.Context, cl *client.Client, id int, key string) Output {
	done := wl.History.Begin(id, Input{Kind: Get, Key: key})
	v, ver, err := cl.Get(ctx, []byte(key))
	var out Output
	switch {
	case errors.Is(err, client.ErrNotFound):
	case err != nil:
		out.Unknown = true
	default:
		out = Output{Found: true, Value: string(v), Version: ver}
	}
	done(out)
	return out
}

// FinalReads reads every key once more, as client number Workers, after
// the faults have ended. It reports how many reads failed.
func (wl *Workload) FinalReads(timeout time.Duration) (failed int, err error) {
	cl, err := client.New(wl.Addrs...)
	if err != nil {
		return 0, err
	}
	defer func() { _ = cl.Close() }()
	for k := range wl.Keys {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		if wl.get(ctx, cl, wl.Workers, fmt.Sprintf("k%d", k)).Unknown {
			failed++
		}
		cancel()
	}
	return failed, nil
}
