package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/pkg/client"
)

// runConfig is one benchmark run's configuration, as recorded with its
// results.
type runConfig struct {
	Addrs       []string          `json:"-"`
	Admin       []string          `json:"-"`
	Workload    string            `json:"workload"`
	Dist        string            `json:"distribution"`
	Records     uint64            `json:"records"`
	ValueSize   int               `json:"value_size"`
	Concurrency int               `json:"concurrency"`
	Rate        float64           `json:"rate"` // ops/s for open loop; 0: closed loop
	Consistency string            `json:"consistency"`
	Preload     bool              `json:"preload"`
	Warmup      time.Duration     `json:"-"`
	Duration    time.Duration     `json:"-"`
	OpTimeout   time.Duration     `json:"-"`
	Seed        uint64            `json:"seed"`
	EventAt     time.Duration     `json:"-"`
	EventCmd    string            `json:"event_cmd,omitempty"`
	Timeseries  bool              `json:"timeseries"`
	Labels      map[string]string `json:"-"`
	Out         string            `json:"-"`
	WarmupS     float64           `json:"warmup_s"`
	DurationS   float64           `json:"duration_s"`
	OpTimeoutS  float64           `json:"op_timeout_s"`
	EventAtS    float64           `json:"event_at_s,omitempty"`
}

type labelFlags map[string]string

func (l labelFlags) String() string { return fmt.Sprint(map[string]string(l)) }
func (l labelFlags) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok || k == "" {
		return fmt.Errorf("label %q: want key=value", v)
	}
	l[k] = val
	return nil
}

func parseRun(args []string, stderr io.Writer) (*runConfig, error) {
	fs := flag.NewFlagSet("dkv-bench run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	c := &runConfig{Labels: labelFlags{}}
	addrs := fs.String("addr", "", "node public addresses host:port, comma-separated (required)")
	admin := fs.String("admin", "", "node admin HTTP addresses, comma-separated: server-side numbers are read from them")
	fs.StringVar(&c.Workload, "workload", "A", "A (50% reads), B (95%), C (100%) or W (writes only)")
	fs.StringVar(&c.Dist, "dist", "uniform", "key distribution: uniform or zipfian")
	fs.Uint64Var(&c.Records, "records", 100_000, "records preloaded and then read and updated")
	fs.IntVar(&c.ValueSize, "value-size", 100, "value size in bytes")
	fs.IntVar(&c.Concurrency, "concurrency", 32, "workers (closed loop: each waits for its answer; open loop: the pool serving the arrivals)")
	fs.Float64Var(&c.Rate, "rate", 0, "open loop: arrivals per second across all workers; 0 runs closed loop")
	fs.StringVar(&c.Consistency, "consistency", "linearizable", "reads: linearizable or stale")
	fs.BoolVar(&c.Preload, "preload", true, "write every record before the warm-up")
	fs.DurationVar(&c.Warmup, "warmup", 30*time.Second, "load before measuring; not counted")
	fs.DurationVar(&c.Duration, "duration", 60*time.Second, "measured time")
	fs.DurationVar(&c.OpTimeout, "op-timeout", 2*time.Second, "deadline for one operation, retries included")
	fs.Uint64Var(&c.Seed, "seed", 1, "random seed of the operation stream")
	fs.DurationVar(&c.EventAt, "event-at", 0, "run --event-cmd this long into the measured time")
	fs.StringVar(&c.EventCmd, "event-cmd", "", "a shell command to run at --event-at (for example, kill a leader)")
	fs.BoolVar(&c.Timeseries, "timeseries", false, "record operations per second")
	fs.Var(labelFlags(c.Labels), "label", "key=value recorded with the results (repeatable): the experiment and its configuration")
	fs.StringVar(&c.Out, "out", "", "result file (JSON); default bench/results/<time>-<experiment>.json")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if *addrs == "" {
		return nil, errors.New("--addr is required")
	}
	c.Addrs = strings.Split(*addrs, ",")
	if *admin != "" {
		c.Admin = strings.Split(*admin, ",")
	}
	if _, ok := workloads[c.Workload]; !ok {
		return nil, fmt.Errorf("unknown workload %q: want A, B, C or W", c.Workload)
	}
	if c.Consistency != "linearizable" && c.Consistency != "stale" {
		return nil, fmt.Errorf("--consistency must be linearizable or stale, got %q", c.Consistency)
	}
	if c.Records == 0 || c.Concurrency < 1 || c.ValueSize < 0 || c.Duration <= 0 {
		return nil, errors.New("--records, --concurrency and --duration must be positive")
	}
	if (c.EventCmd == "") != (c.EventAt == 0) {
		return nil, errors.New("--event-at and --event-cmd go together")
	}
	c.WarmupS, c.DurationS, c.OpTimeoutS, c.EventAtS = c.Warmup.Seconds(), c.Duration.Seconds(), c.OpTimeout.Seconds(), c.EventAt.Seconds()
	return c, nil
}

// latencyHist records microseconds from 1 µs to 1 minute with three
// significant digits.
func latencyHist() *hdrhistogram.Histogram { return hdrhistogram.New(1, 60_000_000, 3) }

// recorder collects the outcomes of the measured operations.
type recorder struct {
	mu        sync.Mutex
	all       *hdrhistogram.Histogram
	byOp      map[string]*hdrhistogram.Histogram
	ok        atomic.Int64
	errs      atomic.Int64
	timeouts  atomic.Int64
	measuring atomic.Bool
	start     time.Time
	perSecond []second
}

type second struct {
	S      int   `json:"s"`
	OK     int64 `json:"ok"`
	Errors int64 `json:"errors"`
}

func newRecorder() *recorder {
	return &recorder{all: latencyHist(), byOp: map[string]*hdrhistogram.Histogram{"read": latencyHist(), "update": latencyHist()}}
}

func (r *recorder) record(op string, took time.Duration, err error) {
	if !r.measuring.Load() {
		return
	}
	us := max(took.Microseconds(), 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	s := int(time.Since(r.start) / time.Second)
	for len(r.perSecond) <= s {
		r.perSecond = append(r.perSecond, second{S: len(r.perSecond)})
	}
	switch {
	case err == nil:
		r.ok.Add(1)
		r.perSecond[s].OK++
		_ = r.all.RecordValue(us)      // beyond a minute: clamped out, counted in ok
		_ = r.byOp[op].RecordValue(us) // same
	case status.Code(err) == codes.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded):
		r.timeouts.Add(1)
		r.perSecond[s].Errors++
	default:
		r.errs.Add(1)
		r.perSecond[s].Errors++
	}
}

// latencySummary is a histogram's percentiles, in microseconds.
type latencySummary struct {
	Count int64   `json:"count"`
	Mean  float64 `json:"mean"`
	P50   int64   `json:"p50"`
	P95   int64   `json:"p95"`
	P99   int64   `json:"p99"`
	P999  int64   `json:"p999"`
	Max   int64   `json:"max"`
}

func summarize(h *hdrhistogram.Histogram) latencySummary {
	return latencySummary{Count: h.TotalCount(), Mean: h.Mean(), P50: h.ValueAtQuantile(50), P95: h.ValueAtQuantile(95),
		P99: h.ValueAtQuantile(99), P999: h.ValueAtQuantile(99.9), Max: h.Max()}
}

// result is one run, as written to its JSON file.
type result struct {
	Name     string            `json:"name"`
	Time     time.Time         `json:"time"`
	GitSHA   string            `json:"git_sha"`
	GitDirty bool              `json:"git_dirty"`
	Go       string            `json:"go"`
	Machine  machine           `json:"machine"`
	Labels   map[string]string `json:"labels"`
	Config   *runConfig        `json:"config"`
	Preload  *phase            `json:"preload,omitempty"`
	Results  measured          `json:"results"`
	// Histogram is the raw HDR histogram of every successful measured
	// operation (microseconds), to recompute any percentile.
	Histogram  *hdrhistogram.Snapshot `json:"histogram"`
	Timeseries []second               `json:"timeseries,omitempty"`
	Server     *serverSide            `json:"server,omitempty"`
}

type phase struct {
	Ops     int64   `json:"ops"`
	Errors  int64   `json:"errors"`
	Seconds float64 `json:"seconds"`
}

type measured struct {
	Seconds    float64                   `json:"seconds"`
	OK         int64                     `json:"ok"`
	Errors     int64                     `json:"errors"`
	Timeouts   int64                     `json:"timeouts"`
	Throughput float64                   `json:"throughput"` // successful operations per second
	Latency    latencySummary            `json:"latency_us"`
	ByOp       map[string]latencySummary `json:"by_op_latency_us"`
	// Backlog is the most open-loop arrivals that waited for a worker at
	// once (0 in closed loop).
	Backlog int64 `json:"max_backlog"`
}

type machine struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	CPUs     int    `json:"cpus"`
	CPUModel string `json:"cpu_model"`
	MemBytes int64  `json:"memory_bytes"`
	Kernel   string `json:"kernel"`
}

func describeMachine() machine {
	m := machine{OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU()}
	if f, err := os.Open("/proc/cpuinfo"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if k, v, ok := strings.Cut(sc.Text(), ":"); ok && strings.TrimSpace(k) == "model name" {
				m.CPUModel = strings.TrimSpace(v)
				break
			}
		}
		_ = f.Close()
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		var kb int64
		if _, err := fmt.Sscanf(string(b), "MemTotal: %d kB", &kb); err == nil {
			m.MemBytes = kb * 1024
		}
	}
	if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		m.Kernel = strings.TrimSpace(string(b))
	}
	return m
}

func buildRevision() (sha string, dirty bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown", false
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			sha = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if sha == "" {
		sha = "unknown"
	}
	return sha, dirty
}

// bench runs one benchmark and returns its result.
func bench(ctx context.Context, c *runConfig, log io.Writer) (*result, error) {
	readShare := workloads[c.Workload]
	keys, err := newKeyChooser(c.Dist, c.Records)
	if err != nil {
		return nil, err
	}
	// One client per 16 workers: each keeps a connection to every node.
	nClients := (c.Concurrency + 15) / 16
	clients := make([]*client.Client, nClients)
	for i := range clients {
		cl, err := client.New(c.Addrs...)
		if err != nil {
			return nil, err
		}
		defer func() { _ = cl.Close() }()
		clients[i] = cl
	}
	sha, dirty := buildRevision()
	res := &result{Name: c.Labels["experiment"], Time: time.Now().UTC(), GitSHA: sha, GitDirty: dirty, Go: runtime.Version(),
		Machine: describeMachine(), Labels: c.Labels, Config: c}

	if c.Preload {
		start := time.Now()
		var next atomic.Uint64
		var ok, errs atomic.Int64
		var wg sync.WaitGroup
		for w := range c.Concurrency {
			wg.Go(func() {
				cl := clients[w%nClients]
				for i := next.Add(1) - 1; i < c.Records && ctx.Err() == nil; i = next.Add(1) - 1 {
					octx, cancel := context.WithTimeout(ctx, c.OpTimeout*5)
					_, err := cl.Put(octx, keyName(i), value(c.ValueSize, i, 0))
					cancel()
					if err != nil {
						errs.Add(1)
						continue
					}
					ok.Add(1)
				}
			})
		}
		wg.Wait()
		res.Preload = &phase{Ops: ok.Load(), Errors: errs.Load(), Seconds: time.Since(start).Seconds()}
		_, _ = fmt.Fprintf(log, "preloaded %d records in %.1fs (%d errors)\n", ok.Load(), res.Preload.Seconds, errs.Load())
		if errs.Load() > 0 {
			return nil, fmt.Errorf("preload: %d of %d writes failed", errs.Load(), c.Records)
		}
	}

	before := serverStats(ctx, c.Admin)
	rec := newRecorder()
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	op := func(cl *client.Client, rng *rand.Rand, n uint64) {
		i := keys.next(rng)
		octx, cancel := context.WithTimeout(runCtx, c.OpTimeout)
		defer cancel()
		start := time.Now()
		var err error
		kind := "read"
		if rng.Float64() < readShare {
			if c.Consistency == "stale" {
				_, _, err = cl.GetStale(octx, keyName(i), 0)
			} else {
				_, _, err = cl.Get(octx, keyName(i))
			}
			if errors.Is(err, client.ErrNotFound) {
				err = nil // without a preload, a missing key is an answer
			}
		} else {
			kind = "update"
			_, err = cl.Put(octx, keyName(i), value(c.ValueSize, i, n))
		}
		if runCtx.Err() == nil {
			rec.record(kind, time.Since(start), err)
		}
	}
	var backlog atomic.Int64
	var maxBacklog atomic.Int64
	var wg sync.WaitGroup
	if c.Rate == 0 {
		for w := range c.Concurrency {
			wg.Go(func() {
				rng := rand.New(rand.NewPCG(c.Seed, uint64(w)))
				cl := clients[w%nClients]
				for n := uint64(1); runCtx.Err() == nil; n++ {
					op(cl, rng, n)
				}
			})
		}
	} else {
		// Open loop: arrivals at fixed intervals; latency counts from the
		// intended start, so a slow server cannot hide its own queueing.
		arrivals := make(chan time.Time, 1<<16)
		interval := time.Duration(float64(time.Second) / c.Rate)
		wg.Go(func() {
			defer close(arrivals)
			next := time.Now()
			for runCtx.Err() == nil {
				next = next.Add(interval)
				time.Sleep(time.Until(next))
				select {
				case arrivals <- next:
					b := backlog.Add(1)
					for m := maxBacklog.Load(); b > m && !maxBacklog.CompareAndSwap(m, b); m = maxBacklog.Load() {
					}
				default: // over 65,536 waiting: counted as an error
					rec.record("update", 0, errors.New("arrival queue full"))
				}
			}
		})
		for w := range c.Concurrency {
			wg.Go(func() {
				rng := rand.New(rand.NewPCG(c.Seed, uint64(w)))
				cl := clients[w%nClients]
				n := uint64(0)
				for intended := range arrivals {
					backlog.Add(-1)
					n++
					i := keys.next(rng)
					octx, cancel := context.WithTimeout(runCtx, c.OpTimeout)
					var err error
					kind := "read"
					if rng.Float64() < readShare {
						if c.Consistency == "stale" {
							_, _, err = cl.GetStale(octx, keyName(i), 0)
						} else {
							_, _, err = cl.Get(octx, keyName(i))
						}
						if errors.Is(err, client.ErrNotFound) {
							err = nil
						}
					} else {
						kind = "update"
						_, err = cl.Put(octx, keyName(i), value(c.ValueSize, i, n))
					}
					cancel()
					if runCtx.Err() == nil {
						rec.record(kind, time.Since(intended), err)
					}
				}
			})
		}
	}

	_, _ = fmt.Fprintf(log, "warming up for %v\n", c.Warmup)
	if !sleepCtx(ctx, c.Warmup) {
		return nil, ctx.Err()
	}
	rec.start = time.Now()
	rec.measuring.Store(true)
	_, _ = fmt.Fprintf(log, "measuring for %v\n", c.Duration)
	var eventErr error
	if c.EventCmd != "" {
		wg.Go(func() {
			if !sleepCtx(runCtx, c.EventAt) {
				return
			}
			out, err := exec.CommandContext(ctx, "sh", "-c", c.EventCmd).CombinedOutput()
			_, _ = fmt.Fprintf(log, "event at %v: %s %s\n", c.EventAt, strings.TrimSpace(string(out)), errString(err))
			eventErr = err
		})
	}
	sleepCtx(ctx, c.Duration)
	rec.measuring.Store(false)
	elapsed := time.Since(rec.start)
	stop()
	wg.Wait()
	if eventErr != nil {
		return nil, fmt.Errorf("--event-cmd failed: %w", eventErr)
	}
	after := serverStats(ctx, c.Admin)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	res.Results = measured{Seconds: elapsed.Seconds(), OK: rec.ok.Load(), Errors: rec.errs.Load(), Timeouts: rec.timeouts.Load(),
		Throughput: float64(rec.ok.Load()) / elapsed.Seconds(), Latency: summarize(rec.all),
		ByOp:    map[string]latencySummary{"read": summarize(rec.byOp["read"]), "update": summarize(rec.byOp["update"])},
		Backlog: maxBacklog.Load()}
	res.Histogram = rec.all.Export()
	if c.Timeseries {
		// Whole seconds only: the last, partial one would look like a dip.
		res.Timeseries = rec.perSecond[:min(len(rec.perSecond), int(elapsed/time.Second))]
	}
	if before != nil && after != nil {
		res.Server = serverDelta(before, after, c)
	}
	return res, nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return "(" + err.Error() + ")"
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// serverStats reads every node's statistics; nil if any node does not
// answer (the server-side numbers are then left out).
func serverStats(ctx context.Context, admin []string) map[string]*dkvv1.StatsResponse {
	if len(admin) == 0 {
		return nil
	}
	out := map[string]*dkvv1.StatsResponse{}
	for _, a := range admin {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		req, err := http.NewRequestWithContext(rctx, http.MethodGet, "http://"+a+"/v1/stats", nil)
		if err != nil {
			cancel()
			return nil
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			return nil
		}
		b, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		cancel()
		st := &dkvv1.StatsResponse{}
		if err != nil || resp.StatusCode != http.StatusOK || protojson.Unmarshal(b, st) != nil {
			return nil
		}
		out[a] = st
	}
	return out
}

// serverSide is what the nodes did during the measured time.
type serverSide struct {
	// RaftLogSyncUS and CommitUS: latency percentiles of persisting a
	// durable Raft Ready (fsync included) and of a write from proposal to
	// apply on its leader, over every node.
	RaftLogSyncUS *latencyPercentiles `json:"raft_log_sync_us,omitempty"`
	CommitUS      *latencyPercentiles `json:"commit_us,omitempty"`
	RaftLogSyncs  uint64              `json:"raft_log_syncs"`
	// Bytes the engines took from users, and wrote: flushes, compactions
	// and their own logs (with storage.engine_wal).
	UserBytes       int64 `json:"user_bytes"`
	FlushBytes      int64 `json:"flush_bytes"`
	CompactionBytes int64 `json:"compaction_bytes_written"`
	EngineWALBytes  int64 `json:"engine_wal_bytes"`
	// WriteAmplification: engine bytes written per user byte.
	WriteAmplification float64 `json:"write_amplification"`
	// SpaceAmplification: bytes stored (tables and memtables, every
	// replica) per byte of live data times the replication factor in the
	// "rf" label; 0 without it.
	SpaceAmplification float64 `json:"space_amplification"`
	StoredBytes        int64   `json:"stored_bytes"`
}

type latencyPercentiles struct {
	P50 float64 `json:"p50"`
	P99 float64 `json:"p99"`
}

func serverDelta(before, after map[string]*dkvv1.StatsResponse, c *runConfig) *serverSide {
	s := &serverSide{}
	var bounds []float64
	sync, commit := []uint64{}, []uint64{}
	var syncN, commitN uint64
	for addr, a := range after {
		b := before[addr]
		if b == nil || b.GetStartedUnixNano() != a.GetStartedUnixNano() {
			continue // restarted during the run: its counters reset
		}
		bounds = a.GetLatencyBoundsSeconds()
		prev := map[string]*dkvv1.OpStats{}
		for _, o := range b.GetInternal() {
			prev[o.GetOp()] = o
		}
		for _, o := range a.GetInternal() {
			p := prev[o.GetOp()]
			d := make([]uint64, len(o.GetLatencyCumulative()))
			for i, v := range o.GetLatencyCumulative() {
				if i < len(p.GetLatencyCumulative()) {
					v -= min(v, p.GetLatencyCumulative()[i])
				}
				d[i] = v
			}
			n := o.GetOk() - min(o.GetOk(), p.GetOk())
			switch o.GetOp() {
			case "raft_log_sync":
				sync, syncN = addCum(sync, d), syncN+n
			case "commit":
				commit, commitN = addCum(commit, d), commitN+n
			}
		}
		engines := func(st *dkvv1.StatsResponse) (user, flush, comp, wal, stored int64) {
			for _, r := range st.GetReplicas() {
				e := r.GetEngine()
				user += e.GetUserBytes()
				flush += e.GetFlushBytes()
				comp += e.GetCompactionBytesWritten()
				wal += e.GetWalBytes()
				stored += int64(e.GetTableBytes()) + e.GetMemtableBytes()
			}
			return
		}
		u0, f0, c0, w0, _ := engines(b)
		u1, f1, c1, w1, st := engines(a)
		s.UserBytes += u1 - u0
		s.FlushBytes += f1 - f0
		s.CompactionBytes += c1 - c0
		s.EngineWALBytes += w1 - w0
		s.StoredBytes += st
	}
	s.RaftLogSyncs = syncN
	if syncN > 0 {
		s.RaftLogSyncUS = percentilesUS(bounds, sync, syncN)
	}
	if commitN > 0 {
		s.CommitUS = percentilesUS(bounds, commit, commitN)
	}
	if s.UserBytes > 0 {
		s.WriteAmplification = float64(s.FlushBytes+s.CompactionBytes+s.EngineWALBytes) / float64(s.UserBytes)
	}
	var rf float64
	if _, err := fmt.Sscan(c.Labels["rf"], &rf); err == nil && rf > 0 {
		live := float64(c.Records) * float64(len(keyName(0))+c.ValueSize)
		s.SpaceAmplification = float64(s.StoredBytes) / (live * rf)
	}
	return s
}

func addCum(a, b []uint64) []uint64 {
	if len(a) < len(b) {
		a = append(a, make([]uint64, len(b)-len(a))...)
	}
	for i, v := range b {
		a[i] += v
	}
	return a
}

// percentilesUS interpolates p50 and p99, in microseconds, from cumulative
// bucket counts (as the console does; requests beyond the last bound count
// as the last bound).
func percentilesUS(bounds []float64, cum []uint64, total uint64) *latencyPercentiles {
	q := func(p float64) float64 {
		rank := p * float64(total)
		lower, below := 0.0, uint64(0)
		for i, b := range bounds {
			if i < len(cum) && float64(cum[i]) >= rank {
				in := cum[i] - below
				v := lower
				if in > 0 {
					v += (b - lower) * (rank - float64(below)) / float64(in)
				}
				return v * 1e6
			}
			if i < len(cum) {
				lower, below = b, cum[i]
			}
		}
		return lower * 1e6
	}
	return &latencyPercentiles{P50: q(0.5), P99: q(0.99)}
}

// writeResult saves res as JSON, by default under bench/results.
func writeResult(res *result, out string) (string, error) {
	if out == "" {
		name := res.Labels["experiment"]
		if name == "" {
			name = "run"
		}
		out = filepath.Join("bench", "results", res.Time.Format("20060102T150405Z")+"-"+strings.ReplaceAll(name, "/", "_")+".json")
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return "", err
	}
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return "", err
	}
	return out, os.WriteFile(out, append(b, '\n'), 0o644)
}
