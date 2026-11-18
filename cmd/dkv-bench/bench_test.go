package main

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.uber.org/goleak"

	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/server/servertest"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// share returns the fraction of draws the most frequent 1% of records got.
func share(t *testing.T, k keyChooser, n uint64, draws int) float64 {
	t.Helper()
	rng := rand.New(rand.NewPCG(1, 2))
	counts := make([]int, n)
	for range draws {
		i := k.next(rng)
		if i >= n {
			t.Fatalf("drew record %d of %d", i, n)
		}
		counts[i]++
	}
	slices.Sort(counts)
	top := 0
	for _, c := range counts[len(counts)-int(n/100):] {
		top += c
	}
	return float64(top) / float64(draws)
}

// TestKeyDistributions: under the Zipfian chooser the hottest 1% of
// records take a large share of the operations (YCSB's theta 0.99 gives
// about a third to a half), under the uniform one about 1%.
func TestKeyDistributions(t *testing.T) {
	const n, draws = 10_000, 200_000
	z, err := newKeyChooser("zipfian", n)
	if err != nil {
		t.Fatal(err)
	}
	if s := share(t, z, n, draws); s < 0.3 || s > 0.7 {
		t.Fatalf("zipfian: the hottest 1%% of records got %.1f%% of the draws", s*100)
	}
	u, _ := newKeyChooser("uniform", n)
	if s := share(t, u, n, draws); s > 0.03 {
		t.Fatalf("uniform: the hottest 1%% of records got %.1f%% of the draws", s*100)
	}
	if _, err := newKeyChooser("pareto", n); err == nil {
		t.Fatal("an unknown distribution was accepted")
	}
}

func benchConfig(t *testing.T, addr, admin string, args ...string) *runConfig {
	t.Helper()
	base := []string{"--addr=" + addr, "--admin=" + admin, "--records=300", "--concurrency=4", "--warmup=200ms",
		"--duration=1500ms", "--label", "experiment=test", "--label", "rf=1"}
	c, err := parseRun(append(base, args...), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestBenchAgainstAServer runs closed- and open-loop benchmarks against a
// real server: the workload's read share is kept, every measured
// operation is counted once, the nodes' own numbers are read, the event
// command runs, and the result and its report survive a round trip
// through their files.
func TestBenchAgainstAServer(t *testing.T) {
	node := servertest.Start(t, func(c *config.Config) { c.Server.AdminAddr = "127.0.0.1:0" })
	dir := t.TempDir()
	marker := filepath.Join(dir, "event-ran")
	c := benchConfig(t, node.Addr, node.AdminAddr, "--workload=B", "--timeseries", "--event-at=500ms", "--event-cmd=touch "+marker,
		"--label", "variant=closed")
	res, err := bench(context.Background(), c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	r := res.Results
	reads, updates := r.ByOp["read"].Count, r.ByOp["update"].Count
	if r.OK == 0 || r.Errors+r.Timeouts != 0 || res.Preload.Ops != 300 || r.Latency.Count != r.OK || reads+updates != r.OK {
		t.Fatalf("results %+v, preload %+v", r, res.Preload)
	}
	if f := float64(reads) / float64(r.OK); f < 0.9 || f > 0.99 {
		t.Fatalf("workload B read %.2f of its operations; want about 0.95", f)
	}
	var perSecond int64
	for _, s := range res.Timeseries {
		perSecond += s.OK
	}
	var histTotal int64
	for _, c := range res.Histogram.Counts {
		histTotal += c
	}
	if perSecond > r.OK || perSecond < r.OK/2 || len(res.Timeseries) != 1 || histTotal != r.OK {
		t.Fatalf("time series %+v (whole seconds of 1.5 s), histogram total %d; want %d operations", res.Timeseries, histTotal, r.OK)
	}
	if res.Server == nil || res.Server.UserBytes <= 0 || res.Server.SpaceAmplification <= 0 {
		t.Fatalf("server-side numbers %+v", res.Server)
	}
	if res.Machine.CPUs == 0 || res.GitSHA == "" {
		t.Fatalf("environment %+v, git %q", res.Machine, res.GitSHA)
	}
	if !fileExists(marker) {
		t.Fatal("the event command did not run")
	}

	open := benchConfig(t, node.Addr, node.AdminAddr, "--workload=A", "--rate=200", "--preload=false", "--label", "variant=open")
	ores, err := bench(context.Background(), open, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if got := ores.Results.Throughput; got < 150 || got > 250 {
		t.Fatalf("open loop at 200 ops/s served %.0f ops/s", got)
	}

	for _, x := range []*result{res, ores} {
		if _, err := writeResult(x, filepath.Join(dir, "results", x.Labels["variant"]+".json")); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := loadResults(filepath.Join(dir, "results"))
	if err != nil || len(loaded) != 2 {
		t.Fatalf("loaded %d results: %v", len(loaded), err)
	}
	var md bytes.Buffer
	if err := writeReport(&md, loaded); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"## test", "| closed | 1 |", "| open | 1 |", "Machine:", "loop: closed loop, open loop at 200 ops/s",
		"300 records"} {
		if !strings.Contains(md.String(), want) {
			t.Fatalf("report lacks %q:\n%s", want, md.String())
		}
	}
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// TestStat: a variant's number is the median of its runs, with the range.
func TestStat(t *testing.T) {
	if got := stat([]float64{3, 1, 2}, "%.0f"); got != "2 (1–3)" {
		t.Fatalf("stat = %q", got)
	}
	if got := stat([]float64{4, 1}, "%.1f"); got != "2.5 (1.0–4.0)" {
		t.Fatalf("stat of two = %q", got)
	}
	if got := stat(nil, "%.0f"); got != "–" {
		t.Fatalf("stat of none = %q", got)
	}
}
