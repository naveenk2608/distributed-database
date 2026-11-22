package main

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.uber.org/goleak"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
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
	if err := writeReport(&md, loaded, nil, nil); err != nil {
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

// TestServerDeltaCountsEveryLog: write amplification counts the Raft logs
// as well as the engines' logs, flushes and compactions, over the measured
// window only, and skips a node that restarted in between.
func TestServerDeltaCountsEveryLog(t *testing.T) {
	stats := func(started int64, user, raftLog, wal, flush int64) *dkvv1.StatsResponse {
		return &dkvv1.StatsResponse{StartedUnixNano: started, Replicas: []*dkvv1.ReplicaStats{{
			RaftLogBytes: raftLog,
			Engine:       &dkvv1.EngineStats{UserBytes: user, WalBytes: wal, FlushBytes: flush},
		}}}
	}
	before := map[string]*dkvv1.StatsResponse{"a": stats(1, 100, 500, 0, 0), "b": stats(1, 0, 0, 0, 0), "c": stats(1, 0, 0, 0, 0)}
	after := map[string]*dkvv1.StatsResponse{
		"a": stats(1, 1100, 1700, 0, 300),  // 1000 user bytes; 1200 to the Raft log, 300 flushed
		"b": stats(1, 1000, 1000, 100, 0),  // 1000 user bytes; 1000 to the Raft log, 100 to the engine's log
		"c": stats(2, 9999, 9999, 9999, 0), // restarted: not counted
	}
	s := serverDelta(before, after, &runConfig{Records: 10, ValueSize: 10})
	if s.UserBytes != 2000 || s.RaftLogBytes != 2200 || s.EngineWALBytes != 100 || s.FlushBytes != 300 {
		t.Fatalf("deltas %+v", s)
	}
	if want := 2600.0 / 2000; s.WriteAmplification != want {
		t.Fatalf("write amplification %v; want %v", s.WriteAmplification, want)
	}
}

// TestDescribeLeavesOutReadsForWrites: a write-only experiment has no read
// consistency to report.
func TestDescribeLeavesOutReadsForWrites(t *testing.T) {
	runs := []*result{{Config: &runConfig{Workload: "W", Dist: "uniform", Consistency: "linearizable"}}}
	same, varies := describe(runs)
	if strings.Contains(strings.Join(same, ", "), "reads") || len(varies) != 0 {
		t.Fatalf("same %v, varies %v", same, varies)
	}
	runs[0].Config.Workload = "A"
	if same, _ := describe(runs); !slices.Contains(same, "linearizable reads") {
		t.Fatalf("workload A: %v", same)
	}
}

// TestReportListsReruns: a repeated run's attempts are listed with their
// own throughput, the one used is marked, and an index whose used attempt
// is not the run's file in the results directory is refused.
func TestReportListsReruns(t *testing.T) {
	dir := t.TempDir()
	run := func(tput float64) *result {
		return &result{Labels: map[string]string{"experiment": "sync", "variant": "always"},
			Config: &runConfig{Workload: "W", Dist: "uniform", Seed: 1}, Results: measured{Throughput: tput}}
	}
	for path, r := range map[string]*result{"sync--always--1.json": run(891), "reruns/sync--always--1--matrix.json": run(453)} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, path)), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := writeResult(r, filepath.Join(dir, path)); err != nil {
			t.Fatal(err)
		}
	}
	index := func(used, rerunTime string) {
		b := `{"method": "Screened by outside CPU.", "runs": [{"run": "sync--always--1", "attempts": [
			{"label": "matrix", "file": "reruns/sync--always--1--matrix.json", "time": "0001-01-01T00:00:00Z",
			 "outside_cpu_s": 4.33, "excess_cpu_s": 3.06, "busiest": "gopls 3.18", "used": ` + strconv.FormatBool(used == "matrix") + `},
			{"label": "rerun 1", "file": "sync--always--1.json", "time": "` + rerunTime + `",
			 "outside_cpu_s": 0.78, "excess_cpu_s": -0.49, "busiest": "MainThread 0.42", "used": ` + strconv.FormatBool(used == "rerun 1") + `}]}]}`
		if err := os.WriteFile(filepath.Join(dir, "reruns", "index.json"), []byte(b), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	index("rerun 1", "0001-01-01T00:00:00Z")
	results, err := loadResults(dir)
	if err != nil || len(results) != 1 {
		t.Fatalf("loaded %d results: %v; want only the run used", len(results), err)
	}
	rr, err := loadReruns(dir)
	if err != nil {
		t.Fatal(err)
	}
	var md bytes.Buffer
	if err := writeReport(&md, results, rr, nil); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"1 of the 1 runs were repeated", "## Reruns", "Screened by outside CPU.",
		"| sync / always, run 1 | matrix | 453 | 4.33 (3.06) | gopls 3.18 |  |",
		"|  | rerun 1 | 891 | 0.78 (0.00) | MainThread 0.42 | yes |"} {
		if !strings.Contains(md.String(), want) {
			t.Fatalf("report lacks %q:\n%s", want, md.String())
		}
	}

	index("matrix", "0001-01-01T00:00:00Z")
	if _, err := loadReruns(dir); err == nil || !strings.Contains(err.Error(), "the attempt used is reruns/sync--always--1--matrix.json") {
		t.Fatalf("an index using an attempt the tables do not count: %v", err)
	}
	// A later run of the experiment replaced the file the index describes.
	index("rerun 1", "2026-11-21T14:40:00Z")
	if _, err := loadReruns(dir); err == nil || !strings.Contains(err.Error(), "not the attempt of") {
		t.Fatalf("an index describing a run since replaced: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "reruns", "index.json")); err != nil {
		t.Fatal(err)
	}
	if rr, err := loadReruns(dir); rr != nil || err != nil {
		t.Fatalf("without an index: %v, %v", rr, err)
	}
}

// A subdirectory of the results, such as aws/, is another environment: the
// report renders it as a part of its own after the main experiments, and
// its runs stay out of the main tables and the main environment.
func TestReportRendersOtherEnvironmentsAsParts(t *testing.T) {
	dir := t.TempDir()
	run := func(exp, variant, cpu string, labels ...string) *result {
		r := &result{Labels: map[string]string{"experiment": exp, "title": exp + " title", "variant": variant},
			Machine: machine{CPUModel: cpu, CPUs: 2, OS: "linux", Arch: "amd64"},
			Config:  &runConfig{Workload: "A", Dist: "uniform", Seed: 1}, Results: measured{Throughput: 100}}
		for i := 0; i+1 < len(labels); i += 2 {
			r.Labels[labels[i]] = labels[i+1]
		}
		return r
	}
	aws := []string{"part", "AWS", "environment", "Three nodes in ap-south-1."}
	files := map[string]*result{
		"nodes--3--1.json":           run("nodes", "3 nodes", "laptop", "environment", "One machine."),
		"aws/workloads--A--1.json":   run("workloads", "A", "xeon", aws...),
		"aws/workloads--A--2.json":   run("workloads", "A", "xeon", aws...),
		"aws/meta/deployment.json":   nil, // not a result: below the part's own files
		"reruns/nodes--3--1--x.json": run("nodes", "3 nodes", "laptop"),
	}
	for path, r := range files {
		full := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if r == nil {
			if err := os.WriteFile(full, []byte(`{"region": "ap-south-1"}`), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := writeResult(r, full); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}

	results, err := loadResults(dir)
	if err != nil || len(results) != 1 {
		t.Fatalf("loaded %d main results: %v; want 1", len(results), err)
	}
	parts, err := loadParts(dir)
	if err != nil || len(parts) != 1 || parts[0].dir != "aws" || len(parts[0].results) != 2 {
		t.Fatalf("parts %+v, %v; want aws/ alone, with its 2 results", parts, err)
	}
	var md bytes.Buffer
	if err := writeReport(&md, results, nil, parts); err != nil {
		t.Fatal(err)
	}
	s, at := md.String(), 0
	for _, want := range []string{"AWS (2 result files in `bench/results/aws/`)",
		"## Environment\n\n- Machine: laptop", "- One machine.", "## nodes title", "| 3 nodes | 1 |",
		"## AWS\n\n### Environment\n\n- Load generator: xeon", "- Three nodes in ap-south-1.",
		"### workloads title", "| A | 2 |"} {
		i := strings.Index(s[at:], want)
		if i < 0 {
			t.Fatalf("report lacks %q after offset %d:\n%s", want, at, s)
		}
		at += i + len(want)
	}
	if strings.Count(s, "xeon") != 1 || strings.Count(s, "laptop") != 1 {
		t.Fatalf("the environments' machines are mixed:\n%s", s)
	}
}
