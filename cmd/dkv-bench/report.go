package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The report is generated from the result files alone: every number in it
// is a median (with the spread) over the runs that carry the same
// "experiment" and "variant" labels. Labels say what an experiment and a
// variant are: "title" and "what" describe the experiment, "order" sorts
// experiments and "variant_order" variants.

func parseReport(args []string, stderr io.Writer) (dir, out string, err error) {
	fs := flag.NewFlagSet("dkv-bench report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&dir, "results", "bench/results", "directory of result files")
	fs.StringVar(&out, "out", "docs/benchmarks.md", "report to write")
	err = fs.Parse(args)
	return dir, out, err
}

func loadResults(dir string) ([]*result, error) {
	names, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	var out []*result
	for _, n := range names {
		b, err := os.ReadFile(n)
		if err != nil {
			return nil, err
		}
		var r result
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", n, err)
		}
		out = append(out, &r)
	}
	return out, nil
}

// reruns lists the runs that were repeated because something outside the
// benchmark competed with them for the machine, with every attempt. It is
// read from reruns/index.json in the results directory; the attempts not
// used keep their result files under reruns/, where loadResults does not
// look, and the attempt used is the run's file in the results directory.
type reruns struct {
	// Method says how runs were screened and which were repeated; the
	// report prints it as is.
	Method string     `json:"method"`
	Runs   []rerunRun `json:"runs"`
}

type rerunRun struct {
	Run      string         `json:"run"` // the result file's name without .json
	Attempts []rerunAttempt `json:"attempts"`
}

type rerunAttempt struct {
	Label string    `json:"label"` // which session ran it, e.g. "matrix", "rerun 1"
	File  string    `json:"file"`  // its result file, relative to the results directory
	Time  time.Time `json:"time"`  // the result's own time, to tell a later run from it
	// OutsideCPU is the CPU-seconds processes outside the benchmark used
	// during the measured window; ExcessCPU the part above the machine's
	// idle floor.
	OutsideCPU float64 `json:"outside_cpu_s"`
	ExcessCPU  float64 `json:"excess_cpu_s"`
	Busiest    string  `json:"busiest"`         // the busiest outside processes in that window
	Event      string  `json:"event,omitempty"` // e.g. another WSL distro starting
	Used       bool    `json:"used"`

	result *result
}

// loadReruns reads dir/reruns/index.json and each attempt's result file;
// it returns nil if there is no index.
func loadReruns(dir string) (*reruns, error) {
	b, err := os.ReadFile(filepath.Join(dir, "reruns", "index.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rr reruns
	if err := json.Unmarshal(b, &rr); err != nil {
		return nil, fmt.Errorf("reruns/index.json: %w", err)
	}
	for _, run := range rr.Runs {
		used := 0
		for i := range run.Attempts {
			a := &run.Attempts[i]
			b, err := os.ReadFile(filepath.Join(dir, a.File))
			if err != nil {
				return nil, fmt.Errorf("rerun of %s: %w", run.Run, err)
			}
			a.result = new(result)
			if err := json.Unmarshal(b, a.result); err != nil {
				return nil, fmt.Errorf("rerun of %s: %s: %w", run.Run, a.File, err)
			}
			if !a.result.Time.Equal(a.Time) {
				return nil, fmt.Errorf("rerun of %s: %s is a run of %v, not the attempt of %v the index describes",
					run.Run, a.File, a.result.Time, a.Time)
			}
			if a.Used {
				used++
				// The tables count the results directory's files: the
				// attempt used must be the one there.
				if a.File != run.Run+".json" {
					return nil, fmt.Errorf("rerun of %s: the attempt used is %s, not %s.json", run.Run, a.File, run.Run)
				}
			}
		}
		if used != 1 || len(run.Attempts) < 2 {
			return nil, fmt.Errorf("rerun of %s: %d attempts, %d used; want at least 2, exactly 1 used", run.Run, len(run.Attempts), used)
		}
	}
	return &rr, nil
}

// group is the runs of one variant of one experiment.
type group struct {
	variant string
	order   int
	runs    []*result
}

func median(xs []float64) float64 {
	s := slices.Clone(xs)
	slices.Sort(s)
	if len(s) == 0 {
		return 0
	}
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// stat formats the median of xs with its range over the runs.
func stat(xs []float64, format string) string {
	if len(xs) == 0 {
		return "–"
	}
	m := median(xs)
	if len(xs) == 1 {
		return fmt.Sprintf(format, m)
	}
	return fmt.Sprintf(format+" (%s–%s)", m, fmt.Sprintf(format, slices.Min(xs)), fmt.Sprintf(format, slices.Max(xs)))
}

func collect(runs []*result, f func(*result) (float64, bool)) []float64 {
	var out []float64
	for _, r := range runs {
		if v, ok := f(r); ok {
			out = append(out, v)
		}
	}
	return out
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 1 << 30
	}
	return n
}

// writeReport renders the results as markdown, with the reruns (nil if
// none) at the end.
func writeReport(w io.Writer, results []*result, rr *reruns) error {
	byExp := map[string][]*result{}
	for _, r := range results {
		byExp[r.Labels["experiment"]] = append(byExp[r.Labels["experiment"]], r)
	}
	exps := slices.Collect(maps.Keys(byExp))
	slices.SortFunc(exps, func(a, b string) int {
		if d := atoi(byExp[a][0].Labels["order"]) - atoi(byExp[b][0].Labels["order"]); d != 0 {
			return d
		}
		return strings.Compare(a, b)
	})

	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	p("# dkv benchmarks\n\n")
	p("Generated by `dkv-bench report` from the %d result files in `bench/results/` (`make bench-local`\n", len(results))
	p("runs the matrix and regenerates this file). Every number is the median over a variant's runs,\n")
	p("with the lowest and highest in parentheses. Latencies are client-side, from issuing an\n")
	p("operation to its answer (in open loop, from its intended start), in milliseconds.\n\n")
	p("The nodes' own numbers cover the measured time: \"Fsync\" is persisting a Raft Ready that needs\n")
	p("an fsync, \"Commit\" a write from proposal to apply on its leader, both over every replica of\n")
	p("every node. \"Write amp.\" is the bytes written to the Raft logs, the engines' own logs, flushes\n")
	p("and compactions per byte of keys and values applied, over every replica: about 1 plus the log\n")
	p("records' overhead until memtables flush. Raw data: each result file holds the run's full\n")
	p("configuration, the latency histogram and these counters.\n\n")

	p("## Environment\n\n")
	machines, shas := map[string]int{}, map[string]int{}
	for _, r := range results {
		m := r.Machine
		machines[fmt.Sprintf("%s, %d CPUs, %.1f GiB memory, %s/%s, kernel %s", m.CPUModel, m.CPUs,
			float64(m.MemBytes)/(1<<30), m.OS, m.Arch, m.Kernel)]++
		sha := r.GitSHA
		if r.GitDirty {
			sha += " (uncommitted changes)"
		}
		shas[sha+", "+r.Go]++
	}
	for _, k := range slices.Sorted(maps.Keys(machines)) {
		p("- Machine: %s (%d runs)\n", k, machines[k])
	}
	for _, k := range slices.Sorted(maps.Keys(shas)) {
		p("- Build: git %s (%d runs)\n", k, shas[k])
	}
	if env := results[0].Labels["environment"]; env != "" {
		p("- %s\n", env)
	}
	if rr != nil {
		p("- %d of the %d runs were repeated because something outside the benchmark competed with them for\n", len(rr.Runs), len(results))
		p("  the machine; \"Reruns\" at the end lists every attempt.\n")
	}
	p("\n")

	for _, e := range exps {
		runs := byExp[e]
		first := runs[0]
		title := first.Labels["title"]
		if title == "" {
			title = e
		}
		p("## %s\n\n", title)
		if what := first.Labels["what"]; what != "" {
			p("%s\n\n", what)
		}
		same, varies := describe(runs)
		p("%s.", strings.Join(same, ", "))
		if len(varies) > 0 {
			p(" Varying between variants: %s.", strings.Join(varies, "; "))
		}
		p("\n\n")

		byVar := map[string]*group{}
		for _, r := range runs {
			v := r.Labels["variant"]
			if byVar[v] == nil {
				byVar[v] = &group{variant: v, order: atoi(r.Labels["variant_order"])}
			}
			byVar[v].runs = append(byVar[v].runs, r)
		}
		groups := slices.Collect(maps.Values(byVar))
		slices.SortFunc(groups, func(a, b *group) int {
			if a.order != b.order {
				return a.order - b.order
			}
			return strings.Compare(a.variant, b.variant)
		})
		p("| Variant | Runs | Throughput (ops/s) | p50 | p99 | p99.9 | Errors | Fsync p50 / p99 | Commit p50 / p99 | Write amp. |\n")
		p("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
		for _, g := range groups {
			ms := func(f func(*result) int64) []float64 {
				return collect(g.runs, func(r *result) (float64, bool) { return float64(f(r)) / 1000, r.Results.Latency.Count > 0 })
			}
			server := func(f func(*serverSide) (float64, bool)) []float64 {
				return collect(g.runs, func(r *result) (float64, bool) {
					if r.Server == nil {
						return 0, false
					}
					return f(r.Server)
				})
			}
			fsync50 := server(func(s *serverSide) (float64, bool) { return pct(s.RaftLogSyncUS, false) })
			fsync99 := server(func(s *serverSide) (float64, bool) { return pct(s.RaftLogSyncUS, true) })
			commit50 := server(func(s *serverSide) (float64, bool) { return pct(s.CommitUS, false) })
			commit99 := server(func(s *serverSide) (float64, bool) { return pct(s.CommitUS, true) })
			wamp := server(func(s *serverSide) (float64, bool) { return s.WriteAmplification, s.UserBytes > 0 })
			errs := collect(g.runs, func(r *result) (float64, bool) {
				return float64(r.Results.Errors+r.Results.Timeouts) / max(1, float64(r.Results.OK+r.Results.Errors+r.Results.Timeouts)) * 100, true
			})
			p("| %s | %d | %s | %s | %s | %s | %s | %s / %s | %s / %s | %s |\n", g.variant, len(g.runs),
				stat(collect(g.runs, func(r *result) (float64, bool) { return r.Results.Throughput, true }), "%.0f"),
				stat(ms(func(r *result) int64 { return r.Results.Latency.P50 }), "%.2f"),
				stat(ms(func(r *result) int64 { return r.Results.Latency.P99 }), "%.2f"),
				stat(ms(func(r *result) int64 { return r.Results.Latency.P999 }), "%.2f"),
				stat(errs, "%.2f%%"),
				stat(fsync50, "%.2f"), stat(fsync99, "%.2f"), stat(commit50, "%.2f"), stat(commit99, "%.2f"),
				stat(wamp, "%.2f"))
		}
		p("\n")
		for _, g := range groups {
			if ts := g.runs[0].Timeseries; len(ts) > 0 {
				p("Operations per second, %s, first run (the event at %.0f s: %s):\n\n", g.variant, g.runs[0].Config.EventAtS,
					g.runs[0].Labels["event"])
				p("| Second | %s |\n| --- |%s\n", joinInts(ts, func(s second) int { return s.S }), strings.Repeat(" --- |", len(ts)))
				p("| OK | %s |\n| Errors | %s |\n\n", joinInts(ts, func(s second) int { return int(s.OK) }), joinInts(ts, func(s second) int { return int(s.Errors) }))
				if rec := recovery(g.runs); rec != "" {
					p("%s\n\n", rec)
				}
			}
		}
		if note := first.Labels["note"]; note != "" {
			p("%s\n\n", note)
		}
	}
	if rr != nil {
		writeReruns(p, rr)
	}
	return nil
}

// writeReruns renders every attempt of every repeated run, in the order
// the index lists them.
func writeReruns(p func(string, ...any), rr *reruns) {
	p("## Reruns\n\n")
	if rr.Method != "" {
		p("%s\n\n", rr.Method)
	}
	p("| Run | Attempt | Throughput (ops/s) | Outside CPU-s in the measured window (above the idle floor) | Busiest outside processes | Used |\n")
	p("| --- | --- | --- | --- | --- | --- |\n")
	for _, run := range rr.Runs {
		for i, a := range run.Attempts {
			name := ""
			if i == 0 {
				r := a.result
				name = fmt.Sprintf("%s / %s, run %d", r.Labels["experiment"], r.Labels["variant"], r.Config.Seed)
			}
			busiest := a.Busiest
			if a.Event != "" {
				busiest = a.Event + "; " + busiest
			}
			used := ""
			if a.Used {
				used = "yes"
			}
			p("| %s | %s | %.0f | %.2f (%.2f) | %s | %s |\n", name, a.Label, a.result.Results.Throughput, a.OutsideCPU,
				max(a.ExcessCPU, 0), busiest, used)
		}
	}
	p("\n")
}

func pct(l *latencyPercentiles, p99 bool) (float64, bool) {
	if l == nil {
		return 0, false
	}
	if p99 {
		return l.P99 / 1000, true
	}
	return l.P50 / 1000, true
}

func joinInts(ts []second, f func(second) int) string {
	parts := make([]string, len(ts))
	for i, s := range ts {
		parts[i] = strconv.Itoa(f(s))
	}
	return strings.Join(parts, " | ")
}

// recovery measures, in each run with a time series and an event, how
// long throughput stayed below half of its median before the event.
func recovery(runs []*result) string {
	var gaps []float64
	for _, r := range runs {
		at := int(r.Config.EventAtS)
		ts := r.Timeseries
		if at <= 0 || at >= len(ts) {
			continue
		}
		var before []float64
		for _, s := range ts[:at] {
			before = append(before, float64(s.OK))
		}
		half := median(before) / 2
		low := 0
		for _, s := range ts[at:] {
			if float64(s.OK) < half {
				low++
			}
		}
		gaps = append(gaps, float64(low))
	}
	if len(gaps) == 0 {
		return ""
	}
	return fmt.Sprintf("Seconds after the event with less than half the throughput before it: %s, over %d runs.",
		stat(gaps, "%.0f"), len(gaps))
}

// describe lists the settings every run of an experiment shares, and those
// that vary with the values they take.
func describe(runs []*result) (same, varies []string) {
	fields := []struct {
		name string
		f    func(*runConfig) string
	}{
		{"workload", func(c *runConfig) string { return "workload " + c.Workload }},
		{"keys", func(c *runConfig) string { return c.Dist + " keys" }},
		{"records", func(c *runConfig) string { return fmt.Sprintf("%d records", c.Records) }},
		{"value size", func(c *runConfig) string { return fmt.Sprintf("%d-byte values", c.ValueSize) }},
		{"workers", func(c *runConfig) string { return fmt.Sprintf("%d workers", c.Concurrency) }},
		{"loop", func(c *runConfig) string {
			if c.Rate > 0 {
				return fmt.Sprintf("open loop at %.0f ops/s", c.Rate)
			}
			return "closed loop"
		}},
		{"reads", func(c *runConfig) string {
			if c.Workload == "W" {
				return "" // writes only
			}
			return c.Consistency + " reads"
		}},
		{"timing", func(c *runConfig) string {
			return fmt.Sprintf("%.0f s warm-up, %.0f s measured", c.WarmupS, c.DurationS)
		}},
	}
	for _, fl := range fields {
		seen := map[string]bool{}
		var order []string
		for _, r := range runs {
			if v := fl.f(r.Config); v != "" && !seen[v] {
				seen[v] = true
				order = append(order, v)
			}
		}
		if len(order) == 0 {
			continue
		}
		if len(order) == 1 {
			same = append(same, order[0])
		} else {
			varies = append(varies, fl.name+": "+strings.Join(order, ", "))
		}
	}
	return same, varies
}
