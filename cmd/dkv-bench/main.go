// Command dkv-bench is dkv's load generator: YCSB-style workloads against a
// running cluster, with latencies in HDR histograms and every run saved as
// a JSON file (configuration, git revision, machine, raw histogram, the
// nodes' own numbers), and a report generated from those files.
//
//	dkv-bench run --addr=127.0.0.1:7071,127.0.0.1:7072 --workload=A --label experiment=nodes ...
//	dkv-bench report --results=bench/results --out=docs/benchmarks.md
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	usage := func() {
		_, _ = fmt.Fprint(stderr, "Usage: dkv-bench run [flags]   (run -h for the flags)\n       dkv-bench report [flags]\n       dkv-bench leader --admin=... --shard=N\n")
	}
	if len(args) == 0 {
		usage()
		return 2
	}
	switch args[0] {
	case "run":
		cfg, err := parseRun(args[1:], stderr)
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "dkv-bench: %v\n", err)
			return 2
		}
		res, err := bench(ctx, cfg, stderr)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "dkv-bench: %v\n", err)
			return 1
		}
		path, err := writeResult(res, cfg.Out)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "dkv-bench: %v\n", err)
			return 1
		}
		r := res.Results
		_, _ = fmt.Fprintf(stdout, "%.0f ops/s, p50 %.2f ms, p99 %.2f ms, p99.9 %.2f ms, %d errors, %d timeouts; %s\n", r.Throughput,
			float64(r.Latency.P50)/1000, float64(r.Latency.P99)/1000, float64(r.Latency.P999)/1000, r.Errors, r.Timeouts, path)
		return 0
	case "report":
		dir, out, err := parseReport(args[1:], stderr)
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		if err != nil {
			return 2
		}
		results, err := loadResults(dir)
		if err != nil || len(results) == 0 {
			_, _ = fmt.Fprintf(stderr, "dkv-bench: no results in %s: %v\n", dir, err)
			return 1
		}
		f, err := os.Create(out)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "dkv-bench: %v\n", err)
			return 1
		}
		werr := writeReport(f, results)
		if cerr := f.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			_, _ = fmt.Fprintf(stderr, "dkv-bench: %v\n", werr)
			return 1
		}
		_, _ = fmt.Fprintf(stdout, "wrote %s from %d results\n", out, len(results))
		return 0
	case "leader":
		// leader --admin=a,b,c --shard=N prints the admin address of the
		// node that leads shard N (for scripts that kill it).
		fs := flag.NewFlagSet("dkv-bench leader", flag.ContinueOnError)
		fs.SetOutput(stderr)
		admin := fs.String("admin", "", "node admin HTTP addresses, comma-separated")
		shard := fs.Int("shard", 0, "shard (-1: the meta group)")
		if err := fs.Parse(args[1:]); err != nil || *admin == "" {
			return 2
		}
		addr, err := leaderOf(ctx, strings.Split(*admin, ","), int32(*shard))
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "dkv-bench: %v\n", err)
			return 1
		}
		_, _ = fmt.Fprintln(stdout, addr)
		return 0
	default:
		usage()
		return 2
	}
}

// leaderOf returns the admin address of the node whose replica of shard
// leads at the highest term.
func leaderOf(ctx context.Context, admin []string, shard int32) (string, error) {
	best, term := "", uint64(0)
	for _, a := range admin {
		st := serverStats(ctx, []string{a})
		for _, r := range st[a].GetReplicas() {
			if rs := r.GetRaft(); rs.GetShard() == shard && rs.GetRole() == "leader" && rs.GetTerm() > term {
				best, term = a, rs.GetTerm()
			}
		}
	}
	if best == "" {
		return "", fmt.Errorf("no node answering leads shard %d", shard)
	}
	return best, nil
}
