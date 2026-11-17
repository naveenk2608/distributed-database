// Command dkvctl is the dkv command-line client.
//
//	dkvctl [flags] put <key> <value>
//	dkvctl [flags] get <key>
//	dkvctl [flags] delete <key>
//	dkvctl [flags] cas <key> <expected-version> <value>
//	dkvctl [flags] status
//	dkvctl [flags] shards
//
// Flags go before the command. Exit status follows grep: 0 on success,
// 1 when get finds no value or cas finds another version, 2 on any error.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/pkg/client"
)

const (
	exitOK       = 0
	exitNotFound = 1
	exitError    = 2
)

// addrEnv deliberately avoids the DKV_ prefix, which belongs to the server's
// configuration and is checked strictly there.
const addrEnv = "DKVCTL_ADDR"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// output writes results to stdout and diagnostics to stderr. A failed
// stdout write (a full disk, a closed pipe) is remembered so the command can
// exit with an error instead of reporting success. Diagnostics are best
// effort: there is nowhere left to report a failed stderr write.
type output struct {
	stdout, stderr io.Writer
	err            error
}

func (o *output) result(format string, args ...any) {
	if _, err := fmt.Fprintf(o.stdout, format, args...); err != nil && o.err == nil {
		o.err = err
	}
}

func (o *output) resultBytes(b []byte) {
	if _, err := o.stdout.Write(b); err != nil && o.err == nil {
		o.err = err
	}
}

func (o *output) errorf(format string, args ...any) {
	_, _ = fmt.Fprintf(o.stderr, "dkvctl: "+format+"\n", args...)
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	out := &output{stdout: stdout, stderr: stderr}
	code := runCommand(ctx, args, getenv, out)
	if out.err != nil {
		out.errorf("writing output: %v", out.err)
		return exitError
	}
	return code
}

func runCommand(ctx context.Context, args []string, getenv func(string) string, out *output) int {
	stderr := out.stderr
	fs := flag.NewFlagSet("dkvctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", getenv(addrEnv), "node addresses host:port, comma-separated; requests move to the next when one is unreachable (env "+addrEnv+")")
	timeout := fs.Duration("timeout", 5*time.Second, "deadline for the request, including retries")
	stale := fs.Bool("stale", false, "get: read from any replica without confirming it is up to date")
	minVersion := fs.Uint64("min-version", 0, "get with --stale: wait until the replica has applied this version")
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `Usage: dkvctl [flags] <command> [args]

Commands:
  put <key> <value>   store value under key; prints the new version
  get <key>           print the value of key (exit status 1 if not found)
  delete <key>        delete key; prints the version of the delete
  cas <key> <expected-version> <value>
                      store value only if key is at expected-version (0: key
                      must not exist); exit status 1 if it is not
  status              show the node's view of the cluster
  shards              show where each shard is placed and the node's local replicas

Flags:
`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitError
	}

	usage := func(msg string) int {
		out.errorf("%s\n", msg)
		fs.Usage()
		return exitError
	}
	cmd := fs.Args()
	if len(cmd) == 0 {
		return usage("missing command")
	}
	wantArgs := map[string]int{"put": 2, "get": 1, "delete": 1, "cas": 3, "status": 0, "shards": 0}
	n, ok := wantArgs[cmd[0]]
	if !ok {
		return usage(fmt.Sprintf("unknown command %q", cmd[0]))
	}
	if len(cmd)-1 != n {
		return usage(fmt.Sprintf("%s takes %d argument(s), got %d", cmd[0], n, len(cmd)-1))
	}
	if *addr == "" {
		return usage("no server address: pass --addr or set " + addrEnv)
	}

	c, err := client.New(strings.Split(*addr, ",")...)
	if err != nil {
		out.errorf("%v", err)
		return exitError
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	if cmd[0] == "status" {
		resp, err := c.ClusterStatus(ctx)
		if err != nil {
			out.errorf("%v", err)
			return exitError
		}
		printStatus(out, resp, time.Now())
		return exitOK
	}
	if cmd[0] == "shards" {
		resp, err := c.ShardMap(ctx)
		if err != nil {
			out.errorf("%v", err)
			return exitError
		}
		printShards(out, resp)
		return exitOK
	}
	key := []byte(cmd[1])
	switch cmd[0] {
	case "put":
		ver, err := c.Put(ctx, key, []byte(cmd[2]))
		if err != nil {
			out.errorf("%v", err)
			return exitError
		}
		out.result("OK (version %d)\n", ver)
	case "get":
		var val []byte
		var err error
		if *stale {
			val, _, err = c.GetStale(ctx, key, *minVersion)
		} else {
			val, _, err = c.Get(ctx, key)
		}
		if errors.Is(err, client.ErrNotFound) {
			out.errorf("key %q not found", cmd[1])
			return exitNotFound
		}
		if err != nil {
			out.errorf("%v", err)
			return exitError
		}
		out.resultBytes(append(val, '\n'))
	case "delete":
		found, ver, err := c.Delete(ctx, key)
		if err != nil {
			out.errorf("%v", err)
			return exitError
		}
		if found {
			out.result("deleted (version %d)\n", ver)
		} else {
			out.result("key did not exist (version %d)\n", ver)
		}
	case "cas":
		expected, err := strconv.ParseUint(cmd[2], 10, 64)
		if err != nil {
			return usage(fmt.Sprintf("expected-version %q is not a version number", cmd[2]))
		}
		ok, ver, err := c.CompareAndSet(ctx, key, []byte(cmd[3]), expected)
		if err != nil {
			out.errorf("%v", err)
			return exitError
		}
		if !ok {
			if ver == 0 {
				out.errorf("not set: key %q does not exist", cmd[1])
			} else {
				out.errorf("not set: key %q is at version %d", cmd[1], ver)
			}
			return exitNotFound
		}
		out.result("OK (version %d)\n", ver)
	}
	return exitOK
}

// printStatus writes one line per member: address, node ID, state, when it
// last answered, round-trip time and last error.
func printStatus(out *output, resp *dkvv1.ClusterStatusResponse, now time.Time) {
	var b strings.Builder
	tw := tabwriter.NewWriter(&b, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ADDRESS\tNODE ID\tSTATE\tLAST SEEN\tRTT\tERROR")
	for _, m := range resp.GetMembers() {
		id := m.GetNode().GetNodeId()
		if id == "" {
			id = "-"
		}
		seen, rtt := "never", "-"
		switch {
		case m.GetSelf():
			seen = "(this node)"
		case m.GetLastSeenUnixNano() != 0:
			seen = now.Sub(time.Unix(0, m.GetLastSeenUnixNano())).Round(time.Millisecond).String() + " ago"
		}
		if m.GetRttNanos() > 0 {
			rtt = time.Duration(m.GetRttNanos()).Round(10 * time.Microsecond).String()
		}
		state := strings.TrimPrefix(m.GetState().String(), "MEMBER_STATE_")
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", m.GetAddr(), id, state, seen, rtt, m.GetLastError())
	}
	_ = tw.Flush() // writes to a strings.Builder cannot fail
	out.result("%s", b.String())
}

// printShards writes one line per shard: its replicas, and for a replica
// the node holds, its Raft role, the leader it knows of, its term, commit
// index and applied index.
func printShards(out *output, resp *dkvv1.ShardMapResponse) {
	var b strings.Builder
	fmt.Fprintf(&b, "%d shards, replication factor %d\n", resp.GetShardCount(), resp.GetReplicationFactor())
	tw := tabwriter.NewWriter(&b, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "SHARD\tREPLICAS\tLOCAL\tROLE\tLEADER\tTERM\tCOMMIT\tAPPLIED")
	dash := func(v string) string {
		if v == "" || v == "0" {
			return "-"
		}
		return v
	}
	for _, s := range resp.GetShards() {
		local, role, leader, term, commit, applied := "no", "-", "-", "-", "-", "-"
		if s.GetLocal() {
			local, applied = "yes", fmt.Sprint(s.GetLastSeq())
			role, leader = dash(s.GetRole()), dash(s.GetLeader())
			term, commit = dash(fmt.Sprint(s.GetTerm())), dash(fmt.Sprint(s.GetCommitIndex()))
		}
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", s.GetShard(), strings.Join(s.GetReplicas(), ","),
			local, role, leader, term, commit, applied)
	}
	_ = tw.Flush() // writes to a strings.Builder cannot fail
	out.result("%s", b.String())
}
