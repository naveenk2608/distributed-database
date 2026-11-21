// Command dkvctl is the dkv command-line client.
//
//	dkvctl [flags] put <key> <value>
//	dkvctl [flags] get <key>
//	dkvctl [flags] delete <key>
//	dkvctl [flags] cas <key> <expected-version> <value>
//	dkvctl [flags] status
//	dkvctl [flags] shards
//	dkvctl [flags] leaders
//	dkvctl [flags] transfer-leader <shard> <node-id>
//	dkvctl [flags] drain <node-id>
//	dkvctl [flags] undrain <node-id>
//	dkvctl [flags] decommission <node-id>
//
// Flags go before the command. Exit status follows grep: 0 on success,
// 1 when get finds no value or cas finds another version, 2 on any error.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

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

// json writes v as one line of JSON.
func (o *output) json(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		o.errorf("encoding JSON: %v", err)
		if o.err == nil {
			o.err = err
		}
		return
	}
	o.resultBytes(append(b, '\n'))
}

// proto writes m in its JSON form (field names as in the .proto files).
func (o *output) proto(m proto.Message) {
	b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
	if err != nil {
		o.errorf("encoding JSON: %v", err)
		if o.err == nil {
			o.err = err
		}
		return
	}
	o.resultBytes(append(b, '\n'))
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
	timeout := fs.Duration("timeout", 5*time.Second, "deadline for the request, including retries (leaders: for each node, all asked at once)")
	consistency := fs.String("consistency", "linearizable", "get: linearizable (confirmed by the shard's leader) or stale (any replica)")
	stale := fs.Bool("stale", false, "get: shorthand for --consistency=stale")
	minVersion := fs.Uint64("min-version", 0, "get with stale consistency: wait until the replica has applied this version")
	asJSON := fs.Bool("json", false, "print results as JSON")
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
  leaders             show each shard's leader and term (ask every node:
                      pass all of them in --addr; they are asked at once,
                      each for up to --timeout, and any that cannot be
                      reached is reported on stderr)
  transfer-leader <shard> <node-id>
                      hand a shard's leadership to one of its voters (-1 is
                      the meta group); the rebalancer may move it again
  drain <node-id>     move every leadership off a node and give it none
  undrain <node-id>   let a drained node lead again
  decommission <node-id>
                      move every replica off a node, then mark it decommissioned

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
	wantArgs := map[string]int{"put": 2, "get": 1, "delete": 1, "cas": 3, "status": 0, "shards": 0, "leaders": 0,
		"transfer-leader": 2, "drain": 1, "undrain": 1, "decommission": 1}
	n, ok := wantArgs[cmd[0]]
	if !ok {
		return usage(fmt.Sprintf("unknown command %q", cmd[0]))
	}
	if len(cmd)-1 != n {
		return usage(fmt.Sprintf("%s takes %d argument(s), got %d", cmd[0], n, len(cmd)-1))
	}
	if *stale {
		*consistency = "stale"
	}
	if *consistency != "linearizable" && *consistency != "stale" {
		return usage(fmt.Sprintf("--consistency must be linearizable or stale, got %q", *consistency))
	}
	if *addr == "" {
		return usage("no server address: pass --addr or set " + addrEnv)
	}
	var shard int64
	if cmd[0] == "transfer-leader" {
		var err error
		if shard, err = strconv.ParseInt(cmd[1], 10, 32); err != nil {
			return usage(fmt.Sprintf("shard %q is not a shard number", cmd[1]))
		}
	}
	var expected uint64
	if cmd[0] == "cas" {
		var err error
		if expected, err = strconv.ParseUint(cmd[2], 10, 64); err != nil {
			return usage(fmt.Sprintf("expected-version %q is not a version number", cmd[2]))
		}
	}

	addrs := strings.Split(*addr, ",")
	c, err := client.New(addrs...)
	if err != nil {
		out.errorf("%v", err)
		return exitError
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	fail := func(err error) int {
		out.errorf("%v", err)
		return exitError
	}
	done := func(text string, fields map[string]any) int {
		if *asJSON {
			fields["ok"] = true
			out.json(fields)
		} else {
			out.result("%s\n", text)
		}
		return exitOK
	}

	switch cmd[0] {
	case "status":
		resp, err := c.ClusterStatus(ctx)
		if err != nil {
			return fail(err)
		}
		if *asJSON {
			out.proto(resp)
		} else {
			printStatus(out, resp, time.Now())
		}
	case "shards":
		resp, err := c.ShardMap(ctx)
		if err != nil {
			return fail(err)
		}
		if *asJSON {
			out.proto(resp)
		} else {
			printShards(out, resp)
		}
	case "leaders":
		// Every node is asked at once and has the whole --timeout, so one
		// that never answers hides no other node's view.
		shardMaps, errs := c.ShardMaps(ctx, *timeout)
		var answered []*dkvv1.ShardMapResponse
		for i, err := range errs {
			if err != nil {
				out.errorf("%s", nodeProblem(addrs[i], err, *timeout))
				continue
			}
			answered = append(answered, shardMaps[i])
		}
		if len(answered) == 0 {
			return fail(errors.New("no node answered"))
		}
		leaders := mergeLeaders(answered)
		if *asJSON {
			out.json(leaders)
		} else {
			printLeaders(out, leaders)
		}
	case "transfer-leader":
		if err := c.TransferLeader(ctx, int32(shard), cmd[2]); err != nil {
			return fail(err)
		}
		return done(fmt.Sprintf("shard %d is now led by %s", shard, cmd[2]), map[string]any{"shard": shard, "leader": cmd[2]})
	case "drain", "undrain":
		if err := c.Drain(ctx, cmd[1], cmd[0] == "drain"); err != nil {
			return fail(err)
		}
		if cmd[0] == "drain" {
			return done(fmt.Sprintf("node %s is drained; its leaderships move to other nodes", cmd[1]), map[string]any{"node_id": cmd[1], "drained": true})
		}
		return done(fmt.Sprintf("node %s may lead again", cmd[1]), map[string]any{"node_id": cmd[1], "drained": false})
	case "decommission":
		if err := c.Decommission(ctx, cmd[1]); err != nil {
			return fail(err)
		}
		return done(fmt.Sprintf("node %s is being decommissioned; its replicas move to other nodes", cmd[1]),
			map[string]any{"node_id": cmd[1]})
	case "put":
		ver, err := c.Put(ctx, []byte(cmd[1]), []byte(cmd[2]))
		if err != nil {
			return fail(err)
		}
		return done(fmt.Sprintf("OK (version %d)", ver), map[string]any{"version": ver})
	case "get":
		var val []byte
		var ver uint64
		if *consistency == "stale" {
			val, ver, err = c.GetStale(ctx, []byte(cmd[1]), *minVersion)
		} else {
			val, ver, err = c.Get(ctx, []byte(cmd[1]))
		}
		switch {
		case errors.Is(err, client.ErrNotFound):
			if *asJSON {
				out.json(map[string]any{"key": cmd[1], "found": false})
			} else {
				out.errorf("key %q not found", cmd[1])
			}
			return exitNotFound
		case err != nil:
			return fail(err)
		case *asJSON:
			out.json(valueFields(map[string]any{"key": cmd[1], "found": true, "version": ver}, val))
		default:
			out.resultBytes(append(val, '\n'))
		}
	case "delete":
		found, ver, err := c.Delete(ctx, []byte(cmd[1]))
		if err != nil {
			return fail(err)
		}
		text := fmt.Sprintf("deleted (version %d)", ver)
		if !found {
			text = fmt.Sprintf("key did not exist (version %d)", ver)
		}
		return done(text, map[string]any{"found": found, "version": ver})
	case "cas":
		ok, ver, err := c.CompareAndSet(ctx, []byte(cmd[1]), []byte(cmd[3]), expected)
		if err != nil {
			return fail(err)
		}
		if !ok {
			switch {
			case *asJSON:
				out.json(map[string]any{"succeeded": false, "version": ver})
			case ver == 0:
				out.errorf("not set: key %q does not exist", cmd[1])
			default:
				out.errorf("not set: key %q is at version %d", cmd[1], ver)
			}
			return exitNotFound
		}
		if *asJSON {
			out.json(map[string]any{"succeeded": true, "version": ver})
		} else {
			out.result("OK (version %d)\n", ver)
		}
	}
	return exitOK
}

// valueFields adds a value to JSON fields: as a string when it is valid
// UTF-8, else base64-encoded under value_base64.
func valueFields(fields map[string]any, val []byte) map[string]any {
	if utf8.Valid(val) {
		fields["value"] = string(val)
	} else {
		fields["value_base64"] = base64.StdEncoding.EncodeToString(val)
	}
	return fields
}

// leaderInfo is one shard's leader as the nodes asked report it.
type leaderInfo struct {
	Shard  int32  `json:"shard"`
	Leader string `json:"leader"` // empty when no node asked holds a replica that knows it
	Term   uint64 `json:"term"`
}

// mergeLeaders keeps, for every shard, the leader reported at the highest
// term by the nodes that hold a replica of it.
func mergeLeaders(shardMaps []*dkvv1.ShardMapResponse) []leaderInfo {
	byShard := map[int32]leaderInfo{}
	for _, m := range shardMaps {
		for _, s := range m.GetShards() {
			id := int32(s.GetShard())
			cur, seen := byShard[id]
			if !seen {
				byShard[id] = leaderInfo{Shard: id}
			}
			if s.GetLocal() && s.GetLeader() != "" && (s.GetTerm() > cur.Term || cur.Leader == "") {
				byShard[id] = leaderInfo{Shard: id, Leader: s.GetLeader(), Term: s.GetTerm()}
			}
		}
	}
	out := slices.Collect(maps.Values(byShard))
	slices.SortFunc(out, func(a, b leaderInfo) int { return int(a.Shard - b.Shard) })
	return out
}

// nodeProblem says why the node at addr sent no shard map: unreachable when
// it could not be connected to or gave no answer within timeout, otherwise
// the error it answered with.
func nodeProblem(addr string, err error, timeout time.Duration) string {
	switch status.Code(err) {
	case codes.DeadlineExceeded:
		return fmt.Sprintf("%s unreachable: no answer within %s", addr, timeout)
	case codes.Unavailable:
		msg := err.Error()
		var se interface{ GRPCStatus() *status.Status }
		if errors.As(err, &se) {
			msg = se.GRPCStatus().Message()
		}
		return fmt.Sprintf("%s unreachable: %s", addr, msg)
	}
	return fmt.Sprintf("%s: %v", addr, err)
}

// printLeaders writes one line per shard: its leader and term.
func printLeaders(out *output, leaders []leaderInfo) {
	var b strings.Builder
	tw := tabwriter.NewWriter(&b, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "SHARD\tLEADER\tTERM")
	for _, l := range leaders {
		leader, term := l.Leader, fmt.Sprint(l.Term)
		if leader == "" {
			leader, term = "unknown", "-"
		}
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\n", l.Shard, leader, term)
	}
	_ = tw.Flush() // writes to a strings.Builder cannot fail
	out.result("%s", b.String())
}

// printStatus writes one line per member: address, node ID, state, when it
// last answered, round-trip time and last error.
func printStatus(out *output, resp *dkvv1.ClusterStatusResponse, now time.Time) {
	var b strings.Builder
	tw := tabwriter.NewWriter(&b, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ADDRESS\tNODE ID\tSTATE\tNODE STATE\tLAST SEEN\tRTT\tERROR")
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
		nodeState := "-"
		if ns := m.GetNodeState(); ns != dkvv1.NodeState_NODE_STATE_UNSPECIFIED {
			nodeState = strings.ToLower(strings.TrimPrefix(ns.String(), "NODE_STATE_"))
		}
		if m.GetSuspect() {
			nodeState += " (suspect)"
		}
		if m.GetDrained() {
			nodeState += " (drained)"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", m.GetAddr(), id, state, nodeState, seen, rtt, m.GetLastError())
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
