package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/protobuf/encoding/protojson"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/node/nodetest"
	"github.com/naveenk2608/distributed-database/internal/porttest"
	"github.com/naveenk2608/distributed-database/internal/server/servertest"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type result struct {
	code           int
	stdout, stderr string
}

func ctl(t *testing.T, env map[string]string, args ...string) result {
	t.Helper()
	var out, errOut strings.Builder
	code := run(context.Background(), args, func(k string) string { return env[k] }, &out, &errOut)
	return result{code, out.String(), errOut.String()}
}

func TestPutGetDelete(t *testing.T) {
	node := servertest.Start(t, nil)
	a := "--addr=" + node.Addr

	steps := []struct {
		args       []string
		code       int
		stdout     string
		stderrPart string
	}{
		{[]string{a, "get", "greeting"}, exitNotFound, "", `key "greeting" not found`},
		{[]string{a, "put", "greeting", "hello world"}, exitOK, "OK (version 1)\n", ""},
		{[]string{a, "get", "greeting"}, exitOK, "hello world\n", ""},
		{[]string{a, "put", "greeting", "hi"}, exitOK, "OK (version 2)\n", ""},
		{[]string{a, "get", "greeting"}, exitOK, "hi\n", ""},
		{[]string{a, "delete", "greeting"}, exitOK, "deleted (version 3)\n", ""},
		{[]string{a, "get", "greeting"}, exitNotFound, "", "not found"},
		{[]string{a, "delete", "greeting"}, exitOK, "key did not exist (version 4)\n", ""},
		{[]string{a, "cas", "greeting", "0", "new"}, exitOK, "OK (version 5)\n", ""},
		{[]string{a, "cas", "greeting", "0", "again"}, exitNotFound, "", `not set: key "greeting" is at version 5`},
		{[]string{a, "cas", "other", "3", "x"}, exitNotFound, "", `not set: key "other" does not exist`},
		{[]string{a, "cas", "greeting", "5", "newer"}, exitOK, "OK (version 6)\n", ""},
		{[]string{a, "cas", "greeting", "five", "x"}, exitError, "", `expected-version "five" is not a version number`},
		{[]string{a, "--stale", "--min-version=6", "get", "greeting"}, exitOK, "newer\n", ""},
	}
	for _, s := range steps {
		r := ctl(t, nil, s.args...)
		if r.code != s.code || r.stdout != s.stdout || !strings.Contains(r.stderr, s.stderrPart) {
			t.Fatalf("dkvctl %q = exit %d, stdout %q, stderr %q; want exit %d, stdout %q, stderr containing %q",
				s.args[1:], r.code, r.stdout, r.stderr, s.code, s.stdout, s.stderrPart)
		}
	}
}

func TestAddressFromEnvironment(t *testing.T) {
	node := servertest.Start(t, nil)
	r := ctl(t, map[string]string{addrEnv: node.Addr}, "put", "k", "v")
	if r.code != exitOK {
		t.Fatalf("put with %s set: exit %d, stderr %q", addrEnv, r.code, r.stderr)
	}
	// The flag wins over the environment.
	r = ctl(t, map[string]string{addrEnv: "127.0.0.1:1"}, "--addr="+node.Addr, "get", "k")
	if r.code != exitOK || r.stdout != "v\n" {
		t.Fatalf("--addr did not override %s: %+v", addrEnv, r)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "missing command"},
		{[]string{"--addr=x:1", "frobnicate"}, `unknown command "frobnicate"`},
		{[]string{"--addr=x:1", "put", "k"}, "put takes 2 argument(s), got 1"},
		{[]string{"--addr=x:1", "get"}, "get takes 1 argument(s), got 0"},
		{[]string{"get", "k"}, "no server address"},
		{[]string{"--bogus"}, "flag provided but not defined"},
	} {
		r := ctl(t, nil, tc.args...)
		if r.code != exitError || !strings.Contains(r.stderr, tc.want) {
			t.Errorf("dkvctl %q: exit %d, stderr %q; want exit 2 mentioning %q", tc.args, r.code, r.stderr, tc.want)
		}
	}
	if r := ctl(t, nil, "-h"); r.code != exitOK || !strings.Contains(r.stderr, "Commands:") {
		t.Errorf("dkvctl -h: %+v", r)
	}
}

func TestServerErrorsExitWithError(t *testing.T) {
	node := servertest.Start(t, func(c *config.Config) { c.Limits.MaxKeyBytes = 3 })
	r := ctl(t, nil, "--addr="+node.Addr, "put", "toolong", "v")
	if r.code != exitError || !strings.Contains(r.stderr, "InvalidArgument") {
		t.Fatalf("put with oversized key: %+v; want exit 2 with InvalidArgument", r)
	}
	// A listener that never speaks gRPC: the request must time out.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lis.Close() }()
	r = ctl(t, nil, "--addr="+lis.Addr().String(), "--timeout=500ms", "get", "k")
	if r.code != exitError || !strings.Contains(r.stderr, "DeadlineExceeded") {
		t.Fatalf("get against unresponsive server: %+v; want exit 2 with DeadlineExceeded", r)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestOutputWriteFailureIsAnError(t *testing.T) {
	node := servertest.Start(t, nil)
	if r := ctl(t, nil, "--addr="+node.Addr, "put", "k", "v"); r.code != exitOK {
		t.Fatalf("put: %+v", r)
	}
	var errOut strings.Builder
	code := run(context.Background(), []string{"--addr=" + node.Addr, "get", "k"},
		func(string) string { return "" }, failingWriter{}, &errOut)
	if code != exitError || !strings.Contains(errOut.String(), "writing output: disk full") {
		t.Fatalf("get with failing stdout: exit %d, stderr %q; want exit 2 reporting the write error", code, errOut.String())
	}
}

func TestStatusStandalone(t *testing.T) {
	node := servertest.Start(t, nil)
	r := ctl(t, nil, "--addr="+node.Addr, "status")
	if r.code != exitOK {
		t.Fatalf("status: exit %d, stderr %q", r.code, r.stderr)
	}
	lines := strings.Split(strings.TrimSpace(r.stdout), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "ADDRESS") {
		t.Fatalf("status output:\n%s", r.stdout)
	}
	for _, want := range []string{"test-node", "UP", "(this node)"} {
		if !strings.Contains(lines[1], want) {
			t.Errorf("member line %q does not contain %q", lines[1], want)
		}
	}
	if r := ctl(t, nil, "--addr="+node.Addr, "status", "extra"); r.code != exitError {
		t.Errorf("status with an argument: exit %d", r.code)
	}
	if r := ctl(t, nil, "--addr="+node.Addr, "decommission", "abc"); r.code != exitError || !strings.Contains(r.stderr, "standalone") {
		t.Errorf("decommission on a standalone node: exit %d, %q", r.code, r.stderr)
	}
}

func TestShardsStandalone(t *testing.T) {
	node := servertest.Start(t, nil)
	r := ctl(t, nil, "--addr="+node.Addr, "put", "k", "v")
	if r.code != exitOK {
		t.Fatal(r.stderr)
	}
	r = ctl(t, nil, "--addr="+node.Addr, "shards")
	if r.code != exitOK || !strings.HasPrefix(r.stdout, "1 shards, replication factor 1\n") {
		t.Fatalf("shards: exit %d\n%s%s", r.code, r.stdout, r.stderr)
	}
	lines := strings.Split(strings.TrimSpace(r.stdout), "\n")
	if len(lines) != 3 || !strings.Contains(lines[2], "yes") || !strings.HasSuffix(strings.TrimSpace(lines[2]), "1") {
		t.Errorf("shards output:\n%s", r.stdout)
	}
}

// TestJSONOutput: --json prints one JSON object per command, including for
// a missing key and a failed compare-and-set, with the usual exit codes;
// --consistency selects stale reads.
func TestJSONOutput(t *testing.T) {
	node := servertest.Start(t, nil)
	a := "--addr=" + node.Addr
	for _, s := range []struct {
		args   []string
		code   int
		stdout string
	}{
		{[]string{a, "--json", "get", "k"}, exitNotFound, `{"found":false,"key":"k"}` + "\n"},
		{[]string{a, "--json", "put", "k", "v1"}, exitOK, `{"ok":true,"version":1}` + "\n"},
		{[]string{a, "--json", "get", "k"}, exitOK, `{"found":true,"key":"k","value":"v1","version":1}` + "\n"},
		{[]string{a, "--json", "put", "bin", "\xff\x00"}, exitOK, `{"ok":true,"version":2}` + "\n"},
		{[]string{a, "--json", "get", "bin"}, exitOK, `{"found":true,"key":"bin","value_base64":"/wA=","version":2}` + "\n"},
		{[]string{a, "--json", "cas", "k", "7", "x"}, exitNotFound, `{"succeeded":false,"version":1}` + "\n"},
		{[]string{a, "--json", "cas", "k", "1", "v2"}, exitOK, `{"succeeded":true,"version":3}` + "\n"},
		{[]string{a, "--json", "delete", "k"}, exitOK, `{"found":true,"ok":true,"version":4}` + "\n"},
		{[]string{a, "--consistency=stale", "--min-version=2", "get", "bin"}, exitOK, "\xff\x00\n"},
	} {
		if r := ctl(t, nil, s.args...); r.code != s.code || r.stdout != s.stdout {
			t.Fatalf("dkvctl %q = exit %d, stdout %q, stderr %q; want exit %d, stdout %q", s.args[1:], r.code, r.stdout, r.stderr, s.code, s.stdout)
		}
	}
	r := ctl(t, nil, a, "--json", "shards")
	var sm dkvv1.ShardMapResponse
	if r.code != exitOK || protojson.Unmarshal([]byte(r.stdout), &sm) != nil || sm.GetShardCount() != 1 {
		t.Fatalf("shards --json: %+v", r)
	}
	for _, args := range [][]string{
		{a, "--consistency=eventual", "get", "k"},
		{a, "transfer-leader", "zero", "n1"},
	} {
		if r := ctl(t, nil, args...); r.code != exitError || !strings.Contains(r.stderr, "Usage") {
			t.Fatalf("dkvctl %q = %+v; want a usage error", args[1:], r)
		}
	}
	// A standalone node has no leadership to move.
	for _, args := range [][]string{{a, "drain", "test-node"}, {a, "transfer-leader", "0", "test-node"}} {
		if r := ctl(t, nil, args...); r.code != exitError || !strings.Contains(r.stderr, "FailedPrecondition") {
			t.Fatalf("dkvctl %q = %+v; want a FailedPrecondition error", args[1:], r)
		}
	}
}

// TestClusterCommands: leaders merges every node's view; transfer-leader,
// drain and undrain move leadership as asked.
func TestClusterCommands(t *testing.T) {
	c := nodetest.Start(t, 3, func(_ int, cfg *config.Config) { cfg.Cluster.Shards = 3 })
	nodetest.WaitFor(t, 30*time.Second, "every group to elect a leader", c.Settled)
	nodetest.WaitFor(t, 30*time.Second, "the meta group to record every node", func() bool { return metaKnowsAll(c) })
	var addrs []string
	for _, m := range c.Members {
		addrs = append(addrs, m.PublicAddr)
	}
	a := "--addr=" + strings.Join(addrs, ",")
	leaders := func() map[int32]string {
		t.Helper()
		r := ctl(t, nil, a, "--json", "leaders")
		var ls []leaderInfo
		if r.code != exitOK || json.Unmarshal([]byte(r.stdout), &ls) != nil {
			t.Fatalf("leaders --json: %+v", r)
		}
		out := map[int32]string{}
		for _, l := range ls {
			out[l.Shard] = l.Leader
		}
		return out
	}
	ls := leaders()
	if len(ls) != 3 || ls[0] == "" || ls[1] == "" || ls[2] == "" {
		t.Fatalf("leaders %v; want one for each of 3 shards", ls)
	}
	if r := ctl(t, nil, a, "leaders"); r.code != exitOK || !strings.Contains(r.stdout, "SHARD") || strings.Contains(r.stdout, "unknown") {
		t.Fatalf("leaders: %+v", r)
	}
	var to string
	for _, m := range c.Members {
		if id := m.NodeID(); id != ls[0] {
			to = id
		}
	}
	if r := ctl(t, nil, a, "transfer-leader", "0", to); r.code != exitOK || !strings.Contains(r.stdout, "now led by "+to) {
		t.Fatalf("transfer-leader: %+v", r)
	}
	if got := leaders()[0]; got != to {
		t.Fatalf("shard 0 led by %s after transfer-leader to %s", got, to)
	}
	if r := ctl(t, nil, a, "--json", "drain", to); r.code != exitOK || !strings.Contains(r.stdout, `"drained":true`) {
		t.Fatalf("drain: %+v", r)
	}
	nodetest.WaitFor(t, 30*time.Second, "the drained node to lead no shard", func() bool {
		for _, l := range leaders() {
			if l == to {
				return false
			}
		}
		return true
	})
	if r := ctl(t, nil, a, "undrain", to); r.code != exitOK || !strings.Contains(r.stdout, "may lead again") {
		t.Fatalf("undrain: %+v", r)
	}
}

// TestLeadersWithDeadNodesFirst: leaders asks every node at once, each for
// up to --timeout, so dead nodes listed first in --addr hide no other
// node's view: one that accepts connections but never answers (a stopped
// container behind Docker's port proxy) and one that refuses them (a
// stopped process). Both are reported as unreachable; the table still shows
// every shard's leader, as the live nodes see it.
func TestLeadersWithDeadNodesFirst(t *testing.T) {
	c := nodetest.Start(t, 3, func(_ int, cfg *config.Config) { cfg.Cluster.Shards = 3 })
	nodetest.WaitFor(t, 30*time.Second, "every group to elect a leader", c.Settled)
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = silent.Close() }()
	stoppedID, stoppedAddr := c.Members[2].NodeID(), c.Members[2].PublicAddr
	c.Stop(2)
	live := c.Members[0].PublicAddr + "," + c.Members[1].PublicAddr
	leaders := func(r result) []leaderInfo {
		var ls []leaderInfo
		if r.code != exitOK || json.Unmarshal([]byte(r.stdout), &ls) != nil {
			return nil
		}
		return ls
	}
	ledByLive := func(ls []leaderInfo) bool {
		for _, l := range ls {
			if l.Leader == "" || l.Leader == stoppedID {
				return false
			}
		}
		return len(ls) == 3
	}
	nodetest.WaitFor(t, 30*time.Second, "the two live nodes to lead every shard", func() bool {
		return ledByLive(leaders(ctl(t, nil, "--addr="+live, "--json", "leaders")))
	})

	const timeout = 2 * time.Second
	args := []string{"--addr=" + silent.Addr().String() + "," + stoppedAddr + "," + live, "--timeout=" + timeout.String()}
	start := time.Now()
	r := ctl(t, nil, append(args, "leaders")...)
	elapsed := time.Since(start)
	if r.code != exitOK {
		t.Fatalf("leaders with dead nodes first: exit %d\nstdout:\n%sstderr:\n%s", r.code, r.stdout, r.stderr)
	}
	if lines := strings.Split(strings.TrimSpace(r.stdout), "\n"); len(lines) != 4 || strings.Contains(r.stdout, "unknown") {
		t.Errorf("leaders with dead nodes first: want a leader for each of 3 shards:\n%s", r.stdout)
	}
	for _, dead := range []string{silent.Addr().String(), stoppedAddr} {
		if !strings.Contains(r.stderr, dead+" unreachable") {
			t.Errorf("stderr does not report %s as unreachable:\n%s", dead, r.stderr)
		}
	}
	if elapsed > timeout+time.Second {
		t.Errorf("leaders took %v; every node is asked at once, each for up to %v", elapsed, timeout)
	}

	r = ctl(t, nil, append(args, "--json", "leaders")...)
	if ls := leaders(r); !ledByLive(ls) || !strings.Contains(r.stderr, silent.Addr().String()+" unreachable") {
		t.Errorf("leaders --json with dead nodes first: exit %d, leaders %+v\nstderr:\n%s", r.code, ls, r.stderr)
	}
}

// TestAdminCommandsMoveOnFromDeadAddresses: status, shards, transfer-leader,
// drain, undrain and decommission succeed with two dead addresses first in
// --addr, as its help says: one that accepts connections and never answers
// (a stopped container behind Docker's port proxy) and one that resets
// them (a stopped process). Each command starts again at the first address.
func TestAdminCommandsMoveOnFromDeadAddresses(t *testing.T) {
	c := nodetest.Start(t, 3, func(_ int, cfg *config.Config) { cfg.Cluster.Shards = 3 })
	nodetest.WaitFor(t, 30*time.Second, "every group to elect a leader", c.Settled)
	nodetest.WaitFor(t, 30*time.Second, "the meta group to record every node", func() bool { return metaKnowsAll(c) })
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = silent.Close() }()
	var live []string
	for _, m := range c.Members {
		live = append(live, m.PublicAddr)
	}
	a := "--addr=" + strings.Join(append([]string{silent.Addr().String(), porttest.Refusing(t)}, live...), ",")

	// With the default 5 s and five addresses, the silent one has a fifth.
	start := time.Now()
	r := ctl(t, nil, a, "status")
	if lines := strings.Split(strings.TrimSpace(r.stdout), "\n"); r.code != exitOK || len(lines) != 4 {
		t.Fatalf("status with dead addresses first: exit %d\n%s%s", r.code, r.stdout, r.stderr)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("status took %v; the silent address's share is 1 s", elapsed)
	}
	if r := ctl(t, nil, a, "shards"); r.code != exitOK || !strings.HasPrefix(r.stdout, "3 shards, replication factor 3\n") {
		t.Fatalf("shards with dead addresses first: exit %d\n%s%s", r.code, r.stdout, r.stderr)
	}

	r = ctl(t, nil, "--addr="+strings.Join(live, ","), "--json", "leaders")
	var ls []leaderInfo
	if r.code != exitOK || json.Unmarshal([]byte(r.stdout), &ls) != nil || len(ls) != 3 || ls[0].Leader == "" {
		t.Fatalf("leaders: %+v", r)
	}
	var to string
	for _, m := range c.Members {
		if id := m.NodeID(); id != ls[0].Leader {
			to = id
		}
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"transfer-leader", "0", to}, "shard 0 is now led by " + to},
		{[]string{"drain", to}, "node " + to + " is drained"},
		{[]string{"undrain", to}, "node " + to + " may lead again"},
		{[]string{"decommission", to}, "node " + to + " is being decommissioned"},
	} {
		if r := ctl(t, nil, append([]string{a}, tc.args...)...); r.code != exitOK || !strings.Contains(r.stdout, tc.want) {
			t.Errorf("%v with dead addresses first: exit %d, stdout %q, stderr %q", tc.args, r.code, r.stdout, r.stderr)
		}
	}
}

// metaKnowsAll reports whether node 0 has read every member's record from
// the meta group; until then, commands naming a node (drain) find none.
func metaKnowsAll(c *nodetest.Cluster) bool {
	st, err := c.Status(0)
	if err != nil {
		return false
	}
	known := 0
	for _, m := range st.GetMembers() {
		if m.GetNodeState() == dkvv1.NodeState_NODE_STATE_ALIVE {
			known++
		}
	}
	return known == len(c.Members)
}
