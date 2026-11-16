package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"go.uber.org/goleak"

	"github.com/naveenk2608/distributed-database/internal/config"
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
}
