// Package e2e runs the real dkv binaries as separate processes.
package e2e

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func build(t *testing.T, dir, pkg string) string {
	t.Helper()
	out := filepath.Join(dir, filepath.Base(pkg))
	cmd := exec.Command("go", "build", "-race", "-o", out, pkg)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s", pkg, err, b)
	}
	return out
}

// TestDkvctlAgainstRunningServer is the Phase 1 gate check that dkvctl put,
// get and delete work against a running dkv-server process.
func TestDkvctlAgainstRunningServer(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs binaries")
	}
	bin := t.TempDir()
	serverBin := build(t, bin, "github.com/naveenk2608/distributed-database/cmd/dkv-server")
	ctlBin := build(t, bin, "github.com/naveenk2608/distributed-database/cmd/dkvctl")

	srv := exec.Command(serverBin, "--server.grpc-addr=127.0.0.1:0", "--log.format=json")
	srv.Env = []string{} // the test must not depend on the caller's DKV_* variables
	stderr, err := srv.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	// The reader goroutine owns logs until waitDone is closed.
	var logs bytes.Buffer
	var waitErr error
	waitDone := make(chan struct{})
	addrCh := make(chan string, 1)
	go func() {
		defer close(waitDone)
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			logs.Write(append(sc.Bytes(), '\n'))
			var rec struct{ Msg, Addr string }
			if json.Unmarshal(sc.Bytes(), &rec) == nil && rec.Msg == "serving KV API" {
				addrCh <- rec.Addr
			}
		}
		waitErr = srv.Wait()
	}()
	stopServer := func() {
		_ = srv.Process.Kill() // no-op if it already exited
		<-waitDone
	}
	t.Cleanup(stopServer)

	var addr string
	select {
	case addr = <-addrCh:
	case <-waitDone:
		t.Fatalf("server exited early: %v\n%s", waitErr, logs.String())
	case <-time.After(30 * time.Second):
		stopServer()
		t.Fatalf("server did not report its address\n%s", logs.String())
	}

	ctl := func(wantCode int, wantOut string, args ...string) {
		t.Helper()
		cmd := exec.Command(ctlBin, append([]string{"--addr=" + addr}, args...)...)
		cmd.Env = []string{}
		var out, errOut bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &errOut
		err := cmd.Run()
		code := 0
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		} else if err != nil {
			t.Fatalf("dkvctl %q: %v", args, err)
		}
		if code != wantCode || out.String() != wantOut {
			t.Fatalf("dkvctl %q: exit %d, stdout %q, stderr %q; want exit %d, stdout %q",
				args, code, out.String(), errOut.String(), wantCode, wantOut)
		}
		t.Logf("$ dkvctl %q -> exit %d %q", args, code, out.String())
	}

	ctl(0, "OK (version 1)\n", "put", "city", "Chennai")
	ctl(0, "Chennai\n", "get", "city")
	ctl(0, "OK (version 2)\n", "put", "city", "Madurai")
	ctl(0, "Madurai\n", "get", "city")
	ctl(0, "deleted (version 3)\n", "delete", "city")
	ctl(1, "", "get", "city")
	ctl(0, "key did not exist (version 4)\n", "delete", "city")

	if err := srv.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waitDone:
		if waitErr != nil {
			t.Fatalf("server exit after SIGTERM: %v\n%s", waitErr, logs.String())
		}
	case <-time.After(30 * time.Second):
		stopServer()
		t.Fatalf("server did not stop after SIGTERM\n%s", logs.String())
	}
	t.Logf("server logs:\n%s", logs.String())
}
