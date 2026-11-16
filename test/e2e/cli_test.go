// Package e2e runs the real dkv binaries as separate processes.
package e2e

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func build(dir, pkg string) (string, error) {
	out := filepath.Join(dir, filepath.Base(pkg))
	cmd := exec.Command("go", "build", "-race", "-o", out, pkg)
	if b, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("go build %s: %w\n%s", pkg, err, b)
	}
	return out, nil
}

var (
	buildOnce         sync.Once
	serverBin, ctlBin string
	buildErr          error
	buildDir          string
)

// binaries builds dkv-server and dkvctl once per test binary run.
func binaries(t *testing.T) (server, ctl string) {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs binaries")
	}
	buildOnce.Do(func() {
		buildDir, buildErr = os.MkdirTemp("", "dkv-e2e-bin-")
		if buildErr != nil {
			return
		}
		serverBin, buildErr = build(buildDir, "github.com/naveenk2608/distributed-database/cmd/dkv-server")
		if buildErr != nil {
			return
		}
		ctlBin, buildErr = build(buildDir, "github.com/naveenk2608/distributed-database/cmd/dkvctl")
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return serverBin, ctlBin
}

func TestMain(m *testing.M) {
	code := m.Run()
	if buildDir != "" {
		_ = os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

// serverProc is a running dkv-server process.
type serverProc struct {
	t        *testing.T
	cmd      *exec.Cmd
	addr     string
	logs     *bytes.Buffer // owned by the reader goroutine until waitDone closes
	waitErr  error
	waitDone chan struct{}
}

// startServer starts dkv-server on a free port with dataDir and waits for
// it to report its address. If wantExit is true it instead waits for the
// process to exit and returns it.
func startServer(t *testing.T, bin, dataDir string, wantExit bool) *serverProc {
	t.Helper()
	cmd := exec.Command(bin, "--server.grpc-addr=127.0.0.1:0", "--log.format=json", "--storage.dir="+dataDir)
	cmd.Env = []string{} // the test must not depend on the caller's DKV_* variables
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &serverProc{t: t, cmd: cmd, logs: &bytes.Buffer{}, waitDone: make(chan struct{})}
	addrCh := make(chan string, 1)
	go func() {
		defer close(p.waitDone)
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			p.logs.Write(append(sc.Bytes(), '\n'))
			var rec struct{ Msg, Addr string }
			if json.Unmarshal(sc.Bytes(), &rec) == nil && rec.Msg == "serving KV API" {
				addrCh <- rec.Addr
			}
		}
		p.waitErr = cmd.Wait()
	}()
	t.Cleanup(p.kill)
	if wantExit {
		select {
		case <-p.waitDone:
		case <-time.After(30 * time.Second):
			p.kill()
			t.Fatalf("server did not exit\n%s", p.logs.String())
		}
		return p
	}
	select {
	case p.addr = <-addrCh:
	case <-p.waitDone:
		t.Fatalf("server exited early: %v\n%s", p.waitErr, p.logs.String())
	case <-time.After(30 * time.Second):
		p.kill()
		t.Fatalf("server did not report its address\n%s", p.logs.String())
	}
	return p
}

// kill sends SIGKILL (a no-op if the process already exited) and waits.
func (p *serverProc) kill() {
	_ = p.cmd.Process.Kill()
	<-p.waitDone
}

// stop sends SIGTERM and requires a clean exit.
func (p *serverProc) stop() {
	p.t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		p.t.Fatal(err)
	}
	select {
	case <-p.waitDone:
		if p.waitErr != nil {
			p.t.Fatalf("server exit after SIGTERM: %v\n%s", p.waitErr, p.logs.String())
		}
	case <-time.After(30 * time.Second):
		p.kill()
		p.t.Fatalf("server did not stop after SIGTERM\n%s", p.logs.String())
	}
}

// ctl runs dkvctl against addr and checks its exit code and stdout.
func ctl(t *testing.T, bin, addr string, wantCode int, wantOut string, args ...string) {
	t.Helper()
	cmd := exec.Command(bin, append([]string{"--addr=" + addr}, args...)...)
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

// TestDkvctlAgainstRunningServer is the Phase 1 gate check that dkvctl put,
// get and delete work against a running dkv-server process.
func TestDkvctlAgainstRunningServer(t *testing.T) {
	serverBin, ctlBin := binaries(t)
	srv := startServer(t, serverBin, t.TempDir(), false)
	ctl(t, ctlBin, srv.addr, 0, "OK (version 1)\n", "put", "city", "Chennai")
	ctl(t, ctlBin, srv.addr, 0, "Chennai\n", "get", "city")
	ctl(t, ctlBin, srv.addr, 0, "OK (version 2)\n", "put", "city", "Madurai")
	ctl(t, ctlBin, srv.addr, 0, "Madurai\n", "get", "city")
	ctl(t, ctlBin, srv.addr, 0, "deleted (version 3)\n", "delete", "city")
	ctl(t, ctlBin, srv.addr, 1, "", "get", "city")
	ctl(t, ctlBin, srv.addr, 0, "key did not exist (version 4)\n", "delete", "city")
	srv.stop()
	t.Logf("server logs:\n%s", srv.logs.String())
}

// TestDataSurvivesKill9AndRestart checks persistence through the real
// binaries: writes acknowledged before a SIGKILL are there after a restart,
// and versions keep increasing from where they were.
func TestDataSurvivesKill9AndRestart(t *testing.T) {
	serverBin, ctlBin := binaries(t)
	data := t.TempDir()
	srv := startServer(t, serverBin, data, false)
	ctl(t, ctlBin, srv.addr, 0, "OK (version 1)\n", "put", "a", "1")
	ctl(t, ctlBin, srv.addr, 0, "OK (version 2)\n", "put", "b", "2")
	ctl(t, ctlBin, srv.addr, 0, "deleted (version 3)\n", "delete", "a")
	srv.kill()

	srv = startServer(t, serverBin, data, false)
	ctl(t, ctlBin, srv.addr, 1, "", "get", "a")
	ctl(t, ctlBin, srv.addr, 0, "2\n", "get", "b")
	ctl(t, ctlBin, srv.addr, 0, "OK (version 4)\n", "put", "c", "3")
	srv.stop()

	srv = startServer(t, serverBin, data, false)
	ctl(t, ctlBin, srv.addr, 0, "3\n", "get", "c")
	srv.stop()
}

// TestCorruptLogStopsStartup damages a WAL record that has valid records
// after it. dkv-server must refuse to start, exit non-zero, and say why.
func TestCorruptLogStopsStartup(t *testing.T) {
	serverBin, ctlBin := binaries(t)
	data := t.TempDir()
	srv := startServer(t, serverBin, data, false)
	for i := range 20 {
		ctl(t, ctlBin, srv.addr, 0, fmt.Sprintf("OK (version %d)\n", i+1), "put", fmt.Sprint("key", i), strings.Repeat("v", 50))
	}
	srv.stop()

	segs, err := filepath.Glob(filepath.Join(data, "*.wal"))
	if err != nil || len(segs) == 0 {
		t.Fatalf("no WAL segments in %s: %v", data, err)
	}
	slices.Sort(segs)
	var target string
	for _, s := range segs {
		if fi, err := os.Stat(s); err == nil && fi.Size() > 1000 {
			target = s
		}
	}
	if target == "" {
		t.Fatalf("no segment holds the writes: %v", segs)
	}
	b, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xff
	if err := os.WriteFile(target, b, 0o644); err != nil {
		t.Fatal(err)
	}

	srv = startServer(t, serverBin, data, true)
	var exitErr *exec.ExitError
	if !errors.As(srv.waitErr, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("dkv-server on a corrupt log exited with %v, want exit status 1\n%s", srv.waitErr, srv.logs.String())
	}
	logs := srv.logs.String()
	for _, want := range []string{"opening storage engine failed", "data corruption", filepath.Base(target), "offset", "refusing to start"} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs do not mention %q:\n%s", want, logs)
		}
	}
	if strings.Contains(logs, "serving KV API") {
		t.Error("server accepted connections on a corrupt log")
	}
}
