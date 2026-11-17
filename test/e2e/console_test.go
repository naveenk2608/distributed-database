package e2e

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestConsoleWatchesRealProcesses: three dkv-server processes serve their
// admin HTTP API (server.admin_addr) and a dkv-console process, configured
// by flags alone, polls them, serves their overview, and the dashboard's
// files with its routes; it stops cleanly on SIGTERM.
func TestConsoleWatchesRealProcesses(t *testing.T) {
	serverBin, _ := binaries(t)
	consoleBin, err := build(buildDir, "github.com/naveenk2608/distributed-database/cmd/dkv-console")
	if err != nil {
		t.Fatal(err)
	}
	addrs := freeAddrs(t, 7)
	internal, admin, listen := addrs[:3], addrs[3:6], addrs[6]
	for i := range 3 {
		startServer(t, serverBin, t.TempDir(), false, "--cluster.internal-addr="+internal[i],
			"--cluster.peers="+strings.Join(internal, ","), "--server.admin-addr="+admin[i])
	}
	web := t.TempDir()
	if err := os.WriteFile(filepath.Join(web, "index.html"), []byte("<html>dkv dashboard test page</html>"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		args []string
		code int
	}{{[]string{"-h"}, 0}, {[]string{"--console.listen-addr=" + listen}, 2}} {
		cmd := exec.Command(consoleBin, tc.args...)
		cmd.Env = []string{}
		if err := cmd.Run(); exitCode(err) != tc.code {
			t.Fatalf("dkv-console %v: %v; want exit status %d", tc.args, err, tc.code)
		}
	}

	cmd := exec.Command(consoleBin, "--console.listen-addr="+listen, "--console.nodes="+strings.Join(admin, ","),
		"--console.poll-interval=300ms", "--console.poll-timeout=250ms", "--console.web-dir="+web, "--log.format=json")
	cmd.Env = []string{}
	var logs strings.Builder
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case err := <-exited:
			_ = err // already reported, or a kill at cleanup
		case <-time.After(10 * time.Second):
		}
	})

	get := func(url string) (int, string) {
		resp, err := http.Get(url) //nolint:noctx // a test against local processes
		if err != nil {
			return 0, err.Error()
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	base := "http://" + listen
	deadline := time.Now().Add(60 * time.Second)
	for {
		code, body := get(base + "/api/overview")
		var ov struct {
			Nodes []struct {
				Up     bool   `json:"up"`
				NodeID string `json:"node_id"`
			} `json:"nodes"`
			Shards []struct {
				Leader string `json:"leader"`
			} `json:"shards"`
		}
		if code == http.StatusOK && json.Unmarshal([]byte(body), &ov) == nil && len(ov.Nodes) == 3 &&
			ov.Nodes[0].Up && ov.Nodes[1].Up && ov.Nodes[2].Up && len(ov.Shards) > 0 && ov.Shards[0].Leader != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the console never saw three nodes up and a leader: %d %s\n%s", code, body, logs.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, path := range []string{"/", "/shards", "/events"} {
		if code, body := get(base + path); code != http.StatusOK || !strings.Contains(body, "dkv dashboard test page") {
			t.Fatalf("GET %s: %d %q", path, code, body)
		}
	}
	if code, body := get("http://" + admin[0] + "/metrics"); code != http.StatusOK || !strings.Contains(body, "dkv_raft_term") {
		t.Fatalf("a node's /metrics: %d", code)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("console exit after SIGTERM: %v\n%s", err, logs.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("console did not stop after SIGTERM\n%s", logs.String())
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	if ee := (*exec.ExitError)(nil); errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
}
