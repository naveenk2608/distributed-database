package e2e

import (
	"bytes"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// freeAddrs returns n loopback addresses that were free a moment ago.
func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	var out []string
	var ls []net.Listener
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ls = append(ls, l)
		out = append(out, l.Addr().String())
	}
	for _, l := range ls {
		_ = l.Close()
	}
	return out
}

// memberStates runs dkvctl status against addr and returns each member's
// state by internal address.
func memberStates(t *testing.T, ctlBin, addr string) map[string]string {
	t.Helper()
	cmd := exec.Command(ctlBin, "--addr="+addr, "--timeout=2s", "status")
	cmd.Env = []string{}
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil
	}
	states := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n")[1:] {
		if f := strings.Fields(line); len(f) >= 3 {
			states[f[0]] = f[2]
		}
	}
	return states
}

// TestThreeProcessesFormACluster runs three real dkv-server processes from
// one bootstrap peer list, checks with dkvctl that each sees all three up,
// then kills one with SIGKILL and checks the others report it down. It
// stands in for the Docker Compose check where Docker is unavailable.
func TestThreeProcessesFormACluster(t *testing.T) {
	serverBin, ctlBin := binaries(t)
	internal := freeAddrs(t, 3)
	peers := strings.Join(internal, ",")
	const window = 2 * time.Second
	var procs []*serverProc
	for i := range 3 {
		procs = append(procs, startServer(t, serverBin, t.TempDir(), false,
			"--cluster.internal-addr="+internal[i], "--cluster.peers="+peers,
			"--cluster.ping-interval=200ms", "--cluster.ping-timeout=200ms", "--cluster.down-after="+window.String()))
	}

	deadline := time.Now().Add(30 * time.Second)
	for i := 0; i < 3; {
		st := memberStates(t, ctlBin, procs[i].addr)
		if st[internal[0]] == "UP" && st[internal[1]] == "UP" && st[internal[2]] == "UP" {
			i++
			continue
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d never saw all three nodes up: %v", i, st)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("all three processes see each other up")

	// Any process accepts any key: write through one, read through the
	// others, for keys spread over many shards.
	for k := range 12 {
		// Versions count per shard, so only check that the put succeeded.
		key := fmt.Sprintf("city-%d", k)
		cmd := exec.Command(ctlBin, "--addr="+procs[k%3].addr, "put", key, "v"+key)
		cmd.Env = []string{}
		if out, err := cmd.Output(); err != nil || !strings.HasPrefix(string(out), "OK (version ") {
			t.Fatalf("put %s via node %d: %q, %v", key, k%3, out, err)
		}
	}
	for k := range 12 {
		key := fmt.Sprintf("city-%d", k)
		ctl(t, ctlBin, procs[(k+1)%3].addr, 0, "v"+key+"\n", "get", key)
	}

	procs[2].kill()
	killed := time.Now()
	for {
		st := memberStates(t, ctlBin, procs[0].addr)
		if st[internal[2]] == "DOWN" {
			if st[internal[1]] != "UP" {
				t.Errorf("surviving node 1 is %s", st[internal[1]])
			}
			break
		}
		if time.Since(killed) > window+10*time.Second {
			t.Fatalf("node 2 still %q long after SIGKILL", st[internal[2]])
		}
		time.Sleep(50 * time.Millisecond)
	}
	took := time.Since(killed)
	t.Logf("node 0 reported the killed node down %v after SIGKILL (window %v; includes dkvctl process start-up)",
		took.Round(time.Millisecond), window)
	// Each dkvctl run is a separate race-instrumented process, so allow
	// generous slack; the in-process test measures the window precisely.
	if took > window+3*time.Second {
		t.Errorf("detection took %v with a %v window", took, window)
	}
	for _, p := range procs[:2] {
		p.stop()
	}
}
