package e2e

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/naveenk2608/distributed-database/internal/porttest"
)

// startCluster starts n dkv-server processes that bootstrap one cluster.
// Their internal addresses form the peer list, so they are chosen before
// the processes bind them: outside the kernel's ephemeral range
// (porttest.Pick), where no connection's local port can take them, and all
// chosen again, with fresh data directories, if a process still finds its
// address taken. extra gives process i's further flags. It returns the
// processes, their internal addresses and their data directories.
func startCluster(t *testing.T, bin string, n int, extra func(i int) []string) (procs []*serverProc, internal, dirs []string) {
	t.Helper()
	for attempt := 1; ; attempt++ {
		internal, dirs, procs = make([]string, n), make([]string, n), nil
		for i := range n {
			internal[i], dirs[i] = porttest.Pick(t), t.TempDir()
		}
		var err error
		for i := range n {
			var p *serverProc
			if p, err = launchServer(t, bin, dirs[i], clusterArgs(internal, i, extra(i))...); err != nil {
				break
			}
			procs = append(procs, p)
		}
		if err == nil {
			return procs, internal, dirs
		}
		for _, p := range procs {
			p.kill()
		}
		if !addrInUse(err) || attempt == 5 {
			t.Fatal(err)
		}
		t.Logf("attempt %d: an internal address was taken; choosing new ones", attempt)
	}
}

// clusterArgs is process i's flags in a cluster of the given internal
// addresses: its own address, the peer list and extra.
func clusterArgs(internal []string, i int, extra []string) []string {
	return append([]string{"--cluster.internal-addr=" + internal[i], "--cluster.peers=" + strings.Join(internal, ",")}, extra...)
}

// restartServer starts a stopped cluster process again on its data
// directory with the same flags, so on the same internal address, retrying
// for a while if that address is still in use.
func restartServer(t *testing.T, bin, dir string, args ...string) *serverProc {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		p, err := launchServer(t, bin, dir, args...)
		if err == nil {
			return p
		}
		if !addrInUse(err) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
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
	const window = 2 * time.Second
	procs, internal, _ := startCluster(t, serverBin, 3, func(int) []string {
		return []string{"--cluster.ping-interval=200ms", "--cluster.ping-timeout=200ms", "--cluster.down-after=" + window.String()}
	})

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
