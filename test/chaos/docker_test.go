package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
)

// toolsImage holds iptables and tc; see deploy/docker/nettools.Dockerfile.
const toolsImage = "dkv-nettools:dev"

// cluster is a Compose project of dkv nodes, driven through the docker
// CLI. Services, their published public ports and the internal port all
// come from the Compose files.
type cluster struct {
	t        *testing.T
	files    []string
	project  string
	services []string          // sorted
	public   map[string]string // service -> host:port of its public API

	mu      sync.Mutex
	stopped map[string]bool // killed or stopped, not yet started again
	frozen  map[string]bool // SIGSTOPped
}

func run(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

// docker runs a docker command, failing the test if it fails.
func (c *cluster) docker(args ...string) string {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	out, err := run(ctx, "docker", args...)
	if err != nil {
		c.t.Fatal(err)
	}
	return out
}

func (c *cluster) compose(args ...string) string {
	c.t.Helper()
	full := []string{"compose", "-p", c.project}
	for _, f := range c.files {
		full = append(full, "-f", f)
	}
	return c.docker(append(full, args...)...)
}

// up builds the images (once per process) and starts a fresh cluster,
// removed again at the end of the test.
func up(t *testing.T, files []string, project string, build bool) *cluster {
	t.Helper()
	c := &cluster{t: t, files: files, project: project, public: map[string]string{}, stopped: map[string]bool{}, frozen: map[string]bool{}}
	c.compose("down", "-v", "--remove-orphans")
	if build {
		c.docker("build", "-q", "-t", toolsImage, "-f", "../../deploy/docker/nettools.Dockerfile", "../../deploy/docker")
		c.compose("build", "-q")
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(c.compose("logs", "--no-color", "--tail", "200"))
		}
		c.compose("down", "-v", "--remove-orphans")
	})
	c.compose("up", "-d", "--no-build")

	var cfg struct {
		Services map[string]struct {
			Ports []struct {
				Target    int `json:"target"`
				Published any `json:"published"`
			} `json:"ports"`
		} `json:"services"`
	}
	if err := json.Unmarshal([]byte(c.compose("config", "--format", "json")), &cfg); err != nil {
		t.Fatal(err)
	}
	for name, s := range cfg.Services {
		if len(s.Ports) == 0 {
			continue
		}
		c.services = append(c.services, name)
		c.public[name] = fmt.Sprintf("localhost:%v", s.Ports[0].Published)
	}
	slices.Sort(c.services)
	if len(c.services) < 3 {
		t.Fatalf("chaos needs at least three nodes with a published port, got %v", c.services)
	}
	return c
}

func (c *cluster) addrs() []string {
	var out []string
	for _, s := range c.services {
		out = append(out, c.public[s])
	}
	return out
}

func (c *cluster) container(service string) string {
	return strings.TrimSpace(c.compose("ps", "-a", "-q", service))
}

// ip returns the service's address on the Compose network (it can change
// when a container restarts).
func (c *cluster) ip(service string) string {
	return strings.TrimSpace(c.docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", c.container(service)))
}

// kill sends SIGKILL: the process dies without any cleanup.
func (c *cluster) kill(service string) {
	c.docker("kill", "-s", "KILL", c.container(service))
	c.mu.Lock()
	c.stopped[service] = true
	c.mu.Unlock()
}

// stop sends SIGTERM and waits for a graceful exit (a rolling restart).
func (c *cluster) stop(service string) {
	c.docker("stop", "-t", "15", c.container(service))
	c.mu.Lock()
	c.stopped[service] = true
	c.mu.Unlock()
}

func (c *cluster) start(service string) {
	c.docker("start", c.container(service))
	c.mu.Lock()
	delete(c.stopped, service)
	c.mu.Unlock()
}

// freeze sends SIGSTOP: the process stops dead, sockets stay open, and
// it resumes on SIGCONT unaware that time passed.
func (c *cluster) freeze(service string, frozen bool) {
	sig := "CONT"
	if frozen {
		sig = "STOP"
	}
	c.docker("kill", "-s", sig, c.container(service))
	c.mu.Lock()
	c.frozen[service] = frozen
	c.mu.Unlock()
}

// wipe deletes a stopped node's data volume contents, as a replaced disk.
func (c *cluster) wipe(service string) {
	c.docker("run", "--rm", "--volumes-from", c.container(service), toolsImage,
		"sh", "-c", "rm -rf /data/* /data/.[!.]*")
}

// net runs a shell script in the service's network namespace with
// NET_ADMIN; $IF is its interface on the Compose network.
func (c *cluster) net(service, script string) {
	c.docker("run", "--rm", "--network", "container:"+c.container(service), "--cap-add", "NET_ADMIN", toolsImage,
		"sh", "-ec", `IF=$(ip -o route show default | awk '{print $5}'); `+script)
}

// partition cuts every node of each side off from the nodes of the other
// sides, in both directions. Clients on the host still reach every node.
func (c *cluster) partition(sides ...[]string) {
	ips := map[string]string{}
	for _, s := range c.services {
		ips[s] = c.ip(s)
	}
	for a, sa := range sides {
		for _, x := range sa {
			var rules []string
			for b, sb := range sides {
				if a == b {
					continue
				}
				for _, y := range sb {
					rules = append(rules,
						"iptables -A INPUT -s "+ips[y]+" -j DROP",
						"iptables -A OUTPUT -d "+ips[y]+" -j DROP")
				}
			}
			if len(rules) > 0 {
				c.net(x, strings.Join(rules, "; "))
			}
		}
	}
}

// block drops packets from one node to another. TCP needs both
// directions, so connections the other side opens stall too; what stays
// asymmetric is which side's packets are lost.
func (c *cluster) block(from, to string) {
	c.net(from, "iptables -A OUTPUT -d "+c.ip(to)+" -j DROP")
}

// netem gives every node's interface the netem discipline described by
// args (for example "delay 20ms 10ms loss 5%").
func (c *cluster) netem(args string) {
	for _, s := range c.services {
		c.net(s, "tc qdisc replace dev $IF root netem "+args)
	}
}

// heal removes every network fault from running nodes.
func (c *cluster) heal() {
	for _, s := range c.services {
		c.mu.Lock()
		down := c.stopped[s] || c.frozen[s]
		c.mu.Unlock()
		if !down {
			c.net(s, "iptables -F INPUT; iptables -F OUTPUT; tc qdisc del dev $IF root 2>/dev/null || true")
		}
	}
}

// recover heals the network and resumes or restarts every node.
func (c *cluster) recover() {
	c.mu.Lock()
	var frozen, stopped []string
	for s, f := range c.frozen {
		if f {
			frozen = append(frozen, s)
		}
	}
	for s := range c.stopped {
		stopped = append(stopped, s)
	}
	c.mu.Unlock()
	for _, s := range frozen {
		c.freeze(s, false)
	}
	for _, s := range stopped {
		c.start(s)
	}
	c.heal()
}

func (c *cluster) shardMap(service string) (*dkvv1.ShardMapResponse, error) {
	conn, err := grpc.NewClient(c.public[service], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return dkvv1.NewAdminServiceClient(conn).ShardMap(ctx, &dkvv1.ShardMapRequest{})
}

// leaderOf returns the service leading shard (by the highest term among
// running nodes that claim it), or "".
func (c *cluster) leaderOf(shard uint32) string {
	best, term := "", uint64(0)
	for _, s := range c.services {
		sm, err := c.shardMap(s)
		if err != nil {
			continue
		}
		for _, in := range sm.GetShards() {
			if in.GetShard() == shard && in.GetLocal() && in.GetRole() == "leader" && in.GetTerm() > term {
				best, term = s, in.GetTerm()
			}
		}
	}
	return best
}

// shardCount asks any node for the cluster's shard count.
func (c *cluster) shardCount() uint32 {
	for _, s := range c.services {
		if sm, err := c.shardMap(s); err == nil && sm.GetShardCount() > 0 {
			return sm.GetShardCount()
		}
	}
	return 0
}
