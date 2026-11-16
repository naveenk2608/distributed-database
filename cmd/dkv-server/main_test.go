package main

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestRunServesUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var logs syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"--server.grpc-addr=127.0.0.1:0", "--log.format=text", "--storage.dir=" + t.TempDir()}, nil, &logs)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs.String(), "serving KV API") {
		if time.Now().After(deadline) {
			t.Fatalf("server did not start; logs:\n%s", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("run returned %d after cancel; logs:\n%s", code, logs.String())
	}
	if !strings.Contains(logs.String(), "msg=stopped") {
		t.Errorf("no clean-stop log line:\n%s", logs.String())
	}
}

func TestRunRejectsBadConfig(t *testing.T) {
	var logs syncBuffer
	if code := run(context.Background(), nil, nil, &logs); code != 2 {
		t.Errorf("run without an address = %d, want 2", code)
	}
	if !strings.Contains(logs.String(), "server.grpc_addr: required") {
		t.Errorf("missing explanation in output:\n%s", logs.String())
	}
	if code := run(context.Background(), []string{"-h"}, nil, &logs); code != 0 {
		t.Errorf("run -h = %d, want 0", code)
	}
}

func TestRunFailsWhenAddressInUse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var first syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"--server.grpc-addr=127.0.0.1:0", "--storage.dir=" + t.TempDir()}, nil, &first)
	}()
	var addr string
	deadline := time.Now().Add(10 * time.Second)
	for addr == "" {
		if time.Now().After(deadline) {
			t.Fatalf("first server did not start:\n%s", first.String())
		}
		addr = boundAddr(first.String())
		time.Sleep(10 * time.Millisecond)
	}
	var second syncBuffer
	if code := run(context.Background(), []string{"--server.grpc-addr=" + addr, "--storage.dir=" + t.TempDir()}, nil, &second); code != 1 {
		t.Errorf("second server on %s: exit %d, want 1; logs:\n%s", addr, code, second.String())
	}
	cancel()
	<-done
}

// boundAddr extracts the public listen address from JSON logs, or "".
func boundAddr(logs string) string {
	for _, line := range strings.Split(logs, "\n") {
		var rec struct{ Msg, Addr string }
		if json.Unmarshal([]byte(line), &rec) == nil && rec.Msg == "serving KV API" {
			return rec.Addr
		}
	}
	return ""
}

// TestRunAsSingleNodeCluster starts the binary's run function in cluster
// mode (a one-node cluster), which takes a different path through startup
// and shutdown than standalone mode.
func TestRunAsSingleNodeCluster(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	internal := l.Addr().String()
	_ = l.Close()
	ctx, cancel := context.WithCancel(context.Background())
	var logs syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"--server.grpc-addr=127.0.0.1:0", "--log.format=json",
			"--cluster.internal-addr=" + internal, "--cluster.peers=" + internal,
			"--cluster.replication-factor=1", "--cluster.shards=4", "--storage.dir=" + t.TempDir()}, nil, &logs)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for boundAddr(logs.String()) == "" {
		select {
		case code := <-done:
			t.Fatalf("run exited with %d:\n%s", code, logs.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("cluster node did not start:\n%s", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(logs.String(), `"msg":"shard engines open"`) {
		t.Errorf("no shard engines in the logs:\n%s", logs.String())
	}
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("run returned %d after cancel:\n%s", code, logs.String())
	}
}
