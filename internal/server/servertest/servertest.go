// Package servertest starts real in-process dkv servers for tests.
package servertest

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/server"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
)

// Node is a running in-process server.
type Node struct {
	Addr string  // host:port the server listens on
	DB   *lsm.DB // the server's engine
	stop func()
}

// Stop shuts the server down and waits for it. It is idempotent and also
// runs automatically at test cleanup.
func (n *Node) Stop() { n.stop() }

// Start runs a standalone server with an in-memory engine on a free
// loopback port. configure, if non-nil, may adjust the configuration.
func Start(t testing.TB, configure func(*config.Config)) *Node {
	t.Helper()
	cfg := config.Default()
	cfg.Server.GRPCAddr = "127.0.0.1:0"
	cfg.Server.ShutdownTimeout = 5 * time.Second
	if configure != nil {
		configure(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("servertest: %v", err)
	}
	lis, err := net.Listen("tcp", cfg.Server.GRPCAddr)
	if err != nil {
		t.Fatalf("servertest: listen: %v", err)
	}
	db := lsm.OpenInMemory()
	srv := server.New(&cfg, server.NewStandaloneBackend(db), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, lis) }()

	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-done; err != nil {
			t.Errorf("servertest: server exited with error: %v", err)
		}
		_ = db.Close() // may already be closed by the test
	}
	t.Cleanup(stop)
	return &Node{Addr: lis.Addr().String(), DB: db, stop: stop}
}
