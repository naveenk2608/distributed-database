// Package servertest starts real in-process dkv servers for tests.
package servertest

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/server"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
)

// Node is a running in-process server.
type Node struct {
	Addr      string  // host:port the server listens on
	AdminAddr string  // the admin HTTP API's, if server.admin_addr was set
	DB        *lsm.DB // the server's engine
	stop      func()
}

// Stop shuts the server down and waits for it. It is idempotent and also
// runs automatically at test cleanup.
func (n *Node) Stop() { n.stop() }

// Start runs a standalone server on a free loopback port with a durable
// engine in a fresh temporary directory. configure, if non-nil, may adjust
// the configuration.
func Start(t testing.TB, configure func(*config.Config)) *Node {
	t.Helper()
	cfg := config.Default()
	cfg.Server.GRPCAddr = "127.0.0.1:0"
	cfg.Server.ShutdownTimeout = 5 * time.Second
	cfg.Storage.Dir = t.TempDir()
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
	db, err := lsm.Open(cfg.Storage.Dir, lsm.Options{MemtableBytes: int64(cfg.Storage.MemtableBytes)})
	if err != nil {
		_ = lis.Close()
		t.Fatalf("servertest: open engine: %v", err)
	}
	self := &dkvv1.NodeInfo{NodeId: "test-node", ClusterName: cfg.Cluster.Name}
	srv := server.New(&cfg, server.NewStandaloneBackend(db), server.StaticStatus{Self: self, DB: db}, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, lis) }()
	adminAddr, adminDone := "", make(chan error, 1)
	if cfg.Server.AdminAddr == "" {
		adminDone <- nil
	} else if alis, err := net.Listen("tcp", cfg.Server.AdminAddr); err != nil {
		adminDone <- nil
		t.Errorf("servertest: listen for the admin API: %v", err)
	} else {
		adminAddr = alis.Addr().String()
		go func() { adminDone <- srv.RunAdmin(ctx, alis) }()
	}

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
		if err := <-adminDone; err != nil {
			t.Errorf("servertest: admin API exited with error: %v", err)
		}
		_ = db.Close() // may already be closed by the test
	}
	t.Cleanup(stop)
	return &Node{Addr: lis.Addr().String(), AdminAddr: adminAddr, DB: db, stop: stop}
}
