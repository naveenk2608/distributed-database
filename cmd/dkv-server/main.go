// Command dkv-server runs a dkv node.
//
// A node serves the public KV and admin API on server.grpc_addr. With
// cluster.internal_addr and cluster.peers set it also serves the
// node-to-node API and tracks its peers; otherwise it runs standalone.
// Run with -h for configuration options.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/node"
	"github.com/naveenk2608/distributed-database/internal/obs"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Environ(), os.Stderr)
	stop()
	os.Exit(code)
}

// run starts the node and blocks until ctx is cancelled. Logs go to logOut.
func run(ctx context.Context, args, environ []string, logOut io.Writer) int {
	cfg, err := config.Load("dkv-server", args, environ, logOut)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		_, _ = fmt.Fprintf(logOut, "dkv-server: %v\n", err) // best effort: nowhere else to report
		return 2
	}
	log, err := obs.NewLogger(logOut, cfg.Log)
	if err != nil {
		_, _ = fmt.Fprintf(logOut, "dkv-server: %v\n", err) // best effort: nowhere else to report
		return 2
	}

	// Open (and recover) the engine before binding any port, so a node
	// whose data is damaged never accepts a connection.
	n, err := node.Open(cfg, log)
	if err != nil {
		log.Error("opening storage engine failed", "dir", cfg.Storage.Dir, "error", err)
		return 1
	}
	log = log.With("node_id", n.ID())
	log.Info("storage engine open", "dir", cfg.Storage.Dir, "sync", cfg.Storage.Sync, "last_seq", n.DB().LastSeq())
	closeDB := func() {
		if err := n.DB().Close(); err != nil {
			log.Error("closing storage engine", "error", err)
		}
	}
	public, err := net.Listen("tcp", cfg.Server.GRPCAddr)
	if err != nil {
		log.Error("listen failed", "addr", cfg.Server.GRPCAddr, "error", err)
		closeDB()
		return 1
	}
	var internal net.Listener
	if cfg.Cluster.InternalAddr != "" {
		if internal, err = net.Listen("tcp", cfg.Cluster.InternalAddr); err != nil {
			log.Error("listen failed", "addr", cfg.Cluster.InternalAddr, "error", err)
			_ = public.Close()
			closeDB()
			return 1
		}
	}
	if err := n.Run(ctx, public, internal); err != nil {
		log.Error("node failed", "error", err)
		return 1
	}
	return 0
}
