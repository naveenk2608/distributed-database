// Command dkv-server runs a dkv node.
//
// Phase 1 runs a single standalone node with an in-memory engine: data does
// not survive a restart. Run with -h for configuration options.
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
	"github.com/naveenk2608/distributed-database/internal/obs"
	"github.com/naveenk2608/distributed-database/internal/server"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
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

	lis, err := net.Listen("tcp", cfg.Server.GRPCAddr)
	if err != nil {
		log.Error("listen failed", "addr", cfg.Server.GRPCAddr, "error", err)
		return 1
	}
	db := lsm.OpenInMemory()
	log.Warn("storage engine is in-memory only (Phase 1): data is lost when the process exits")

	srv := server.New(cfg, server.NewStandaloneBackend(db), log)
	runErr := srv.Run(ctx, lis)
	// Close the engine only after the server has stopped serving requests.
	if err := db.Close(); err != nil {
		log.Error("closing storage engine", "error", err)
	}
	if runErr != nil {
		log.Error("server failed", "error", runErr)
		return 1
	}
	return 0
}
