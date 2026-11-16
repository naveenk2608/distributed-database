// Command dkv-server runs a dkv node.
//
// Phase 2 runs a single standalone node whose engine logs every write to a
// WAL in storage.dir and recovers it on restart. Run with -h for
// configuration options.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
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

	// Open (and recover) the engine before binding the port, so a node
	// whose data is damaged never accepts a connection.
	db, err := lsm.Open(cfg.Storage.Dir, storageOptions(cfg.Storage, log))
	if err != nil {
		log.Error("opening storage engine failed", "dir", cfg.Storage.Dir, "error", err)
		return 1
	}
	log.Info("storage engine open", "dir", cfg.Storage.Dir, "sync", cfg.Storage.Sync, "last_seq", db.LastSeq())
	lis, err := net.Listen("tcp", cfg.Server.GRPCAddr)
	if err != nil {
		log.Error("listen failed", "addr", cfg.Server.GRPCAddr, "error", err)
		if err := db.Close(); err != nil {
			log.Error("closing storage engine", "error", err)
		}
		return 1
	}

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

// storageOptions maps validated configuration to engine options.
func storageOptions(c config.Storage, log *slog.Logger) lsm.Options {
	opts := lsm.Options{
		MemtableBytes: int64(c.MemtableBytes),
		SyncInterval:  c.SyncInterval,
		Logger:        log.With("component", "lsm"),
	}
	switch c.Sync {
	case "interval":
		opts.Sync = lsm.SyncInterval
	case "never":
		opts.Sync = lsm.SyncNever
	default: // "always"; config.Validate rejects anything else
		opts.Sync = lsm.SyncAlways
	}
	return opts
}
