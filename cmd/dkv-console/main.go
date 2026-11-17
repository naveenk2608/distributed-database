// Command dkv-console is the dkv dashboard backend: it polls every node's
// admin HTTP API, keeps the history in PostgreSQL (or in memory), and
// serves the dashboard with its JSON API and server-sent events. The
// cluster never depends on it (docs/adr/0014, 0015).
//
//	dkv-console --config=dkv-console.yaml
//	dkv-console --console.listen-addr=:8080 --console.nodes=n1:7200,n2:7200,n3:7200
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
	"github.com/naveenk2608/distributed-database/internal/console"
	"github.com/naveenk2608/distributed-database/internal/obs"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Environ(), os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args, environ []string, logOut io.Writer) int {
	cfg, err := config.LoadConsole("dkv-console", args, environ, logOut)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		_, _ = fmt.Fprintf(logOut, "dkv-console: %v\n", err) // best effort: nowhere else to report
		return 2
	}
	log, err := obs.NewLogger(logOut, cfg.Log)
	if err != nil {
		_, _ = fmt.Fprintf(logOut, "dkv-console: %v\n", err) // best effort: nowhere else to report
		return 2
	}
	var store console.Store
	if dsn := cfg.Console.PostgresDSN; dsn != "" {
		pg, err := openPG(ctx, dsn)
		if err != nil {
			log.Error("cannot use the history store", "error", err)
			return 2
		}
		defer pg.Close()
		store = pg
		log.Info("history in PostgreSQL", "retention", cfg.Console.Retention)
	} else {
		log.Info("history in memory only", "samples_per_node", cfg.Console.MemorySamples)
	}
	lis, err := net.Listen("tcp", cfg.Console.ListenAddr)
	if err != nil {
		log.Error("listen failed", "addr", cfg.Console.ListenAddr, "error", err)
		return 1
	}
	if err := console.New(cfg.Console, store, log).Run(ctx, lis); err != nil {
		log.Error("console failed", "error", err)
		return 1
	}
	return 0
}
