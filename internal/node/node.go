// Package node assembles a dkv node from its configuration: the storage
// engine, the public API server, and in a cluster the internal transport
// and membership. cmd/dkv-server and the in-process test cluster
// (nodetest) both build nodes with it.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/cluster"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/server"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

// Node is one dkv node.
type Node struct {
	cfg *config.Config
	log *slog.Logger
	id  string
	db  *lsm.DB
}

// Open opens (and recovers) the storage engine and loads the node ID,
// creating it on first start. It does not listen; a node whose data is
// damaged therefore never accepts a connection.
func Open(cfg *config.Config, log *slog.Logger) (*Node, error) {
	db, err := lsm.Open(cfg.Storage.Dir, storageOptions(cfg.Storage, log))
	if err != nil {
		return nil, err
	}
	id, err := LoadOrCreateID(cfg.Storage.Dir)
	if err != nil {
		_ = db.Close() // the ID error is the one worth reporting
		return nil, err
	}
	return &Node{cfg: cfg, log: log.With("node_id", id), id: id, db: db}, nil
}

// ID returns the node's persistent ID.
func (n *Node) ID() string { return n.id }

// DB returns the node's storage engine.
func (n *Node) DB() *lsm.DB { return n.db }

// Run serves the public API on public and, if internal is non-nil, the
// node-to-node API on internal while pinging the configured peers. It
// returns when ctx is cancelled or a server fails, after closing both
// listeners and the storage engine and waiting for every goroutine it
// started.
func (n *Node) Run(ctx context.Context, public, internal net.Listener) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg          sync.WaitGroup
		status      server.StatusSource
		internalSrv *grpc.Server
		pool        *transport.Pool
		internalErr error
	)
	if internal != nil {
		advertise := n.cfg.Cluster.AdvertiseAddr
		if advertise == "" {
			advertise = internal.Addr().String()
		}
		tcfg := transportConfig(n.cfg.Transport)
		pool = transport.NewPool(tcfg)
		m := cluster.New(cluster.Config{
			ClusterName:  n.cfg.Cluster.Name,
			NodeID:       n.id,
			Advertise:    advertise,
			Peers:        n.cfg.Cluster.Peers,
			PingInterval: n.cfg.Cluster.PingInterval,
			PingTimeout:  n.cfg.Cluster.PingTimeout,
			DownAfter:    n.cfg.Cluster.DownAfter,
		}, pool, nil, n.log.With("component", "membership"))
		internalSrv = transport.NewServer(tcfg)
		dkvv1.RegisterInternalServiceServer(internalSrv, cluster.NewPingService(m))
		n.log.Info("serving internal API", "addr", internal.Addr().String(), "advertise", advertise,
			"cluster", n.cfg.Cluster.Name, "peers", n.cfg.Cluster.Peers)
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := internalSrv.Serve(internal); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				internalErr = fmt.Errorf("internal API: %w", err)
				cancel() // a node that peers cannot reach must not keep serving clients
			}
		}()
		go func() {
			defer wg.Done()
			m.Run(runCtx)
		}()
		status = m
	} else {
		status = server.StaticStatus{Self: &dkvv1.NodeInfo{
			NodeId: n.id, ClusterName: n.cfg.Cluster.Name, StartedAtUnixNano: time.Now().UnixNano(),
		}}
	}

	srv := server.New(n.cfg, server.NewStandaloneBackend(n.db), status, n.log)
	runErr := srv.Run(runCtx, public)
	cancel()
	if internalSrv != nil {
		internalSrv.Stop() // peers retry; nothing to drain on this API yet
	}
	wg.Wait()
	if pool != nil {
		if err := pool.Close(); err != nil {
			n.log.Warn("closing peer connections", "error", err)
		}
	}
	// Close the engine only after both servers have stopped.
	dbErr := n.db.Close()
	return errors.Join(runErr, internalErr, dbErr)
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

func transportConfig(c config.Transport) transport.Config {
	return transport.Config{
		DialTimeout:      c.DialTimeout,
		KeepaliveTime:    c.KeepaliveTime,
		KeepaliveTimeout: c.KeepaliveTimeout,
	}
}
