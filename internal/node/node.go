// Package node assembles a dkv node from its configuration: the storage
// engines, the public API server, and in a cluster the internal transport,
// membership, one Raft replica per shard held, and the router. cmd/dkv-server and the in-process test cluster
// (nodetest) both build nodes with it.
//
// Data directory layout:
//
//	standalone:  <dir>/NODE_ID, and one engine in <dir> itself
//	cluster:     <dir>/LOCK, NODE_ID, CLUSTER, and per shard replica the
//	             node holds <dir>/shards/NNNN/kv (engine) and raft (log)
package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/cluster"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/placement"
	"github.com/naveenk2608/distributed-database/internal/raft"
	"github.com/naveenk2608/distributed-database/internal/replica"
	"github.com/naveenk2608/distributed-database/internal/router"
	"github.com/naveenk2608/distributed-database/internal/server"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
	"github.com/naveenk2608/distributed-database/internal/storage/sstable"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

// Node is one dkv node.
type Node struct {
	cfg *config.Config
	log *slog.Logger
	id  string

	db *lsm.DB // standalone

	lock        io.Closer // cluster: the data directory lock
	advertise   string
	pmap        *placement.Map
	fingerprint string
	pool        *transport.Pool
	net         *raftNet
	replicas    map[int]*replica.Replica // cluster: one per shard replica held
}

// ShardDir returns the directory of a shard replica under the data dir:
// its engine in kv/ and its Raft log in raft/.
func ShardDir(dataDir string, shard int) string {
	return filepath.Join(dataDir, "shards", fmt.Sprintf("%04d", shard))
}

// Open opens (and recovers) the storage engines and Raft logs and loads
// the node ID, creating it on first start. It does not listen; a node
// whose data is damaged therefore never accepts a connection.
func Open(cfg *config.Config, log *slog.Logger) (*Node, error) {
	if cfg.Cluster.Clustered() {
		return openCluster(cfg, log)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.Dir, markerFile)); err == nil {
		return nil, fmt.Errorf("node: %s belongs to a cluster; start it with that cluster's settings", cfg.Storage.Dir)
	}
	db, err := lsm.Open(cfg.Storage.Dir, storageOptions(cfg.Storage, log, nil, nil))
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

func openCluster(cfg *config.Config, log *slog.Logger) (_ *Node, err error) {
	dir := cfg.Storage.Dir
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("node: create %s: %w", dir, err)
	}
	lock, err := vfs.Default.Lock(filepath.Join(dir, "LOCK"))
	if err != nil {
		return nil, fmt.Errorf("node: lock %s: %w", dir, err)
	}
	n := &Node{cfg: cfg, lock: lock, replicas: map[int]*replica.Replica{}}
	defer func() {
		if err != nil {
			_ = n.closeReplicas() // the open error is the one worth reporting
			if n.pool != nil {
				_ = n.pool.Close()
			}
			_ = lock.Close()
		}
	}()
	if _, err := os.Stat(filepath.Join(dir, "MANIFEST")); err == nil {
		return nil, fmt.Errorf("node: %s holds a standalone engine; a cluster node needs its own data directory", dir)
	}
	if n.id, err = LoadOrCreateID(dir); err != nil {
		return nil, err
	}
	n.log = log.With("node_id", n.id)
	if err := checkOrWriteMarker(dir, cfg.Cluster); err != nil {
		return nil, err
	}
	n.advertise = cfg.Cluster.Advertised()
	if n.pmap, err = placement.Compute(cfg.Cluster.Peers, cfg.Cluster.Shards, cfg.Cluster.ReplicationFactor); err != nil {
		return nil, err
	}
	n.fingerprint = placement.Fingerprint(cfg.Cluster.Peers, cfg.Cluster.Shards, cfg.Cluster.ReplicationFactor)

	n.pool = transport.NewPool(transportConfig(cfg.Transport))
	// A Raft batch that takes longer than an election timeout to deliver
	// is useless anyway; give up on it then.
	n.net = newRaftNet(n.pool, n.advertise, cfg.Cluster.Peers,
		time.Duration(cfg.Raft.ElectionTicks)*cfg.Raft.TickInterval, n.log.With("component", "raftnet"))
	cache := sstable.NewCache(int64(cfg.Storage.BlockCacheBytes))
	pool := lsm.NewCompactionPool(cfg.Storage.CompactionConcurrency)
	held := n.pmap.ShardsOf(n.advertise)
	for _, s := range held {
		shardLog := n.log.With("shard", s)
		rp, err := replica.Open(replica.Config{
			Shard: s, ID: n.advertise, Peers: n.pmap.ReplicaSet(s), Dir: ShardDir(dir, s),
			Engine:       storageOptions(cfg.Storage, shardLog, cache, pool),
			ElectionTick: cfg.Raft.ElectionTicks, HeartbeatTick: cfg.Raft.HeartbeatTicks,
			SessionTTL: cfg.Raft.SessionTTL, Sender: n.net, Log: shardLog,
		})
		if err != nil {
			return nil, fmt.Errorf("node: shard %d: %w", s, err)
		}
		n.replicas[s] = rp
	}
	n.warnAboutUnheldShards(held)
	n.log.Info("shard replicas open", "shards", cfg.Cluster.Shards, "replication_factor", cfg.Cluster.ReplicationFactor,
		"held", len(held), "placement", n.fingerprint)
	return n, nil
}

// warnAboutUnheldShards logs shard directories this node no longer holds
// a replica of; Phase 7's rebalancer moves such data, until then it is
// left alone.
func (n *Node) warnAboutUnheldShards(held []int) {
	entries, err := os.ReadDir(filepath.Join(n.cfg.Storage.Dir, "shards"))
	if err != nil {
		return
	}
	for _, e := range entries {
		s, err := strconv.Atoi(e.Name())
		if err == nil && !slices.Contains(held, s) {
			n.log.Warn("data for a shard this node no longer holds is left untouched", "shard", s, "dir", e.Name())
		}
	}
}

func (n *Node) closeReplicas() error {
	var errs []error
	for s, rp := range n.replicas {
		if err := rp.Close(); err != nil {
			errs = append(errs, fmt.Errorf("shard %d: %w", s, err))
		}
	}
	return errors.Join(errs...)
}

// Close releases a node that was opened but never run: its replicas and
// its data directory lock. Run does this itself when it returns.
func (n *Node) Close() error {
	if n.db != nil {
		return n.db.Close()
	}
	return errors.Join(n.closeReplicas(), n.pool.Close(), n.lock.Close())
}

// ID returns the node's persistent ID.
func (n *Node) ID() string { return n.id }

// DB returns a standalone node's storage engine, or nil in a cluster.
func (n *Node) DB() *lsm.DB { return n.db }

// Replica returns the shard replica this node holds, or nil.
func (n *Node) Replica(shard int) *replica.Replica { return n.replicas[shard] }

// Run serves the public API on public and, in a cluster, the node-to-node
// API on internal while pinging the configured peers and driving every
// shard replica's Raft clock. It returns when ctx is cancelled or a server
// fails, after closing both listeners, every replica and storage engine,
// and waiting for every goroutine it started.
func (n *Node) Run(ctx context.Context, public, internal net.Listener) error {
	if !n.cfg.Cluster.Clustered() {
		admin := server.StaticStatus{DB: n.db, Self: &dkvv1.NodeInfo{
			NodeId: n.id, ClusterName: n.cfg.Cluster.Name, StartedAtUnixNano: time.Now().UnixNano(),
		}}
		runErr := server.New(n.cfg, server.NewStandaloneBackend(n.db), admin, n.log).Run(ctx, public)
		// Close the engine only after the server has stopped.
		return errors.Join(runErr, n.db.Close())
	}
	if internal == nil {
		return errors.Join(errors.New("node: a cluster node needs an internal listener"), n.Close())
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	m := cluster.New(cluster.Config{
		ClusterName:  n.cfg.Cluster.Name,
		NodeID:       n.id,
		Advertise:    n.advertise,
		Peers:        n.cfg.Cluster.Peers,
		PingInterval: n.cfg.Cluster.PingInterval,
		PingTimeout:  n.cfg.Cluster.PingTimeout,
		DownAfter:    n.cfg.Cluster.DownAfter,
		Placement:    n.fingerprint,
	}, n.pool, nil, n.log.With("component", "membership"))
	local := make(map[int]router.Replica, len(n.replicas))
	for s, rp := range n.replicas {
		local[s] = rp
	}
	rt := router.New(router.Config{
		Self: n.advertise, Shards: n.cfg.Cluster.Shards, Replicas: n.pmap.ReplicaSet,
		Local: local, Pool: n.pool, Log: n.log.With("component", "router"),
	})
	internalSrv := transport.NewServer(transportConfig(n.cfg.Transport))
	dkvv1.RegisterInternalServiceServer(internalSrv, &internalAPI{
		ping: cluster.NewPingService(m), router: rt, self: n.advertise, replicas: n.replicas,
	})
	n.log.Info("serving internal API", "addr", internal.Addr().String(), "advertise", n.advertise,
		"cluster", n.cfg.Cluster.Name, "peers", n.cfg.Cluster.Peers)

	var wg sync.WaitGroup
	var internalErr error
	wg.Add(4)
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
	go func() {
		defer wg.Done()
		n.net.run(runCtx)
	}()
	go func() {
		defer wg.Done()
		n.tick(runCtx)
	}()

	admin := &clusterAdmin{m: m, pmap: n.pmap, replicas: n.replicas}
	runErr := server.New(n.cfg, rt, admin, n.log).Run(runCtx, public)
	cancel()
	internalSrv.Stop() // in-flight forwards fail; the forwarding node retries
	wg.Wait()
	// Close the replicas only after both servers and the ticker have
	// stopped, and the connections only after the replicas, which send
	// through them.
	repErr := n.closeReplicas()
	if err := n.pool.Close(); err != nil {
		n.log.Warn("closing peer connections", "error", err)
	}
	return errors.Join(runErr, internalErr, repErr, n.lock.Close())
}

// tick drives every replica's Raft clock from one ticker.
func (n *Node) tick(ctx context.Context) {
	t := time.NewTicker(n.cfg.Raft.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, rp := range n.replicas {
				rp.Tick()
			}
		}
	}
}

// internalAPI serves the node-to-node API.
type internalAPI struct {
	dkvv1.UnimplementedInternalServiceServer
	ping     *cluster.PingService
	router   *router.Router
	self     string
	replicas map[int]*replica.Replica
}

func (a *internalAPI) Ping(ctx context.Context, req *dkvv1.PingRequest) (*dkvv1.PingResponse, error) {
	return a.ping.Ping(ctx, req)
}

func (a *internalAPI) Forward(ctx context.Context, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	return a.router.Forward(ctx, req)
}

// Raft hands each message in the batch to its shard's replica. Messages
// for a shard this node does not hold, for another node, or of an unknown
// type are dropped; Raft treats them as lost.
func (a *internalAPI) Raft(_ context.Context, req *dkvv1.RaftRequest) (*dkvv1.RaftResponse, error) {
	for _, pm := range req.GetMessages() {
		rp := a.replicas[int(pm.GetShard())]
		if rp == nil || pm.GetTo() != a.self || !validMessage(pm) {
			continue
		}
		rp.Step(fromProto(pm))
	}
	return &dkvv1.RaftResponse{}, nil
}

func validMessage(pm *dkvv1.RaftMessage) bool {
	if pm.GetType() < uint32(raft.MsgPreVote) || pm.GetType() > uint32(raft.MsgHeartbeatResp) {
		return false
	}
	for _, e := range pm.GetEntries() {
		if e.GetType() > uint32(raft.EntryConfChange) {
			return false
		}
	}
	return true
}

// clusterAdmin answers the operator API for a cluster node.
type clusterAdmin struct {
	m        *cluster.Membership
	pmap     *placement.Map
	replicas map[int]*replica.Replica
}

func (a *clusterAdmin) Status() *dkvv1.ClusterStatusResponse { return a.m.Status() }

func (a *clusterAdmin) ShardMap() *dkvv1.ShardMapResponse {
	resp := &dkvv1.ShardMapResponse{ShardCount: uint32(a.pmap.Shards), ReplicationFactor: uint32(a.pmap.RF)}
	for s := range a.pmap.Shards {
		info := &dkvv1.ShardInfo{Shard: uint32(s), Replicas: a.pmap.ReplicaSet(s)}
		if rp, ok := a.replicas[s]; ok {
			st := rp.Status()
			info.Local, info.LastSeq = true, st.Applied
			info.Leader, info.Term, info.Role, info.CommitIndex = st.Leader, st.Term, st.Role.String(), st.Commit
		}
		resp.Shards = append(resp.Shards, info)
	}
	return resp
}

// storageOptions maps validated configuration to engine options. cache and
// pool, if non-nil, are shared by every engine on the node.
func storageOptions(c config.Storage, log *slog.Logger, cache *sstable.Cache, pool *lsm.CompactionPool) lsm.Options {
	opts := lsm.Options{
		MemtableBytes:  int64(c.MemtableBytes),
		SyncInterval:   c.SyncInterval,
		Logger:         log.With("component", "lsm"),
		BlockCache:     cache,
		CompactionPool: pool,
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
