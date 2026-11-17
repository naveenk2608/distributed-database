// Package node assembles a dkv node from its configuration: the storage
// engines, the public API server, and in a cluster the internal transport,
// membership pings, the replicas of every group the node belongs to (data
// shards and possibly the meta group), the router, and the background
// work of Phase 7: bootstrap or join, the directory refresh, heartbeats,
// and on the meta leader, liveness decisions and the rebalancer.
// cmd/dkv-server and the in-process test cluster (nodetest) both build
// nodes with it.
//
// Data directory layout:
//
//	standalone:  <dir>/NODE_ID, and one engine in <dir> itself
//	cluster:     <dir>/LOCK, NODE_ID, CLUSTER, MEMBERS, DIRECTORY, and per
//	             group replica <dir>/shards/NNNN/ (or shards/meta/)
package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	"github.com/naveenk2608/distributed-database/internal/meta"
	"github.com/naveenk2608/distributed-database/internal/placement"
	"github.com/naveenk2608/distributed-database/internal/replica"
	"github.com/naveenk2608/distributed-database/internal/router"
	"github.com/naveenk2608/distributed-database/internal/server"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
	"github.com/naveenk2608/distributed-database/internal/storage/sstable"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

// membersFile records the node IDs the cluster was bootstrapped with, once
// this node has bootstrapped or joined.
const membersFile = "MEMBERS"

type membersRecord struct {
	Members []string `json:"members"`
	// Complete is set once every bootstrap replica of this node exists.
	Complete bool `json:"complete"`
	// Joined is set on a node that joined an existing cluster.
	Joined bool `json:"joined,omitempty"`
}

// Node is one dkv node.
type Node struct {
	cfg *config.Config
	log *slog.Logger
	id  string

	db *lsm.DB // standalone

	lock        io.Closer // cluster: the data directory lock
	advertise   string
	fingerprint string
	dir         *directory
	pool        *transport.Pool
	net         *raftNet
	cache       *sstable.Cache
	compactions *lsm.CompactionPool

	mu       sync.Mutex
	replicas map[int]*replica.Replica // by shard; meta.Shard is the meta group
	created  map[int]time.Time
	members  membersRecord
	closed   bool

	// Set by Run.
	mship *cluster.Membership
	rt    *router.Router

	lvMu sync.Mutex
	lv   *meta.Liveness // while this node's meta replica leads
}

// ShardDir returns the directory of a group replica under the data dir.
func ShardDir(dataDir string, shard int) string {
	if shard == meta.Shard {
		return filepath.Join(dataDir, "shards", "meta")
	}
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
	n := &Node{cfg: cfg, lock: lock, replicas: map[int]*replica.Replica{}, created: map[int]time.Time{}}
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
	n.fingerprint = placement.Fingerprint(cfg.Cluster.Peers, cfg.Cluster.Shards, cfg.Cluster.ReplicationFactor)
	if err := n.loadMembers(); err != nil {
		return nil, err
	}
	n.dir = newDirectory(dir)
	if err := n.dir.load(); err != nil {
		n.log.Warn("ignoring a damaged directory cache", "error", err)
	}
	n.dir.Learn(n.id, n.advertise)

	n.pool = transport.NewPool(transportConfig(cfg.Transport))
	// A Raft batch that takes longer than an election timeout to deliver
	// is useless anyway; give up on it then.
	n.net = newRaftNet(n.pool, n.dir, time.Duration(cfg.Raft.ElectionTicks)*cfg.Raft.TickInterval, n.log.With("component", "raftnet"))
	n.cache = sstable.NewCache(int64(cfg.Storage.BlockCacheBytes))
	n.compactions = lsm.NewCompactionPool(cfg.Storage.CompactionConcurrency)

	// Reopen every replica this node has on disk.
	entries, err := os.ReadDir(filepath.Join(dir, "shards"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("node: list shards: %w", err)
	}
	for _, e := range entries {
		shard := meta.Shard
		if e.Name() != "meta" {
			s, err := strconv.Atoi(e.Name())
			if err != nil || s < 0 || s >= cfg.Cluster.Shards {
				n.log.Warn("ignoring an unknown directory under shards", "dir", e.Name())
				continue
			}
			shard = s
		}
		if _, err := n.openReplica(shard, nil); err != nil {
			return nil, err
		}
	}
	n.log.Info("replicas open", "count", len(n.replicas), "shards", cfg.Cluster.Shards,
		"replication_factor", cfg.Cluster.ReplicationFactor, "bootstrapped", len(n.members.Members) > 0)
	return n, nil
}

func (n *Node) loadMembers() error {
	b, err := os.ReadFile(filepath.Join(n.cfg.Storage.Dir, membersFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("node: read %s: %w", membersFile, err)
	}
	if err := json.Unmarshal(b, &n.members); err != nil {
		return fmt.Errorf("node: %s is damaged: %w", membersFile, err)
	}
	return nil
}

func (n *Node) saveMembers(rec membersRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := writeFileDurably(filepath.Join(n.cfg.Storage.Dir, membersFile), append(b, '\n')); err != nil {
		return err
	}
	n.mu.Lock()
	n.members = rec
	n.mu.Unlock()
	return nil
}

func (n *Node) membersRecord() membersRecord {
	n.mu.Lock()
	defer n.mu.Unlock()
	rec := n.members
	rec.Members = slices.Clone(rec.Members)
	return rec
}

// openReplica opens (or creates) this node's replica of a group; peers
// bootstraps a new group, and nil leaves a new replica waiting for the
// group's leader.
func (n *Node) openReplica(shard int, peers []string) (*replica.Replica, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, errors.New("node: closed")
	}
	if rp := n.replicas[shard]; rp != nil {
		return rp, nil
	}
	shardLog := n.log.With("shard", shard)
	rp, err := replica.Open(replica.Config{
		Shard: shard, ID: n.id, Peers: peers, Dir: ShardDir(n.cfg.Storage.Dir, shard),
		Engine:       storageOptions(n.cfg.Storage, shardLog, n.cache, n.compactions),
		ElectionTick: n.cfg.Raft.ElectionTicks, HeartbeatTick: n.cfg.Raft.HeartbeatTicks,
		SessionTTL: n.cfg.Raft.SessionTTL, CompactEntries: n.cfg.Raft.CompactEntries,
		Sender: n.net, Snapshots: n.net, Log: shardLog,
	})
	if err != nil {
		return nil, fmt.Errorf("node: shard %d: %w", shard, err)
	}
	n.replicas[shard] = rp
	n.created[shard] = time.Now()
	return rp, nil
}

// Replica returns this node's replica of a group, or nil.
func (n *Node) Replica(shard int) *replica.Replica {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.replicas[shard]
}

func (n *Node) localReplica(shard int) router.Replica {
	if rp := n.Replica(shard); rp != nil {
		return rp
	}
	return nil
}

func (n *Node) allReplicas() map[int]*replica.Replica {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[int]*replica.Replica, len(n.replicas))
	for s, rp := range n.replicas {
		out[s] = rp
	}
	return out
}

// destroyReplica deletes this node's replica of a group.
func (n *Node) destroyReplica(shard int) error {
	n.mu.Lock()
	rp := n.replicas[shard]
	delete(n.replicas, shard)
	delete(n.created, shard)
	n.mu.Unlock()
	if rp == nil {
		return nil
	}
	return rp.Destroy()
}

func (n *Node) closeReplicas() error {
	n.mu.Lock()
	n.closed = true
	reps := n.replicas
	n.replicas = map[int]*replica.Replica{}
	n.mu.Unlock()
	var errs []error
	for s, rp := range reps {
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

// SetRaftFilter makes the node discard, in both directions, the Raft
// messages of every group for which drop returns true; nil stops it. It
// is a fault-injection hook for tests (a partition of chosen groups).
func (n *Node) SetRaftFilter(drop func(shard int) bool) {
	if drop == nil {
		n.net.drop.Store(nil)
		return
	}
	n.net.drop.Store(&drop)
}

// resolve returns a member's internal address.
func (n *Node) resolve(id string) string { return n.dir.Addr(id) }

// Run serves the public API on public and, in a cluster, the node-to-node
// API on internal, and runs the cluster's background work. It returns when
// ctx is cancelled or a server fails, after closing both listeners, every
// replica and storage engine, and waiting for every goroutine it started.
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
	n.mship = cluster.New(cluster.Config{
		ClusterName:  n.cfg.Cluster.Name,
		NodeID:       n.id,
		Advertise:    n.advertise,
		Peers:        n.cfg.Cluster.Peers,
		PingInterval: n.cfg.Cluster.PingInterval,
		PingTimeout:  n.cfg.Cluster.PingTimeout,
		DownAfter:    n.cfg.Cluster.DownAfter,
		Placement:    n.fingerprint,
	}, n.pool, nil, n.log.With("component", "membership"))
	if m := n.membersRecord().Members; len(m) > 0 {
		n.mship.SetBootstrapMembers(m)
	}
	n.rt = router.New(router.Config{
		Self: n.id, Shards: n.cfg.Cluster.Shards, Replicas: n.dir.Voters, Resolve: n.resolve,
		Local: n.localReplica, Pool: n.pool, Log: n.log.With("component", "router"),
	})
	internalSrv := transport.NewServer(transportConfig(n.cfg.Transport))
	dkvv1.RegisterInternalServiceServer(internalSrv, &internalAPI{n: n, ping: cluster.NewPingService(n.mship)})
	n.log.Info("serving internal API", "addr", internal.Addr().String(), "advertise", n.advertise,
		"cluster", n.cfg.Cluster.Name, "peers", n.cfg.Cluster.Peers, "join", n.cfg.Cluster.Join)

	var wg sync.WaitGroup
	var internalErr error
	goRun := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
	}
	goRun(func() {
		if err := internalSrv.Serve(internal); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			internalErr = fmt.Errorf("internal API: %w", err)
			cancel() // a node that peers cannot reach must not keep serving clients
		}
	})
	goRun(func() { n.mship.Run(runCtx) })
	goRun(func() { n.net.run(runCtx) })
	goRun(func() { n.tick(runCtx) })
	goRun(func() { n.control(runCtx) })

	runErr := server.New(n.cfg, n.rt, &clusterAdmin{n: n}, n.log).Run(runCtx, public)
	cancel()
	internalSrv.Stop() // in-flight forwards fail; the forwarding node retries
	wg.Wait()
	// Close the replicas only after both servers and every loop have
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
			for _, rp := range n.allReplicas() {
				rp.Tick()
			}
		}
	}
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
