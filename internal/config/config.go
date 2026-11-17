// Package config loads and validates node configuration.
//
// Sources, from lowest to highest priority: built-in defaults, a YAML file,
// DKV_* environment variables, and command-line flags. Every setting is
// declared once in the settings table below; its environment variable and
// flag names are derived from its YAML path:
//
//	YAML  server.grpc_addr
//	env   DKV_SERVER_GRPC_ADDR
//	flag  --server.grpc-addr
//
// The YAML file is named by --config or DKV_CONFIG. Unknown YAML fields and
// unknown DKV_* variables are errors, so typos fail loudly at startup.
// See docs/adr/0016-configuration-sources.md.
package config

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config is the complete configuration of a dkv node.
type Config struct {
	Server    Server    `yaml:"server"`
	Cluster   Cluster   `yaml:"cluster"`
	Transport Transport `yaml:"transport"`
	Raft      Raft      `yaml:"raft"`
	Liveness  Liveness  `yaml:"liveness"`
	Rebalance Rebalance `yaml:"rebalance"`
	Storage   Storage   `yaml:"storage"`
	Limits    Limits    `yaml:"limits"`
	Log       Log       `yaml:"log"`
}

// Server configures the public API server.
type Server struct {
	// GRPCAddr is the host:port the public gRPC API listens on. Required.
	// Port 0 picks a free port; the bound address is logged at startup.
	GRPCAddr string `yaml:"grpc_addr"`
	// ShutdownTimeout bounds how long a graceful shutdown waits for
	// in-flight requests before closing connections.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	// AdminAddr is the host:port of the admin HTTP API: JSON status, shard
	// map, statistics and events for dkv-console and operators, and
	// Prometheus metrics. Empty disables it.
	AdminAddr string `yaml:"admin_addr"`
}

// Cluster configures membership. With neither an internal address nor
// peers, the node runs standalone.
type Cluster struct {
	// Name must be the same on every node of a cluster; a peer that reports
	// another name is never counted as a member.
	Name string `yaml:"name"`
	// InternalAddr is the host:port the node-to-node API listens on.
	// Required when Peers is set.
	InternalAddr string `yaml:"internal_addr"`
	// AdvertiseAddr is the internal address other nodes use to reach this
	// node. It defaults to InternalAddr and is required when InternalAddr
	// has no specific host (empty, 0.0.0.0 or ::).
	AdvertiseAddr string `yaml:"advertise_addr"`
	// Peers is the bootstrap peer list: the internal addresses of the
	// cluster's nodes. Listing this node's own address is allowed.
	Peers []string `yaml:"peers"`
	// PingInterval is how often each peer is pinged.
	PingInterval time.Duration `yaml:"ping_interval"`
	// PingTimeout bounds a single ping.
	PingTimeout time.Duration `yaml:"ping_timeout"`
	// DownAfter is how long a peer may go without answering before it is
	// reported down.
	DownAfter time.Duration `yaml:"down_after"`
	// Shards is the number of shards keys are hashed into. It is fixed when
	// the cluster is bootstrapped and recorded in every node's data
	// directory; a node refuses to start with a different value.
	Shards int `yaml:"shards"`
	// ReplicationFactor is the number of replicas of each shard. Recorded
	// like Shards.
	ReplicationFactor int `yaml:"replication_factor"`
	// MetaReplicas is the meta group's size, fixed at bootstrap.
	MetaReplicas int `yaml:"meta_replicas"`
	// Join lists internal addresses of nodes of an existing cluster that a
	// new node, one not in peers, contacts to join it.
	Join []string `yaml:"join"`
}

// Transport configures the gRPC connections between nodes.
type Transport struct {
	// DialTimeout bounds establishing a connection to a peer.
	DialTimeout time.Duration `yaml:"dial_timeout"`
	// KeepaliveTime is how long a connection may be idle before a keepalive
	// ping checks it. gRPC raises values below 10s to 10s.
	KeepaliveTime time.Duration `yaml:"keepalive_time"`
	// KeepaliveTimeout is how long to wait for a keepalive answer before
	// closing the connection.
	KeepaliveTimeout time.Duration `yaml:"keepalive_timeout"`
}

// Raft configures the Raft group of every shard replica on a cluster node.
// Timeouts are counted in ticks of one node-wide ticker.
type Raft struct {
	// TickInterval is the period of the node's Raft ticker.
	TickInterval time.Duration `yaml:"tick_interval"`
	// ElectionTicks is the minimum election timeout; each replica draws
	// its timeout at random from [ElectionTicks, 2*ElectionTicks).
	ElectionTicks int `yaml:"election_ticks"`
	// HeartbeatTicks is the leader's heartbeat period.
	HeartbeatTicks int `yaml:"heartbeat_ticks"`
	// SessionTTL is how long a client's deduplication session lives
	// without a write. A retry arriving later than this may apply twice.
	SessionTTL time.Duration `yaml:"session_ttl"`
	// CompactEntries is the Raft log length, in applied entries, at which
	// a replica compacts its log; followers further behind get a snapshot.
	CompactEntries int `yaml:"compact_entries"`
}

// Liveness configures failure detection: every node heartbeats the meta
// group's leader, which keeps the timestamps in memory and writes only its
// decisions (docs/adr/0013).
type Liveness struct {
	// HeartbeatInterval is how often a node heartbeats the meta leader.
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	// SuspectAfter: silence after which a node is reported suspect.
	SuspectAfter time.Duration `yaml:"suspect_after"`
	// DeadAfter: silence after which the meta leader declares a node dead
	// and its replicas are recreated elsewhere. A new meta leader waits
	// this long before declaring anyone dead.
	DeadAfter time.Duration `yaml:"dead_after"`
}

// Rebalance configures the rebalancer, which runs on the meta leader.
type Rebalance struct {
	// Interval is how often it compares the wanted placement with each
	// group's membership and takes the next steps.
	Interval time.Duration `yaml:"interval"`
}

// Storage configures the node's storage engine.
type Storage struct {
	// Dir is the data directory. Required; created if missing.
	Dir string `yaml:"dir"`
	// Sync is the WAL sync policy: always (fsync before acknowledging, the
	// default), interval (fsync every SyncInterval; can lose the last
	// interval's acknowledged writes on power loss) or never (benchmarks
	// only). In a cluster, never also skips the fsync of every Raft log
	// write (entries, term and vote), which lets a power loss break Raft's
	// guarantees.
	Sync string `yaml:"sync"`
	// SyncInterval is the fsync period when Sync is interval.
	SyncInterval time.Duration `yaml:"sync_interval"`
	// MemtableBytes is the memtable size at which a new memtable and WAL
	// segment start. A cluster node has one engine per shard replica it
	// holds, each with its own memtables.
	MemtableBytes int `yaml:"memtable_bytes"`
	// BlockCacheBytes is the size of the block cache shared by every engine
	// on the node.
	BlockCacheBytes int `yaml:"block_cache_bytes"`
	// CompactionConcurrency caps how many engines on the node compact at
	// once.
	CompactionConcurrency int `yaml:"compaction_concurrency"`
}

// Limits bounds request sizes.
type Limits struct {
	MaxKeyBytes   int `yaml:"max_key_bytes"`
	MaxValueBytes int `yaml:"max_value_bytes"`
}

// Log configures logging.
type Log struct {
	// Level is one of debug, info, warn, error.
	Level string `yaml:"level"`
	// Format is json or text.
	Format string `yaml:"format"`
}

// Default returns the built-in defaults. They contain no addresses, ports
// or paths: those must always come from the operator.
func Default() Config {
	return Config{
		Server: Server{ShutdownTimeout: 10 * time.Second},
		Cluster: Cluster{Name: "dkv", PingInterval: time.Second, PingTimeout: 500 * time.Millisecond, DownAfter: 5 * time.Second,
			Shards: 64, ReplicationFactor: 3, MetaReplicas: 3},
		Transport: Transport{DialTimeout: 2 * time.Second, KeepaliveTime: 10 * time.Second, KeepaliveTimeout: 3 * time.Second},
		Raft:      Raft{TickInterval: 50 * time.Millisecond, ElectionTicks: 10, HeartbeatTicks: 2, SessionTTL: time.Hour, CompactEntries: 10000},
		Liveness:  Liveness{HeartbeatInterval: time.Second, SuspectAfter: 3 * time.Second, DeadAfter: 30 * time.Second},
		Rebalance: Rebalance{Interval: time.Second},
		Storage: Storage{Sync: "always", SyncInterval: 10 * time.Millisecond, MemtableBytes: 4 << 20,
			BlockCacheBytes: 64 << 20, CompactionConcurrency: 2},
		Limits: Limits{MaxKeyBytes: 4 << 10, MaxValueBytes: 1 << 20},
		Log:    Log{Level: "info", Format: "json"},
	}
}

// Validate reports every problem with c, joined into one error.
func (c *Config) Validate() error {
	var errs []error
	bad := func(key, format string, args ...any) {
		errs = append(errs, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
	}

	if c.Server.GRPCAddr == "" {
		bad("server.grpc_addr", "required (set it in the config file, DKV_SERVER_GRPC_ADDR or --server.grpc-addr)")
	} else if err := checkListenAddr(c.Server.GRPCAddr); err != nil {
		errs = append(errs, fmt.Errorf("server.grpc_addr: %w", err))
	}
	if c.Server.AdminAddr != "" {
		if err := checkListenAddr(c.Server.AdminAddr); err != nil {
			errs = append(errs, fmt.Errorf("server.admin_addr: %w", err))
		} else if _, port, _ := net.SplitHostPort(c.Server.AdminAddr); c.Server.AdminAddr == c.Server.GRPCAddr && port != "0" {
			bad("server.admin_addr", "must differ from server.grpc_addr")
		}
	}
	if c.Server.ShutdownTimeout <= 0 {
		bad("server.shutdown_timeout", "must be positive, got %v", c.Server.ShutdownTimeout)
	}

	errs = append(errs, c.validateCluster()...)
	if c.Storage.Dir == "" {
		bad("storage.dir", "required (set it in the config file, DKV_STORAGE_DIR or --storage.dir)")
	}
	switch c.Storage.Sync {
	case "always", "never":
	case "interval":
		if c.Storage.SyncInterval <= 0 {
			bad("storage.sync_interval", "must be positive when storage.sync is interval, got %v", c.Storage.SyncInterval)
		}
	default:
		bad("storage.sync", "must be always, interval or never, got %q", c.Storage.Sync)
	}
	if c.Storage.MemtableBytes < 64<<10 || c.Storage.MemtableBytes > 1<<30 {
		bad("storage.memtable_bytes", "must be between %d and %d, got %d", 64<<10, 1<<30, c.Storage.MemtableBytes)
	}
	if c.Storage.BlockCacheBytes < 1<<20 {
		bad("storage.block_cache_bytes", "must be at least %d, got %d", 1<<20, c.Storage.BlockCacheBytes)
	}
	if c.Storage.CompactionConcurrency < 1 {
		bad("storage.compaction_concurrency", "must be at least 1, got %d", c.Storage.CompactionConcurrency)
	}
	const maxLimit = 1 << 30 // keeps derived message-size limits far from int overflow
	if c.Limits.MaxKeyBytes < 1 || c.Limits.MaxKeyBytes > maxLimit {
		bad("limits.max_key_bytes", "must be between 1 and %d, got %d", maxLimit, c.Limits.MaxKeyBytes)
	}
	if c.Limits.MaxValueBytes < 0 || c.Limits.MaxValueBytes > maxLimit {
		bad("limits.max_value_bytes", "must be between 0 and %d, got %d", maxLimit, c.Limits.MaxValueBytes)
	}

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(c.Log.Level)); err != nil {
		bad("log.level", "must be debug, info, warn or error, got %q", c.Log.Level)
	}
	if c.Log.Format != "json" && c.Log.Format != "text" {
		bad("log.format", "must be json or text, got %q", c.Log.Format)
	}
	return errors.Join(errs...)
}

// checkListenAddr checks that addr is host:port with a valid port.
func checkListenAddr(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return fmt.Errorf("invalid port %q: %w", port, err)
	}
	return nil
}

// setting binds one configuration key of a configuration T (Config, or
// ConsoleConfig) to its parser.
type setting[T any] struct {
	key   string // dotted YAML path
	usage string
	set   func(c *T, v string) error
}

func (s setting[T]) envName() string {
	return "DKV_" + strings.ToUpper(strings.ReplaceAll(s.key, ".", "_"))
}

func (s setting[T]) flagName() string { return strings.ReplaceAll(s.key, "_", "-") }

func stringSetting[T any](key, usage string, field func(*T) *string) setting[T] {
	return setting[T]{key, usage, func(c *T, v string) error {
		*field(c) = v
		return nil
	}}
}

func intSetting[T any](key, usage string, field func(*T) *int) setting[T] {
	return setting[T]{key, usage, func(c *T, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("not an integer: %w", err)
		}
		*field(c) = n
		return nil
	}}
}

// stringListSetting parses a comma-separated list; YAML uses a sequence.
func stringListSetting[T any](key, usage string, field func(*T) *[]string) setting[T] {
	return setting[T]{key, usage, func(c *T, v string) error {
		var out []string
		for _, item := range strings.Split(v, ",") {
			if item = strings.TrimSpace(item); item != "" {
				out = append(out, item)
			}
		}
		*field(c) = out
		return nil
	}}
}

func durationSetting[T any](key, usage string, field func(*T) *time.Duration) setting[T] {
	return setting[T]{key, usage, func(c *T, v string) error {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("not a duration (e.g. 5s, 250ms): %w", err)
		}
		*field(c) = d
		return nil
	}}
}

// settings lists every configuration key. TestSettingsCoverConfig fails if
// a field of Config is missing here.
var settings = []setting[Config]{
	stringSetting("server.grpc_addr", "host:port for the public gRPC API (required)",
		func(c *Config) *string { return &c.Server.GRPCAddr }),
	durationSetting("server.shutdown_timeout", "how long graceful shutdown waits for in-flight requests",
		func(c *Config) *time.Duration { return &c.Server.ShutdownTimeout }),
	stringSetting("server.admin_addr", "host:port for the admin HTTP API (status, metrics, events); empty disables it",
		func(c *Config) *string { return &c.Server.AdminAddr }),
	stringSetting("cluster.name", "cluster name; must match on every node",
		func(c *Config) *string { return &c.Cluster.Name }),
	stringSetting("cluster.internal_addr", "host:port for the node-to-node API (required with cluster.peers)",
		func(c *Config) *string { return &c.Cluster.InternalAddr }),
	stringSetting("cluster.advertise_addr", "internal address other nodes use to reach this one (default cluster.internal_addr)",
		func(c *Config) *string { return &c.Cluster.AdvertiseAddr }),
	stringListSetting("cluster.peers", "bootstrap peer list: comma-separated internal addresses",
		func(c *Config) *[]string { return &c.Cluster.Peers }),
	durationSetting("cluster.ping_interval", "how often each peer is pinged",
		func(c *Config) *time.Duration { return &c.Cluster.PingInterval }),
	durationSetting("cluster.ping_timeout", "deadline for one ping",
		func(c *Config) *time.Duration { return &c.Cluster.PingTimeout }),
	durationSetting("cluster.down_after", "silence after which a peer is reported down",
		func(c *Config) *time.Duration { return &c.Cluster.DownAfter }),
	intSetting("cluster.shards", "number of shards; fixed when the cluster is bootstrapped",
		func(c *Config) *int { return &c.Cluster.Shards }),
	intSetting("cluster.replication_factor", "replicas per shard; fixed when the cluster is bootstrapped",
		func(c *Config) *int { return &c.Cluster.ReplicationFactor }),
	intSetting("cluster.meta_replicas", "replicas of the meta group; fixed when the cluster is bootstrapped",
		func(c *Config) *int { return &c.Cluster.MetaReplicas }),
	stringListSetting("cluster.join", "internal addresses of an existing cluster's nodes, for a new node to join through",
		func(c *Config) *[]string { return &c.Cluster.Join }),
	durationSetting("transport.dial_timeout", "deadline for connecting to a peer",
		func(c *Config) *time.Duration { return &c.Transport.DialTimeout }),
	durationSetting("transport.keepalive_time", "idle time before a keepalive ping on a peer connection",
		func(c *Config) *time.Duration { return &c.Transport.KeepaliveTime }),
	durationSetting("transport.keepalive_timeout", "wait for a keepalive answer before closing the connection",
		func(c *Config) *time.Duration { return &c.Transport.KeepaliveTimeout }),
	durationSetting("raft.tick_interval", "period of the node's Raft ticker",
		func(c *Config) *time.Duration { return &c.Raft.TickInterval }),
	intSetting("raft.election_ticks", "minimum election timeout, in ticks (randomized up to twice this)",
		func(c *Config) *int { return &c.Raft.ElectionTicks }),
	intSetting("raft.heartbeat_ticks", "leader heartbeat period, in ticks",
		func(c *Config) *int { return &c.Raft.HeartbeatTicks }),
	durationSetting("raft.session_ttl", "how long a client deduplication session lives without a write",
		func(c *Config) *time.Duration { return &c.Raft.SessionTTL }),
	intSetting("raft.compact_entries", "raft log length, in applied entries, that triggers compaction",
		func(c *Config) *int { return &c.Raft.CompactEntries }),
	durationSetting("liveness.heartbeat_interval", "how often a node heartbeats the meta leader",
		func(c *Config) *time.Duration { return &c.Liveness.HeartbeatInterval }),
	durationSetting("liveness.suspect_after", "heartbeat silence after which a node is reported suspect",
		func(c *Config) *time.Duration { return &c.Liveness.SuspectAfter }),
	durationSetting("liveness.dead_after", "heartbeat silence after which a node is declared dead and its replicas are moved",
		func(c *Config) *time.Duration { return &c.Liveness.DeadAfter }),
	durationSetting("rebalance.interval", "how often the rebalancer on the meta leader runs",
		func(c *Config) *time.Duration { return &c.Rebalance.Interval }),
	stringSetting("storage.dir", "data directory (required)",
		func(c *Config) *string { return &c.Storage.Dir }),
	stringSetting("storage.sync", "WAL sync policy: always, interval or never",
		func(c *Config) *string { return &c.Storage.Sync }),
	durationSetting("storage.sync_interval", "fsync period when storage.sync is interval",
		func(c *Config) *time.Duration { return &c.Storage.SyncInterval }),
	intSetting("storage.memtable_bytes", "memtable size that starts a new memtable and WAL segment, in bytes",
		func(c *Config) *int { return &c.Storage.MemtableBytes }),
	intSetting("storage.block_cache_bytes", "block cache shared by every engine on the node, in bytes",
		func(c *Config) *int { return &c.Storage.BlockCacheBytes }),
	intSetting("storage.compaction_concurrency", "engines on the node that may compact at once",
		func(c *Config) *int { return &c.Storage.CompactionConcurrency }),
	intSetting("limits.max_key_bytes", "largest accepted key, in bytes",
		func(c *Config) *int { return &c.Limits.MaxKeyBytes }),
	intSetting("limits.max_value_bytes", "largest accepted value, in bytes",
		func(c *Config) *int { return &c.Limits.MaxValueBytes }),
	stringSetting("log.level", "log level: debug, info, warn or error",
		func(c *Config) *string { return &c.Log.Level }),
	stringSetting("log.format", "log format: json or text",
		func(c *Config) *string { return &c.Log.Format }),
}

const configEnv = "DKV_CONFIG"

// flagRecorder captures flag values so they can be applied after the YAML
// file and the environment, which must be read first.
type flagRecorder[T any] struct {
	s       *setting[T]
	pending *[]pendingFlag[T]
}

type pendingFlag[T any] struct {
	s     *setting[T]
	value string
}

func (f flagRecorder[T]) String() string { return "" }

func (f flagRecorder[T]) Set(v string) error {
	*f.pending = append(*f.pending, pendingFlag[T]{f.s, v})
	return nil
}

// Load builds a validated Config from args (command-line arguments without
// the program name), environ (KEY=VALUE pairs, as from os.Environ) and the
// YAML file they name. Usage and flag errors are written to output. If args
// ask for help, Load returns flag.ErrHelp.
func Load(program string, args, environ []string, output io.Writer) (*Config, error) {
	return loadConfig(program, args, environ, output, settings, Default(), (*Config).Validate)
}

// loadConfig is Load for any configuration type T, with its settings table,
// defaults and validation.
func loadConfig[T any](program string, args, environ []string, output io.Writer, settings []setting[T], cfg T, validate func(*T) error) (*T, error) {
	fs := flag.NewFlagSet(program, flag.ContinueOnError)
	fs.SetOutput(output)
	configPath := fs.String("config", "", "path to a YAML config file (env "+configEnv+")")
	var pending []pendingFlag[T]
	for i := range settings {
		s := &settings[i]
		fs.Var(flagRecorder[T]{s, &pending}, s.flagName(), fmt.Sprintf("%s (env %s)", s.usage, s.envName()))
	}
	fs.Usage = func() {
		_, _ = fmt.Fprintf(output, "Usage: %s [flags]\n\nPriority: flags > DKV_* environment > config file > defaults.\n\nFlags:\n", program)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected arguments: %q", fs.Args())
	}

	env, err := parseEnviron(environ, settings)
	if err != nil {
		return nil, err
	}

	path := *configPath
	if path == "" {
		path = env[configEnv]
	}
	if path != "" {
		if err := loadFile(&cfg, path); err != nil {
			return nil, err
		}
	}

	for i := range settings {
		s := &settings[i]
		if v, ok := env[s.envName()]; ok {
			if err := s.set(&cfg, v); err != nil {
				return nil, fmt.Errorf("environment variable %s: %w", s.envName(), err)
			}
		}
	}

	for _, p := range pending {
		if err := p.s.set(&cfg, p.value); err != nil {
			return nil, fmt.Errorf("flag --%s: %w", p.s.flagName(), err)
		}
	}

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration:\n%w", err)
	}
	return &cfg, nil
}

// parseEnviron returns the DKV_* variables in environ, rejecting those
// that are not in settings.
func parseEnviron[T any](environ []string, settings []setting[T]) (map[string]string, error) {
	known := map[string]bool{configEnv: true}
	for _, s := range settings {
		known[s.envName()] = true
	}
	env := map[string]string{}
	var unknown []string
	for _, kv := range environ {
		k, v, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(k, "DKV_") {
			continue
		}
		if !known[k] {
			unknown = append(unknown, k)
			continue
		}
		env[k] = v
	}
	if len(unknown) > 0 {
		return nil, fmt.Errorf("unknown environment variables %v (run with -h to list the supported ones)", unknown)
	}
	return env, nil
}

func loadFile[T any](cfg *T, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("parse config file %s: %w", path, err)
	}
	return nil
}

// validateCluster checks the cluster and transport sections.
func (c *Config) validateCluster() []error {
	var errs []error
	bad := func(key, format string, args ...any) {
		errs = append(errs, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
	}
	cl := c.Cluster
	if cl.Name == "" {
		bad("cluster.name", "must not be empty")
	}
	if cl.InternalAddr == "" {
		if len(cl.Peers) > 0 {
			bad("cluster.internal_addr", "required when cluster.peers is set")
		}
		if cl.AdvertiseAddr != "" {
			bad("cluster.advertise_addr", "set without cluster.internal_addr")
		}
	} else {
		host, err := splitHostPort(cl.InternalAddr, true)
		if err != nil {
			errs = append(errs, fmt.Errorf("cluster.internal_addr: %w", err))
		} else if cl.AdvertiseAddr == "" && unspecifiedHost(host) {
			bad("cluster.advertise_addr", "required because cluster.internal_addr %q has no specific host", cl.InternalAddr)
		}
		if cl.AdvertiseAddr != "" {
			if host, err := splitHostPort(cl.AdvertiseAddr, false); err != nil {
				errs = append(errs, fmt.Errorf("cluster.advertise_addr: %w", err))
			} else if unspecifiedHost(host) {
				bad("cluster.advertise_addr", "%q has no specific host", cl.AdvertiseAddr)
			}
		}
		if cl.InternalAddr == c.Server.GRPCAddr {
			bad("cluster.internal_addr", "must differ from server.grpc_addr")
		}
	}
	seen := map[string]bool{}
	for _, p := range cl.Peers {
		if _, err := splitHostPort(p, false); err != nil {
			errs = append(errs, fmt.Errorf("cluster.peers: %w", err))
		}
		if seen[p] {
			bad("cluster.peers", "%q is listed twice", p)
		}
		seen[p] = true
	}
	if cl.Shards < 1 || cl.Shards > 4096 {
		bad("cluster.shards", "must be between 1 and 4096, got %d", cl.Shards)
	}
	if cl.ReplicationFactor < 1 {
		bad("cluster.replication_factor", "must be at least 1, got %d", cl.ReplicationFactor)
	}
	if cl.MetaReplicas < 1 {
		bad("cluster.meta_replicas", "must be at least 1, got %d", cl.MetaReplicas)
	}
	for _, j := range cl.Join {
		if _, err := splitHostPort(j, false); err != nil {
			errs = append(errs, fmt.Errorf("cluster.join: %w", err))
		}
	}
	if len(cl.Join) > 0 && cl.InternalAddr == "" {
		bad("cluster.internal_addr", "required when cluster.join is set")
	}
	if cl.InternalAddr != "" && len(cl.Peers) == 0 && len(cl.Join) == 0 {
		bad("cluster.peers", "a cluster node needs cluster.peers (to bootstrap) or cluster.join (to join an existing cluster)")
	}
	if len(cl.Peers) > 0 {
		if cl.ReplicationFactor > len(seen) {
			bad("cluster.replication_factor", "%d needs at least that many peers, have %d", cl.ReplicationFactor, len(seen))
		}
		// Placement is computed from these exact strings on every node, so
		// each node must find its own address among them.
		if self := c.Cluster.Advertised(); self != "" && !seen[self] {
			bad("cluster.peers", "must include this node's advertise address %q", self)
		}
	}
	if cl.PingInterval <= 0 {
		bad("cluster.ping_interval", "must be positive, got %v", cl.PingInterval)
	}
	if cl.PingTimeout <= 0 || cl.PingTimeout > cl.PingInterval {
		bad("cluster.ping_timeout", "must be positive and at most cluster.ping_interval (%v), got %v", cl.PingInterval, cl.PingTimeout)
	}
	if cl.DownAfter <= cl.PingInterval {
		bad("cluster.down_after", "must be longer than cluster.ping_interval (%v), got %v", cl.PingInterval, cl.DownAfter)
	}
	t := c.Transport
	if t.DialTimeout <= 0 {
		bad("transport.dial_timeout", "must be positive, got %v", t.DialTimeout)
	}
	if t.KeepaliveTime < time.Second {
		bad("transport.keepalive_time", "must be at least 1s, got %v", t.KeepaliveTime)
	}
	if t.KeepaliveTimeout <= 0 {
		bad("transport.keepalive_timeout", "must be positive, got %v", t.KeepaliveTimeout)
	}
	r := c.Raft
	if r.TickInterval < time.Millisecond {
		bad("raft.tick_interval", "must be at least 1ms, got %v", r.TickInterval)
	}
	if r.HeartbeatTicks < 1 {
		bad("raft.heartbeat_ticks", "must be at least 1, got %d", r.HeartbeatTicks)
	}
	// Followers must hear several heartbeats per election timeout, or
	// one lost heartbeat starts an election.
	if r.ElectionTicks < 3*r.HeartbeatTicks {
		bad("raft.election_ticks", "must be at least 3 * raft.heartbeat_ticks (%d), got %d", 3*r.HeartbeatTicks, r.ElectionTicks)
	}
	if r.SessionTTL < time.Minute {
		bad("raft.session_ttl", "must be at least 1m, got %v", r.SessionTTL)
	}
	if r.CompactEntries < 10 {
		bad("raft.compact_entries", "must be at least 10, got %d", r.CompactEntries)
	}
	l := c.Liveness
	if l.HeartbeatInterval <= 0 {
		bad("liveness.heartbeat_interval", "must be positive, got %v", l.HeartbeatInterval)
	}
	if l.SuspectAfter <= l.HeartbeatInterval {
		bad("liveness.suspect_after", "must be longer than liveness.heartbeat_interval (%v), got %v", l.HeartbeatInterval, l.SuspectAfter)
	}
	if l.DeadAfter < l.SuspectAfter {
		bad("liveness.dead_after", "must be at least liveness.suspect_after (%v), got %v", l.SuspectAfter, l.DeadAfter)
	}
	if c.Rebalance.Interval <= 0 {
		bad("rebalance.interval", "must be positive, got %v", c.Rebalance.Interval)
	}
	return errs
}

// splitHostPort checks a host:port address and returns the host. Port 0
// (pick a free port) is allowed only for listen addresses.
func splitHostPort(addr string, listen bool) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return "", fmt.Errorf("invalid port %q in %q: %w", port, addr, err)
	}
	if n == 0 && !listen {
		return "", fmt.Errorf("port 0 in %q: only a listen address may pick a free port", addr)
	}
	return host, nil
}

func unspecifiedHost(host string) bool {
	if host == "" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

// Advertised returns the internal address other nodes use to reach this
// node: AdvertiseAddr, or InternalAddr if that is unset.
func (c Cluster) Advertised() string {
	if c.AdvertiseAddr != "" {
		return c.AdvertiseAddr
	}
	return c.InternalAddr
}

// Clustered reports whether the node runs as part of a cluster.
func (c Cluster) Clustered() bool { return c.InternalAddr != "" }
