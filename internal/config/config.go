package config

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for a dkv node
type Config struct {
	Node        NodeConfig        `yaml:"node" json:"node"`
	Cluster     ClusterConfig     `yaml:"cluster" json:"cluster"`
	Storage     StorageConfig     `yaml:"storage" json:"storage"`
	Raft        RaftConfig        `yaml:"raft" json:"raft"`
	Transport   TransportConfig   `yaml:"transport" json:"transport"`
	Server      ServerConfig      `yaml:"server" json:"server"`
	Observability ObsConfig      `yaml:"observability" json:"observability"`
}

// NodeConfig holds node-specific settings
type NodeConfig struct {
	// NodeID is the unique identifier for this node.
	// If empty, one will be generated at startup.
	NodeID string `yaml:"node_id" json:"node_id"`

	// DataDir is the directory where this node stores its data.
	// Required.
	DataDir string `yaml:"data_dir" json:"data_dir"`

	// Address is the host:port this node listens on for internal gRPC.
	// Required.
	Address string `yaml:"address" json:"address"`

	// AdvertiseAddress is the address other nodes use to connect to this node.
	// If empty, Address is used.
	AdvertiseAddress string `yaml:"advertise_address" json:"advertise_address"`
}

// ClusterConfig holds cluster-wide settings
type ClusterConfig struct {
	// NumShards is the total number of shards in the cluster.
	// Fixed at bootstrap, default 64.
	NumShards uint64 `yaml:"num_shards" json:"num_shards"`

	// ReplicationFactor is the number of replicas per shard.
	// Must be odd, default 3.
	ReplicationFactor uint64 `yaml:"replication_factor" json:"replication_factor"`

	// BootstrapPeers is a list of seed node addresses for joining the cluster.
	// Format: "node_id@host:port". At least one required for non-bootstrap nodes.
	BootstrapPeers []string `yaml:"bootstrap_peers" json:"bootstrap_peers"`

	// Bootstrap indicates this node is a bootstrap node (part of initial meta group).
	// Exactly one of the first RF nodes must set this to true.
	Bootstrap bool `yaml:"bootstrap" json:"bootstrap"`
}

// StorageConfig holds storage engine settings
type StorageConfig struct {
	// MemtableSizeBytes is the maximum size of a memtable before flush.
	// Default 4MB.
	MemtableSizeBytes uint64 `yaml:"memtable_size_bytes" json:"memtable_size_bytes"`

	// MaxMemtables is the maximum number of immutable memtables before stall.
	// Default 4.
	MaxMemtables int `yaml:"max_memtables" json:"max_memtables"`

	// BlockSizeBytes is the target size of SSTable data blocks.
	// Default 4KB.
	BlockSizeBytes uint64 `yaml:"block_size_bytes" json:"block_size_bytes"`

	// BloomFilterBitsPerKey is the bits per key for bloom filters.
	// Default 10 (~1% false positive rate).
	BloomFilterBitsPerKey uint32 `yaml:"bloom_filter_bits_per_key" json:"bloom_filter_bits_per_key"`

	// SyncPolicy controls WAL fsync behavior.
	// One of: "always", "interval", "never". Default "always".
	SyncPolicy string `yaml:"sync_policy" json:"sync_policy"`

	// SyncInterval is the interval for SyncInterval policy.
	// Default 100ms.
	SyncInterval time.Duration `yaml:"sync_interval" json:"sync_interval"`

	// CompactionWorkers is the number of concurrent compaction workers.
	// Default: number of CPUs.
	CompactionWorkers int `yaml:"compaction_workers" json:"compaction_workers"`

	// CompactionRateLimitMBps limits compaction throughput.
	// 0 = unlimited. Default 0.
	CompactionRateLimitMBps uint64 `yaml:"compaction_rate_limit_mbps" json:"compaction_rate_limit_mbps"`

	// BlockCacheSizeBytes is the size of the shared block cache.
	// Default 64MB.
	BlockCacheSizeBytes uint64 `yaml:"block_cache_size_bytes" json:"block_cache_size_bytes"`

	// SnapshotThresholdEntries triggers a Raft snapshot at this many entries.
	// Default 10000.
	SnapshotThresholdEntries uint64 `yaml:"snapshot_threshold_entries" json:"snapshot_threshold_entries"`

	// SnapshotThresholdBytes triggers a Raft snapshot at this many bytes.
	// Default 64MB.
	SnapshotThresholdBytes uint64 `yaml:"snapshot_threshold_bytes" json:"snapshot_threshold_bytes"`
}

// RaftConfig holds Raft consensus settings
type RaftConfig struct {
	// TickInterval is the interval between Raft ticks.
	// Default 50ms.
	TickInterval time.Duration `yaml:"tick_interval" json:"tick_interval"`

	// HeartbeatTicks is the number of ticks between heartbeats.
	// Default 2.
	HeartbeatTicks int `yaml:"heartbeat_ticks" json:"heartbeat_ticks"`

	// ElectionTimeoutTicksMin is the minimum election timeout in ticks.
	// Default 10.
	ElectionTimeoutTicksMin int `yaml:"election_timeout_ticks_min" json:"election_timeout_ticks_min"`

	// ElectionTimeoutTicksMax is the maximum election timeout in ticks.
	// Default 20.
	ElectionTimeoutTicksMax int `yaml:"election_timeout_ticks_max" json:"election_timeout_ticks_max"`

	// MaxInflightMsgs is the max number of in-flight AppendEntries per follower.
	// Default 256.
	MaxInflightMsgs int `yaml:"max_inflight_msgs" json:"max_inflight_msgs"`

	// SnapshotCatchUpEntries is the threshold for sending snapshots instead of entries.
	// Default 10000.
	SnapshotCatchUpEntries uint64 `yaml:"snapshot_catch_up_entries" json:"snapshot_catch_up_entries"`

	// PreVote enables the PreVote protocol.
	// Default true.
	PreVote bool `yaml:"pre_vote" json:"pre_vote"`

	// CheckQuorum enables CheckQuorum for leaders.
	// Default true.
	CheckQuorum bool `yaml:"check_quorum" json:"check_quorum"`
}

// TransportConfig holds internal gRPC transport settings
type TransportConfig struct {
	// MaxMsgSize is the maximum gRPC message size in bytes.
	// Default 4MB.
	MaxMsgSize int `yaml:"max_msg_size" json:"max_msg_size"`

	// ConnectTimeout is the timeout for establishing connections.
	// Default 5s.
	ConnectTimeout time.Duration `yaml:"connect_timeout" json:"connect_timeout"`

	// RequestTimeout is the default timeout for RPCs.
	// Default 10s.
	RequestTimeout time.Duration `yaml:"request_timeout" json:"request_timeout"`

	// KeepaliveTime is the interval for gRPC keepalives.
	// Default 30s.
	KeepaliveTime time.Duration `yaml:"keepalive_time" json:"keepalive_time"`

	// KeepaliveTimeout is the timeout for keepalive pings.
	// Default 10s.
	KeepaliveTimeout time.Duration `yaml:"keepalive_timeout" json:"keepalive_timeout"`

	// MaxConcurrentStreams is the max concurrent streams per connection.
	// Default 100.
	MaxConcurrentStreams uint32 `yaml:"max_concurrent_streams" json:"max_concurrent_streams"`
}

// ServerConfig holds public API server settings
type ServerConfig struct {
	// GRPCAddress is the host:port for the public gRPC KV service.
	// If empty, no public gRPC server is started.
	GRPCAddress string `yaml:"grpc_address" json:"grpc_address"`

	// HTTPAddress is the host:port for the admin HTTP API.
	// If empty, no HTTP server is started.
	HTTPAddress string `yaml:"http_address" json:"http_address"`

	// MaxConcurrentStreams is the max concurrent gRPC streams.
	// Default 1000.
	MaxConcurrentStreams uint32 `yaml:"max_concurrent_streams" json:"max_concurrent_streams"`
}

// ObsConfig holds observability settings
type ObsConfig struct {
	// LogLevel is the minimum log level: debug, info, warn, error.
	// Default "info".
	LogLevel string `yaml:"log_level" json:"log_level"`

	// LogFormat is the log format: "json" or "text".
	// Default "json".
	LogFormat string `yaml:"log_format" json:"log_format"`

	// MetricsAddress is the host:port for the Prometheus metrics endpoint.
	// If empty, no metrics endpoint is started.
	MetricsAddress string `yaml:"metrics_address" json:"metrics_address"`

	// EnableProfiling enables pprof endpoints on the HTTP server.
	// Default false.
	EnableProfiling bool `yaml:"enable_profiling" json:"enable_profiling"`
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			NodeID:           "",
			DataDir:          "",
			Address:          "",
			AdvertiseAddress: "",
		},
		Cluster: ClusterConfig{
			NumShards:         64,
			ReplicationFactor: 3,
			BootstrapPeers:    nil,
			Bootstrap:         false,
		},
		Storage: StorageConfig{
			MemtableSizeBytes:         4 << 20,  // 4MB
			MaxMemtables:              4,
			BlockSizeBytes:            4 << 10,  // 4KB
			BloomFilterBitsPerKey:     10,
			SyncPolicy:                "always",
			SyncInterval:              100 * time.Millisecond,
			CompactionWorkers:         0,  // 0 = auto
			CompactionRateLimitMBps:   0,
			BlockCacheSizeBytes:       64 << 20,  // 64MB
			SnapshotThresholdEntries:  10000,
			SnapshotThresholdBytes:    64 << 20,  // 64MB
		},
		Raft: RaftConfig{
			TickInterval:             50 * time.Millisecond,
			HeartbeatTicks:           2,
			ElectionTimeoutTicksMin:  10,
			ElectionTimeoutTicksMax:  20,
			MaxInflightMsgs:          256,
			SnapshotCatchUpEntries:   10000,
			PreVote:                  true,
			CheckQuorum:              true,
		},
		Transport: TransportConfig{
			MaxMsgSize:            4 << 20,  // 4MB
			ConnectTimeout:        5 * time.Second,
			RequestTimeout:        10 * time.Second,
			KeepaliveTime:         30 * time.Second,
			KeepaliveTimeout:      10 * time.Second,
			MaxConcurrentStreams:  100,
		},
		Server: ServerConfig{
			GRPCAddress:          "",
			HTTPAddress:          "",
			MaxConcurrentStreams: 1000,
		},
		Observability: ObsConfig{
			LogLevel:         "info",
			LogFormat:        "json",
			MetricsAddress:   "",
			EnableProfiling:  false,
		},
	}
}

// Load loads configuration from file, environment variables, and flags.
// Priority: flags > env vars > config file > defaults.
func Load() (*Config, error) {
	cfg := DefaultConfig()

	// Config file flag
	configFile := flag.String("config", "", "Path to config file (YAML)")
	flag.Parse()

	// Load from file if specified
	if *configFile != "" {
		if err := loadFromFile(cfg, *configFile); err != nil {
			return nil, fmt.Errorf("load config file: %w", err)
		}
	}

	// Load from environment variables
	loadFromEnv(cfg)

	// Apply flag overrides (flag package already parsed into cfg via custom flag set)
	// We need to re-parse with our own flag set to override
	applyFlags(cfg)

	// Validate
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

// loadFromFile loads configuration from a YAML file
func loadFromFile(cfg *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("parse config file: %w", err)
	}

	return nil
}

// loadFromEnv loads configuration from environment variables
// DKV_<SECTION>_<FIELD> (uppercase, underscores)
func loadFromEnv(cfg *Config) {
	// Node
	if v := os.Getenv("DKV_NODE_ID"); v != "" {
		cfg.Node.NodeID = v
	}
	if v := os.Getenv("DKV_DATA_DIR"); v != "" {
		cfg.Node.DataDir = v
	}
	if v := os.Getenv("DKV_ADDRESS"); v != "" {
		cfg.Node.Address = v
	}
	if v := os.Getenv("DKV_ADVERTISE_ADDRESS"); v != "" {
		cfg.Node.AdvertiseAddress = v
	}

	// Cluster
	if v := os.Getenv("DKV_NUM_SHARDS"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Cluster.NumShards)
	}
	if v := os.Getenv("DKV_REPLICATION_FACTOR"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Cluster.ReplicationFactor)
	}
	if v := os.Getenv("DKV_BOOTSTRAP_PEERS"); v != "" {
		cfg.Cluster.BootstrapPeers = strings.Split(v, ",")
	}
	if v := os.Getenv("DKV_BOOTSTRAP"); v != "" {
		cfg.Cluster.Bootstrap = v == "true" || v == "1"
	}

	// Storage
	if v := os.Getenv("DKV_MEMTABLE_SIZE_BYTES"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Storage.MemtableSizeBytes)
	}
	if v := os.Getenv("DKV_SYNC_POLICY"); v != "" {
		cfg.Storage.SyncPolicy = v
	}
	if v := os.Getenv("DKV_COMPACTION_WORKERS"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Storage.CompactionWorkers)
	}

	// Raft
	if v := os.Getenv("DKV_TICK_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Raft.TickInterval = d
		}
	}

	// Transport
	if v := os.Getenv("DKV_CONNECT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Transport.ConnectTimeout = d
		}
	}
	if v := os.Getenv("DKV_REQUEST_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Transport.RequestTimeout = d
		}
	}

	// Server
	if v := os.Getenv("DKV_GRPC_ADDRESS"); v != "" {
		cfg.Server.GRPCAddress = v
	}
	if v := os.Getenv("DKV_HTTP_ADDRESS"); v != "" {
		cfg.Server.HTTPAddress = v
	}

	// Observability
	if v := os.Getenv("DKV_LOG_LEVEL"); v != "" {
		cfg.Observability.LogLevel = v
	}
	if v := os.Getenv("DKV_LOG_FORMAT"); v != "" {
		cfg.Observability.LogFormat = v
	}
	if v := os.Getenv("DKV_METRICS_ADDRESS"); v != "" {
		cfg.Observability.MetricsAddress = v
	}
}

// applyFlags applies command-line flag overrides
// We define flags here that override the config struct
func applyFlags(cfg *Config) {
	fs := flag.NewFlagSet("dkv", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	// Node
	fs.StringVar(&cfg.Node.NodeID, "node-id", cfg.Node.NodeID, "Node ID")
	fs.StringVar(&cfg.Node.DataDir, "data-dir", cfg.Node.DataDir, "Data directory")
	fs.StringVar(&cfg.Node.Address, "address", cfg.Node.Address, "Listen address")
	fs.StringVar(&cfg.Node.AdvertiseAddress, "advertise-address", cfg.Node.AdvertiseAddress, "Advertise address")

	// Cluster
	fs.Uint64Var(&cfg.Cluster.NumShards, "num-shards", cfg.Cluster.NumShards, "Number of shards")
	fs.Uint64Var(&cfg.Cluster.ReplicationFactor, "replication-factor", cfg.Cluster.ReplicationFactor, "Replication factor")
	fs.BoolVar(&cfg.Cluster.Bootstrap, "bootstrap", cfg.Cluster.Bootstrap, "Bootstrap node")

	// Storage
	fs.Uint64Var(&cfg.Storage.MemtableSizeBytes, "memtable-size", cfg.Storage.MemtableSizeBytes, "Memtable size")
	fs.StringVar(&cfg.Storage.SyncPolicy, "sync-policy", cfg.Storage.SyncPolicy, "Sync policy (always/interval/never)")

	// Server
	fs.StringVar(&cfg.Server.GRPCAddress, "grpc-address", cfg.Server.GRPCAddress, "Public gRPC address")
	fs.StringVar(&cfg.Server.HTTPAddress, "http-address", cfg.Server.HTTPAddress, "Admin HTTP address")

	// Observability
	fs.StringVar(&cfg.Observability.LogLevel, "log-level", cfg.Observability.LogLevel, "Log level")
	fs.StringVar(&cfg.Observability.LogFormat, "log-format", cfg.Observability.LogFormat, "Log format")
	fs.StringVar(&cfg.Observability.MetricsAddress, "metrics-address", cfg.Observability.MetricsAddress, "Metrics address")

	// Parse remaining args (after -config)
	_ = fs.Parse(os.Args[1:])
}

// Validate checks that the configuration is valid
func (c *Config) Validate() error {
	// Node
	if c.Node.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}
	if c.Node.Address == "" {
		return fmt.Errorf("address is required")
	}
	if c.Node.AdvertiseAddress == "" {
		c.Node.AdvertiseAddress = c.Node.Address
	}

	// Ensure data dir exists
	if err := os.MkdirAll(c.Node.DataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	// Make data dir absolute
	abs, err := filepath.Abs(c.Node.DataDir)
	if err != nil {
		return fmt.Errorf("absolute data dir: %w", err)
	}
	c.Node.DataDir = abs

	// Cluster
	if c.Cluster.NumShards == 0 {
		return fmt.Errorf("num_shards must be > 0")
	}
	if c.Cluster.ReplicationFactor == 0 {
		return fmt.Errorf("replication_factor must be > 0")
	}
	if c.Cluster.ReplicationFactor%2 == 0 {
		return fmt.Errorf("replication_factor must be odd")
	}
	if !c.Cluster.Bootstrap && len(c.Cluster.BootstrapPeers) == 0 {
		return fmt.Errorf("bootstrap_peers required for non-bootstrap nodes")
	}

	// Storage
	if c.Storage.MemtableSizeBytes == 0 {
		return fmt.Errorf("memtable_size_bytes must be > 0")
	}
	if c.Storage.MaxMemtables <= 0 {
		return fmt.Errorf("max_memtables must be > 0")
	}
	if c.Storage.BlockSizeBytes == 0 {
		return fmt.Errorf("block_size_bytes must be > 0")
	}
	if c.Storage.BloomFilterBitsPerKey == 0 {
		return fmt.Errorf("bloom_filter_bits_per_key must be > 0")
	}
	if c.Storage.SyncPolicy != "always" && c.Storage.SyncPolicy != "interval" && c.Storage.SyncPolicy != "never" {
		return fmt.Errorf("sync_policy must be 'always', 'interval', or 'never'")
	}
	if c.Storage.SyncPolicy == "interval" && c.Storage.SyncInterval <= 0 {
		return fmt.Errorf("sync_interval must be > 0 for interval policy")
	}
	if c.Storage.BlockCacheSizeBytes == 0 {
		return fmt.Errorf("block_cache_size_bytes must be > 0")
	}

	// Raft
	if c.Raft.TickInterval <= 0 {
		return fmt.Errorf("tick_interval must be > 0")
	}
	if c.Raft.HeartbeatTicks <= 0 {
		return fmt.Errorf("heartbeat_ticks must be > 0")
	}
	if c.Raft.ElectionTimeoutTicksMin <= 0 {
		return fmt.Errorf("election_timeout_ticks_min must be > 0")
	}
	if c.Raft.ElectionTimeoutTicksMax <= c.Raft.ElectionTimeoutTicksMin {
		return fmt.Errorf("election_timeout_ticks_max must be > election_timeout_ticks_min")
	}
	if c.Raft.MaxInflightMsgs <= 0 {
		return fmt.Errorf("max_inflight_msgs must be > 0")
	}

	// Transport
	if c.Transport.MaxMsgSize <= 0 {
		return fmt.Errorf("max_msg_size must be > 0")
	}
	if c.Transport.ConnectTimeout <= 0 {
		return fmt.Errorf("connect_timeout must be > 0")
	}
	if c.Transport.RequestTimeout <= 0 {
		return fmt.Errorf("request_timeout must be > 0")
	}
	if c.Transport.KeepaliveTime <= 0 {
		return fmt.Errorf("keepalive_time must be > 0")
	}
	if c.Transport.KeepaliveTimeout <= 0 {
		return fmt.Errorf("keepalive_timeout must be > 0")
	}
	if c.Transport.MaxConcurrentStreams == 0 {
		return fmt.Errorf("max_concurrent_streams must be > 0")
	}

	// Server
	if c.Server.MaxConcurrentStreams == 0 {
		return fmt.Errorf("server max_concurrent_streams must be > 0")
	}

	// Observability
	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[c.Observability.LogLevel] {
		return fmt.Errorf("log_level must be one of: debug, info, warn, error")
	}
	if c.Observability.LogFormat != "json" && c.Observability.LogFormat != "text" {
		return fmt.Errorf("log_format must be 'json' or 'text'")
	}

	return nil
}

// ElectionTimeoutMin returns the minimum election timeout as a duration
func (c *Config) ElectionTimeoutMin() time.Duration {
	return c.Raft.TickInterval * time.Duration(c.Raft.ElectionTimeoutTicksMin)
}

// ElectionTimeoutMax returns the maximum election timeout as a duration
func (c *Config) ElectionTimeoutMax() time.Duration {
	return c.Raft.TickInterval * time.Duration(c.Raft.ElectionTimeoutTicksMax)
}

// HeartbeatInterval returns the heartbeat interval as a duration
func (c *Config) HeartbeatInterval() time.Duration {
	return c.Raft.TickInterval * time.Duration(c.Raft.HeartbeatTicks)
}