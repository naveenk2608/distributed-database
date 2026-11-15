package obs

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Global metrics
var (
	// Request metrics
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dkv_requests_total",
			Help: "Total number of requests by type and status",
		},
		[]string{"type", "status"},
	)

	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dkv_request_duration_seconds",
			Help:    "Request latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)

	// Raft metrics
	RaftTerm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dkv_raft_term",
			Help: "Current Raft term",
		},
		[]string{"shard"},
	)

	RaftState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dkv_raft_state",
			Help: "Raft state (0=follower, 1=candidate, 2=leader)",
		},
		[]string{"shard"},
	)

	RaftCommitIndex = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dkv_raft_commit_index",
			Help: "Raft commit index",
		},
		[]string{"shard"},
	)

	RaftAppliedIndex = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dkv_raft_applied_index",
			Help: "Raft applied index",
		},
		[]string{"shard"},
	)

	RaftReplicationLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dkv_raft_replication_lag",
			Help: "Raft replication lag (commit - applied)",
		},
		[]string{"shard"},
	)

	// Storage metrics
	StorageMemtableSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dkv_storage_memtable_bytes",
			Help: "Current memtable size in bytes",
		},
		[]string{"shard"},
	)

	StorageSSTableCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dkv_storage_sstable_count",
			Help: "Number of SSTables",
		},
		[]string{"shard"},
	)

	StorageWALSegments = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dkv_storage_wal_segments",
			Help: "Number of WAL segments",
		},
		[]string{"shard"},
	)

	StorageCompactionBytesRead = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dkv_storage_compaction_bytes_read_total",
			Help: "Total bytes read during compaction",
		},
		[]string{"shard"},
	)

	StorageCompactionBytesWritten = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dkv_storage_compaction_bytes_written_total",
			Help: "Total bytes written during compaction",
		},
		[]string{"shard"},
	)

	StorageCompactionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dkv_storage_compaction_duration_seconds",
			Help:    "Compaction duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"shard"},
	)

	// Transport metrics
	TransportMessagesSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dkv_transport_messages_sent_total",
			Help: "Total Raft messages sent",
		},
		[]string{"type"},
	)

	TransportMessagesReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dkv_transport_messages_received_total",
			Help: "Total Raft messages received",
		},
		[]string{"type"},
	)

	TransportBatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dkv_transport_batch_size",
			Help:    "Raft message batch size",
			Buckets: []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000},
		},
	)

	// Cluster metrics
	ClusterNodes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dkv_cluster_nodes",
			Help: "Number of nodes in cluster",
		},
	)

	ClusterShards = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dkv_cluster_shards",
			Help: "Number of shards",
		},
	)

	// Go runtime metrics
	GoGoroutines = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dkv_go_goroutines",
			Help: "Number of goroutines",
		},
	)

	GoGCCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "dkv_go_gc_total",
			Help: "Number of GC cycles",
		},
	)

	GoGCPause = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dkv_go_gc_pause_seconds",
			Help:    "GC pause duration in seconds",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
	)
)

// InitLogger initializes the global slog logger
func InitLogger(level string, format string) *slog.Logger {
	var logLevel slog.Level
	switch level {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Rename time to timestamp for consistency
			if a.Key == slog.TimeKey {
				return slog.Attr{Key: "timestamp", Value: a.Value}
			}
			return a
		},
	}

	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

// InitMetrics starts the metrics HTTP server if address is provided
func InitMetrics(addr string) (*http.Server, error) {
	if addr == "" {
		return nil, nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "error", err)
		}
	}()

	slog.Info("metrics server started", "addr", addr)
	return server, nil
}

// ShutdownMetrics gracefully shuts down the metrics server
func ShutdownMetrics(ctx context.Context, server *http.Server) error {
	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}

// StartRuntimeMetricsCollector starts a background goroutine that collects Go runtime metrics
func StartRuntimeMetricsCollector(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastGCCount uint32
	var lastGCPause time.Duration

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Goroutines
			GoGoroutines.Set(float64(runtime.NumGoroutine()))

			// GC stats
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			gcCount := memStats.NumGC
			if gcCount > lastGCCount {
				GoGCCount.Add(float64(gcCount - lastGCCount))
				lastGCCount = gcCount
			}

			// Approximate GC pause from last GC
			if memStats.NumGC > 0 {
				pauseNs := memStats.PauseNs[(memStats.NumGC+255)%256]
				if pauseNs > 0 {
					GoGCPause.Observe(float64(pauseNs) / 1e9)
				}
			}

			_ = lastGCPause // avoid unused warning
		}
	}
}

// EventLog is a simple in-memory event log for cluster events
type EventLog struct {
	events []Event
	mu     chan struct{} // simple mutex
}

type Event struct {
	Sequence  uint64
	Timestamp int64
	Type      EventType
	NodeID    string
	ShardID   uint64
	Message   string
	Fields    map[string]string
}

type EventType string

const (
	EventTypeElection        EventType = "election"
	EventTypeLeaderChange    EventType = "leader_change"
	EventTypeNodeJoin        EventType = "node_join"
	EventTypeNodeLeave       EventType = "node_leave"
	EventTypeNodeDead        EventType = "node_dead"
	EventTypeRebalanceStart  EventType = "rebalance_start"
	EventTypeRebalanceComplete EventType = "rebalance_complete"
	EventTypeSnapshotStart   EventType = "snapshot_start"
	EventTypeSnapshotComplete EventType = "snapshot_complete"
	EventTypeCompactionStart EventType = "compaction_start"
	EventTypeCompactionComplete EventType = "compaction_complete"
	EventTypeError           EventType = "error"
)

// NewEventLog creates a new event log
func NewEventLog() *EventLog {
	return &EventLog{
		events: make([]Event, 0),
		mu:     make(chan struct{}, 1),
	}
}

// Add adds an event to the log
func (l *EventLog) Add(e Event) {
	l.mu <- struct{}{}
	defer func() { <-l.mu }()

	e.Sequence = uint64(len(l.events)) + 1
	if e.Timestamp == 0 {
		e.Timestamp = time.Now().UnixNano()
	}
	l.events = append(l.events, e)

	// Keep only last 10000 events
	if len(l.events) > 10000 {
		l.events = l.events[len(l.events)-10000:]
	}
}

// GetSince returns events after the given sequence number
func (l *EventLog) GetSince(seq uint64, limit int) []Event {
	l.mu <- struct{}{}
	defer func() { <-l.mu }()

	var result []Event
	for _, e := range l.events {
		if e.Sequence > seq {
			result = append(result, e)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result
}

// GetAll returns all events
func (l *EventLog) GetAll() []Event {
	l.mu <- struct{}{}
	defer func() { <-l.mu }()

	result := make([]Event, len(l.events))
	copy(result, l.events)
	return result
}

// LogEvent is a helper to log an event with structured logging
func LogEvent(logger *slog.Logger, e Event) {
	attrs := []slog.Attr{
		slog.String("event_type", string(e.Type)),
		slog.String("node_id", e.NodeID),
		slog.Uint64("shard_id", e.ShardID),
		slog.String("message", e.Message),
	}

	for k, v := range e.Fields {
		attrs = append(attrs, slog.String(k, v))
	}

	logger.LogAttrs(context.Background(), slog.LevelInfo, "cluster_event", attrs...)
}

// RecordRequest records a request metric
func RecordRequest(reqType, status string, duration time.Duration) {
	RequestsTotal.WithLabelValues(reqType, status).Inc()
	RequestDuration.WithLabelValues(reqType).Observe(duration.Seconds())
}

// RecordRaftMetrics records Raft-related metrics for a shard
func RecordRaftMetrics(shardID uint64, term uint64, state int, commitIdx, appliedIdx uint64) {
	shard := fmt.Sprintf("%d", shardID)
	RaftTerm.WithLabelValues(shard).Set(float64(term))
	RaftState.WithLabelValues(shard).Set(float64(state))
	RaftCommitIndex.WithLabelValues(shard).Set(float64(commitIdx))
	RaftAppliedIndex.WithLabelValues(shard).Set(float64(appliedIdx))
	RaftReplicationLag.WithLabelValues(shard).Set(float64(commitIdx - appliedIdx))
}

// RecordStorageMetrics records storage-related metrics for a shard
func RecordStorageMetrics(shardID uint64, memtableBytes, sstableCount, walSegments uint64) {
	shard := fmt.Sprintf("%d", shardID)
	StorageMemtableSize.WithLabelValues(shard).Set(float64(memtableBytes))
	StorageSSTableCount.WithLabelValues(shard).Set(float64(sstableCount))
	StorageWALSegments.WithLabelValues(shard).Set(float64(walSegments))
}