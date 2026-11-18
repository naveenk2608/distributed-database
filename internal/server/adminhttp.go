package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/obs"
	"github.com/naveenk2608/distributed-database/internal/storage/sstable"
)

// ops are the public API operations the server counts, by method.
var ops = map[string]string{
	dkvv1.KVService_Put_FullMethodName:           "put",
	dkvv1.KVService_Get_FullMethodName:           "get",
	dkvv1.KVService_Delete_FullMethodName:        "delete",
	dkvv1.KVService_CompareAndSet_FullMethodName: "cas",
}

// Stats is the admin's statistics with the server's request counters.
func (s *Server) Stats() *dkvv1.StatsResponse {
	resp := s.admin.Stats()
	resp.UnixNano = time.Now().UnixNano()
	for _, b := range obs.LatencyBuckets {
		resp.LatencyBoundsSeconds = append(resp.LatencyBoundsSeconds, b.Seconds())
	}
	for _, o := range s.requests.Snapshot() {
		resp.Ops = append(resp.Ops, &dkvv1.OpStats{Op: o.Op, Ok: o.OK, Errors: o.Errors,
			LatencySumSeconds: o.Sum.Seconds(), LatencyCumulative: o.Cumulative})
	}
	return resp
}

func cacheStats(c sstable.CacheStats) *dkvv1.CacheStats {
	return &dkvv1.CacheStats{CapacityBytes: c.Capacity, UsedBytes: c.Used, Hits: c.Hits, Misses: c.Misses}
}

// CacheStats converts block cache statistics for the operator API.
func CacheStats(c sstable.CacheStats) *dkvv1.CacheStats { return cacheStats(c) }

// AdminHandler serves the read-only admin HTTP API, for dkv-console and
// operators. Changes (drain, transfer, decommission) go through the gRPC
// AdminService only, so nothing reading this API can affect the cluster.
//
//	GET /v1/status                 ClusterStatusResponse
//	GET /v1/shards                 ShardMapResponse
//	GET /v1/stats                  StatsResponse
//	GET /v1/events?after=N&limit=M EventsResponse
//	GET /metrics                   Prometheus text format
//	GET /healthz                   "ok"
//	GET /debug/pprof/...           Go's profiler, with server.admin_pprof
//
// Responses are the protobuf messages in their JSON form (protojson,
// field names as in the .proto files, zero values included; 64-bit
// integers are strings, as protojson writes them).
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.admin.Status()) })
	mux.HandleFunc("GET /v1/shards", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.admin.ShardMap()) })
	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.Stats()) })
	mux.HandleFunc("GET /v1/events", func(w http.ResponseWriter, r *http.Request) {
		after, err := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		if err != nil && r.URL.Query().Has("after") {
			http.Error(w, "after: not a sequence number", http.StatusBadRequest)
			return
		}
		limit, err := strconv.ParseUint(r.URL.Query().Get("limit"), 10, 32)
		if err != nil && r.URL.Query().Has("limit") {
			http.Error(w, "limit: not a number", http.StatusBadRequest)
			return
		}
		writeJSON(w, s.admin.Events(after, eventLimit(uint32(limit))))
	})
	reg := prometheus.NewRegistry()
	reg.MustRegister(statsCollector{s.Stats})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	if s.cfg.AdminPprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}

var jsonOptions = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}

func writeJSON(w http.ResponseWriter, m proto.Message) {
	b, err := jsonOptions.Marshal(m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b) // the client went away; nothing to do
}

// RunAdmin serves the admin HTTP API on lis until ctx is cancelled, then
// shuts it down within the configured shutdown timeout. It closes lis.
func (s *Server) RunAdmin(ctx context.Context, lis net.Listener) error {
	s.log.Info("serving admin HTTP API", "addr", lis.Addr().String())
	srv := &http.Server{Handler: s.AdminHandler(), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	select {
	case err := <-serveErr:
		return fmt.Errorf("admin HTTP: %w", err)
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	shutErr := srv.Shutdown(sctx)
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("admin HTTP: %w", err)
	}
	if shutErr != nil {
		_ = srv.Close() // shutdown timed out: drop the remaining connections
	}
	return nil
}

// statsCollector exposes a node's statistics in Prometheus format,
// collected from Stats at scrape time.
type statsCollector struct{ stats func() *dkvv1.StatsResponse }

var (
	descRequests   = prometheus.NewDesc("dkv_requests_total", "Public API requests by operation and outcome.", []string{"op", "outcome"}, nil)
	descLatency    = prometheus.NewDesc("dkv_request_duration_seconds", "Public API request latency.", []string{"op"}, nil)
	descTerm       = prometheus.NewDesc("dkv_raft_term", "Current Raft term of a local replica.", []string{"shard"}, nil)
	descLeader     = prometheus.NewDesc("dkv_raft_leader", "1 if the local replica leads its group.", []string{"shard"}, nil)
	descCommit     = prometheus.NewDesc("dkv_raft_commit_index", "Commit index of a local replica.", []string{"shard"}, nil)
	descApplied    = prometheus.NewDesc("dkv_raft_applied_index", "Applied index of a local replica.", []string{"shard"}, nil)
	descLag        = prometheus.NewDesc("dkv_raft_follower_lag_entries", "On a leader: entries a member's log is behind the leader's.", []string{"shard", "member"}, nil)
	descSnapIn     = prometheus.NewDesc("dkv_raft_snapshots_installed_total", "Snapshots a local replica installed.", []string{"shard"}, nil)
	descSnapOut    = prometheus.NewDesc("dkv_raft_snapshots_sent_total", "Snapshots a local replica sent.", []string{"shard"}, nil)
	descMemBytes   = prometheus.NewDesc("dkv_engine_memtable_bytes", "Bytes in a replica's memtables.", []string{"shard"}, nil)
	descTables     = prometheus.NewDesc("dkv_engine_tables", "SSTables of a replica's engine.", []string{"shard"}, nil)
	descTableBytes = prometheus.NewDesc("dkv_engine_table_bytes", "Bytes in a replica's SSTables.", []string{"shard"}, nil)
	descFlushes    = prometheus.NewDesc("dkv_engine_flushes_total", "Memtable flushes of a replica's engine.", []string{"shard"}, nil)
	descCompact    = prometheus.NewDesc("dkv_engine_compactions_total", "Compactions of a replica's engine.", []string{"shard"}, nil)
	descCompRead   = prometheus.NewDesc("dkv_engine_compaction_read_bytes_total", "Bytes read by compactions.", []string{"shard"}, nil)
	descCompWrite  = prometheus.NewDesc("dkv_engine_compaction_written_bytes_total", "Bytes written by compactions.", []string{"shard"}, nil)
	descStalls     = prometheus.NewDesc("dkv_engine_write_stalls_total", "Writes that waited for a flush or compaction.", []string{"shard"}, nil)
	descCacheUsed  = prometheus.NewDesc("dkv_block_cache_used_bytes", "Bytes in the node's block cache.", nil, nil)
	descCacheHits  = prometheus.NewDesc("dkv_block_cache_hits_total", "Block cache hits.", nil, nil)
	descCacheMiss  = prometheus.NewDesc("dkv_block_cache_misses_total", "Block cache misses.", nil, nil)
)

func (c statsCollector) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(c, ch) }

func (c statsCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.stats()
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	counter := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
	}
	for _, o := range st.GetOps() {
		counter(descRequests, float64(o.GetOk()), o.GetOp(), "ok")
		counter(descRequests, float64(o.GetErrors()), o.GetOp(), "error")
		buckets := map[float64]uint64{}
		for i, b := range st.GetLatencyBoundsSeconds() {
			buckets[b] = o.GetLatencyCumulative()[i]
		}
		ch <- prometheus.MustNewConstHistogram(descLatency, o.GetOk()+o.GetErrors(), o.GetLatencySumSeconds(), buckets, o.GetOp())
	}
	for _, r := range st.GetReplicas() {
		rs, e := r.GetRaft(), r.GetEngine()
		shard := strconv.Itoa(int(rs.GetShard()))
		gauge(descTerm, float64(rs.GetTerm()), shard)
		leader := 0.0
		if rs.GetRole() == "leader" {
			leader = 1
			for member, match := range rs.GetProgress() {
				gauge(descLag, float64(rs.GetLastIndex()-min(match, rs.GetLastIndex())), shard, member)
			}
		}
		gauge(descLeader, leader, shard)
		gauge(descCommit, float64(rs.GetCommit()), shard)
		gauge(descApplied, float64(rs.GetApplied()), shard)
		counter(descSnapIn, float64(rs.GetSnapshotsInstalled()), shard)
		counter(descSnapOut, float64(rs.GetSnapshotsSent()), shard)
		gauge(descMemBytes, float64(e.GetMemtableBytes()), shard)
		gauge(descTables, float64(e.GetTables()), shard)
		gauge(descTableBytes, float64(e.GetTableBytes()), shard)
		counter(descFlushes, float64(e.GetFlushes()), shard)
		counter(descCompact, float64(e.GetCompactions()), shard)
		counter(descCompRead, float64(e.GetCompactionBytesRead()), shard)
		counter(descCompWrite, float64(e.GetCompactionBytesWritten()), shard)
		counter(descStalls, float64(e.GetWriteStalls()), shard)
	}
	if c := st.GetBlockCache(); c != nil {
		gauge(descCacheUsed, float64(c.GetUsedBytes()))
		counter(descCacheHits, float64(c.GetHits()))
		counter(descCacheMiss, float64(c.GetMisses()))
	}
}
