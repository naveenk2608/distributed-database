package console

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
)

// Sample is what the console keeps of one node at one poll, for the
// charts and the history store.
type Sample struct {
	Time          time.Time `json:"time"`
	Addr          string    `json:"addr"`
	NodeID        string    `json:"node_id"`
	Up            bool      `json:"up"`
	Rates         Rates     `json:"rates"`
	MemtableBytes int64     `json:"memtable_bytes"`
	TableBytes    uint64    `json:"table_bytes"`
	Leaders       int       `json:"leaders"`
}

// Store keeps the console's history beyond its own lifetime (PostgreSQL in
// dkv-console). The console works without one, keeping a bounded history
// in memory; a failing store is reported and never stops the polling.
type Store interface {
	// Save records one poll round's samples and the events first seen in it.
	Save(ctx context.Context, samples []Sample, events []EventView) error
	// Recent returns the samples and events since t, oldest first.
	Recent(ctx context.Context, since time.Time) ([]Sample, []EventView, error)
	// Prune deletes what is older than t.
	Prune(ctx context.Context, before time.Time) error
}

// maxEvents bounds the merged events kept in memory.
const maxEvents = 5000

// Console polls the nodes and serves what it learns. Create it with New
// and start it with Run.
type Console struct {
	cfg   config.Console
	log   *slog.Logger
	store Store
	http  *http.Client
	now   func() time.Time

	mu       sync.Mutex
	nodes    []*nodeState // in configuration order
	events   []EventView  // merged, oldest first
	seen     map[string]bool
	history  map[string][]Sample // by node address, oldest first
	overview Overview
	storeErr string
	subs     map[chan []byte]struct{}
}

type nodeState struct {
	addr     string
	nodeID   string
	up       bool
	err      string
	lastSeen time.Time
	status   *dkvv1.ClusterStatusResponse
	stats    *dkvv1.StatsResponse
	prev     *dkvv1.StatsResponse
	// cursor is the last event sequence number read from the node's
	// current process; started identifies that process.
	cursor  uint64
	started int64
}

// New returns a console for cfg; store may be nil (history in memory only).
func New(cfg config.Console, store Store, log *slog.Logger) *Console {
	c := &Console{cfg: cfg, log: log, store: store, now: time.Now,
		http:    &http.Client{Timeout: cfg.PollTimeout},
		seen:    map[string]bool{},
		history: map[string][]Sample{},
		subs:    map[chan []byte]struct{}{},
	}
	for _, a := range cfg.Nodes {
		c.nodes = append(c.nodes, &nodeState{addr: a})
	}
	c.overview = Overview{GeneratedAt: c.now(), PollIntervalMs: cfg.PollInterval.Milliseconds(),
		Nodes: []NodeView{}, Shards: []ShardView{}, Events: []EventView{}}
	return c
}

// Run polls the nodes every poll interval and serves the API (and the
// dashboard, if cfg.WebDir is set) on lis until ctx ends. It closes lis.
func (c *Console) Run(ctx context.Context, lis net.Listener) error {
	c.reload(ctx)
	srv := &http.Server{Handler: c.Handler(), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	c.log.Info("serving the dashboard", "addr", lis.Addr().String(), "nodes", c.cfg.Nodes, "poll_interval", c.cfg.PollInterval)

	var wg sync.WaitGroup
	pollCtx, stopPolling := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.pollLoop(pollCtx)
	}()

	var err error
	select {
	case err = <-serveErr:
		err = fmt.Errorf("console: serve: %w", err)
	case <-ctx.Done():
	}
	stopPolling()
	wg.Wait()
	c.closeSubscribers()
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if serr := srv.Shutdown(sctx); serr != nil {
		_ = srv.Close()
	}
	if err == nil {
		if serr := <-serveErr; serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			err = fmt.Errorf("console: serve: %w", serr)
		}
	}
	return err
}

// reload fills the in-memory history from the store, so a restarted
// console shows what it saw before.
func (c *Console) reload(ctx context.Context) {
	if c.store == nil {
		return
	}
	window := time.Duration(c.cfg.MemorySamples) * c.cfg.PollInterval
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	samples, events, err := c.store.Recent(rctx, c.now().Add(-window))
	if err != nil {
		c.log.Warn("history store unavailable; starting with an empty history", "error", err)
		c.storeErr = err.Error()
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range samples {
		c.history[s.Addr] = append(c.history[s.Addr], s)
	}
	c.addEventsLocked(events)
	c.log.Info("history reloaded", "samples", len(samples), "events", len(events))
}

func (c *Console) pollLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.PollInterval)
	defer t.Stop()
	lastPrune := c.now()
	for {
		c.PollOnce(ctx)
		if c.store != nil && c.now().Sub(lastPrune) > time.Hour {
			lastPrune = c.now()
			pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := c.store.Prune(pctx, c.now().Add(-c.cfg.Retention)); err != nil {
				c.log.Warn("pruning the history store", "error", err)
			}
			cancel()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// PollOnce polls every node once, concurrently, then rebuilds the
// overview, records the round and tells the subscribers.
func (c *Console) PollOnce(ctx context.Context) {
	type result struct {
		status *dkvv1.ClusterStatusResponse
		stats  *dkvv1.StatsResponse
		events *dkvv1.EventsResponse
		err    error
	}
	c.mu.Lock()
	cursors := make([]struct {
		after   uint64
		started int64
	}, len(c.nodes))
	for i, n := range c.nodes {
		cursors[i].after, cursors[i].started = n.cursor, n.started
	}
	c.mu.Unlock()
	results := make([]result, len(c.nodes))
	var wg sync.WaitGroup
	for i, n := range c.nodes {
		wg.Go(func() {
			pctx, cancel := context.WithTimeout(ctx, c.cfg.PollTimeout)
			defer cancel()
			var r result
			r.stats = &dkvv1.StatsResponse{}
			if r.err = c.get(pctx, n.addr, "/v1/stats", r.stats); r.err == nil {
				after := cursors[i].after
				if r.stats.GetStartedUnixNano() != cursors[i].started {
					after = 0 // a new process: its events start again at 1
				}
				r.status, r.events = &dkvv1.ClusterStatusResponse{}, &dkvv1.EventsResponse{}
				r.err = errors.Join(c.get(pctx, n.addr, "/v1/status", r.status),
					c.get(pctx, n.addr, fmt.Sprintf("/v1/events?after=%d&limit=1000", after), r.events))
			}
			results[i] = r
		})
	}
	wg.Wait()
	if ctx.Err() != nil {
		return // shutting down: a failed poll here says nothing about the nodes
	}

	now := c.now()
	c.mu.Lock()
	var fresh []EventView
	for i, n := range c.nodes {
		r := results[i]
		if r.err != nil {
			if n.up {
				c.log.Info("node stopped answering", "addr", n.addr, "error", r.err)
			}
			n.up, n.err = false, r.err.Error()
			continue
		}
		if !n.up {
			c.log.Info("node answering", "addr", n.addr, "node_id", r.stats.GetNodeId())
		}
		if r.stats.GetStartedUnixNano() != n.started {
			n.cursor, n.started, n.prev = 0, r.stats.GetStartedUnixNano(), nil
		} else {
			n.prev = n.stats
		}
		if o := r.events.GetOldest(); o > n.cursor+1 && n.cursor > 0 {
			c.log.Warn("missed events: the node's log moved past them", "addr", n.addr, "after", n.cursor, "oldest", o)
		}
		n.up, n.err, n.lastSeen = true, "", now
		n.nodeID, n.status, n.stats = r.stats.GetNodeId(), r.status, r.stats
		for _, e := range r.events.GetEvents() {
			n.cursor = max(n.cursor, e.GetSeq())
			fresh = append(fresh, eventView(n.nodeID, e))
		}
	}
	fresh = c.addEventsLocked(fresh)
	c.overview = c.buildLocked(now)
	samples := make([]Sample, 0, len(c.overview.Nodes))
	for _, nv := range c.overview.Nodes {
		s := Sample{Time: now, Addr: nv.Addr, NodeID: nv.NodeID, Up: nv.Up, Rates: nv.Rates,
			MemtableBytes: nv.Storage.MemtableBytes, TableBytes: nv.Storage.TableBytes, Leaders: nv.Leaders}
		samples = append(samples, s)
		h := append(c.history[nv.Addr], s)
		c.history[nv.Addr] = h[max(0, len(h)-c.cfg.MemorySamples):]
	}
	msg := c.encodeLocked()
	c.mu.Unlock()

	c.broadcast(msg)
	if c.store != nil {
		sctx, cancel := context.WithTimeout(ctx, c.cfg.PollInterval)
		err := c.store.Save(sctx, samples, fresh)
		cancel()
		c.mu.Lock()
		if err != nil {
			if c.storeErr == "" {
				c.log.Warn("history store write failed; history stays in memory", "error", err)
			}
			c.storeErr = err.Error()
		} else {
			c.storeErr = ""
		}
		c.mu.Unlock()
	}
}

// get fetches one admin API path from a node into m.
func (c *Console) get(ctx context.Context, addr, path string, m proto.Message) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", path, resp.Status)
	}
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(b, m)
}

// addEventsLocked merges events, dropping ones already known, keeps the
// newest maxEvents, and returns those that were new.
func (c *Console) addEventsLocked(events []EventView) []EventView {
	var fresh []EventView
	for _, e := range events {
		if e.Kind == "" || (e.Shard == nil && isMembership(e.Kind)) {
			continue
		}
		if k := e.Key(); !c.seen[k] {
			c.seen[k] = true
			fresh = append(fresh, e)
		}
	}
	if len(fresh) == 0 {
		return nil
	}
	c.events = append(c.events, fresh...)
	slices.SortStableFunc(c.events, func(a, b EventView) int { return a.Time.Compare(b.Time) })
	if n := len(c.events) - maxEvents; n > 0 {
		for _, e := range c.events[:n] {
			delete(c.seen, e.Key())
		}
		c.events = slices.Clone(c.events[n:])
	}
	return fresh
}

func isMembership(kind string) bool {
	return kind == "replica_added" || kind == "replica_promoted" || kind == "replica_removed"
}

// overviewEvents is how many recent events the overview carries.
const overviewEvents = 200

// buildLocked derives the overview from the latest poll of every node.
func (c *Console) buildLocked(now time.Time) Overview {
	ov := Overview{GeneratedAt: now, PollIntervalMs: c.cfg.PollInterval.Milliseconds(),
		Nodes: []NodeView{}, Shards: []ShardView{}, Events: []EventView{}}
	stats := map[string]*dkvv1.StatsResponse{}
	records := map[string]*dkvv1.MemberStatus{}
	for _, n := range c.nodes {
		if !n.up {
			continue
		}
		for _, m := range n.status.GetMembers() {
			id := m.GetNode().GetNodeId()
			// Prefer a view that knows the node's record.
			if id != "" && (records[id] == nil || records[id].GetNodeState() == dkvv1.NodeState_NODE_STATE_UNSPECIFIED) {
				records[id] = m
			}
		}
	}
	var total interval
	for _, n := range c.nodes {
		nv := NodeView{Addr: n.addr, NodeID: n.nodeID, Up: n.up, Error: n.err, LastSeen: n.lastSeen, State: "unknown"}
		if rec := records[n.nodeID]; rec != nil {
			nv.State, nv.Drained, nv.Suspect = stateName(rec.GetNodeState()), rec.GetDrained(), rec.GetSuspect()
		}
		if n.up {
			stats[n.nodeID] = n.stats
			nv.StartedAt = time.Unix(0, n.stats.GetStartedUnixNano())
			if iv, ok := delta(n.prev, n.stats); ok {
				nv.Rates = iv.rates
				total = total.add(iv)
			}
			nv.Storage, nv.Replicas, nv.Leaders = storageAndReplicas(n.stats)
		}
		if nv.Replicas == nil {
			nv.Replicas = []Replica{}
		}
		ov.Nodes = append(ov.Nodes, nv)
	}
	ov.Totals = total.rates
	ov.Totals.P50Ms, ov.Totals.P99Ms = total.percentiles()
	if s := shardViews(stats); s != nil {
		ov.Shards = s
	}
	ov.Events = slices.Clone(c.events[max(0, len(c.events)-overviewEvents):])
	return ov
}
