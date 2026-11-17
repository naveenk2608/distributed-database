package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/goleak"

	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/console"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// testDSN is a PostgreSQL database the test may fill and empty: CI's
// console-postgres job sets DKV_TEST_POSTGRES_DSN.
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DKV_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DKV_TEST_POSTGRES_DSN to a scratch PostgreSQL database to run this test (CI does)")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(context.Background(), `DROP TABLE IF EXISTS dkv_console_samples, dkv_console_events`); err != nil {
		t.Fatal(err)
	}
	return dsn
}

// TestPostgresStore: samples and events round-trip, an event reported
// again (by another member, or saved twice) is stored once, Recent filters
// by time and orders oldest first, and Prune deletes what is older.
func TestPostgresStore(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	s, err := openPG(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	t0 := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	p50 := 1.5
	samples := []console.Sample{
		{Time: t0, Addr: "n1:7200", NodeID: "a", Up: true, Rates: console.Rates{WritesPerSec: 10, P50Ms: &p50}, TableBytes: 100},
		{Time: t0.Add(time.Second), Addr: "n1:7200", NodeID: "a", Up: false},
	}
	shard := 3
	added := console.EventView{Time: t0, Kind: "replica_added", Reporter: "a", Shard: &shard, Node: "d", Index: 9, Seq: 4}
	again := added
	again.Reporter, again.Seq = "b", 7 // the same change, as another member reports it
	dead := console.EventView{Time: t0.Add(2 * time.Second), Kind: "node_dead", Reporter: "a", Node: "c", Seq: 5}
	if err := s.Save(ctx, samples, []console.EventView{added, dead}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, nil, []console.EventView{again, dead}); err != nil {
		t.Fatal(err)
	}
	gotS, gotE, err := s.Recent(ctx, t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotS) != 2 || !gotS[0].Time.Equal(t0) || gotS[0].Rates.WritesPerSec != 10 || *gotS[0].Rates.P50Ms != 1.5 || gotS[1].Up {
		t.Fatalf("samples %+v", gotS)
	}
	if len(gotE) != 2 || gotE[0].Kind != "replica_added" || gotE[1].Kind != "node_dead" || *gotE[0].Shard != 3 {
		t.Fatalf("events %+v; want the membership change once, then node_dead", gotE)
	}
	if gotS, gotE, _ = s.Recent(ctx, t0.Add(time.Second)); len(gotS) != 1 || len(gotE) != 1 {
		t.Fatalf("since t0+1s: %d samples, %d events", len(gotS), len(gotE))
	}
	if err := s.Prune(ctx, t0.Add(1500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if gotS, gotE, _ = s.Recent(ctx, time.Time{}); len(gotS) != 0 || len(gotE) != 1 {
		t.Fatalf("after pruning: %d samples, %d events", len(gotS), len(gotE))
	}
}

// TestConsoleReloadsHistory: a console that restarts on the same database
// starts with the history the previous one saved.
func TestConsoleReloadsHistory(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	s, err := openPG(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	addr := "127.0.0.1:1" // nothing answers: the node shows as down
	cfg := config.DefaultConsole().Console
	cfg.Nodes, cfg.PollInterval, cfg.PollTimeout = []string{addr}, 50*time.Millisecond, 40*time.Millisecond
	if err := s.Save(ctx, []console.Sample{{Time: time.Now().Add(-time.Second), Addr: addr, NodeID: "earlier", Up: true}}, nil); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	c := console.New(cfg, s, slog.New(slog.DiscardHandler))
	done := make(chan error, 1)
	go func() { done <- c.Run(cctx, lis) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		h := historyOf(t, "http://"+lis.Addr().String(), addr)
		if len(h) >= 2 && h[0].NodeID == "earlier" && !h[len(h)-1].Up {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("history %+v; want the saved sample first, then the console's own", h)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func historyOf(t *testing.T, base, addr string) []console.Sample {
	t.Helper()
	resp, err := http.Get(base + "/api/history?addr=" + addr) //nolint:noctx // a test against a local server
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var h []console.Sample
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		t.Fatal(err)
	}
	return h
}
