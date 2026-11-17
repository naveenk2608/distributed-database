package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/naveenk2608/distributed-database/internal/console"
)

// schema creates the console's tables. Samples and events are kept as
// JSON documents with the columns queries filter on; nothing else reads
// them (docs/adr/0014).
const schema = `
CREATE TABLE IF NOT EXISTS dkv_console_samples (
	time    timestamptz NOT NULL,
	addr    text        NOT NULL,
	node_id text        NOT NULL,
	data    jsonb       NOT NULL
);
CREATE INDEX IF NOT EXISTS dkv_console_samples_time ON dkv_console_samples (time);
CREATE TABLE IF NOT EXISTS dkv_console_events (
	key  text        PRIMARY KEY,
	time timestamptz NOT NULL,
	kind text        NOT NULL,
	data jsonb       NOT NULL
);
CREATE INDEX IF NOT EXISTS dkv_console_events_time ON dkv_console_events (time);
`

// pgStore keeps the console's history in PostgreSQL. It creates its
// tables on first use, so a database that is down when the console starts
// is used once it comes up; until then every call fails and the console
// keeps its history in memory.
type pgStore struct {
	pool *pgxpool.Pool

	mu       sync.Mutex
	migrated bool
}

func openPG(ctx context.Context, dsn string) (*pgStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse console.postgres_dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	return &pgStore{pool: pool}, nil
}

func (s *pgStore) Close() { s.pool.Close() }

func (s *pgStore) migrate(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.migrated {
		return nil
	}
	if _, err := s.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("postgres: create tables: %w", err)
	}
	s.migrated = true
	return nil
}

// Save records samples and events in one transaction; an event already
// stored is skipped.
func (s *pgStore) Save(ctx context.Context, samples []console.Sample, events []console.EventView) error {
	if err := s.migrate(ctx); err != nil {
		return err
	}
	b := &pgx.Batch{}
	for _, smp := range samples {
		data, err := json.Marshal(smp)
		if err != nil {
			return err
		}
		b.Queue(`INSERT INTO dkv_console_samples (time, addr, node_id, data) VALUES ($1, $2, $3, $4)`, smp.Time, smp.Addr, smp.NodeID, data)
	}
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		b.Queue(`INSERT INTO dkv_console_events (key, time, kind, data) VALUES ($1, $2, $3, $4) ON CONFLICT (key) DO NOTHING`,
			e.Key(), e.Time, e.Kind, data)
	}
	if b.Len() == 0 {
		return nil
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return tx.SendBatch(ctx, b).Close()
	})
}

// Recent returns the samples and events since t, oldest first.
func (s *pgStore) Recent(ctx context.Context, since time.Time) ([]console.Sample, []console.EventView, error) {
	if err := s.migrate(ctx); err != nil {
		return nil, nil, err
	}
	samples, err := query[console.Sample](ctx, s.pool, `SELECT data FROM dkv_console_samples WHERE time >= $1 ORDER BY time`, since)
	if err != nil {
		return nil, nil, err
	}
	events, err := query[console.EventView](ctx, s.pool, `SELECT data FROM dkv_console_events WHERE time >= $1 ORDER BY time`, since)
	if err != nil {
		return nil, nil, err
	}
	return samples, events, nil
}

func query[T any](ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) ([]T, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var v T
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf("postgres: a stored row does not decode: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// Prune deletes samples and events older than t.
func (s *pgStore) Prune(ctx context.Context, before time.Time) error {
	if err := s.migrate(ctx); err != nil {
		return err
	}
	_, err1 := s.pool.Exec(ctx, `DELETE FROM dkv_console_samples WHERE time < $1`, before)
	_, err2 := s.pool.Exec(ctx, `DELETE FROM dkv_console_events WHERE time < $1`, before)
	return errors.Join(err1, err2)
}
