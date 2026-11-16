package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
)

// Backend executes key-value operations for the API server. In Phase 1 it is
// a single local engine (StandaloneBackend); from Phase 5 the router
// implements it by forwarding to the shard that owns the key.
type Backend interface {
	// Put stores value under key and returns the new version.
	Put(ctx context.Context, key, value []byte) (version uint64, err error)
	// Get returns the value of key and its version, or ErrNotFound.
	Get(ctx context.Context, key []byte) (value []byte, version uint64, err error)
	// Delete removes key, reporting whether it had a value, and returns the
	// version of the delete.
	Delete(ctx context.Context, key []byte) (found bool, version uint64, err error)
}

// Errors a Backend returns. The server maps them to gRPC status codes.
var (
	ErrNotFound    = errors.New("key not found")
	ErrUnavailable = errors.New("backend unavailable")
)

// StandaloneBackend serves every key from one local lsm.DB.
type StandaloneBackend struct {
	// writeMu makes Delete's read-then-write atomic with respect to other
	// writes, so the found flag is exact. The engine already serializes
	// writes, so this costs no extra concurrency.
	writeMu sync.Mutex
	db      *lsm.DB
}

// NewStandaloneBackend returns a Backend over db. The caller keeps
// ownership of db and closes it after the server has stopped.
func NewStandaloneBackend(db *lsm.DB) *StandaloneBackend {
	return &StandaloneBackend{db: db}
}

func (b *StandaloneBackend) Put(ctx context.Context, key, value []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var batch lsm.Batch
	batch.Set(key, value)
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	seq, err := b.db.Apply(&batch, 0)
	if err != nil {
		return 0, mapEngineErr(err)
	}
	return seq, nil
}

func (b *StandaloneBackend) Get(ctx context.Context, key []byte) ([]byte, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	v, ver, err := b.db.Get(key, nil)
	if err != nil {
		return nil, 0, mapEngineErr(err)
	}
	return v, ver, nil
}

func (b *StandaloneBackend) Delete(ctx context.Context, key []byte) (bool, uint64, error) {
	if err := ctx.Err(); err != nil {
		return false, 0, err
	}
	var batch lsm.Batch
	batch.Delete(key)
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	_, _, err := b.db.Get(key, nil)
	found := err == nil
	if err != nil && !errors.Is(err, lsm.ErrNotFound) {
		return false, 0, mapEngineErr(err)
	}
	// Deleting a missing key still writes a tombstone and gets a version,
	// matching the clustered path where every delete goes through the log.
	seq, err := b.db.Apply(&batch, 0)
	if err != nil {
		return false, 0, mapEngineErr(err)
	}
	return found, seq, nil
}

func mapEngineErr(err error) error {
	switch {
	case errors.Is(err, lsm.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, lsm.ErrClosed):
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	default:
		return err
	}
}
