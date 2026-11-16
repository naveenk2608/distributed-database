// Package kvstore runs key-value operations against one storage engine.
// The standalone server uses one Store; the router uses one per shard it
// serves.
package kvstore

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
)

// Store executes Put, Get and Delete on one engine. Errors are gRPC status
// errors where a client can act on them: a closed engine is Unavailable,
// detected corruption is DataLoss.
type Store struct {
	// writeMu makes Delete's read-then-write atomic with respect to other
	// writes, so its found flag is exact. The engine serializes writes
	// anyway, so this costs no concurrency.
	writeMu sync.Mutex
	db      *lsm.DB
}

// New returns a Store over db. The caller keeps ownership of db.
func New(db *lsm.DB) *Store { return &Store{db: db} }

// DB returns the engine.
func (s *Store) DB() *lsm.DB { return s.db }

// Put stores value under key and returns the new version.
func (s *Store) Put(ctx context.Context, key, value []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var b lsm.Batch
	b.Set(key, value)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	seq, err := s.db.Apply(&b, 0)
	if err != nil {
		return 0, mapErr(err)
	}
	return seq, nil
}

// Get returns the value of key and its version; found is false if the key
// has no value.
func (s *Store) Get(ctx context.Context, key []byte) ([]byte, uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, false, err
	}
	v, ver, err := s.db.Get(key, nil)
	if errors.Is(err, lsm.ErrNotFound) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, mapErr(err)
	}
	return v, ver, true, nil
}

// Delete removes key, reporting whether it had a value, and returns the
// version of the delete. Deleting a missing key still writes a tombstone
// and gets a version, as every delete goes through the log once replicated.
func (s *Store) Delete(ctx context.Context, key []byte) (bool, uint64, error) {
	if err := ctx.Err(); err != nil {
		return false, 0, err
	}
	var b lsm.Batch
	b.Delete(key)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, _, err := s.db.Get(key, nil)
	found := err == nil
	if err != nil && !errors.Is(err, lsm.ErrNotFound) {
		return false, 0, mapErr(err)
	}
	seq, err := s.db.Apply(&b, 0)
	if err != nil {
		return false, 0, mapErr(err)
	}
	return found, seq, nil
}

func mapErr(err error) error {
	switch {
	case errors.Is(err, lsm.ErrClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, lsm.ErrCorruption):
		return status.Error(codes.DataLoss, err.Error())
	default:
		return err
	}
}
