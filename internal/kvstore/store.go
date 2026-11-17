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

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
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

// Put stores value under key. Client session fields are ignored: a
// standalone node has no replicated session table.
func (s *Store) Put(ctx context.Context, req *dkvv1.PutRequest) (*dkvv1.PutResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var b lsm.Batch
	b.Set(req.GetKey(), req.GetValue())
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	seq, err := s.db.Apply(&b, 0)
	if err != nil {
		return nil, mapErr(err)
	}
	return &dkvv1.PutResponse{Version: seq}, nil
}

// Get returns the value of key. A single engine is always linearizable;
// applied_index is the engine's last sequence number.
func (s *Store) Get(ctx context.Context, req *dkvv1.GetRequest) (*dkvv1.GetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	applied := s.db.LastSeq()
	v, ver, err := s.db.Get(req.GetKey(), nil)
	if errors.Is(err, lsm.ErrNotFound) {
		return &dkvv1.GetResponse{AppliedIndex: applied}, nil
	}
	if err != nil {
		return nil, mapErr(err)
	}
	return &dkvv1.GetResponse{Found: true, Value: v, Version: ver, AppliedIndex: applied}, nil
}

// Delete removes key, reporting whether it had a value, and returns the
// version of the delete. Deleting a missing key still writes a tombstone
// and gets a version, as every delete does in a cluster.
func (s *Store) Delete(ctx context.Context, req *dkvv1.DeleteRequest) (*dkvv1.DeleteResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var b lsm.Batch
	b.Delete(req.GetKey())
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, _, err := s.db.Get(req.GetKey(), nil)
	found := err == nil
	if err != nil && !errors.Is(err, lsm.ErrNotFound) {
		return nil, mapErr(err)
	}
	seq, err := s.db.Apply(&b, 0)
	if err != nil {
		return nil, mapErr(err)
	}
	return &dkvv1.DeleteResponse{Found: found, Version: seq}, nil
}

// CompareAndSet writes value if the key's current version is the expected
// one (0: the key has no value); the check and the write are atomic.
func (s *Store) CompareAndSet(ctx context.Context, req *dkvv1.CompareAndSetRequest) (*dkvv1.CompareAndSetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	cur := uint64(0)
	_, ver, err := s.db.Get(req.GetKey(), nil)
	switch {
	case err == nil:
		cur = ver
	case !errors.Is(err, lsm.ErrNotFound):
		return nil, mapErr(err)
	}
	if cur != req.GetExpectedVersion() {
		return &dkvv1.CompareAndSetResponse{Version: cur}, nil
	}
	var b lsm.Batch
	b.Set(req.GetKey(), req.GetValue())
	seq, err := s.db.Apply(&b, 0)
	if err != nil {
		return nil, mapErr(err)
	}
	return &dkvv1.CompareAndSetResponse{Succeeded: true, Version: seq}, nil
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
