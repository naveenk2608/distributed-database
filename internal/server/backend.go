package server

import (
	"context"

	"github.com/naveenk2608/distributed-database/internal/kvstore"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
)

// Backend executes key-value operations for the API server: one local
// engine in standalone mode (kvstore.Store), the router in a cluster.
// Errors that are gRPC status errors reach the client unchanged; context
// errors become Canceled or DeadlineExceeded; anything else is Internal.
type Backend interface {
	// Put stores value under key and returns the new version.
	Put(ctx context.Context, key, value []byte) (version uint64, err error)
	// Get returns the value of key and its version; found is false if the
	// key has no value.
	Get(ctx context.Context, key []byte) (value []byte, version uint64, found bool, err error)
	// Delete removes key, reporting whether it had a value, and returns the
	// version of the delete.
	Delete(ctx context.Context, key []byte) (found bool, version uint64, err error)
}

// NewStandaloneBackend returns a Backend serving every key from db. The
// caller keeps ownership of db and closes it after the server has stopped.
func NewStandaloneBackend(db *lsm.DB) Backend { return kvstore.New(db) }
