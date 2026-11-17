package server

import (
	"context"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/kvstore"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
)

// Backend executes key-value operations for the API server: one local
// engine in standalone mode (kvstore.Store), the router in a cluster. The
// server has already validated key and value sizes. Errors that are gRPC
// status errors reach the client unchanged; context errors become Canceled
// or DeadlineExceeded; anything else is Internal.
type Backend interface {
	Put(ctx context.Context, req *dkvv1.PutRequest) (*dkvv1.PutResponse, error)
	Get(ctx context.Context, req *dkvv1.GetRequest) (*dkvv1.GetResponse, error)
	Delete(ctx context.Context, req *dkvv1.DeleteRequest) (*dkvv1.DeleteResponse, error)
	CompareAndSet(ctx context.Context, req *dkvv1.CompareAndSetRequest) (*dkvv1.CompareAndSetResponse, error)
}

// NewStandaloneBackend returns a Backend serving every key from db. The
// caller keeps ownership of db and closes it after the server has stopped.
func NewStandaloneBackend(db *lsm.DB) Backend { return kvstore.New(db) }
