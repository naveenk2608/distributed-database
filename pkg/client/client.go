// Package client is the Go client for dkv's public KV API.
//
// Phase 1 talks to one node. Client sessions with request IDs, retries and
// leader-aware routing arrive with replication (see docs/PLAN.md).
package client

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
)

// ErrNotFound is returned by Get when the key has no value.
var ErrNotFound = errors.New("dkv: key not found")

// maxResponseBytes bounds the size of a response the client accepts. gRPC's
// 4 MiB default would make values the server legitimately accepts (its
// limits.max_value_bytes may be up to 1 GiB) impossible to read back, so the
// client accepts anything up to that ceiling plus message overhead.
const maxResponseBytes = 1<<30 + 1<<20

// Client is a connection to a dkv node. It is safe for concurrent use.
type Client struct {
	conn *grpc.ClientConn
	kv   dkvv1.KVServiceClient
}

// New returns a client for the node at addr (host:port). It connects
// lazily, on the first request. Connections are unencrypted and
// unauthenticated: authentication is out of scope for v1.
func New(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxResponseBytes)))
	if err != nil {
		return nil, fmt.Errorf("dkv: connect to %s: %w", addr, err)
	}
	return &Client{conn: conn, kv: dkvv1.NewKVServiceClient(conn)}, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }

// Put stores value under key and returns the new version.
func (c *Client) Put(ctx context.Context, key, value []byte) (version uint64, err error) {
	resp, err := c.kv.Put(ctx, &dkvv1.PutRequest{Key: key, Value: value})
	if err != nil {
		return 0, fmt.Errorf("dkv: put: %w", err)
	}
	return resp.GetVersion(), nil
}

// Get returns the value of key and its version, or ErrNotFound.
func (c *Client) Get(ctx context.Context, key []byte) (value []byte, version uint64, err error) {
	resp, err := c.kv.Get(ctx, &dkvv1.GetRequest{Key: key})
	if err != nil {
		return nil, 0, fmt.Errorf("dkv: get: %w", err)
	}
	if !resp.GetFound() {
		return nil, 0, ErrNotFound
	}
	return resp.GetValue(), resp.GetVersion(), nil
}

// Delete removes key. found reports whether it had a value; version is the
// version of the delete.
func (c *Client) Delete(ctx context.Context, key []byte) (found bool, version uint64, err error) {
	resp, err := c.kv.Delete(ctx, &dkvv1.DeleteRequest{Key: key})
	if err != nil {
		return false, 0, fmt.Errorf("dkv: delete: %w", err)
	}
	return resp.GetFound(), resp.GetVersion(), nil
}

// ClusterStatus returns the node's view of the cluster: every member in its
// bootstrap list and whether it is reachable.
func (c *Client) ClusterStatus(ctx context.Context) (*dkvv1.ClusterStatusResponse, error) {
	resp, err := dkvv1.NewAdminServiceClient(c.conn).ClusterStatus(ctx, &dkvv1.ClusterStatusRequest{})
	if err != nil {
		return nil, fmt.Errorf("dkv: cluster status: %w", err)
	}
	return resp, nil
}
