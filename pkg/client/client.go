// Package client is the Go client for dkv's public KV API.
//
// A client talks to any node of a cluster; the node routes each request to
// the leader of the key's shard. The client retries requests that fail
// with codes.Unavailable (the node, or the shard's leader, could not be
// reached or changed) with jittered exponential backoff, moving to the
// next configured node each time, until the caller's context ends or the
// attempt limit is reached.
//
// Retried writes are applied exactly once in a cluster. Each write carries
// a session ID and a request ID; the shard's replicas remember the latest
// request of every session and answer a repeat with the stored result
// instead of applying it again. A session has at most one write in flight,
// so a client holds a small pool of sessions and gives each concurrent
// write its own. See docs/adr/0008-idempotency.md. A standalone node does
// not deduplicate.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
)

// ErrNotFound is returned by Get when the key has no value.
var ErrNotFound = errors.New("dkv: key not found")

// maxResponseBytes bounds the size of a response the client accepts. gRPC's
// 4 MiB default would make values the server legitimately accepts (its
// limits.max_value_bytes may be up to 1 GiB) impossible to read back, so the
// client accepts anything up to that ceiling plus message overhead.
const maxResponseBytes = 1<<30 + 1<<20

// Options configures a Client.
type Options struct {
	// Addrs lists nodes (host:port of their public API). Requests go to
	// one of them and move on to the next when it is unreachable.
	Addrs []string
	// MaxAttempts bounds the tries per request; 0 means 8.
	MaxAttempts int
	// InitialBackoff and MaxBackoff shape the wait between tries; 0 means
	// 50ms and 1s.
	InitialBackoff, MaxBackoff time.Duration
	// DialOptions are added to the gRPC dial options (for example
	// interceptors).
	DialOptions []grpc.DialOption
}

// Client is a connection to a dkv cluster. It is safe for concurrent use.
type Client struct {
	opts  Options
	conns []*grpc.ClientConn
	kvs   []dkvv1.KVServiceClient
	next  atomic.Uint32 // index of the node to try first

	mu       sync.Mutex
	sessions []*session // idle sessions
}

// session is one deduplication session: an ID and the next request ID.
// Only one write uses a session at a time.
type session struct {
	id   string
	next uint64
}

// New returns a client for the nodes at addrs (host:port). It connects
// lazily, on the first request. Connections are unencrypted and
// unauthenticated: authentication is out of scope for v1.
func New(addrs ...string) (*Client, error) {
	return NewWithOptions(Options{Addrs: addrs})
}

// NewWithOptions returns a client configured by opts.
func NewWithOptions(opts Options) (*Client, error) {
	if len(opts.Addrs) == 0 {
		return nil, errors.New("dkv: no node address")
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 8
	}
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = 50 * time.Millisecond
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = time.Second
	}
	c := &Client{opts: opts}
	dial := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxResponseBytes)),
	}, opts.DialOptions...)
	for _, addr := range opts.Addrs {
		conn, err := grpc.NewClient(addr, dial...)
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("dkv: connect to %s: %w", addr, err)
		}
		c.conns = append(c.conns, conn)
		c.kvs = append(c.kvs, dkvv1.NewKVServiceClient(conn))
	}
	return c, nil
}

// Close closes the connections.
func (c *Client) Close() error {
	var errs []error
	for _, conn := range c.conns {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}

func (c *Client) takeSession() *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := len(c.sessions); n > 0 {
		s := c.sessions[n-1]
		c.sessions = c.sessions[:n-1]
		return s
	}
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails
	return &session{id: hex.EncodeToString(b[:]), next: 1}
}

func (c *Client) returnSession(s *session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessions = append(c.sessions, s)
}

// withSession runs a write under an idle session with a fresh request ID,
// which every retry of the write reuses.
func withSession[T any](c *Client, f func(clientID string, requestID uint64) (T, error)) (T, error) {
	s := c.takeSession()
	defer c.returnSession(s)
	id := s.next
	s.next++
	return f(s.id, id)
}

// retryable reports whether a failed try may be repeated. Unavailable means
// the node or the shard's leader could not serve the request; a write may
// still have been applied, which the session makes harmless to repeat.
func retryable(err error) bool { return status.Code(err) == codes.Unavailable }

// call runs try against the nodes in turn until it succeeds, fails with a
// non-retryable error, the attempt limit is reached or ctx ends.
func call[T any](ctx context.Context, c *Client, op string, try func(kv dkvv1.KVServiceClient) (T, error)) (T, error) {
	var zero T
	backoff := c.opts.InitialBackoff
	node := int(c.next.Load()) % len(c.kvs)
	for attempt := 1; ; attempt++ {
		resp, err := try(c.kvs[node])
		if err == nil {
			return resp, nil
		}
		if !retryable(err) || attempt >= c.opts.MaxAttempts {
			return zero, fmt.Errorf("dkv: %s: %w", op, err)
		}
		node = (node + 1) % len(c.kvs)
		c.next.Store(uint32(node))
		wait := backoff/2 + time.Duration(mrand.Int64N(int64(backoff)))
		backoff = min(2*backoff, c.opts.MaxBackoff)
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return zero, fmt.Errorf("dkv: %s: %w (last error: %w)", op, status.FromContextError(ctx.Err()).Err(), err)
		case <-t.C:
		}
	}
}

// Put stores value under key and returns the new version.
func (c *Client) Put(ctx context.Context, key, value []byte) (version uint64, err error) {
	return withSession(c, func(clientID string, requestID uint64) (uint64, error) {
		req := &dkvv1.PutRequest{Key: key, Value: value, ClientId: clientID, RequestId: requestID}
		resp, err := call(ctx, c, "put", func(kv dkvv1.KVServiceClient) (*dkvv1.PutResponse, error) { return kv.Put(ctx, req) })
		return resp.GetVersion(), err
	})
}

// Get returns the value of key and its version, or ErrNotFound. The read
// is linearizable: it reflects every write acknowledged before it began.
func (c *Client) Get(ctx context.Context, key []byte) (value []byte, version uint64, err error) {
	return c.get(ctx, &dkvv1.GetRequest{Key: key})
}

// GetStale reads key from whichever replica the node reaches first,
// without confirming leadership, once that replica has applied at least
// minVersion (pass a version this client wrote to read its own writes).
// It may miss recent writes of other clients.
func (c *Client) GetStale(ctx context.Context, key []byte, minVersion uint64) (value []byte, version uint64, err error) {
	return c.get(ctx, &dkvv1.GetRequest{Key: key, Consistency: dkvv1.Consistency_CONSISTENCY_STALE, MinVersion: minVersion})
}

func (c *Client) get(ctx context.Context, req *dkvv1.GetRequest) ([]byte, uint64, error) {
	resp, err := call(ctx, c, "get", func(kv dkvv1.KVServiceClient) (*dkvv1.GetResponse, error) { return kv.Get(ctx, req) })
	if err != nil {
		return nil, 0, err
	}
	if !resp.GetFound() {
		return nil, 0, ErrNotFound
	}
	return resp.GetValue(), resp.GetVersion(), nil
}

// Delete removes key. found reports whether it had a value; version is the
// version of the delete.
func (c *Client) Delete(ctx context.Context, key []byte) (found bool, version uint64, err error) {
	type res struct {
		found   bool
		version uint64
	}
	r, err := withSession(c, func(clientID string, requestID uint64) (res, error) {
		req := &dkvv1.DeleteRequest{Key: key, ClientId: clientID, RequestId: requestID}
		resp, err := call(ctx, c, "delete", func(kv dkvv1.KVServiceClient) (*dkvv1.DeleteResponse, error) { return kv.Delete(ctx, req) })
		return res{resp.GetFound(), resp.GetVersion()}, err
	})
	return r.found, r.version, err
}

// CompareAndSet stores value under key only if the key's current version
// is expected (0: the key must have no value). It reports whether it did,
// and the key's version afterwards: the new version on success, the
// current one otherwise.
func (c *Client) CompareAndSet(ctx context.Context, key, value []byte, expected uint64) (succeeded bool, version uint64, err error) {
	type res struct {
		ok      bool
		version uint64
	}
	r, err := withSession(c, func(clientID string, requestID uint64) (res, error) {
		req := &dkvv1.CompareAndSetRequest{Key: key, Value: value, ExpectedVersion: expected, ClientId: clientID, RequestId: requestID}
		resp, err := call(ctx, c, "compare-and-set", func(kv dkvv1.KVServiceClient) (*dkvv1.CompareAndSetResponse, error) {
			return kv.CompareAndSet(ctx, req)
		})
		return res{resp.GetSucceeded(), resp.GetVersion()}, err
	})
	return r.ok, r.version, err
}

// ClusterStatus returns the node's view of the cluster: every member in its
// bootstrap list and whether it is reachable.
func (c *Client) ClusterStatus(ctx context.Context) (*dkvv1.ClusterStatusResponse, error) {
	resp, err := dkvv1.NewAdminServiceClient(c.conns[0]).ClusterStatus(ctx, &dkvv1.ClusterStatusRequest{})
	if err != nil {
		return nil, fmt.Errorf("dkv: cluster status: %w", err)
	}
	return resp, nil
}

// ShardMap returns where every shard is placed and, for the shards the node
// holds, the state of its local replica.
func (c *Client) ShardMap(ctx context.Context) (*dkvv1.ShardMapResponse, error) {
	resp, err := dkvv1.NewAdminServiceClient(c.conns[0]).ShardMap(ctx, &dkvv1.ShardMapRequest{})
	if err != nil {
		return nil, fmt.Errorf("dkv: shard map: %w", err)
	}
	return resp, nil
}

// ShardMaps asks every node the client was given for its shard map, all at
// once. A node reports leaders only for the shards it holds, so merging the
// maps of all nodes gives every shard's leader. Each node has perNode to
// answer (when perNode > 0) within ctx, so a node that never answers delays
// the call by at most perNode and takes no time from the others. The slices
// are in address order; errs[i] is set where maps[i] is nil.
func (c *Client) ShardMaps(ctx context.Context, perNode time.Duration) (maps []*dkvv1.ShardMapResponse, errs []error) {
	maps = make([]*dkvv1.ShardMapResponse, len(c.conns))
	errs = make([]error, len(c.conns))
	// One goroutine per node, owned by this call: each ends when its
	// request does, which perNode or ctx bounds, and the call waits for all.
	var wg sync.WaitGroup
	for i, conn := range c.conns {
		wg.Go(func() {
			nodeCtx := ctx
			if perNode > 0 {
				var cancel context.CancelFunc
				nodeCtx, cancel = context.WithTimeout(ctx, perNode)
				defer cancel()
			}
			resp, err := dkvv1.NewAdminServiceClient(conn).ShardMap(nodeCtx, &dkvv1.ShardMapRequest{})
			if err != nil {
				errs[i] = fmt.Errorf("dkv: shard map from %s: %w", conn.Target(), err)
				return
			}
			maps[i] = resp
		})
	}
	wg.Wait()
	return maps, errs
}

// Drain asks the cluster to move every leadership off the node with the
// given ID and give it none (drain), or to let it lead again (!drain).
func (c *Client) Drain(ctx context.Context, nodeID string, drain bool) error {
	if _, err := dkvv1.NewAdminServiceClient(c.conns[0]).Drain(ctx, &dkvv1.DrainRequest{NodeId: nodeID, Drain: drain}); err != nil {
		return fmt.Errorf("dkv: drain: %w", err)
	}
	return nil
}

// TransferLeader asks a shard's leader to hand leadership to the voter
// with the given node ID, and returns once it has.
func (c *Client) TransferLeader(ctx context.Context, shard int32, nodeID string) error {
	req := &dkvv1.AdminServiceTransferLeaderRequest{Shard: shard, NodeId: nodeID}
	if _, err := dkvv1.NewAdminServiceClient(c.conns[0]).TransferLeader(ctx, req); err != nil {
		return fmt.Errorf("dkv: transfer leader: %w", err)
	}
	return nil
}

// Decommission asks the cluster to move every replica off the node with
// the given ID; the node is then marked decommissioned.
func (c *Client) Decommission(ctx context.Context, nodeID string) error {
	if _, err := dkvv1.NewAdminServiceClient(c.conns[0]).Decommission(ctx, &dkvv1.DecommissionRequest{NodeId: nodeID}); err != nil {
		return fmt.Errorf("dkv: decommission: %w", err)
	}
	return nil
}
