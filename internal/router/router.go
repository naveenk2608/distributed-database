// Package router sends each key-value request to the node that owns the
// key's shard.
//
// Any node accepts any request. The router computes the key's shard, serves
// it from the local engine if this node is the shard's primary, and
// otherwise forwards it over the internal transport. A node that receives a
// forwarded request for a shard it does not own answers NotOwner with the
// owner it knows of instead of forwarding again, so every attempt is at most
// one hop. Unreachable owners and NotOwner answers are retried with jittered
// exponential backoff until the caller's deadline. In Phase 5 only a
// shard's primary serves it; Phase 6 routes to Raft leaders instead. See
// docs/adr/0024-routing.md.
package router

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/kvstore"
	"github.com/naveenk2608/distributed-database/internal/placement"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

// Retry policy. Attempts are bounded as well as the backoff, so a client
// without a deadline still gets an answer.
const (
	maxAttempts    = 10
	initialBackoff = 20 * time.Millisecond
	maxBackoff     = 500 * time.Millisecond
)

// Config configures a Router.
type Config struct {
	// Self is this node's internal (advertise) address.
	Self string
	// Shards is the cluster's shard count.
	Shards int
	// Primary returns the address of the node that serves shard.
	Primary func(shard int) string
	// Local holds the stores of the shards this node serves.
	Local map[int]*kvstore.Store
	Pool  *transport.Pool
	Log   *slog.Logger
}

// Router implements the API server's backend for a cluster node.
type Router struct {
	cfg Config

	forwarded, notOwner, retries atomic.Int64
}

// New returns a Router.
func New(cfg Config) *Router { return &Router{cfg: cfg} }

func (r *Router) Put(ctx context.Context, key, value []byte) (uint64, error) {
	resp, err := r.route(ctx, key, &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Put{Put: &dkvv1.PutRequest{Key: key, Value: value}}})
	if err != nil {
		return 0, err
	}
	return resp.GetPut().GetVersion(), nil
}

func (r *Router) Get(ctx context.Context, key []byte) ([]byte, uint64, bool, error) {
	resp, err := r.route(ctx, key, &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Get{Get: &dkvv1.GetRequest{Key: key}}})
	if err != nil {
		return nil, 0, false, err
	}
	g := resp.GetGet()
	return g.GetValue(), g.GetVersion(), g.GetFound(), nil
}

func (r *Router) Delete(ctx context.Context, key []byte) (bool, uint64, error) {
	resp, err := r.route(ctx, key, &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Delete{Delete: &dkvv1.DeleteRequest{Key: key}}})
	if err != nil {
		return false, 0, err
	}
	return resp.GetDelete().GetFound(), resp.GetDelete().GetVersion(), nil
}

// route runs req on the owner of key's shard, retrying as described on the
// package.
func (r *Router) route(ctx context.Context, key []byte, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	shard := placement.ShardOf(key, r.cfg.Shards)
	req.Shard = uint32(shard)
	target := r.cfg.Primary(shard)
	backoff := initialBackoff
	var lastErr error
	for attempt := 1; ; attempt++ {
		var resp *dkvv1.ForwardResponse
		var err error
		if target == r.cfg.Self {
			resp, err = r.serveLocal(ctx, shard, req)
		} else {
			resp, err = r.forward(ctx, target, req)
		}
		switch {
		case err == nil && resp.GetNotOwner() == nil:
			return resp, nil
		case err == nil:
			r.notOwner.Add(1)
			lastErr = status.Errorf(codes.Unavailable, "node %s does not own shard %d (hint %q)", target, shard, resp.GetNotOwner().GetOwnerHint())
			if hint := resp.GetNotOwner().GetOwnerHint(); hint != "" {
				target = hint
			}
		case status.Code(err) == codes.Unavailable:
			lastErr = err
		default:
			return nil, err // the owner answered; not a routing problem
		}
		if attempt >= maxAttempts {
			return nil, lastErr
		}
		r.retries.Add(1)
		// Full jitter in [backoff/2, backoff*3/2) spreads retries from many
		// clients after an owner comes back.
		wait := backoff/2 + time.Duration(rand.Int64N(int64(backoff)))
		backoff = min(2*backoff, maxBackoff)
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			code := status.FromContextError(ctx.Err()).Code()
			return nil, status.Errorf(code, "shard %d: %v; last error: %v", shard, ctx.Err(), lastErr)
		case <-t.C:
		}
	}
}

func (r *Router) forward(ctx context.Context, target string, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	conn, err := r.cfg.Pool.Conn(target)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	r.forwarded.Add(1)
	return dkvv1.NewInternalServiceClient(conn).Forward(ctx, req)
}

// Forward serves a request forwarded by another node.
func (r *Router) Forward(ctx context.Context, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	shard := int(req.GetShard())
	if shard < 0 || shard >= r.cfg.Shards {
		return nil, status.Errorf(codes.InvalidArgument, "shard %d out of range [0, %d)", shard, r.cfg.Shards)
	}
	if owner := r.cfg.Primary(shard); owner != r.cfg.Self {
		return &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_NotOwner{NotOwner: &dkvv1.NotOwner{
			Shard: uint32(shard), OwnerHint: owner,
		}}}, nil
	}
	return r.serveLocal(ctx, shard, req)
}

func (r *Router) serveLocal(ctx context.Context, shard int, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	st, ok := r.cfg.Local[shard]
	if !ok {
		return nil, status.Errorf(codes.Unavailable, "shard %d has no local replica on %s", shard, r.cfg.Self)
	}
	switch o := req.GetOp().(type) {
	case *dkvv1.ForwardRequest_Put:
		ver, err := st.Put(ctx, o.Put.GetKey(), o.Put.GetValue())
		if err != nil {
			return nil, err
		}
		return &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_Put{Put: &dkvv1.PutResponse{Version: ver}}}, nil
	case *dkvv1.ForwardRequest_Get:
		v, ver, found, err := st.Get(ctx, o.Get.GetKey())
		if err != nil {
			return nil, err
		}
		return &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_Get{Get: &dkvv1.GetResponse{Found: found, Value: v, Version: ver}}}, nil
	case *dkvv1.ForwardRequest_Delete:
		found, ver, err := st.Delete(ctx, o.Delete.GetKey())
		if err != nil {
			return nil, err
		}
		return &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_Delete{Delete: &dkvv1.DeleteResponse{Found: found, Version: ver}}}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "forwarded request has no operation")
	}
}

// Stats reports routing activity.
type Stats struct {
	Forwarded, NotOwner, Retries int64
}

// Stats returns counters since the router started.
func (r *Router) Stats() Stats {
	return Stats{Forwarded: r.forwarded.Load(), NotOwner: r.notOwner.Load(), Retries: r.retries.Load()}
}
