// Package router sends each key-value request to the Raft leader of the
// key's shard.
//
// Any node accepts any request. The router computes the key's shard and
// sends the request to the shard's leader as it last knew it: the local
// replica if this node is the leader, otherwise over InternalService.Forward.
// A replica that is not the leader answers NotOwner with the leader it knows
// of, and never forwards further, so every attempt is at most one hop. The
// router follows hints, rotates through the replica set when a node is
// unreachable or knows no leader, and retries with jittered exponential
// backoff until the caller's deadline. Stale reads are answered by a local
// replica when this node holds one. See docs/adr/0024-routing.md.
package router

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/placement"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

// Retry policy. Attempts are bounded as well as the backoff, so a client
// without a deadline still gets an answer.
const (
	maxAttempts    = 12
	initialBackoff = 20 * time.Millisecond
	maxBackoff     = 500 * time.Millisecond
)

// Replica is a shard replica this node holds.
type Replica interface {
	Put(ctx context.Context, key, value []byte, clientID string, requestID uint64) (uint64, error)
	Delete(ctx context.Context, key []byte, clientID string, requestID uint64) (bool, uint64, error)
	CompareAndSet(ctx context.Context, key, value []byte, expected uint64, clientID string, requestID uint64) (bool, uint64, error)
	Get(ctx context.Context, key []byte, stale bool, minVersion uint64) ([]byte, uint64, bool, uint64, error)
}

// leaderHinter is implemented by a replica's not-leader error.
type leaderHinter interface{ LeaderHint() string }

// Config configures a Router.
type Config struct {
	// Self is this node's internal (advertise) address.
	Self string
	// Shards is the cluster's shard count.
	Shards int
	// Replicas returns a shard's replica set.
	Replicas func(shard int) []string
	// Local holds the replicas this node has.
	Local map[int]Replica
	Pool  *transport.Pool
	Log   *slog.Logger
}

// Router implements the API server's backend for a cluster node.
type Router struct {
	cfg Config

	mu      sync.Mutex
	leaders map[int]string // shard -> last known leader

	forwarded, notLeader, retries atomic.Int64
}

// New returns a Router.
func New(cfg Config) *Router { return &Router{cfg: cfg, leaders: map[int]string{}} }

func (r *Router) Put(ctx context.Context, req *dkvv1.PutRequest) (*dkvv1.PutResponse, error) {
	resp, err := r.route(ctx, req.GetKey(), &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Put{Put: req}})
	if err != nil {
		return nil, err
	}
	return resp.GetPut(), nil
}

func (r *Router) Get(ctx context.Context, req *dkvv1.GetRequest) (*dkvv1.GetResponse, error) {
	resp, err := r.route(ctx, req.GetKey(), &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Get{Get: req}})
	if err != nil {
		return nil, err
	}
	return resp.GetGet(), nil
}

func (r *Router) Delete(ctx context.Context, req *dkvv1.DeleteRequest) (*dkvv1.DeleteResponse, error) {
	resp, err := r.route(ctx, req.GetKey(), &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Delete{Delete: req}})
	if err != nil {
		return nil, err
	}
	return resp.GetDelete(), nil
}

func (r *Router) CompareAndSet(ctx context.Context, req *dkvv1.CompareAndSetRequest) (*dkvv1.CompareAndSetResponse, error) {
	resp, err := r.route(ctx, req.GetKey(), &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Cas{Cas: req}})
	if err != nil {
		return nil, err
	}
	return resp.GetCas(), nil
}

func isStale(req *dkvv1.ForwardRequest) bool {
	return req.GetGet() != nil && req.GetGet().GetConsistency() == dkvv1.Consistency_CONSISTENCY_STALE
}

func (r *Router) leaderOf(shard int) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaders[shard]
}

func (r *Router) setLeader(shard int, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if addr == "" {
		delete(r.leaders, shard)
	} else {
		r.leaders[shard] = addr
	}
}

// nextReplica returns the replica after cur in shard's replica set.
func (r *Router) nextReplica(shard int, cur string) string {
	set := r.cfg.Replicas(shard)
	i := slices.Index(set, cur)
	return set[(i+1)%len(set)]
}

// route runs req on the leader of key's shard, retrying as described on
// the package.
func (r *Router) route(ctx context.Context, key []byte, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	shard := placement.ShardOf(key, r.cfg.Shards)
	req.Shard = uint32(shard)
	stale := isStale(req)
	target := r.leaderOf(shard)
	switch {
	case stale && r.cfg.Local[shard] != nil:
		target = r.cfg.Self // any replica may answer a stale read
	case target == "" && r.cfg.Local[shard] != nil:
		target = r.cfg.Self
	case target == "":
		target = r.cfg.Replicas(shard)[0]
	}
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
		redirectNow := false
		switch {
		case err == nil && resp.GetNotOwner() == nil:
			if !stale {
				r.setLeader(shard, target)
			}
			return resp, nil
		case err == nil:
			r.notLeader.Add(1)
			hint := resp.GetNotOwner().GetOwnerHint()
			lastErr = status.Errorf(codes.Unavailable, "shard %d: %s is not the leader (hint %q)", shard, target, hint)
			r.setLeader(shard, hint)
			if hint != "" && hint != target {
				target, redirectNow = hint, attempt < 3 // follow a fresh hint at once
			} else {
				target = r.nextReplica(shard, target) // no leader known there: an election may be running
			}
		case status.Code(err) == codes.Unavailable:
			lastErr = err
			r.setLeader(shard, "")
			target = r.nextReplica(shard, target)
		default:
			return nil, err // the leader answered; not a routing problem
		}
		if attempt >= maxAttempts {
			return nil, lastErr
		}
		r.retries.Add(1)
		if redirectNow {
			continue
		}
		// Jitter in [backoff/2, backoff*3/2) spreads retries from many
		// clients after a leader change.
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
	return r.serveLocal(ctx, shard, req)
}

func notOwner(shard int, hint string) *dkvv1.ForwardResponse {
	return &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_NotOwner{NotOwner: &dkvv1.NotOwner{
		Shard: uint32(shard), OwnerHint: hint,
	}}}
}

// serveLocal runs req on this node's replica of shard, or answers NotOwner.
func (r *Router) serveLocal(ctx context.Context, shard int, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	rep := r.cfg.Local[shard]
	if rep == nil {
		hint := r.leaderOf(shard)
		if hint == "" {
			hint = r.cfg.Replicas(shard)[0]
		}
		return notOwner(shard, hint), nil
	}
	var resp *dkvv1.ForwardResponse
	var err error
	switch o := req.GetOp().(type) {
	case *dkvv1.ForwardRequest_Put:
		var ver uint64
		ver, err = rep.Put(ctx, o.Put.GetKey(), o.Put.GetValue(), o.Put.GetClientId(), o.Put.GetRequestId())
		resp = &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_Put{Put: &dkvv1.PutResponse{Version: ver}}}
	case *dkvv1.ForwardRequest_Get:
		g := o.Get
		v, ver, found, applied, gerr := rep.Get(ctx, g.GetKey(), g.GetConsistency() == dkvv1.Consistency_CONSISTENCY_STALE, g.GetMinVersion())
		err = gerr
		resp = &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_Get{Get: &dkvv1.GetResponse{
			Found: found, Value: v, Version: ver, AppliedIndex: applied}}}
	case *dkvv1.ForwardRequest_Delete:
		var found bool
		var ver uint64
		found, ver, err = rep.Delete(ctx, o.Delete.GetKey(), o.Delete.GetClientId(), o.Delete.GetRequestId())
		resp = &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_Delete{Delete: &dkvv1.DeleteResponse{Found: found, Version: ver}}}
	case *dkvv1.ForwardRequest_Cas:
		c := o.Cas
		var ok bool
		var ver uint64
		ok, ver, err = rep.CompareAndSet(ctx, c.GetKey(), c.GetValue(), c.GetExpectedVersion(), c.GetClientId(), c.GetRequestId())
		resp = &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_Cas{Cas: &dkvv1.CompareAndSetResponse{Succeeded: ok, Version: ver}}}
	default:
		return nil, status.Error(codes.InvalidArgument, "forwarded request has no operation")
	}
	var nl leaderHinter
	if errors.As(err, &nl) {
		return notOwner(shard, nl.LeaderHint()), nil
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Stats reports routing activity.
type Stats struct {
	Forwarded, NotLeader, Retries int64
}

// Stats returns counters since the router started.
func (r *Router) Stats() Stats {
	return Stats{Forwarded: r.forwarded.Load(), NotLeader: r.notLeader.Load(), Retries: r.retries.Load()}
}
