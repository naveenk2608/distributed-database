// Package router sends each request to the Raft leader of its group: a
// key's shard, or the meta group.
//
// Any node accepts any request. The router sends it to the group's leader
// as it last knew it: the local replica if this node holds the group,
// otherwise over InternalService.Forward. A replica that is not the leader
// answers NotOwner with the leader it knows of, and never forwards further,
// so every attempt is at most one hop. The router follows hints, rotates
// through the group's voters when a node is unreachable or knows no leader,
// and retries with jittered exponential backoff until the caller's
// deadline. Stale reads are answered by a local replica when this node
// holds one. Members are named by node ID and resolved to addresses.
// See docs/adr/0024-routing.md.
package router

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/placement"
	"github.com/naveenk2608/distributed-database/internal/replica"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

// MetaShard is the meta group's number.
const MetaShard = -1

// Retry policy: retries continue until the caller's deadline; a caller
// without a deadline gets an answer after maxAttempts.
const (
	maxAttempts    = 12
	initialBackoff = 20 * time.Millisecond
	maxBackoff     = 500 * time.Millisecond
)

// Replica is a group replica this node holds.
type Replica interface {
	Put(ctx context.Context, key, value []byte, clientID string, requestID uint64) (uint64, error)
	Delete(ctx context.Context, key []byte, clientID string, requestID uint64) (bool, uint64, error)
	CompareAndSet(ctx context.Context, key, value []byte, expected uint64, clientID string, requestID uint64) (bool, uint64, error)
	Get(ctx context.Context, key []byte, stale bool, minVersion uint64) ([]byte, uint64, bool, uint64, error)
	Txn(ctx context.Context, txn *dkvv1.Txn, clientID string, requestID uint64) (bool, uint64, error)
	Scan(ctx context.Context, prefix []byte, stale bool) ([]replica.KV, error)
}

// leaderHinter is implemented by a replica's not-leader error.
type leaderHinter interface{ LeaderHint() string }

// Config configures a Router.
type Config struct {
	// Self is this node's ID.
	Self string
	// Shards is the cluster's shard count.
	Shards int
	// Replicas returns a group's voters (node IDs), as last known.
	Replicas func(shard int) []string
	// Resolve returns a node's internal address, or "" if unknown.
	Resolve func(id string) string
	// Local returns this node's replica of a group, or nil.
	Local func(shard int) Replica
	Pool  *transport.Pool
	Log   *slog.Logger
}

// Router implements the API server's backend for a cluster node.
type Router struct {
	cfg Config

	mu      sync.Mutex
	leaders map[int]string // group -> last known leader

	forwarded, notLeader, retries atomic.Int64
}

// New returns a Router.
func New(cfg Config) *Router { return &Router{cfg: cfg, leaders: map[int]string{}} }

func (r *Router) Put(ctx context.Context, req *dkvv1.PutRequest) (*dkvv1.PutResponse, error) {
	resp, err := r.route(ctx, r.shardOf(req.GetKey()), &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Put{Put: req}})
	if err != nil {
		return nil, err
	}
	return resp.GetPut(), nil
}

func (r *Router) Get(ctx context.Context, req *dkvv1.GetRequest) (*dkvv1.GetResponse, error) {
	resp, err := r.route(ctx, r.shardOf(req.GetKey()), &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Get{Get: req}})
	if err != nil {
		return nil, err
	}
	return resp.GetGet(), nil
}

func (r *Router) Delete(ctx context.Context, req *dkvv1.DeleteRequest) (*dkvv1.DeleteResponse, error) {
	resp, err := r.route(ctx, r.shardOf(req.GetKey()), &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Delete{Delete: req}})
	if err != nil {
		return nil, err
	}
	return resp.GetDelete(), nil
}

func (r *Router) CompareAndSet(ctx context.Context, req *dkvv1.CompareAndSetRequest) (*dkvv1.CompareAndSetResponse, error) {
	resp, err := r.route(ctx, r.shardOf(req.GetKey()), &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Cas{Cas: req}})
	if err != nil {
		return nil, err
	}
	return resp.GetCas(), nil
}

// MetaTxn runs a conditional transaction on the meta group.
func (r *Router) MetaTxn(ctx context.Context, req *dkvv1.TxnRequest) (*dkvv1.TxnResponse, error) {
	resp, err := r.route(ctx, MetaShard, &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Txn{Txn: req}})
	if err != nil {
		return nil, err
	}
	return resp.GetTxn(), nil
}

// MetaScan reads every meta record with prefix, linearizably.
func (r *Router) MetaScan(ctx context.Context, prefix []byte) ([]*dkvv1.KeyValue, error) {
	resp, err := r.route(ctx, MetaShard, &dkvv1.ForwardRequest{Op: &dkvv1.ForwardRequest_Scan{Scan: &dkvv1.ScanRequest{Prefix: prefix}}})
	if err != nil {
		return nil, err
	}
	return resp.GetScan().GetKvs(), nil
}

func (r *Router) shardOf(key []byte) int { return placement.ShardOf(key, r.cfg.Shards) }

func isStale(req *dkvv1.ForwardRequest) bool {
	return req.GetGet() != nil && req.GetGet().GetConsistency() == dkvv1.Consistency_CONSISTENCY_STALE ||
		req.GetScan() != nil && req.GetScan().GetConsistency() == dkvv1.Consistency_CONSISTENCY_STALE
}

// LeaderOf returns the leader the router last saw for a group.
func (r *Router) LeaderOf(shard int) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaders[shard]
}

// NoteLeader records a group's leader learned elsewhere.
func (r *Router) NoteLeader(shard int, id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id == "" {
		delete(r.leaders, shard)
	} else {
		r.leaders[shard] = id
	}
}

// nextReplica returns the voter after cur in the group's voters.
func (r *Router) nextReplica(shard int, cur string) string {
	set := r.cfg.Replicas(shard)
	if len(set) == 0 {
		return ""
	}
	i := slices.Index(set, cur)
	return set[(i+1)%len(set)]
}

func (r *Router) firstTarget(shard int, stale bool) string {
	local := r.cfg.Local(shard) != nil
	switch target := r.LeaderOf(shard); {
	case stale && local:
		return r.cfg.Self // any replica may answer a stale read
	case target != "":
		return target
	case local:
		return r.cfg.Self
	default:
		if set := r.cfg.Replicas(shard); len(set) > 0 {
			return set[0]
		}
		return ""
	}
}

// route runs req on the leader of a group, retrying as described on the
// package.
func (r *Router) route(ctx context.Context, shard int, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	req.Shard = uint32(int32(shard))
	stale := isStale(req)
	target := r.firstTarget(shard, stale)
	backoff := initialBackoff
	var lastErr error
	for attempt := 1; ; attempt++ {
		var resp *dkvv1.ForwardResponse
		var err error
		switch target {
		case "":
			err = status.Errorf(codes.Unavailable, "%s: no known replica", groupName(shard))
		case r.cfg.Self:
			resp, err = r.serveLocal(ctx, shard, req)
		default:
			resp, err = r.forward(ctx, target, req)
		}
		redirectNow := false
		switch {
		case err == nil && resp.GetNotOwner() == nil:
			if !stale {
				r.NoteLeader(shard, target)
			}
			return resp, nil
		case err == nil:
			r.notLeader.Add(1)
			hint := resp.GetNotOwner().GetOwnerHint()
			lastErr = status.Errorf(codes.Unavailable, "%s: %s is not the leader (hint %q)", groupName(shard), target, hint)
			r.NoteLeader(shard, hint)
			if hint != "" && hint != target {
				target, redirectNow = hint, attempt < 3 // follow a fresh hint at once
			} else {
				target = r.nextReplica(shard, target) // no leader known there: an election may be running
			}
		case status.Code(err) == codes.Unavailable:
			lastErr = err
			r.NoteLeader(shard, "")
			target = r.nextReplica(shard, target)
		default:
			return nil, err // the leader answered; not a routing problem
		}
		if _, ok := ctx.Deadline(); !ok && attempt >= maxAttempts {
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
			return nil, status.Errorf(code, "%s: %v; last error: %v", groupName(shard), ctx.Err(), lastErr)
		case <-t.C:
		}
	}
}

func groupName(shard int) string {
	if shard == MetaShard {
		return "meta group"
	}
	return "shard " + strconv.Itoa(shard)
}

func (r *Router) forward(ctx context.Context, target string, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	addr := r.cfg.Resolve(target)
	if addr == "" {
		return nil, status.Errorf(codes.Unavailable, "no address known for node %s", target)
	}
	conn, err := r.cfg.Pool.Conn(addr)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	r.forwarded.Add(1)
	return dkvv1.NewInternalServiceClient(conn).Forward(ctx, req)
}

// Forward serves a request forwarded by another node.
func (r *Router) Forward(ctx context.Context, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	shard := int(int32(req.GetShard()))
	if shard != MetaShard && (shard < 0 || shard >= r.cfg.Shards) {
		return nil, status.Errorf(codes.InvalidArgument, "shard %d out of range [0, %d)", shard, r.cfg.Shards)
	}
	return r.serveLocal(ctx, shard, req)
}

func notOwner(shard int, hint string) *dkvv1.ForwardResponse {
	return &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_NotOwner{NotOwner: &dkvv1.NotOwner{
		Shard: uint32(int32(shard)), OwnerHint: hint,
	}}}
}

// serveLocal runs req on this node's replica of the group, or answers
// NotOwner.
func (r *Router) serveLocal(ctx context.Context, shard int, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	rep := r.cfg.Local(shard)
	if rep == nil {
		hint := r.LeaderOf(shard)
		if hint == "" {
			if set := r.cfg.Replicas(shard); len(set) > 0 {
				hint = set[0]
			}
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
	case *dkvv1.ForwardRequest_Txn:
		t := o.Txn
		var ok bool
		var ver uint64
		ok, ver, err = rep.Txn(ctx, t.GetTxn(), t.GetClientId(), t.GetRequestId())
		resp = &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_Txn{Txn: &dkvv1.TxnResponse{Succeeded: ok, Version: ver}}}
	case *dkvv1.ForwardRequest_Scan:
		var kvs []replica.KV
		kvs, err = rep.Scan(ctx, o.Scan.GetPrefix(), o.Scan.GetConsistency() == dkvv1.Consistency_CONSISTENCY_STALE)
		sr := &dkvv1.ScanResponse{}
		for _, kv := range kvs {
			sr.Kvs = append(sr.Kvs, &dkvv1.KeyValue{Key: kv.Key, Value: kv.Value, Version: kv.Version})
		}
		resp = &dkvv1.ForwardResponse{Result: &dkvv1.ForwardResponse_Scan{Scan: sr}}
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
