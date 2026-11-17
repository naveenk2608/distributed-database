// Package transport is the gRPC layer between dkv nodes: a server for the
// node-to-node API and a pool of client connections to peers.
//
// Each peer gets one long-lived gRPC connection, shared by every caller:
// HTTP/2 multiplexes concurrent calls over it, so "pooling" means never
// opening a second one. A connection that breaks reconnects on its own with
// jittered exponential backoff, while calls made in the meantime fail fast
// with codes.Unavailable instead of queueing. Keepalive pings detect a peer
// that vanished without closing its sockets. Callers bound every call with
// a context deadline. See docs/adr/0022-internal-transport.md.
package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// Config configures both ends of the transport.
type Config struct {
	// DialTimeout bounds establishing one connection attempt.
	DialTimeout time.Duration
	// MaxBackoff caps the delay between reconnection attempts to a peer
	// that is down. 0 means 1s.
	MaxBackoff time.Duration
	// KeepaliveTime is the idle time before a keepalive ping; gRPC raises
	// client values below 10s to 10s.
	KeepaliveTime time.Duration
	// KeepaliveTimeout is how long a keepalive ping may go unanswered
	// before the connection is closed.
	KeepaliveTimeout time.Duration
	// MaxMessageBytes bounds one message in either direction. 0 means 64 MiB.
	MaxMessageBytes int
	// Self and Faults inject network faults (tests only): every call from
	// Self to a peer first asks Faults what happens to it.
	Self   string
	Faults Faults
}

// Faults decides what happens to one call between two nodes, named by
// their internal addresses. It is the fault-injection seam for tests.
type Faults interface {
	Decide(from, to, method string) Fault
}

// Fault is the fate of one call.
type Fault struct {
	// Drop fails the call with Unavailable, as if the network lost it.
	Drop bool
	// Delay holds the call back before sending it.
	Delay time.Duration
	// Duplicate sends a unary call twice; the caller sees the second
	// answer.
	Duplicate bool
}

func (c Config) withDefaults() Config {
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = time.Second
	}
	if c.MaxMessageBytes <= 0 {
		c.MaxMessageBytes = 64 << 20
	}
	return c
}

// NewServer returns a gRPC server for the node-to-node API whose keepalive
// rules match the pool's.
func NewServer(cfg Config, opts ...grpc.ServerOption) *grpc.Server {
	cfg = cfg.withDefaults()
	base := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: cfg.KeepaliveTime, Timeout: cfg.KeepaliveTimeout}),
		// Accept the clients' keepalive pings at the configured rate, even
		// on idle connections; by default a server closes connections that
		// ping more often than every 5 minutes.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: cfg.KeepaliveTime / 2, PermitWithoutStream: true}),
		grpc.MaxRecvMsgSize(cfg.MaxMessageBytes),
		grpc.MaxSendMsgSize(cfg.MaxMessageBytes),
	}
	return grpc.NewServer(append(base, opts...)...)
}

// ErrClosed is returned by Pool.Conn after Close.
var ErrClosed = errors.New("transport: pool closed")

// Pool holds one client connection per peer address. It is safe for
// concurrent use.
type Pool struct {
	cfg   Config
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	done  bool
}

// NewPool returns an empty pool.
func NewPool(cfg Config) *Pool {
	return &Pool{cfg: cfg.withDefaults(), conns: map[string]*grpc.ClientConn{}}
}

// Conn returns the shared connection to addr, creating it on first use. It
// does not wait for the connection to be established: the first call does.
func (p *Pool) Conn(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return nil, ErrClosed
	}
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  50 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   p.cfg.MaxBackoff,
			},
			MinConnectTimeout: p.cfg.DialTimeout,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                p.cfg.KeepaliveTime,
			Timeout:             p.cfg.KeepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(p.cfg.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(p.cfg.MaxMessageBytes),
		),
		grpc.WithChainUnaryInterceptor(p.unaryFaults(addr)),
		grpc.WithChainStreamInterceptor(p.streamFaults(addr)),
	)
	if err != nil {
		return nil, fmt.Errorf("transport: connection to %s: %w", addr, err)
	}
	p.conns[addr] = c
	return c, nil
}

// apply waits out an injected delay and reports whether to drop the call.
func (p *Pool) apply(ctx context.Context, f Fault) error {
	if f.Delay > 0 {
		t := time.NewTimer(f.Delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return status.FromContextError(ctx.Err()).Err()
		case <-t.C:
		}
	}
	if f.Drop {
		return status.Error(codes.Unavailable, "transport: dropped by an injected fault")
	}
	return nil
}

func (p *Pool) unaryFaults(addr string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if p.cfg.Faults == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		f := p.cfg.Faults.Decide(p.cfg.Self, addr, method)
		if err := p.apply(ctx, f); err != nil {
			return err
		}
		if f.Duplicate {
			_ = invoker(ctx, method, req, reply, cc, opts...) // the first copy's answer is lost
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (p *Pool) streamFaults(addr string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if p.cfg.Faults != nil {
			f := p.cfg.Faults.Decide(p.cfg.Self, addr, method)
			f.Duplicate = false // a stream cannot be replayed
			if err := p.apply(ctx, f); err != nil {
				return nil, err
			}
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// Close closes every connection. Later calls to Conn return ErrClosed.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done = true
	var errs []error
	for addr, c := range p.conns {
		if err := c.Close(); err != nil {
			errs = append(errs, fmt.Errorf("transport: close connection to %s: %w", addr, err))
		}
		delete(p.conns, addr)
	}
	return errors.Join(errs...)
}
