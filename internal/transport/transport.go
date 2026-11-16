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
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
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
	)
	if err != nil {
		return nil, fmt.Errorf("transport: connection to %s: %w", addr, err)
	}
	p.conns[addr] = c
	return c, nil
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
