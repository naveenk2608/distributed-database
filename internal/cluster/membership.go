// Package cluster tracks the nodes of a dkv cluster.
//
// Phase 4 membership is static: every node reads the same bootstrap peer
// list from its configuration, pings each peer every ping interval, and
// reports a peer down once it has gone longer than the down window without
// answering. A peer's state is derived from the time of its last answer
// when status is read, so "down within the window" holds exactly, not just
// at ping boundaries. Phase 7 replaces this with liveness kept by the meta
// group. See docs/adr/0021-node-identity-and-bootstrap-membership.md.
package cluster

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/obs"
	"github.com/naveenk2608/distributed-database/internal/transport"
)

// Clock is the time source membership uses to judge staleness; tests
// substitute a fake one.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Config configures Membership.
type Config struct {
	ClusterName  string
	NodeID       string
	Advertise    string // this node's internal address as peers reach it
	Peers        []string
	PingInterval time.Duration
	PingTimeout  time.Duration
	DownAfter    time.Duration
	// Placement is this node's placement fingerprint; peers that report a
	// different one are flagged in status.
	Placement string
	// OnEvent, if set, is told when a peer goes up or down; it is called
	// with the membership's lock held and must not block or call back.
	OnEvent func(obs.Event)
}

// Membership pings the bootstrap peers and reports their state. It is
// safe for concurrent use.
type Membership struct {
	cfg     Config
	pool    *transport.Pool
	clock   Clock
	log     *slog.Logger
	started time.Time

	mu    sync.Mutex
	self  *dkvv1.NodeInfo  // replaced, never modified, so it can be shared
	peers map[string]*peer // by address
	order []string         // addresses in configuration order
}

type peer struct {
	addr      string
	self      bool
	node      *dkvv1.NodeInfo // identity from the last answer
	everSeen  bool
	lastSeen  time.Time
	rtt       time.Duration
	failures  uint32
	lastErr   string
	lastState dkvv1.MemberState // last state logged
}

// New returns a Membership for cfg. clock may be nil for real time.
func New(cfg Config, pool *transport.Pool, clock Clock, log *slog.Logger) *Membership {
	if clock == nil {
		clock = realClock{}
	}
	m := &Membership{cfg: cfg, pool: pool, clock: clock, log: log, peers: map[string]*peer{}}
	m.started = clock.Now()
	m.self = &dkvv1.NodeInfo{
		NodeId:            cfg.NodeID,
		ClusterName:       cfg.ClusterName,
		InternalAddr:      cfg.Advertise,
		StartedAtUnixNano: m.started.UnixNano(),
		Placement:         cfg.Placement,
	}
	addrs := slices.Clone(cfg.Peers)
	if cfg.Advertise != "" && !slices.Contains(addrs, cfg.Advertise) {
		addrs = append(addrs, cfg.Advertise) // status always lists this node
	}
	for _, a := range addrs {
		m.peers[a] = &peer{addr: a, self: a == cfg.Advertise, lastState: dkvv1.MemberState_MEMBER_STATE_UNKNOWN}
		m.order = append(m.order, a)
	}
	return m
}

// Self returns this node's identity.
func (m *Membership) Self() *dkvv1.NodeInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.self
}

// SetBootstrapMembers records the node IDs the cluster was bootstrapped
// with, which this node reports from now on.
func (m *Membership) SetBootstrapMembers(ids []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	self := proto.Clone(m.self).(*dkvv1.NodeInfo)
	self.BootstrapMembers = slices.Clone(ids)
	m.self = self
	for _, p := range m.peers {
		if p.self {
			p.node = self
		}
	}
}

// Peer is a bootstrap peer as last heard from.
type Peer struct {
	Addr string
	// Node is the identity from its last answer; nil if it never answered.
	Node *dkvv1.NodeInfo
	Up   bool
	Self bool
}

// Peers returns every bootstrap peer, in configuration order.
func (m *Membership) Peers() []Peer {
	now := m.clock.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Peer
	for _, addr := range m.order {
		p := m.peers[addr]
		node := p.node
		if p.self {
			node = m.self
		}
		out = append(out, Peer{Addr: addr, Node: node, Self: p.self, Up: m.stateLocked(p, now) == dkvv1.MemberState_MEMBER_STATE_UP})
	}
	return out
}

// Run pings every peer until ctx is cancelled, from one goroutine per
// peer, and returns after all of them have exited.
func (m *Membership) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, addr := range m.order {
		if m.peers[addr].self {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.pingLoop(ctx, addr)
		}()
	}
	wg.Wait()
}

func (m *Membership) pingLoop(ctx context.Context, addr string) {
	t := time.NewTicker(m.cfg.PingInterval)
	defer t.Stop()
	for {
		if !m.pingOnce(ctx, addr) {
			return // the address turned out to be this node
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// pingOnce pings addr and records the outcome. It returns false if the
// address answered with this node's own ID, so pinging it is pointless.
func (m *Membership) pingOnce(ctx context.Context, addr string) bool {
	conn, err := m.pool.Conn(addr)
	var resp *dkvv1.PingResponse
	start := m.clock.Now()
	if err == nil {
		cctx, cancel := context.WithTimeout(ctx, m.cfg.PingTimeout)
		resp, err = dkvv1.NewInternalServiceClient(conn).Ping(cctx, &dkvv1.PingRequest{From: m.Self()})
		cancel()
	}
	now := m.clock.Now()
	if ctx.Err() != nil {
		return true // shutting down; do not record a failure
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.peers[addr]
	switch {
	case err != nil:
		p.failures++
		p.lastErr = describe(err)
	case resp.GetNode().GetNodeId() == m.cfg.NodeID:
		p.self, p.node = true, m.self
		p.lastErr = ""
		return false
	case resp.GetNode().GetClusterName() != m.cfg.ClusterName:
		p.failures++
		p.lastErr = fmt.Sprintf("peer belongs to cluster %q", resp.GetNode().GetClusterName())
	default:
		p.node = resp.GetNode()
		p.everSeen, p.lastSeen = true, now
		p.rtt = now.Sub(start)
		p.failures, p.lastErr = 0, ""
		if theirs := p.node.GetPlacement(); theirs != m.cfg.Placement {
			// Reachable, so still up, but it would route keys elsewhere.
			p.lastErr = fmt.Sprintf("placement differs: peer has %q, this node %q", theirs, m.cfg.Placement)
		}
	}
	if st := m.stateLocked(p, now); st != p.lastState {
		m.log.Info("peer state changed", "peer", addr, "node_id", p.node.GetNodeId(),
			"from", p.lastState.String(), "to", st.String(), "error", p.lastErr)
		p.lastState = st
		kind := map[dkvv1.MemberState]string{dkvv1.MemberState_MEMBER_STATE_UP: obs.EventPeerUp,
			dkvv1.MemberState_MEMBER_STATE_DOWN: obs.EventPeerDown}[st]
		if kind != "" && m.cfg.OnEvent != nil {
			m.cfg.OnEvent(obs.Event{Kind: kind, Shard: obs.NoShard, Node: cmp.Or(p.node.GetNodeId(), addr), Detail: p.lastErr})
		}
	}
	return true
}

func describe(err error) string {
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.FailedPrecondition {
			return s.Message()
		}
		return s.Code().String() + ": " + s.Message()
	}
	if errors.Is(err, transport.ErrClosed) {
		return "shutting down"
	}
	return err.Error()
}

// stateLocked derives a peer's state at now. Caller holds m.mu.
func (m *Membership) stateLocked(p *peer, now time.Time) dkvv1.MemberState {
	switch {
	case p.self:
		return dkvv1.MemberState_MEMBER_STATE_UP
	case p.everSeen && now.Sub(p.lastSeen) <= m.cfg.DownAfter:
		return dkvv1.MemberState_MEMBER_STATE_UP
	case p.everSeen:
		return dkvv1.MemberState_MEMBER_STATE_DOWN
	case now.Sub(m.started) > m.cfg.DownAfter:
		return dkvv1.MemberState_MEMBER_STATE_DOWN // never answered within a window
	default:
		return dkvv1.MemberState_MEMBER_STATE_UNKNOWN
	}
}

// Status returns this node's view of every peer, in configuration order.
func (m *Membership) Status() *dkvv1.ClusterStatusResponse {
	now := m.clock.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	resp := &dkvv1.ClusterStatusResponse{Self: m.self}
	for _, addr := range m.order {
		p := m.peers[addr]
		ms := &dkvv1.MemberStatus{
			Addr:                addr,
			Node:                p.node,
			State:               m.stateLocked(p, now),
			Self:                p.self,
			RttNanos:            int64(p.rtt),
			ConsecutiveFailures: p.failures,
			LastError:           p.lastErr,
		}
		if p.self {
			ms.Node = m.self
		} else if p.everSeen {
			ms.LastSeenUnixNano = p.lastSeen.UnixNano()
		}
		resp.Members = append(resp.Members, ms)
	}
	return resp
}

// PingService answers other nodes' pings. Register it on the internal
// server.
type PingService struct {
	dkvv1.UnimplementedInternalServiceServer
	m *Membership
}

// NewPingService returns the Ping handler for m.
func NewPingService(m *Membership) *PingService { return &PingService{m: m} }

// Ping returns this node's identity, or FAILED_PRECONDITION if the caller
// belongs to another cluster.
func (s *PingService) Ping(_ context.Context, req *dkvv1.PingRequest) (*dkvv1.PingResponse, error) {
	if from := req.GetFrom(); from != nil && from.GetClusterName() != s.m.cfg.ClusterName {
		return nil, status.Errorf(codes.FailedPrecondition, "node %s belongs to cluster %q, not %q",
			s.m.cfg.NodeID, s.m.cfg.ClusterName, from.GetClusterName())
	}
	return &dkvv1.PingResponse{Node: s.m.Self()}, nil
}
