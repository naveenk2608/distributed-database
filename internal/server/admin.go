package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
)

// Admin answers the operator API.
type Admin interface {
	// Status reports this node's view of the cluster.
	Status() *dkvv1.ClusterStatusResponse
	// ShardMap reports shard placement and the local replicas.
	ShardMap() *dkvv1.ShardMapResponse
	// Decommission asks the cluster to move every replica off a node.
	Decommission(ctx context.Context, nodeID string) error
	// Drain asks the cluster to move every leadership off a node and give
	// it none (drain), or lets it lead again (!drain).
	Drain(ctx context.Context, nodeID string, drain bool) error
	// TransferLeader asks shard's leader to hand leadership to nodeID.
	TransferLeader(ctx context.Context, shard int, nodeID string) error
	// Stats reports the local replicas and the block cache; the server
	// adds its request counters.
	Stats() *dkvv1.StatsResponse
	// Events returns up to limit recent events after sequence number after.
	Events(after uint64, limit int) *dkvv1.EventsResponse
}

// StaticStatus is the Admin of a standalone node: the cluster is this node
// alone, holding one shard in DB (which may be nil in tests). It records no
// events: a standalone node has no elections or membership.
type StaticStatus struct {
	Self *dkvv1.NodeInfo
	DB   *lsm.DB
}

// Status returns this node as the only member, up.
func (s StaticStatus) Status() *dkvv1.ClusterStatusResponse {
	return &dkvv1.ClusterStatusResponse{
		Self: s.Self,
		Members: []*dkvv1.MemberStatus{{
			Addr:  s.Self.GetInternalAddr(),
			Node:  s.Self,
			State: dkvv1.MemberState_MEMBER_STATE_UP,
			Self:  true,
		}},
	}
}

// ShardMap returns the standalone node's single shard.
func (s StaticStatus) ShardMap() *dkvv1.ShardMapResponse {
	info := &dkvv1.ShardInfo{Shard: 0, Replicas: []string{s.Self.GetInternalAddr()}, Local: true}
	if s.DB != nil {
		info.LastSeq = s.DB.LastSeq()
	}
	return &dkvv1.ShardMapResponse{ShardCount: 1, ReplicationFactor: 1, Shards: []*dkvv1.ShardInfo{info}}
}

// Decommission is not possible on a standalone node.
func (s StaticStatus) Decommission(context.Context, string) error {
	return status.Error(codes.FailedPrecondition, "a standalone node cannot be decommissioned")
}

// Drain is not possible on a standalone node.
func (s StaticStatus) Drain(context.Context, string, bool) error {
	return status.Error(codes.FailedPrecondition, "a standalone node has no leadership to drain")
}

// TransferLeader is not possible on a standalone node.
func (s StaticStatus) TransferLeader(context.Context, int, string) error {
	return status.Error(codes.FailedPrecondition, "a standalone node has no Raft group")
}

// Stats reports the standalone node's single engine as shard 0.
func (s StaticStatus) Stats() *dkvv1.StatsResponse {
	resp := &dkvv1.StatsResponse{NodeId: s.Self.GetNodeId(), StartedUnixNano: s.Self.GetStartedAtUnixNano()}
	if s.DB != nil {
		m := s.DB.Metrics()
		resp.Replicas = []*dkvv1.ReplicaStats{{Raft: &dkvv1.ReplicaStatus{Shard: 0, Role: "standalone", Applied: m.LastSeq, Commit: m.LastSeq},
			Engine: EngineStats(m)}}
		resp.BlockCache = cacheStats(m.BlockCache)
	}
	return resp
}

// Events returns none: a standalone node records no events.
func (s StaticStatus) Events(uint64, int) *dkvv1.EventsResponse {
	return &dkvv1.EventsResponse{NodeId: s.Self.GetNodeId()}
}

// EngineStats converts an engine's metrics for the operator API.
func EngineStats(m lsm.Metrics) *dkvv1.EngineStats {
	return &dkvv1.EngineStats{LastSeq: m.LastSeq, Memtables: int32(m.Memtables), MemtableBytes: m.MemtableBytes,
		Tables: int32(m.Tables), TableBytes: m.TableBytes, TableEntries: m.TableEntries, Flushes: m.Flushes,
		FlushBytes: m.FlushBytes, Compactions: m.Compactions, CompactionBytesRead: m.CompactionBytesRead,
		CompactionBytesWritten: m.CompactionBytesWrite, WriteStalls: m.WriteStalls, WriteSlowdowns: m.WriteSlowdowns,
		UserBytes: m.UserBytes, WalBytes: m.WALBytes}
}

type adminService struct {
	dkvv1.UnimplementedAdminServiceServer
	admin Admin
	stats func() *dkvv1.StatsResponse
}

func (a *adminService) ClusterStatus(context.Context, *dkvv1.ClusterStatusRequest) (*dkvv1.ClusterStatusResponse, error) {
	return a.admin.Status(), nil
}

func (a *adminService) ShardMap(context.Context, *dkvv1.ShardMapRequest) (*dkvv1.ShardMapResponse, error) {
	return a.admin.ShardMap(), nil
}

func (a *adminService) Decommission(ctx context.Context, req *dkvv1.DecommissionRequest) (*dkvv1.DecommissionResponse, error) {
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if err := a.admin.Decommission(ctx, req.GetNodeId()); err != nil {
		return nil, toStatus(err)
	}
	return &dkvv1.DecommissionResponse{}, nil
}

func (a *adminService) Drain(ctx context.Context, req *dkvv1.DrainRequest) (*dkvv1.DrainResponse, error) {
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if err := a.admin.Drain(ctx, req.GetNodeId(), req.GetDrain()); err != nil {
		return nil, toStatus(err)
	}
	return &dkvv1.DrainResponse{}, nil
}

func (a *adminService) TransferLeader(ctx context.Context, req *dkvv1.AdminServiceTransferLeaderRequest) (*dkvv1.AdminServiceTransferLeaderResponse, error) {
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if err := a.admin.TransferLeader(ctx, int(req.GetShard()), req.GetNodeId()); err != nil {
		return nil, toStatus(err)
	}
	return &dkvv1.AdminServiceTransferLeaderResponse{}, nil
}

func (a *adminService) Stats(context.Context, *dkvv1.StatsRequest) (*dkvv1.StatsResponse, error) {
	return a.stats(), nil
}

func (a *adminService) Events(_ context.Context, req *dkvv1.EventsRequest) (*dkvv1.EventsResponse, error) {
	return a.admin.Events(req.GetAfter(), eventLimit(req.GetLimit())), nil
}

// eventLimit applies the default and the cap to a requested page size.
func eventLimit(n uint32) int {
	if n == 0 || n > 1000 {
		return 1000
	}
	return int(n)
}
