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
}

// StaticStatus is the Admin of a standalone node: the cluster is this node
// alone, holding one shard in DB (which may be nil in tests).
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

type adminService struct {
	dkvv1.UnimplementedAdminServiceServer
	admin Admin
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
