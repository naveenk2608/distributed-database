package server

import (
	"context"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
)

// StatusSource reports this node's view of the cluster.
type StatusSource interface {
	Status() *dkvv1.ClusterStatusResponse
}

// StaticStatus is the StatusSource of a standalone node: the cluster is
// this node alone.
type StaticStatus struct {
	Self *dkvv1.NodeInfo
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

type adminService struct {
	dkvv1.UnimplementedAdminServiceServer
	status StatusSource
}

func (a *adminService) ClusterStatus(context.Context, *dkvv1.ClusterStatusRequest) (*dkvv1.ClusterStatusResponse, error) {
	return a.status.Status(), nil
}
