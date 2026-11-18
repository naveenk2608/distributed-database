package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/cluster"
	"github.com/naveenk2608/distributed-database/internal/meta"
	"github.com/naveenk2608/distributed-database/internal/obs"
	"github.com/naveenk2608/distributed-database/internal/raft"
	"github.com/naveenk2608/distributed-database/internal/replica"
	"github.com/naveenk2608/distributed-database/internal/server"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

// internalAPI serves the node-to-node API.
type internalAPI struct {
	dkvv1.UnimplementedInternalServiceServer
	n    *Node
	ping *cluster.PingService
}

func (a *internalAPI) Ping(ctx context.Context, req *dkvv1.PingRequest) (*dkvv1.PingResponse, error) {
	if from := req.GetFrom(); from != nil {
		a.n.dir.Learn(from.GetNodeId(), from.GetInternalAddr())
	}
	return a.ping.Ping(ctx, req)
}

func (a *internalAPI) Forward(ctx context.Context, req *dkvv1.ForwardRequest) (*dkvv1.ForwardResponse, error) {
	return a.n.rt.Forward(ctx, req)
}

func (a *internalAPI) validShard(shard int) bool {
	return shard == meta.Shard || shard >= 0 && shard < a.n.cfg.Cluster.Shards
}

// replicaFor returns the local replica of a group a message addresses,
// creating it, uninitialized, when a group's leader contacts this node for
// the first time: that is how a node gets a replica the rebalancer adds.
func (a *internalAPI) replicaFor(pm *dkvv1.RaftMessage) *replica.Replica {
	shard := int(int32(pm.GetShard()))
	if rp := a.n.Replica(shard); rp != nil {
		return rp
	}
	switch raft.MsgType(pm.GetType()) {
	case raft.MsgApp, raft.MsgHeartbeat, raft.MsgSnap:
	default:
		return nil
	}
	if !a.validShard(shard) || len(a.n.membersRecord().Members) == 0 {
		return nil
	}
	rp, err := a.n.openReplica(shard, nil)
	if err != nil {
		a.n.log.Warn("cannot create a replica for a group's leader", "shard", shard, "error", err)
		return nil
	}
	a.n.log.Info("replica created for a group's leader", "shard", shard, "leader", pm.GetFrom())
	return rp
}

// Raft hands each message in the batch to its group's replica. Messages
// for another node or of an unknown type are dropped; Raft treats them as
// lost.
func (a *internalAPI) Raft(_ context.Context, req *dkvv1.RaftRequest) (*dkvv1.RaftResponse, error) {
	for _, pm := range req.GetMessages() {
		if pm.GetTo() != a.n.id || !validMessage(pm) || a.n.net.dropped(int(int32(pm.GetShard()))) {
			continue
		}
		if rp := a.replicaFor(pm); rp != nil {
			rp.Step(fromProto(pm))
		}
	}
	return &dkvv1.RaftResponse{}, nil
}

// InstallSnapshot receives a snapshot stream and stages it with the
// group's replica, which installs it if Raft accepts it.
func (a *internalAPI) InstallSnapshot(stream dkvv1.InternalService_InstallSnapshotServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hdr := first.GetHeader()
	if hdr == nil || hdr.GetSnapshot() == nil || raft.MsgType(hdr.GetType()) != raft.MsgSnap {
		return status.Error(codes.InvalidArgument, "snapshot stream must start with a snapshot header")
	}
	if hdr.GetTo() != a.n.id {
		return status.Errorf(codes.FailedPrecondition, "snapshot for node %s reached node %s", hdr.GetTo(), a.n.id)
	}
	rp := a.replicaFor(hdr)
	if rp == nil {
		return status.Errorf(codes.Unavailable, "no replica for shard %d", int32(hdr.GetShard()))
	}
	err = rp.ReceiveSnapshot(stream.Context(), fromProto(hdr), func(dir string) error {
		return receiveSnapshotFiles(stream, vfs.Default, dir)
	})
	if err != nil {
		return status.Errorf(codes.Unavailable, "stage snapshot: %v", err)
	}
	return stream.SendAndClose(&dkvv1.InstallSnapshotResponse{})
}

// NodeStatus reports every replica this node holds.
func (a *internalAPI) NodeStatus(context.Context, *dkvv1.NodeStatusRequest) (*dkvv1.NodeStatusResponse, error) {
	return a.n.nodeStatus(), nil
}

func (n *Node) nodeStatus() *dkvv1.NodeStatusResponse {
	resp := &dkvv1.NodeStatusResponse{NodeId: n.id}
	for shard, rp := range n.allReplicas() {
		st := rp.Status()
		resp.Replicas = append(resp.Replicas, &dkvv1.ReplicaStatus{
			Shard: int32(shard), Leader: st.Leader, Term: st.Term, Role: st.Role.String(),
			Commit: st.Commit, Applied: st.Applied, Voters: st.Conf.Voters, Learners: st.Conf.Learners,
			Progress: st.Progress, FirstIndex: st.FirstIndex, LastIndex: st.LastIndex,
			SnapshotsSent: st.SnapshotsSent, SnapshotsInstalled: st.SnapshotsInstalled,
		})
	}
	slices.SortFunc(resp.Replicas, func(x, y *dkvv1.ReplicaStatus) int { return int(x.GetShard() - y.GetShard()) })
	return resp
}

func notLeaderOf(shard int, rp *replica.Replica) *dkvv1.NotOwner {
	hint := ""
	if rp != nil {
		hint = rp.Leader()
	}
	return &dkvv1.NotOwner{Shard: uint32(int32(shard)), OwnerHint: hint}
}

// ChangeReplicas makes a membership change in a group this node leads.
func (a *internalAPI) ChangeReplicas(ctx context.Context, req *dkvv1.ChangeReplicasRequest) (*dkvv1.ChangeReplicasResponse, error) {
	shard := int(req.GetShard())
	rp := a.n.Replica(shard)
	if rp == nil || rp.Status().Role != raft.StateLeader {
		return &dkvv1.ChangeReplicasResponse{NotLeader: notLeaderOf(shard, rp)}, nil
	}
	var t raft.ConfChangeType
	switch req.GetChange() {
	case dkvv1.ReplicaChange_REPLICA_CHANGE_ADD_LEARNER:
		t = raft.ConfAddLearner
	case dkvv1.ReplicaChange_REPLICA_CHANGE_PROMOTE:
		t = raft.ConfPromote
	case dkvv1.ReplicaChange_REPLICA_CHANGE_REMOVE:
		t = raft.ConfRemove
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown change %v", req.GetChange())
	}
	err := rp.ChangeMembership(ctx, t, req.GetNodeId())
	var nl *replica.NotLeaderError
	switch {
	case errors.As(err, &nl):
		return &dkvv1.ChangeReplicasResponse{NotLeader: notLeaderOf(shard, rp)}, nil
	case errors.Is(err, raft.ErrConfChangePending):
		return nil, status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, raft.ErrInvalidConfChange):
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	case err != nil:
		return nil, toStatus(err)
	}
	return &dkvv1.ChangeReplicasResponse{}, nil
}

// TransferLeader hands leadership of a group this node leads to another
// voter and waits for it to take over.
func (a *internalAPI) TransferLeader(ctx context.Context, req *dkvv1.TransferLeaderRequest) (*dkvv1.TransferLeaderResponse, error) {
	shard := int(req.GetShard())
	rp := a.n.Replica(shard)
	if rp == nil || rp.Status().Role != raft.StateLeader {
		return &dkvv1.TransferLeaderResponse{NotLeader: notLeaderOf(shard, rp)}, nil
	}
	err := rp.TransferLeader(ctx, req.GetTo())
	var nl *replica.NotLeaderError
	if errors.As(err, &nl) {
		return &dkvv1.TransferLeaderResponse{NotLeader: notLeaderOf(shard, rp)}, nil
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &dkvv1.TransferLeaderResponse{}, nil
}

// Join registers a new node with the meta group.
func (a *internalAPI) Join(ctx context.Context, req *dkvv1.JoinRequest) (*dkvv1.JoinResponse, error) {
	nd := req.GetNode()
	if nd.GetNodeId() == "" || nd.GetInternalAddr() == "" {
		return nil, status.Error(codes.InvalidArgument, "join needs the node's ID and internal address")
	}
	if nd.GetClusterName() != a.n.cfg.Cluster.Name {
		return nil, status.Errorf(codes.FailedPrecondition, "node %s belongs to cluster %q, not %q", nd.GetNodeId(), nd.GetClusterName(), a.n.cfg.Cluster.Name)
	}
	members := a.n.membersRecord().Members
	if len(members) == 0 {
		return nil, status.Error(codes.Unavailable, "this node has not joined a cluster yet")
	}
	st, err := a.n.readMeta(ctx)
	if err != nil {
		return nil, err
	}
	// The first meta leader writes the settings, the bootstrap nodes'
	// records and every descriptor in one transaction. Before it has, the
	// answer would hold no node addresses and no meta voters, and a joiner
	// that kept it could never reach the meta group: it must ask again.
	if st.Settings == nil {
		return nil, status.Error(codes.Unavailable, "the meta group has not recorded the cluster yet; retry")
	}
	if rec := st.Nodes[nd.GetNodeId()]; rec == nil {
		rec = &dkvv1.NodeRecord{NodeId: nd.GetNodeId(), Addr: nd.GetInternalAddr(), State: dkvv1.NodeState_NODE_STATE_ALIVE,
			JoinedUnixNano: time.Now().UnixNano(), StateChangedUnixNano: time.Now().UnixNano()}
		ok, err := a.n.metaTxn(ctx, meta.PutNodeTxn(rec, 0))
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, status.Error(codes.Aborted, "the node record changed concurrently; retry")
		}
		st.Nodes[rec.GetNodeId()] = rec
		a.n.log.Info("node joined", "joined", nd.GetNodeId(), "addr", nd.GetInternalAddr())
	} else if rec.GetAddr() != nd.GetInternalAddr() {
		return nil, status.Errorf(codes.AlreadyExists, "node %s is registered at %s", nd.GetNodeId(), rec.GetAddr())
	}
	a.n.dir.Learn(nd.GetNodeId(), nd.GetInternalAddr())
	resp := &dkvv1.JoinResponse{BootstrapMembers: members}
	for _, id := range slices.Sorted(maps.Keys(st.Nodes)) {
		resp.Nodes = append(resp.Nodes, st.Nodes[id])
	}
	for _, s := range slices.Sorted(maps.Keys(st.Descs)) {
		resp.Descriptors = append(resp.Descriptors, st.Descs[s])
	}
	return resp, nil
}

// Heartbeat records that a node is alive, if this node's meta replica
// leads; otherwise it points at the meta leader it knows of.
func (a *internalAPI) Heartbeat(_ context.Context, req *dkvv1.HeartbeatRequest) (*dkvv1.HeartbeatResponse, error) {
	from := req.GetNode()
	a.n.dir.Learn(from.GetNodeId(), from.GetInternalAddr())
	if !a.n.heartbeat(from.GetNodeId()) {
		return &dkvv1.HeartbeatResponse{NotLeader: notLeaderOf(meta.Shard, a.n.Replica(meta.Shard))}, nil
	}
	return &dkvv1.HeartbeatResponse{}, nil
}

// heartbeat records a heartbeat if this node's meta replica leads.
func (n *Node) heartbeat(id string) bool {
	rp := n.Replica(meta.Shard)
	if rp == nil || rp.Status().Role != raft.StateLeader {
		return false
	}
	n.lvMu.Lock()
	defer n.lvMu.Unlock()
	if n.lv == nil {
		return true // tracking starts with the next rebalance round
	}
	n.lv.Heartbeat(id, time.Now())
	return true
}

func toStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Internal, err.Error())
}

// clusterAdmin answers the operator API for a cluster node.
type clusterAdmin struct{ n *Node }

func (a *clusterAdmin) Status() *dkvv1.ClusterStatusResponse {
	n := a.n
	resp := n.mship.Status()
	seen := map[string]bool{}
	for _, m := range resp.GetMembers() {
		if id := m.GetNode().GetNodeId(); id != "" {
			seen[id] = true
			m.NodeState = n.dir.State(id)
			m.Suspect = n.suspect(id)
			m.Drained = n.dir.Drained(id)
		}
	}
	// Nodes that joined later are not pinged; list them from the meta
	// group's records.
	for _, id := range n.dir.Nodes() {
		if seen[id] {
			continue
		}
		resp.Members = append(resp.Members, &dkvv1.MemberStatus{
			Addr: n.dir.Addr(id), Node: &dkvv1.NodeInfo{NodeId: id, InternalAddr: n.dir.Addr(id)},
			State: dkvv1.MemberState_MEMBER_STATE_UNKNOWN, NodeState: n.dir.State(id), Suspect: n.suspect(id),
			Drained: n.dir.Drained(id), Self: id == n.id,
		})
	}
	return resp
}

// suspect reports whether the meta leader on this node suspects id.
func (n *Node) suspect(id string) bool {
	n.lvMu.Lock()
	defer n.lvMu.Unlock()
	return n.lv != nil && n.lv.Suspect(id, time.Now())
}

func (a *clusterAdmin) ShardMap() *dkvv1.ShardMapResponse {
	n := a.n
	resp := &dkvv1.ShardMapResponse{ShardCount: uint32(n.cfg.Cluster.Shards), ReplicationFactor: uint32(n.cfg.Cluster.ReplicationFactor)}
	for s := range n.cfg.Cluster.Shards {
		voters, learners, _ := n.dir.Group(s)
		info := &dkvv1.ShardInfo{Shard: uint32(s), VoterIds: voters, LearnerIds: learners}
		for _, v := range voters {
			info.Replicas = append(info.Replicas, n.dir.Addr(v))
		}
		if rp := n.Replica(s); rp != nil {
			st := rp.Status()
			info.Local, info.LastSeq = true, st.Applied
			info.Leader, info.Term, info.Role, info.CommitIndex = st.Leader, st.Term, st.Role.String(), st.Commit
		}
		resp.Shards = append(resp.Shards, info)
	}
	return resp
}

// Decommission marks a node for draining; the rebalancer moves its
// replicas away and then marks it decommissioned.
func (a *clusterAdmin) Decommission(ctx context.Context, nodeID string) error {
	n := a.n
	for attempt := 0; attempt < 5; attempt++ {
		st, err := n.readMeta(ctx)
		if err != nil {
			return err
		}
		rec := st.Nodes[nodeID]
		if rec == nil {
			return status.Errorf(codes.NotFound, "no node %s", nodeID)
		}
		switch rec.GetState() {
		case dkvv1.NodeState_NODE_STATE_DECOMMISSIONING, dkvv1.NodeState_NODE_STATE_DECOMMISSIONED:
			return nil
		}
		next := proto.Clone(rec).(*dkvv1.NodeRecord)
		next.State, next.StateChangedUnixNano = dkvv1.NodeState_NODE_STATE_DECOMMISSIONING, time.Now().UnixNano()
		ok, err := n.metaTxn(ctx, meta.PutNodeTxn(next, st.NodeVersions[nodeID]))
		if err != nil {
			return err
		}
		if ok {
			n.log.Info("node marked for decommissioning", "target", nodeID)
			return nil
		}
	}
	return status.Error(codes.Aborted, "the node record kept changing; retry")
}

// readMeta reads the meta group's whole state, linearizably.
func (n *Node) readMeta(ctx context.Context) (meta.State, error) {
	kvs, err := n.rt.MetaScan(ctx, nil)
	if err != nil {
		return meta.State{}, err
	}
	out := make([]meta.KV, len(kvs))
	for i, kv := range kvs {
		out[i] = meta.KV{Key: kv.GetKey(), Value: kv.GetValue(), Version: kv.GetVersion()}
	}
	st, err := meta.Decode(out)
	if err != nil {
		return st, status.Error(codes.DataLoss, err.Error())
	}
	return st, nil
}

// metaTxn runs a transaction on the meta group.
func (n *Node) metaTxn(ctx context.Context, txn *dkvv1.Txn) (bool, error) {
	resp, err := n.rt.MetaTxn(ctx, &dkvv1.TxnRequest{Txn: txn})
	if err != nil {
		return false, fmt.Errorf("meta transaction: %w", err)
	}
	return resp.GetSucceeded(), nil
}

// Drain marks a node as drained (or not) in the meta group; the
// rebalancer then moves its leaderships away and gives it none.
func (a *clusterAdmin) Drain(ctx context.Context, nodeID string, drain bool) error {
	n := a.n
	for range 5 {
		st, err := n.readMeta(ctx)
		if err != nil {
			return err
		}
		rec := st.Nodes[nodeID]
		if rec == nil {
			return status.Errorf(codes.NotFound, "no node %s", nodeID)
		}
		if rec.GetDrained() == drain {
			return nil
		}
		next := proto.Clone(rec).(*dkvv1.NodeRecord)
		next.Drained = drain
		ok, err := n.metaTxn(ctx, meta.PutNodeTxn(next, st.NodeVersions[nodeID]))
		if err != nil {
			return err
		}
		if ok {
			n.log.Info("node drain changed", "target", nodeID, "drained", drain)
			return nil
		}
	}
	return status.Error(codes.Aborted, "the node record kept changing; retry")
}

// TransferLeader finds shard's leader among its voters and asks it to hand
// leadership to nodeID; it returns once the transfer has completed.
func (a *clusterAdmin) TransferLeader(ctx context.Context, shard int, nodeID string) error {
	n := a.n
	if shard != meta.Shard && (shard < 0 || shard >= n.cfg.Cluster.Shards) {
		return status.Errorf(codes.InvalidArgument, "no shard %d (shards are 0 to %d, and %d for the meta group)",
			shard, n.cfg.Cluster.Shards-1, meta.Shard)
	}
	voters, _, _ := n.dir.Group(shard)
	if !slices.Contains(voters, nodeID) {
		return status.Errorf(codes.FailedPrecondition, "node %s is not a voter of shard %d (voters %v)", nodeID, shard, voters)
	}
	tctx, cancel := context.WithTimeout(ctx, 4*time.Duration(n.cfg.Raft.ElectionTicks)*n.cfg.Raft.TickInterval)
	defer cancel()
	req := &dkvv1.TransferLeaderRequest{Shard: int32(shard), To: nodeID}
	try := slices.Clone(voters)
	for len(try) > 0 {
		id := try[0]
		try = try[1:]
		resp, err := n.callLeader(tctx, id, func(c dkvv1.InternalServiceClient) (proto.Message, error) {
			return c.TransferLeader(tctx, req)
		}, func(api *internalAPI) (proto.Message, error) { return api.TransferLeader(tctx, req) })
		if err != nil {
			if tctx.Err() != nil {
				return err
			}
			continue // that voter is unreachable; another may lead
		}
		nl := resp.(*dkvv1.TransferLeaderResponse).GetNotLeader()
		if nl == nil {
			return nil
		}
		if h := nl.GetOwnerHint(); h != "" && h != id {
			try = append([]string{h}, slices.DeleteFunc(try, func(s string) bool { return s == h })...)
		}
	}
	return status.Errorf(codes.Unavailable, "no leader of shard %d answered", shard)
}

// Stats reports every local replica's Raft state and engine, and the
// node's block cache.
func (a *clusterAdmin) Stats() *dkvv1.StatsResponse {
	n := a.n
	resp := &dkvv1.StatsResponse{NodeId: n.id, StartedUnixNano: n.started.UnixNano(), BlockCache: server.CacheStats(n.cache.Stats())}
	for _, o := range n.internal.Snapshot() {
		resp.Internal = append(resp.Internal, &dkvv1.OpStats{Op: o.Op, Ok: o.OK, LatencySumSeconds: o.Sum.Seconds(),
			LatencyCumulative: o.Cumulative})
	}
	byShard := n.allReplicas()
	for _, rs := range n.nodeStatus().GetReplicas() {
		if rp := byShard[int(rs.GetShard())]; rp != nil {
			resp.Replicas = append(resp.Replicas, &dkvv1.ReplicaStats{Raft: rs, Engine: server.EngineStats(rp.EngineMetrics())})
		}
	}
	return resp
}

// Events returns the node's recent events.
func (a *clusterAdmin) Events(after uint64, limit int) *dkvv1.EventsResponse {
	evs, oldest := a.n.events.After(after, limit)
	resp := &dkvv1.EventsResponse{NodeId: a.n.id, Oldest: oldest}
	for _, e := range evs {
		resp.Events = append(resp.Events, EventProto(e))
	}
	return resp
}

// EventProto converts an event for the admin API.
func EventProto(e obs.Event) *dkvv1.Event {
	return &dkvv1.Event{Seq: e.Seq, UnixNano: e.Time.UnixNano(), Kind: e.Kind, Shard: int32(max(e.Shard, meta.Shard)),
		HasShard: e.Shard != obs.NoShard, Node: e.Node, Term: e.Term, Index: e.Index, Detail: e.Detail}
}
