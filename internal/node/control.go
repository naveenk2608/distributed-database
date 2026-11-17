package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/meta"
	"github.com/naveenk2608/distributed-database/internal/obs"
	"github.com/naveenk2608/distributed-database/internal/raft"
)

// Background work timings, in rebalance rounds (rebalance.interval).
const (
	// gcGraceRounds: a replica younger than this is never deleted, so a
	// learner the rebalancer just added, whose descriptor may lag a round,
	// is not mistaken for a leftover.
	gcGraceRounds = 25
	// orphanRounds: an uninitialized replica no leader has contacted for
	// this long is deleted.
	orphanRounds = 120
	// actionWorkers bounds the rebalancer's concurrent steps.
	actionWorkers = 4
)

var errNotYet = errors.New("node: not every bootstrap peer has answered yet")

// control bootstraps or joins the cluster, then runs the periodic work
// until ctx ends.
func (n *Node) control(ctx context.Context) {
	for {
		err := n.bootstrapOrJoin(ctx)
		if err == nil {
			break
		}
		if !errors.Is(err, errNotYet) {
			n.log.Warn("cannot bootstrap or join yet", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	var wg sync.WaitGroup
	every := func(d time.Duration, f func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(d)
			defer t.Stop()
			for {
				f(ctx)
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}
	every(n.cfg.Rebalance.Interval, n.refresh)
	every(n.cfg.Liveness.HeartbeatInterval, n.sendHeartbeat)
	every(n.cfg.Rebalance.Interval, n.metaRound)
	every(5*n.cfg.Rebalance.Interval, n.gcRound)
	wg.Wait()
}

// bootstrapOrJoin makes this node part of a cluster: it finishes an
// interrupted bootstrap, joins through cluster.join, or bootstraps with
// the bootstrap peers once every one of them has answered. A node whose
// peers were bootstrapped without it (its disk was wiped, so it has a new
// ID) joins through them instead.
func (n *Node) bootstrapOrJoin(ctx context.Context) error {
	rec := n.membersRecord()
	if len(rec.Members) > 0 {
		if !rec.Complete {
			if err := n.createBootstrapReplicas(rec.Members); err != nil {
				return err
			}
			rec.Complete = true
			return n.saveMembers(rec)
		}
		return nil
	}
	if len(n.cfg.Cluster.Join) > 0 {
		return n.join(ctx, n.cfg.Cluster.Join)
	}
	var ids, seeds []string
	var known []string
	for _, p := range n.mship.Peers() {
		if p.Node == nil || p.Node.GetNodeId() == "" {
			return errNotYet
		}
		n.dir.Learn(p.Node.GetNodeId(), p.Addr)
		ids = append(ids, p.Node.GetNodeId())
		if !p.Self {
			seeds = append(seeds, p.Addr)
		}
		if bm := p.Node.GetBootstrapMembers(); len(bm) > 0 {
			known = bm
		}
	}
	if len(known) > 0 && !slices.Contains(known, n.id) {
		n.log.Info("the cluster was bootstrapped without this node; joining it", "members", known)
		return n.join(ctx, seeds)
	}
	members := known
	if len(members) == 0 {
		members = slices.Clone(ids)
		slices.Sort(members)
	}
	n.log.Info("bootstrapping the cluster", "members", members)
	if err := n.saveMembers(membersRecord{Members: members}); err != nil {
		return err
	}
	if err := n.createBootstrapReplicas(members); err != nil {
		return err
	}
	if err := n.saveMembers(membersRecord{Members: members, Complete: true}); err != nil {
		return err
	}
	n.mship.SetBootstrapMembers(members)
	return nil
}

// createBootstrapReplicas creates this node's replicas of the initial
// layout, which every bootstrap node computes alike from the members.
func (n *Node) createBootstrapReplicas(members []string) error {
	c := n.cfg.Cluster
	layout := meta.InitialLayout(members, c.Shards, c.ReplicationFactor, c.MetaReplicas)
	for _, shard := range slices.Sorted(maps.Keys(layout)) {
		voters := layout[shard]
		if _, _, ok := n.dir.Group(shard); !ok {
			n.dir.SetGroup(shard, voters, nil)
		}
		if slices.Contains(voters, n.id) {
			if _, err := n.openReplica(shard, voters); err != nil {
				return err
			}
		}
	}
	if err := n.dir.save(); err != nil {
		n.log.Warn("cannot save the directory cache", "error", err)
	}
	return nil
}

// join registers this node through any of seeds and records the cluster's
// bootstrap members and nodes.
func (n *Node) join(ctx context.Context, seeds []string) error {
	var errs []error
	for _, addr := range seeds {
		conn, err := n.pool.Conn(addr)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := dkvv1.NewInternalServiceClient(conn).Join(cctx, &dkvv1.JoinRequest{Node: n.mship.Self()})
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", addr, err))
			continue
		}
		for _, rec := range resp.GetNodes() {
			n.dir.Learn(rec.GetNodeId(), rec.GetAddr())
		}
		for _, d := range resp.GetDescriptors() {
			n.dir.SetGroup(int(d.GetShard()), d.GetVoters(), d.GetLearners())
		}
		rec := membersRecord{Members: resp.GetBootstrapMembers(), Complete: true, Joined: true}
		if err := n.saveMembers(rec); err != nil {
			return err
		}
		n.mship.SetBootstrapMembers(rec.Members)
		if err := n.dir.save(); err != nil {
			n.log.Warn("cannot save the directory cache", "error", err)
		}
		n.log.Info("joined the cluster", "through", addr, "nodes", len(resp.GetNodes()))
		return nil
	}
	return fmt.Errorf("join: no seed accepted: %w", errors.Join(errs...))
}

// refresh updates the directory from the bootstrap peers' pings and the
// meta group's records.
func (n *Node) refresh(ctx context.Context) {
	for _, p := range n.mship.Peers() {
		if id := p.Node.GetNodeId(); id != "" {
			n.dir.Learn(id, p.Addr)
		}
	}
	cctx, cancel := context.WithTimeout(ctx, n.cfg.Rebalance.Interval+time.Second)
	defer cancel()
	st, err := n.readMeta(cctx)
	if err != nil {
		return // routing keeps using the cached directory
	}
	if n.dir.Update(st) {
		if err := n.dir.save(); err != nil {
			n.log.Warn("cannot save the directory cache", "error", err)
		}
	}
}

// sendHeartbeat tells the meta leader this node is alive.
func (n *Node) sendHeartbeat(ctx context.Context) {
	candidates := []string{n.rt.LeaderOf(meta.Shard)}
	candidates = append(candidates, n.dir.Voters(meta.Shard)...)
	tried := map[string]bool{}
	for _, id := range candidates {
		if id == "" || tried[id] {
			continue
		}
		tried[id] = true
		if id == n.id {
			if n.heartbeat(n.id) {
				n.rt.NoteLeader(meta.Shard, n.id)
				return
			}
			continue
		}
		addr := n.dir.Addr(id)
		if addr == "" {
			continue
		}
		conn, err := n.pool.Conn(addr)
		if err != nil {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, n.cfg.Liveness.HeartbeatInterval)
		resp, err := dkvv1.NewInternalServiceClient(conn).Heartbeat(cctx, &dkvv1.HeartbeatRequest{Node: n.mship.Self()})
		cancel()
		if err != nil {
			continue
		}
		if nl := resp.GetNotLeader(); nl != nil {
			if hint := nl.GetOwnerHint(); hint != "" && !tried[hint] {
				candidates = append(candidates, hint)
			}
			continue
		}
		n.rt.NoteLeader(meta.Shard, id)
		return
	}
}

// metaRound is the meta leader's periodic work: bootstrap the meta state,
// liveness decisions, and the rebalancer's next steps.
func (n *Node) metaRound(ctx context.Context) {
	rp := n.Replica(meta.Shard)
	if rp == nil || rp.Status().Role != raft.StateLeader {
		n.lvMu.Lock()
		n.lv = nil
		n.lvMu.Unlock()
		return
	}
	now := time.Now()
	n.lvMu.Lock()
	if n.lv == nil {
		n.lv = meta.NewLiveness(n.cfg.Liveness.SuspectAfter, n.cfg.Liveness.DeadAfter, now)
		n.log.Info("leading the meta group; liveness tracking starts", "dead_after", n.cfg.Liveness.DeadAfter)
	}
	lv := n.lv
	lv.Heartbeat(n.id, now)
	n.lvMu.Unlock()

	rctx, cancel := context.WithTimeout(ctx, 4*n.cfg.Rebalance.Interval+2*time.Second)
	defer cancel()
	kvs, err := rp.Scan(rctx, nil, false)
	if err != nil {
		return
	}
	mkvs := make([]meta.KV, len(kvs))
	for i, kv := range kvs {
		mkvs[i] = meta.KV{Key: kv.Key, Value: kv.Value, Version: kv.Version}
	}
	st, err := meta.Decode(mkvs)
	if err != nil {
		n.log.Error("cannot decode the meta group's state", "error", err)
		return
	}
	if st.Settings == nil {
		n.bootstrapMeta(rctx, rp)
		return
	}
	if n.dir.Update(st) {
		if err := n.dir.save(); err != nil {
			n.log.Warn("cannot save the directory cache", "error", err)
		}
	}
	n.decideLiveness(rctx, st, lv, now)
	groups := n.collectGroups(rctx, st)
	n.execute(rctx, st, groups, meta.Plan(st, groups))
}

// bootstrapMeta writes a new cluster's settings, node records and initial
// descriptors.
func (n *Node) bootstrapMeta(ctx context.Context, rp interface {
	Txn(context.Context, *dkvv1.Txn, string, uint64) (bool, uint64, error)
}) {
	members := n.membersRecord().Members
	c := n.cfg.Cluster
	var recs []*dkvv1.NodeRecord
	now := time.Now().UnixNano()
	for _, id := range members {
		addr := n.dir.Addr(id)
		if addr == "" {
			return // not every bootstrap member's address is known yet
		}
		recs = append(recs, &dkvv1.NodeRecord{NodeId: id, Addr: addr, State: dkvv1.NodeState_NODE_STATE_ALIVE,
			JoinedUnixNano: now, StateChangedUnixNano: now})
	}
	settings := &dkvv1.ClusterSettings{ClusterName: c.Name, Shards: uint32(c.Shards),
		ReplicationFactor: uint32(c.ReplicationFactor), MetaReplicas: uint32(c.MetaReplicas)}
	txn := meta.BootstrapTxn(settings, recs, meta.InitialLayout(members, c.Shards, c.ReplicationFactor, c.MetaReplicas))
	if ok, _, err := rp.Txn(ctx, txn, "", 0); err == nil && ok {
		n.log.Info("meta group bootstrapped", "nodes", len(recs), "shards", c.Shards)
	}
}

// decideLiveness marks nodes dead after liveness.dead_after of silence and
// alive again once they heartbeat. Only these decisions are written.
func (n *Node) decideLiveness(ctx context.Context, st meta.State, lv *meta.Liveness, now time.Time) {
	n.lvMu.Lock()
	type change struct {
		id    string
		state dkvv1.NodeState
	}
	var changes []change
	for _, id := range slices.Sorted(maps.Keys(st.Nodes)) {
		switch st.Nodes[id].GetState() {
		case dkvv1.NodeState_NODE_STATE_ALIVE, dkvv1.NodeState_NODE_STATE_DECOMMISSIONING:
			if id != n.id && lv.Dead(id, now) && st.Nodes[id].GetState() == dkvv1.NodeState_NODE_STATE_ALIVE {
				changes = append(changes, change{id, dkvv1.NodeState_NODE_STATE_DEAD})
			}
		case dkvv1.NodeState_NODE_STATE_DEAD:
			if lv.Heard(id) && !lv.Suspect(id, now) {
				changes = append(changes, change{id, dkvv1.NodeState_NODE_STATE_ALIVE})
			}
		}
	}
	n.lvMu.Unlock()
	for _, c := range changes {
		next := proto.Clone(st.Nodes[c.id]).(*dkvv1.NodeRecord)
		next.State, next.StateChangedUnixNano = c.state, now.UnixNano()
		ok, err := n.metaTxn(ctx, meta.PutNodeTxn(next, st.NodeVersions[c.id]))
		if err == nil && ok {
			n.log.Info("node liveness decided", "target", c.id, "state", c.state.String())
			st.Nodes[c.id] = next
			kind := obs.EventNodeAlive
			if c.state == dkvv1.NodeState_NODE_STATE_DEAD {
				kind = obs.EventNodeDead
			}
			n.recordEvent(obs.Event{Kind: kind, Shard: obs.NoShard, Node: c.id})
		}
	}
}

// collectGroups asks every live node for its replicas and keeps, for each
// group, the report of its leader with the highest term.
func (n *Node) collectGroups(ctx context.Context, st meta.State) map[int]meta.Group {
	var mu sync.Mutex
	best := map[int]*dkvv1.ReplicaStatus{}
	note := func(resp *dkvv1.NodeStatusResponse) {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range resp.GetReplicas() {
			if r.GetRole() != raft.StateLeader.String() || r.GetLeader() != resp.GetNodeId() {
				continue
			}
			if cur := best[int(r.GetShard())]; cur == nil || r.GetTerm() > cur.GetTerm() {
				best[int(r.GetShard())] = r
			}
		}
	}
	var wg sync.WaitGroup
	for id, rec := range st.Nodes {
		switch rec.GetState() {
		case dkvv1.NodeState_NODE_STATE_ALIVE, dkvv1.NodeState_NODE_STATE_DECOMMISSIONING:
		default:
			continue
		}
		if id == n.id {
			note(n.nodeStatus())
			continue
		}
		addr := n.dir.Addr(id)
		if addr == "" {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := n.pool.Conn(addr)
			if err != nil {
				return
			}
			cctx, cancel := context.WithTimeout(ctx, n.cfg.Rebalance.Interval)
			defer cancel()
			if resp, err := dkvv1.NewInternalServiceClient(conn).NodeStatus(cctx, &dkvv1.NodeStatusRequest{}); err == nil {
				note(resp)
			}
		}()
	}
	wg.Wait()
	out := map[int]meta.Group{}
	for shard, r := range best {
		out[shard] = meta.Group{Shard: shard, Leader: r.GetLeader(), Voters: r.GetVoters(), Learners: r.GetLearners(),
			Progress: r.GetProgress(), Commit: r.GetCommit()}
	}
	return out
}

// execute carries out the rebalancer's steps, a few at a time.
func (n *Node) execute(ctx context.Context, st meta.State, groups map[int]meta.Group, plan []meta.Action) {
	work := make(chan meta.Action)
	var wg sync.WaitGroup
	for range min(actionWorkers, len(plan)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for a := range work {
				if err := n.do(ctx, st, groups, a); err != nil {
					n.log.Debug("rebalancer step failed", "step", a.String(), "error", err)
				} else if a.Kind != meta.UpdateDescriptor {
					n.log.Info("rebalancer step", "step", a.String())
				}
			}
		}()
	}
	for _, a := range plan {
		select {
		case work <- a:
		case <-ctx.Done():
		}
	}
	close(work)
	wg.Wait()
}

func (n *Node) do(ctx context.Context, st meta.State, groups map[int]meta.Group, a meta.Action) error {
	g := groups[a.Shard]
	switch a.Kind {
	case meta.AddLearner, meta.Promote, meta.Remove:
		change := map[meta.ActionKind]dkvv1.ReplicaChange{
			meta.AddLearner: dkvv1.ReplicaChange_REPLICA_CHANGE_ADD_LEARNER,
			meta.Promote:    dkvv1.ReplicaChange_REPLICA_CHANGE_PROMOTE,
			meta.Remove:     dkvv1.ReplicaChange_REPLICA_CHANGE_REMOVE,
		}[a.Kind]
		req := &dkvv1.ChangeReplicasRequest{Shard: int32(a.Shard), Change: change, NodeId: a.Node}
		resp, err := n.callLeader(ctx, g.Leader, func(c dkvv1.InternalServiceClient) (proto.Message, error) {
			return c.ChangeReplicas(ctx, req)
		}, func(api *internalAPI) (proto.Message, error) { return api.ChangeReplicas(ctx, req) })
		if err != nil {
			return err
		}
		if resp.(*dkvv1.ChangeReplicasResponse).GetNotLeader() != nil {
			return errors.New("the group's leader changed")
		}
	case meta.TransferLeader:
		tctx, cancel := context.WithTimeout(ctx, 4*time.Duration(n.cfg.Raft.ElectionTicks)*n.cfg.Raft.TickInterval)
		defer cancel()
		req := &dkvv1.TransferLeaderRequest{Shard: int32(a.Shard), To: a.Node}
		resp, err := n.callLeader(tctx, g.Leader, func(c dkvv1.InternalServiceClient) (proto.Message, error) {
			return c.TransferLeader(tctx, req)
		}, func(api *internalAPI) (proto.Message, error) { return api.TransferLeader(tctx, req) })
		if err != nil {
			return err
		}
		if resp.(*dkvv1.TransferLeaderResponse).GetNotLeader() != nil {
			return errors.New("the group's leader changed")
		}
	case meta.UpdateDescriptor:
		voters, learners := slices.Clone(g.Voters), slices.Clone(g.Learners)
		slices.Sort(voters)
		slices.Sort(learners)
		d := &dkvv1.ShardDescriptor{Shard: int32(a.Shard), Voters: voters, Learners: learners}
		if _, err := n.metaTxn(ctx, meta.PutDescTxn(d, st.DescVersions[a.Shard])); err != nil {
			return err
		}
		n.dir.SetGroup(a.Shard, voters, learners)
	case meta.MarkDecommissioned:
		next := proto.Clone(st.Nodes[a.Node]).(*dkvv1.NodeRecord)
		next.State, next.StateChangedUnixNano = dkvv1.NodeState_NODE_STATE_DECOMMISSIONED, time.Now().UnixNano()
		if _, err := n.metaTxn(ctx, meta.PutNodeTxn(next, st.NodeVersions[a.Node])); err != nil {
			return err
		}
	}
	return nil
}

// callLeader calls a group leader's InternalService, directly when this
// node is the leader.
func (n *Node) callLeader(ctx context.Context, id string, remote func(dkvv1.InternalServiceClient) (proto.Message, error),
	local func(*internalAPI) (proto.Message, error)) (proto.Message, error) {
	if id == n.id {
		return local(&internalAPI{n: n})
	}
	addr := n.dir.Addr(id)
	if addr == "" {
		return nil, fmt.Errorf("no address known for node %s", id)
	}
	conn, err := n.pool.Conn(addr)
	if err != nil {
		return nil, err
	}
	return remote(dkvv1.NewInternalServiceClient(conn))
}

// gcRound deletes replicas of groups this node no longer belongs to: ones
// whose group leader's membership leaves it out, and uninitialized ones no
// leader has contacted for a long time.
func (n *Node) gcRound(ctx context.Context) {
	n.mu.Lock()
	created := maps.Clone(n.created)
	n.mu.Unlock()
	round := n.cfg.Rebalance.Interval
	for shard, rp := range n.allReplicas() {
		st := rp.Status()
		age := time.Since(created[shard])
		if st.Role == raft.StateLeader || age < gcGraceRounds*round || slices.Contains(n.dir.Members(shard), n.id) {
			continue
		}
		// Ask the group's leader whether this node is still a member. A
		// removed replica often never hears of its removal and knows no
		// leader, so look for one among the descriptor's voters too.
		candidates := append([]string{st.Leader, n.rt.LeaderOf(shard)}, n.dir.Voters(shard)...)
		verdict := 0 // 1: still a member, -1: removed
		asked := map[string]bool{n.id: true, "": true}
		for _, id := range candidates {
			if asked[id] || verdict != 0 {
				continue
			}
			asked[id] = true
			verdict = n.memberVerdict(ctx, id, shard)
		}
		switch {
		case verdict < 0:
			n.removeReplica(shard, "the group's leader no longer lists this node")
		case verdict == 0 && len(st.Conf.Voters) == 0 && age > orphanRounds*round:
			n.removeReplica(shard, "no leader ever contacted it")
		}
	}
}

// memberVerdict asks node id whether it leads the group and, if so,
// whether its membership lists this node: 1 yes, -1 no, 0 unknown.
func (n *Node) memberVerdict(ctx context.Context, id string, shard int) int {
	addr := n.dir.Addr(id)
	if addr == "" {
		return 0
	}
	conn, err := n.pool.Conn(addr)
	if err != nil {
		return 0
	}
	cctx, cancel := context.WithTimeout(ctx, n.cfg.Rebalance.Interval)
	defer cancel()
	resp, err := dkvv1.NewInternalServiceClient(conn).NodeStatus(cctx, &dkvv1.NodeStatusRequest{})
	if err != nil {
		return 0
	}
	for _, r := range resp.GetReplicas() {
		if int(r.GetShard()) != shard || r.GetRole() != raft.StateLeader.String() {
			continue
		}
		if slices.Contains(r.GetVoters(), n.id) || slices.Contains(r.GetLearners(), n.id) {
			return 1
		}
		return -1
	}
	return 0
}

func (n *Node) removeReplica(shard int, why string) {
	if err := n.destroyReplica(shard); err != nil {
		n.log.Warn("cannot delete a replica", "shard", shard, "error", err)
		return
	}
	n.log.Info("replica deleted", "shard", shard, "reason", why)
}
