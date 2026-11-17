package obs

import (
	"sync"
	"time"
)

// Event kinds. Each event is recorded by the node where it happened; the
// console merges the nodes' logs.
const (
	// EventLeaderElected: a replica on this node became its group's leader.
	EventLeaderElected = "leader_elected"
	// EventLeaderSteppedDown: a replica on this node stopped leading.
	EventLeaderSteppedDown = "leader_stepped_down"
	// EventPeerDown and EventPeerUp: this node's failure detector (pings)
	// stopped, or started again, hearing from a peer.
	EventPeerDown = "peer_down"
	EventPeerUp   = "peer_up"
	// EventNodeDead and EventNodeAlive: the meta leader on this node
	// declared a node dead (its replicas are recreated elsewhere) or alive
	// again.
	EventNodeDead  = "node_dead"
	EventNodeAlive = "node_alive"
	// EventReplicaAdded, EventReplicaPromoted and EventReplicaRemoved: a
	// replica on this node applied a membership change (Node is the member
	// it concerns, Index the entry). Every member of the group applies it,
	// so the console keeps one per shard and index.
	EventReplicaAdded    = "replica_added"
	EventReplicaPromoted = "replica_promoted"
	EventReplicaRemoved  = "replica_removed"
	// EventSnapshotInstalled: a replica on this node caught up from a
	// leader's snapshot up to Index.
	EventSnapshotInstalled = "snapshot_installed"
	// EventLeaderTransfer: a leader on this node started handing its
	// group over to Node.
	EventLeaderTransfer = "leader_transfer"
)

// NoShard marks an event that concerns no Raft group.
const NoShard = -2

// Event is something an operator may want to see happen.
type Event struct {
	Seq    uint64 // per node, from 1, gapless
	Time   time.Time
	Kind   string
	Shard  int    // the group (-1: the meta group), or NoShard
	Node   string // the member the event concerns, if any
	Term   uint64
	Index  uint64
	Detail string
}

// EventLog keeps a node's most recent events. It is safe for concurrent
// use.
type EventLog struct {
	mu   sync.Mutex
	now  func() time.Time
	ring []Event
	next uint64 // Seq of the next event
}

// NewEventLog returns a log that keeps the last capacity events, stamping
// them with now (time.Now outside tests).
func NewEventLog(capacity int, now func() time.Time) *EventLog {
	return &EventLog{now: now, ring: make([]Event, 0, max(capacity, 1)), next: 1}
}

// Record assigns e its sequence number and time, keeps it, and returns it.
func (l *EventLog) Record(e Event) Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	e.Seq, e.Time = l.next, l.now()
	l.next++
	if len(l.ring) < cap(l.ring) {
		l.ring = append(l.ring, e)
	} else {
		l.ring[int((e.Seq-1)%uint64(cap(l.ring)))] = e
	}
	return e
}

// After returns, oldest first, up to limit kept events with a sequence
// number above after, and the sequence number of the oldest event still
// kept (0 if none). A reader that saw up to after and gets oldest >
// after+1 has missed events.
func (l *EventLog) After(after uint64, limit int) (events []Event, oldest uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.ring) == 0 {
		return nil, 0
	}
	oldest = l.next - uint64(len(l.ring))
	for seq := max(after+1, oldest); seq < l.next && len(events) < limit; seq++ {
		events = append(events, l.ring[int((seq-1)%uint64(cap(l.ring)))])
	}
	return events, oldest
}
