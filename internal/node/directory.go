package node

import (
	"encoding/json"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/meta"
)

// directoryFile caches the directory in the data directory, so a restarted
// node can reach the meta group even if it moved off the bootstrap nodes.
const directoryFile = "DIRECTORY"

// directory is this node's view of the cluster: every node's address and
// every group's members, as learned from the meta group (refreshed
// periodically), from pings of the bootstrap peers and from joins. Routing
// and the Raft transport resolve member IDs through it. It is safe for
// concurrent use.
type directory struct {
	path string

	mu     sync.RWMutex
	addrs  map[string]string // node ID -> internal address
	states map[string]dkvv1.NodeState
	groups map[int]group // shard (meta.Shard for the meta group) -> members
	// drained is refreshed from the meta group with states, for status
	// display only, and not cached on disk.
	drained map[string]bool
}

type group struct {
	Voters   []string `json:"voters"`
	Learners []string `json:"learners,omitempty"`
}

type directoryJSON struct {
	Addrs  map[string]string          `json:"addrs"`
	States map[string]dkvv1.NodeState `json:"states,omitempty"`
	Groups map[int]group              `json:"groups"`
}

func newDirectory(dataDir string) *directory {
	return &directory{path: filepath.Join(dataDir, directoryFile),
		addrs: map[string]string{}, states: map[string]dkvv1.NodeState{}, groups: map[int]group{}, drained: map[string]bool{}}
}

// load reads the cached directory; a missing file is not an error.
func (d *directory) load() error {
	b, err := os.ReadFile(d.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var j directoryJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	maps.Copy(d.addrs, j.Addrs)
	maps.Copy(d.states, j.States)
	maps.Copy(d.groups, j.Groups)
	return nil
}

// save writes the directory durably. It is a cache, so a failure is only
// logged by the caller.
func (d *directory) save() error {
	d.mu.RLock()
	b, err := json.Marshal(directoryJSON{Addrs: d.addrs, States: d.states, Groups: d.groups})
	d.mu.RUnlock()
	if err != nil {
		return err
	}
	return writeFileDurably(d.path, append(b, '\n'))
}

// Addr returns a node's internal address, or "".
func (d *directory) Addr(id string) string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.addrs[id]
}

// Learn records a node's address; it reports whether anything changed.
func (d *directory) Learn(id, addr string) bool {
	if id == "" || addr == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.addrs[id] == addr {
		return false
	}
	d.addrs[id] = addr
	return true
}

// SetGroup records a group's members.
func (d *directory) SetGroup(shard int, voters, learners []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.groups[shard] = group{Voters: slices.Clone(voters), Learners: slices.Clone(learners)}
}

// Update replaces the directory's content with the meta group's state and
// reports whether anything changed.
func (d *directory) Update(s meta.State) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	changed := false
	for id, n := range s.Nodes {
		if d.addrs[id] != n.GetAddr() && n.GetAddr() != "" {
			d.addrs[id], changed = n.GetAddr(), true
		}
		if d.states[id] != n.GetState() {
			d.states[id], changed = n.GetState(), true
		}
		d.drained[id] = n.GetDrained()
	}
	for shard, desc := range s.Descs {
		g := group{Voters: slices.Clone(desc.GetVoters()), Learners: slices.Clone(desc.GetLearners())}
		old, ok := d.groups[shard]
		if !ok || !slices.Equal(old.Voters, g.Voters) || !slices.Equal(old.Learners, g.Learners) {
			d.groups[shard], changed = g, true
		}
	}
	return changed
}

// Voters returns a group's voters.
func (d *directory) Voters(shard int) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return slices.Clone(d.groups[shard].Voters)
}

// Members returns a group's voters and learners.
func (d *directory) Members(shard int) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	g := d.groups[shard]
	return slices.Concat(g.Voters, g.Learners)
}

// Group returns a group's voters and learners, and whether it is known.
func (d *directory) Group(shard int) (voters, learners []string, ok bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	g, ok := d.groups[shard]
	return slices.Clone(g.Voters), slices.Clone(g.Learners), ok
}

// Nodes returns every known node's ID, sorted.
func (d *directory) Nodes() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return slices.Sorted(maps.Keys(d.addrs))
}

// Drained reports whether a node was drained, as last read from the meta
// group.
func (d *directory) Drained(id string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.drained[id]
}

// State returns a node's state as last read from the meta group.
func (d *directory) State(id string) dkvv1.NodeState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.states[id]
}
