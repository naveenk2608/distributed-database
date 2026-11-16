package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/naveenk2608/distributed-database/internal/config"
)

// markerFile records, in a cluster node's data directory, the cluster
// settings that must never change once data exists: the cluster name, the
// shard count and the replication factor.
const markerFile = "CLUSTER"

type marker struct {
	Name              string `json:"name"`
	Shards            int    `json:"shards"`
	ReplicationFactor int    `json:"replication_factor"`
}

// checkOrWriteMarker compares cfg with the marker in dir, or writes the
// marker on a node's first start in a cluster.
func checkOrWriteMarker(dir string, cfg config.Cluster) error {
	path := filepath.Join(dir, markerFile)
	want := marker{Name: cfg.Name, Shards: cfg.Shards, ReplicationFactor: cfg.ReplicationFactor}
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		var have marker
		if err := json.Unmarshal(b, &have); err != nil {
			return fmt.Errorf("node: %s is damaged: %w", path, err)
		}
		if have != want {
			return fmt.Errorf("node: %s was created for cluster %q with %d shards and replication factor %d; "+
				"the configuration says cluster %q, %d shards, replication factor %d, which cannot change once data exists",
				dir, have.Name, have.Shards, have.ReplicationFactor, want.Name, want.Shards, want.ReplicationFactor)
		}
		return nil
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("node: read %s: %w", path, err)
	}
	b, err = json.Marshal(want)
	if err != nil {
		return err
	}
	return writeFileDurably(path, append(b, '\n'))
}

// writeFileDurably writes path atomically: a temporary file, fsync, rename,
// and a directory fsync.
func writeFileDurably(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("node: create %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close() // the write error is the one worth reporting
		return fmt.Errorf("node: write %s: %w", tmp, err)
	}
	// Fsync before the rename so the file is never seen empty or partial.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("node: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("node: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("node: install %s: %w", path, err)
	}
	// Fsync the directory so the rename itself is durable.
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("node: open %s: %w", filepath.Dir(path), err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("node: sync %s: %w", filepath.Dir(path), err)
	}
	return d.Close()
}
