package node

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// IDFile is the name of the file in the data directory that holds the node
// ID.
const IDFile = "NODE_ID"

// LoadOrCreateID returns the node ID stored in dir, generating and storing
// a new random one on first start. A node keeps its ID across restarts and
// address changes; a node whose data directory is wiped comes back as a
// new node.
func LoadOrCreateID(dir string) (string, error) {
	path := filepath.Join(dir, IDFile)
	b, err := os.ReadFile(path)
	if err == nil {
		id := strings.TrimSpace(string(b))
		if !validID(id) {
			return "", fmt.Errorf("node: %s holds %q, which is not a node ID; restore the file or wipe the data directory", path, truncate(id))
		}
		return id, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("node: read %s: %w", path, err)
	}

	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("node: generate ID: %w", err)
	}
	id := hex.EncodeToString(raw[:])
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return "", fmt.Errorf("node: create %s: %w", tmp, err)
	}
	if _, err := f.WriteString(id + "\n"); err != nil {
		_ = f.Close() // the write error is the one worth reporting
		return "", fmt.Errorf("node: write %s: %w", tmp, err)
	}
	// Fsync before the rename: a crash must never leave NODE_ID empty, or
	// the node would come back under a new identity.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("node: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("node: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("node: install %s: %w", path, err)
	}
	// Fsync the directory so the rename, and with it the ID, is durable.
	d, err := os.Open(dir)
	if err != nil {
		return "", fmt.Errorf("node: open %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return "", fmt.Errorf("node: sync %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return "", fmt.Errorf("node: close %s: %w", dir, err)
	}
	return id, nil
}

func validID(id string) bool {
	if len(id) != 16 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil && strings.ToLower(id) == id
}

func truncate(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}
