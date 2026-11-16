package node

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNodeIDPersists(t *testing.T) {
	dir := t.TempDir()
	id, err := LoadOrCreateID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !validID(id) {
		t.Fatalf("generated ID %q is not 16 lowercase hex digits", id)
	}
	again, err := LoadOrCreateID(dir)
	if err != nil || again != id {
		t.Fatalf("second load = %q, %v; want %q", again, err, id)
	}
	other, _ := LoadOrCreateID(t.TempDir())
	if other == id {
		t.Error("two data directories got the same ID")
	}
	if _, err := os.Stat(filepath.Join(dir, IDFile+".tmp")); !os.IsNotExist(err) {
		t.Errorf("temporary file left behind: %v", err)
	}
}

func TestDamagedNodeIDIsAnError(t *testing.T) {
	for _, content := range []string{"", "not-an-id\n", "ABCDEF0123456789\n", "0123"} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, IDFile), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := LoadOrCreateID(dir)
		if err == nil || !strings.Contains(err.Error(), "not a node ID") {
			t.Errorf("content %q: %v; want a 'not a node ID' error", content, err)
		}
	}
}
