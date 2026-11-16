package obs

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/naveenk2608/distributed-database/internal/config"
)

func TestNewLoggerJSONAndLevel(t *testing.T) {
	var buf strings.Builder
	log, err := NewLogger(&buf, config.Log{Level: "warn", Format: "json"})
	if err != nil {
		t.Fatal(err)
	}
	log.Info("hidden")
	log.Warn("shown", "k", 7)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1 (info must be filtered at warn): %q", len(lines), buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if rec["msg"] != "shown" || rec["level"] != "WARN" || rec["k"] != float64(7) {
		t.Errorf("unexpected record %v", rec)
	}
}

func TestNewLoggerText(t *testing.T) {
	var buf strings.Builder
	log, err := NewLogger(&buf, config.Log{Level: "debug", Format: "text"})
	if err != nil {
		t.Fatal(err)
	}
	log.Debug("hello", "k", "v")
	if !strings.Contains(buf.String(), "level=DEBUG msg=hello k=v") {
		t.Errorf("text output = %q", buf.String())
	}
}

func TestNewLoggerRejectsBadConfig(t *testing.T) {
	for _, c := range []config.Log{{Level: "loud", Format: "json"}, {Level: "info", Format: "xml"}} {
		if _, err := NewLogger(&strings.Builder{}, c); err == nil {
			t.Errorf("NewLogger(%+v) succeeded", c)
		}
	}
}
