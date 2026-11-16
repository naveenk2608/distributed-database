// Package obs holds dkv's observability helpers. Phase 1 provides structured
// logging; Prometheus metrics and the cluster event log arrive with the
// components they describe (see docs/PROGRESS.md).
package obs

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/naveenk2608/distributed-database/internal/config"
)

// NewLogger returns a slog.Logger writing to w in the configured format and
// at the configured level.
func NewLogger(w io.Writer, cfg config.Log) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.Level)); err != nil {
		return nil, fmt.Errorf("log level: %w", err)
	}
	opts := &slog.HandlerOptions{Level: level}
	switch cfg.Format {
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("log format %q: must be json or text", cfg.Format)
	}
}
