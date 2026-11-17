package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
)

// ConsoleConfig is the complete configuration of dkv-console, the
// dashboard backend. It loads like a node's: defaults < YAML file < DKV_*
// environment < flags.
type ConsoleConfig struct {
	Console Console `yaml:"console"`
	Log     Log     `yaml:"log"`
}

// Console configures the dashboard backend.
type Console struct {
	// ListenAddr is the host:port the dashboard and its API are served on.
	// Required.
	ListenAddr string `yaml:"listen_addr"`
	// Nodes are the admin HTTP addresses (server.admin_addr) of the nodes
	// to poll. Required: the console learns nothing about the cluster
	// except from them.
	Nodes []string `yaml:"nodes"`
	// PollInterval is how often every node is polled; a node that stops
	// answering shows as down after at most one interval.
	PollInterval time.Duration `yaml:"poll_interval"`
	// PollTimeout bounds one poll of one node; it must be shorter than
	// PollInterval.
	PollTimeout time.Duration `yaml:"poll_timeout"`
	// PostgresDSN, if set, is where samples and events are kept (pgx
	// connection string). Empty keeps a bounded history in memory only.
	PostgresDSN string `yaml:"postgres_dsn"`
	// Retention is how long samples and events are kept in PostgreSQL.
	Retention time.Duration `yaml:"retention"`
	// MemorySamples bounds the samples kept in memory per node, for the
	// charts, with or without PostgreSQL.
	MemorySamples int `yaml:"memory_samples"`
	// WebDir is the directory of the built dashboard (web/dist). Empty
	// serves the API only.
	WebDir string `yaml:"web_dir"`
}

// DefaultConsole returns the console's built-in defaults; like Default,
// they contain no address or path.
func DefaultConsole() ConsoleConfig {
	return ConsoleConfig{
		Console: Console{PollInterval: time.Second, PollTimeout: 800 * time.Millisecond, Retention: 24 * time.Hour,
			MemorySamples: 600},
		Log: Log{Level: "info", Format: "json"},
	}
}

// Validate reports every problem with c, joined into one error.
func (c *ConsoleConfig) Validate() error {
	var errs []error
	bad := func(key, format string, args ...any) {
		errs = append(errs, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
	}
	if c.Console.ListenAddr == "" {
		bad("console.listen_addr", "required (set it in the config file, DKV_CONSOLE_LISTEN_ADDR or --console.listen-addr)")
	} else if err := checkListenAddr(c.Console.ListenAddr); err != nil {
		errs = append(errs, fmt.Errorf("console.listen_addr: %w", err))
	}
	if len(c.Console.Nodes) == 0 {
		bad("console.nodes", "required: the admin addresses of the nodes to poll")
	}
	seen := map[string]bool{}
	for _, n := range c.Console.Nodes {
		if err := checkListenAddr(n); err != nil {
			errs = append(errs, fmt.Errorf("console.nodes: %q: %w", n, err))
		}
		if seen[n] {
			bad("console.nodes", "%q is listed twice", n)
		}
		seen[n] = true
	}
	if c.Console.PollInterval <= 0 {
		bad("console.poll_interval", "must be positive, got %v", c.Console.PollInterval)
	}
	if c.Console.PollTimeout <= 0 || c.Console.PollTimeout >= c.Console.PollInterval {
		bad("console.poll_timeout", "must be positive and shorter than console.poll_interval (%v), got %v",
			c.Console.PollInterval, c.Console.PollTimeout)
	}
	if c.Console.Retention <= 0 {
		bad("console.retention", "must be positive, got %v", c.Console.Retention)
	}
	if c.Console.MemorySamples < 1 {
		bad("console.memory_samples", "must be at least 1, got %d", c.Console.MemorySamples)
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(c.Log.Level)); err != nil {
		bad("log.level", "must be debug, info, warn or error, got %q", c.Log.Level)
	}
	if c.Log.Format != "json" && c.Log.Format != "text" {
		bad("log.format", "must be json or text, got %q", c.Log.Format)
	}
	return errors.Join(errs...)
}

// consoleSettings lists every console configuration key;
// TestConsoleSettingsCoverConfig fails if a field is missing.
var consoleSettings = []setting[ConsoleConfig]{
	stringSetting("console.listen_addr", "host:port the dashboard and its API are served on (required)",
		func(c *ConsoleConfig) *string { return &c.Console.ListenAddr }),
	stringListSetting("console.nodes", "admin HTTP addresses of the nodes to poll, comma-separated (required)",
		func(c *ConsoleConfig) *[]string { return &c.Console.Nodes }),
	durationSetting("console.poll_interval", "how often every node is polled",
		func(c *ConsoleConfig) *time.Duration { return &c.Console.PollInterval }),
	durationSetting("console.poll_timeout", "deadline for polling one node",
		func(c *ConsoleConfig) *time.Duration { return &c.Console.PollTimeout }),
	stringSetting("console.postgres_dsn", "PostgreSQL connection string for history; empty keeps it in memory",
		func(c *ConsoleConfig) *string { return &c.Console.PostgresDSN }),
	durationSetting("console.retention", "how long samples and events are kept in PostgreSQL",
		func(c *ConsoleConfig) *time.Duration { return &c.Console.Retention }),
	intSetting("console.memory_samples", "samples kept in memory per node",
		func(c *ConsoleConfig) *int { return &c.Console.MemorySamples }),
	stringSetting("console.web_dir", "directory of the built dashboard; empty serves the API only",
		func(c *ConsoleConfig) *string { return &c.Console.WebDir }),
	stringSetting("log.level", "log level: debug, info, warn or error",
		func(c *ConsoleConfig) *string { return &c.Log.Level }),
	stringSetting("log.format", "log format: json or text",
		func(c *ConsoleConfig) *string { return &c.Log.Format }),
}

// LoadConsole builds a validated ConsoleConfig the way Load builds a
// node's Config.
func LoadConsole(program string, args, environ []string, output io.Writer) (*ConsoleConfig, error) {
	return loadConfig(program, args, environ, output, consoleSettings, DefaultConsole(), (*ConsoleConfig).Validate)
}
