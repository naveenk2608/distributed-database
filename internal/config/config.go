// Package config loads and validates node configuration.
//
// Sources, from lowest to highest priority: built-in defaults, a YAML file,
// DKV_* environment variables, and command-line flags. Every setting is
// declared once in the settings table below; its environment variable and
// flag names are derived from its YAML path:
//
//	YAML  server.grpc_addr
//	env   DKV_SERVER_GRPC_ADDR
//	flag  --server.grpc-addr
//
// The YAML file is named by --config or DKV_CONFIG. Unknown YAML fields and
// unknown DKV_* variables are errors, so typos fail loudly at startup.
// See docs/adr/0016-configuration-sources.md.
package config

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config is the complete configuration of a dkv node.
type Config struct {
	Server Server `yaml:"server"`
	Limits Limits `yaml:"limits"`
	Log    Log    `yaml:"log"`
}

// Server configures the public API server.
type Server struct {
	// GRPCAddr is the host:port the public gRPC API listens on. Required.
	// Port 0 picks a free port; the bound address is logged at startup.
	GRPCAddr string `yaml:"grpc_addr"`
	// ShutdownTimeout bounds how long a graceful shutdown waits for
	// in-flight requests before closing connections.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// Limits bounds request sizes.
type Limits struct {
	MaxKeyBytes   int `yaml:"max_key_bytes"`
	MaxValueBytes int `yaml:"max_value_bytes"`
}

// Log configures logging.
type Log struct {
	// Level is one of debug, info, warn, error.
	Level string `yaml:"level"`
	// Format is json or text.
	Format string `yaml:"format"`
}

// Default returns the built-in defaults. They contain no addresses, ports
// or paths: those must always come from the operator.
func Default() Config {
	return Config{
		Server: Server{ShutdownTimeout: 10 * time.Second},
		Limits: Limits{MaxKeyBytes: 4 << 10, MaxValueBytes: 1 << 20},
		Log:    Log{Level: "info", Format: "json"},
	}
}

// Validate reports every problem with c, joined into one error.
func (c *Config) Validate() error {
	var errs []error
	bad := func(key, format string, args ...any) {
		errs = append(errs, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
	}

	if c.Server.GRPCAddr == "" {
		bad("server.grpc_addr", "required (set it in the config file, DKV_SERVER_GRPC_ADDR or --server.grpc-addr)")
	} else if _, port, err := net.SplitHostPort(c.Server.GRPCAddr); err != nil {
		errs = append(errs, fmt.Errorf("server.grpc_addr: %w", err))
	} else if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		errs = append(errs, fmt.Errorf("server.grpc_addr: invalid port %q: %w", port, err))
	}
	if c.Server.ShutdownTimeout <= 0 {
		bad("server.shutdown_timeout", "must be positive, got %v", c.Server.ShutdownTimeout)
	}

	const maxLimit = 1 << 30 // keeps derived message-size limits far from int overflow
	if c.Limits.MaxKeyBytes < 1 || c.Limits.MaxKeyBytes > maxLimit {
		bad("limits.max_key_bytes", "must be between 1 and %d, got %d", maxLimit, c.Limits.MaxKeyBytes)
	}
	if c.Limits.MaxValueBytes < 0 || c.Limits.MaxValueBytes > maxLimit {
		bad("limits.max_value_bytes", "must be between 0 and %d, got %d", maxLimit, c.Limits.MaxValueBytes)
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

// setting binds one configuration key to its parser.
type setting struct {
	key   string // dotted YAML path
	usage string
	set   func(c *Config, v string) error
}

func (s setting) envName() string {
	return "DKV_" + strings.ToUpper(strings.ReplaceAll(s.key, ".", "_"))
}

func (s setting) flagName() string { return strings.ReplaceAll(s.key, "_", "-") }

func stringSetting(key, usage string, field func(*Config) *string) setting {
	return setting{key, usage, func(c *Config, v string) error {
		*field(c) = v
		return nil
	}}
}

func intSetting(key, usage string, field func(*Config) *int) setting {
	return setting{key, usage, func(c *Config, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("not an integer: %w", err)
		}
		*field(c) = n
		return nil
	}}
}

func durationSetting(key, usage string, field func(*Config) *time.Duration) setting {
	return setting{key, usage, func(c *Config, v string) error {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("not a duration (e.g. 5s, 250ms): %w", err)
		}
		*field(c) = d
		return nil
	}}
}

// settings lists every configuration key. TestSettingsCoverConfig fails if
// a field of Config is missing here.
var settings = []setting{
	stringSetting("server.grpc_addr", "host:port for the public gRPC API (required)",
		func(c *Config) *string { return &c.Server.GRPCAddr }),
	durationSetting("server.shutdown_timeout", "how long graceful shutdown waits for in-flight requests",
		func(c *Config) *time.Duration { return &c.Server.ShutdownTimeout }),
	intSetting("limits.max_key_bytes", "largest accepted key, in bytes",
		func(c *Config) *int { return &c.Limits.MaxKeyBytes }),
	intSetting("limits.max_value_bytes", "largest accepted value, in bytes",
		func(c *Config) *int { return &c.Limits.MaxValueBytes }),
	stringSetting("log.level", "log level: debug, info, warn or error",
		func(c *Config) *string { return &c.Log.Level }),
	stringSetting("log.format", "log format: json or text",
		func(c *Config) *string { return &c.Log.Format }),
}

const configEnv = "DKV_CONFIG"

// flagRecorder captures flag values so they can be applied after the YAML
// file and the environment, which must be read first.
type flagRecorder struct {
	s       *setting
	pending *[]pendingFlag
}

type pendingFlag struct {
	s     *setting
	value string
}

func (f flagRecorder) String() string { return "" }

func (f flagRecorder) Set(v string) error {
	*f.pending = append(*f.pending, pendingFlag{f.s, v})
	return nil
}

// Load builds a validated Config from args (command-line arguments without
// the program name), environ (KEY=VALUE pairs, as from os.Environ) and the
// YAML file they name. Usage and flag errors are written to output. If args
// ask for help, Load returns flag.ErrHelp.
func Load(program string, args, environ []string, output io.Writer) (*Config, error) {
	fs := flag.NewFlagSet(program, flag.ContinueOnError)
	fs.SetOutput(output)
	configPath := fs.String("config", "", "path to a YAML config file (env "+configEnv+")")
	var pending []pendingFlag
	for i := range settings {
		s := &settings[i]
		fs.Var(flagRecorder{s, &pending}, s.flagName(), fmt.Sprintf("%s (env %s)", s.usage, s.envName()))
	}
	fs.Usage = func() {
		_, _ = fmt.Fprintf(output, "Usage: %s [flags]\n\nPriority: flags > DKV_* environment > config file > defaults.\n\nFlags:\n", program)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected arguments: %q", fs.Args())
	}

	env, err := parseEnviron(environ)
	if err != nil {
		return nil, err
	}

	cfg := Default()

	path := *configPath
	if path == "" {
		path = env[configEnv]
	}
	if path != "" {
		if err := loadFile(&cfg, path); err != nil {
			return nil, err
		}
	}

	for i := range settings {
		s := &settings[i]
		if v, ok := env[s.envName()]; ok {
			if err := s.set(&cfg, v); err != nil {
				return nil, fmt.Errorf("environment variable %s: %w", s.envName(), err)
			}
		}
	}

	for _, p := range pending {
		if err := p.s.set(&cfg, p.value); err != nil {
			return nil, fmt.Errorf("flag --%s: %w", p.s.flagName(), err)
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration:\n%w", err)
	}
	return &cfg, nil
}

// parseEnviron returns the DKV_* variables in environ, rejecting unknown ones.
func parseEnviron(environ []string) (map[string]string, error) {
	known := map[string]bool{configEnv: true}
	for _, s := range settings {
		known[s.envName()] = true
	}
	env := map[string]string{}
	var unknown []string
	for _, kv := range environ {
		k, v, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(k, "DKV_") {
			continue
		}
		if !known[k] {
			unknown = append(unknown, k)
			continue
		}
		env[k] = v
	}
	if len(unknown) > 0 {
		return nil, fmt.Errorf("unknown environment variables %v (run with -h to list the supported ones)", unknown)
	}
	return env, nil
}

func loadFile(cfg *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("parse config file %s: %w", path, err)
	}
	return nil
}
