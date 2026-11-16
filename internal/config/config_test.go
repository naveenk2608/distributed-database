package config

import (
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dkv.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func load(t *testing.T, args, env []string) (*Config, error) {
	t.Helper()
	return Load("dkv-server", args, env, io.Discard)
}

// required are the settings with no default: an address and a data
// directory must always come from the operator.
var required = []string{"--server.grpc-addr=127.0.0.1:0", "--storage.dir=/var/lib/dkv-test"}

func TestDefaultsNeedAnAddressAndADataDir(t *testing.T) {
	_, err := load(t, nil, nil)
	for _, want := range []string{"server.grpc_addr: required", "storage.dir: required"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Load with nothing set: %v; want it to contain %q", err, want)
		}
	}
	cfg, err := load(t, required, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := Default()
	want.Server.GRPCAddr = "127.0.0.1:0"
	want.Storage.Dir = "/var/lib/dkv-test"
	if !reflect.DeepEqual(*cfg, want) {
		t.Errorf("got %+v, want defaults %+v", *cfg, want)
	}
}

// TestPrecedence sets one key in every source and checks the highest
// priority source wins, and that lower sources still supply keys the
// higher ones leave alone.
func TestPrecedence(t *testing.T) {
	file := writeFile(t, `
server:
  grpc_addr: "file:1"
  shutdown_timeout: 3s
storage:
  dir: /file/dir
  sync: interval
limits:
  max_key_bytes: 100
  max_value_bytes: 200
log:
  level: debug
`)
	env := []string{
		"DKV_CONFIG=" + file,
		"DKV_SERVER_GRPC_ADDR=env:2",
		"DKV_LIMITS_MAX_KEY_BYTES=111",
		"DKV_STORAGE_SYNC_INTERVAL=25ms",
		"DKV_STORAGE_DIR=/env/dir",
		"HOME=/irrelevant",
	}
	args := []string{"--server.grpc-addr", "flag:3", "--storage.dir=/flag/dir"}
	cfg, err := load(t, args, env)
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		Server:    Server{GRPCAddr: "flag:3", ShutdownTimeout: 3 * time.Second}, // flag > env > file
		Cluster:   Default().Cluster,
		Transport: Default().Transport,
		Storage: Storage{Dir: "/flag/dir", Sync: "interval", SyncInterval: 25 * time.Millisecond, // flag > env > file
			MemtableBytes: 4 << 20}, // default
		Limits: Limits{MaxKeyBytes: 111, MaxValueBytes: 200}, // env > file; file > default
		Log:    Log{Level: "debug", Format: "json"},          // file; default
	}
	if !reflect.DeepEqual(*cfg, want) {
		t.Errorf("got  %+v\nwant %+v", *cfg, want)
	}
}

func TestConfigFlagBeatsConfigEnv(t *testing.T) {
	fromEnv := writeFile(t, "server: {grpc_addr: 'env-file:1'}\n")
	fromFlag := writeFile(t, "server: {grpc_addr: 'flag-file:1'}\n")
	cfg, err := load(t, []string{"--config", fromFlag, "--storage.dir=/d"}, []string{"DKV_CONFIG=" + fromEnv})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.GRPCAddr != "flag-file:1" {
		t.Errorf("grpc_addr = %q, want the value from the --config file", cfg.Server.GRPCAddr)
	}
}

func TestEmptyConfigFile(t *testing.T) {
	path := writeFile(t, "")
	if _, err := load(t, append([]string{"--config", path}, required...), nil); err != nil {
		t.Fatalf("empty config file: %v", err)
	}
}

func TestRejectsMistakes(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  []string
		file string
		want string
	}{
		{name: "unknown yaml field", file: "server: {grpc_adr: ':1'}\n", want: "field grpc_adr not found"},
		{name: "malformed yaml", file: "server: [\n", want: "parse config file"},
		{name: "unknown env var", env: []string{"DKV_SERVER_GRPC_ADR=:1"}, want: "unknown environment variables [DKV_SERVER_GRPC_ADR]"},
		{name: "unknown flag", args: []string{"--server.grpc-adr=:1"}, want: "flag provided but not defined"},
		{name: "positional argument", args: []string{"extra"}, want: "unexpected arguments"},
		{name: "bad int env", env: []string{"DKV_LIMITS_MAX_KEY_BYTES=lots"}, want: "DKV_LIMITS_MAX_KEY_BYTES: not an integer"},
		{name: "bad duration flag", args: []string{"--server.shutdown-timeout=5"}, want: "--server.shutdown-timeout: not a duration"},
		{name: "missing file", args: []string{"--config=/nonexistent/dkv.yaml"}, want: "read config file"},
		{name: "address without port", args: []string{"--server.grpc-addr=localhost"}, want: "server.grpc_addr: address localhost: missing port"},
		{name: "bad port", args: []string{"--server.grpc-addr=:99999"}, want: `invalid port "99999"`},
		{name: "zero timeout", args: []string{"--server.shutdown-timeout=0s"}, want: "server.shutdown_timeout: must be positive"},
		{name: "zero key limit", args: []string{"--limits.max-key-bytes=0"}, want: "limits.max_key_bytes"},
		{name: "bad level", args: []string{"--log.level=loud"}, want: "log.level"},
		{name: "bad format", args: []string{"--log.format=xml"}, want: "log.format"},
		{name: "peers without internal address", args: []string{"--cluster.peers=10.0.0.1:7100"}, want: "cluster.internal_addr: required when cluster.peers is set"},
		{name: "wildcard internal address without advertise", args: []string{"--cluster.internal-addr=0.0.0.0:7100"}, want: "cluster.advertise_addr: required"},
		{name: "advertise without internal address", args: []string{"--cluster.advertise-addr=10.0.0.1:7100"}, want: "cluster.advertise_addr: set without cluster.internal_addr"},
		{name: "peer with port 0", args: []string{"--cluster.internal-addr=127.0.0.1:7100", "--cluster.peers=127.0.0.1:0"}, want: "only a listen address may pick a free port"},
		{name: "duplicate peer", args: []string{"--cluster.internal-addr=127.0.0.1:7100", "--cluster.peers=a:1,a:1"}, want: `"a:1" is listed twice`},
		{name: "same port for both APIs", args: []string{"--server.grpc-addr=127.0.0.1:7000", "--cluster.internal-addr=127.0.0.1:7000"}, want: "must differ from server.grpc_addr"},
		{name: "ping timeout above interval", args: []string{"--cluster.ping-timeout=2s"}, want: "cluster.ping_timeout"},
		{name: "down window not above interval", args: []string{"--cluster.down-after=1s"}, want: "cluster.down_after: must be longer"},
		{name: "short keepalive", args: []string{"--transport.keepalive-time=10ms"}, want: "transport.keepalive_time"},
		{name: "empty cluster name", args: []string{"--cluster.name="}, want: "cluster.name"},
		{name: "bad sync policy", args: []string{"--storage.sync=sometimes"}, want: `storage.sync: must be always, interval or never, got "sometimes"`},
		{name: "interval without period", args: []string{"--storage.sync=interval", "--storage.sync-interval=0s"}, want: "storage.sync_interval: must be positive"},
		{name: "tiny memtable", args: []string{"--storage.memtable-bytes=1024"}, want: "storage.memtable_bytes"},
		{name: "huge memtable", args: []string{"--storage.memtable-bytes=2147483648"}, want: "storage.memtable_bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := tc.args
			if tc.file != "" {
				args = append(args, "--config", writeFile(t, tc.file))
			}
			if !slices.ContainsFunc(args, func(a string) bool { return strings.Contains(a, "grpc-addr") }) {
				args = append(args, "--server.grpc-addr=:7000")
			}
			if !slices.ContainsFunc(args, func(a string) bool { return strings.Contains(a, "storage.dir") }) {
				args = append(args, "--storage.dir=/d")
			}
			_, err := load(t, args, tc.env)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v; want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	cfg := Default()
	cfg.Limits.MaxKeyBytes = 0
	cfg.Log.Format = "xml"
	cfg.Storage.Sync = "maybe"
	err := cfg.Validate()
	for _, key := range []string{"server.grpc_addr", "storage.dir", "storage.sync", "limits.max_key_bytes", "log.format"} {
		if err == nil || !strings.Contains(err.Error(), key) {
			t.Errorf("Validate error %v does not mention %s", err, key)
		}
	}
}

func TestHelp(t *testing.T) {
	var out strings.Builder
	_, err := Load("dkv-server", []string{"-h"}, nil, &out)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("Load(-h) = %v, want flag.ErrHelp", err)
	}
	for _, s := range settings {
		if !strings.Contains(out.String(), s.flagName()) || !strings.Contains(out.String(), s.envName()) {
			t.Errorf("usage does not mention --%s and %s", s.flagName(), s.envName())
		}
	}
}

// TestSettingsCoverConfig fails when a Config field has no entry in the
// settings table, which would make it unreachable from flags and env.
func TestSettingsCoverConfig(t *testing.T) {
	var yamlPaths []string
	var walk func(t reflect.Type, prefix string)
	walk = func(typ reflect.Type, prefix string) {
		for i := range typ.NumField() {
			f := typ.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
			if f.Type.Kind() == reflect.Struct && f.Type != reflect.TypeFor[time.Duration]() {
				walk(f.Type, prefix+name+".")
				continue
			}
			yamlPaths = append(yamlPaths, prefix+name)
		}
	}
	walk(reflect.TypeFor[Config](), "")

	var keys []string
	for _, s := range settings {
		keys = append(keys, s.key)
	}
	slices.Sort(yamlPaths)
	slices.Sort(keys)
	if !slices.Equal(yamlPaths, keys) {
		t.Errorf("Config YAML fields %v\ndo not match settings %v", yamlPaths, keys)
	}
}

// TestExampleConfigLoads keeps the shipped example config valid.
func TestExampleConfigLoads(t *testing.T) {
	cfg, err := load(t, []string{"--config", "../../deploy/config/dkv-server.example.yaml"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.GRPCAddr == "" || cfg.Storage.Dir == "" {
		t.Error("example config does not set server.grpc_addr and storage.dir")
	}
}

// TestClusterPeersFromEachSource checks the list setting in YAML (a
// sequence), the environment and flags (comma-separated).
func TestClusterPeersFromEachSource(t *testing.T) {
	base := append([]string{"--cluster.internal-addr=10.0.0.1:7100"}, required...)
	file := writeFile(t, "cluster:\n  peers: [\"10.0.0.1:7100\", \"10.0.0.2:7100\"]\n")
	for _, tc := range []struct {
		name string
		args []string
		env  []string
		want []string
	}{
		{"yaml", append([]string{"--config", file}, base...), nil, []string{"10.0.0.1:7100", "10.0.0.2:7100"}},
		{"env", base, []string{"DKV_CLUSTER_PEERS=10.0.0.1:7100, 10.0.0.3:7100,"}, []string{"10.0.0.1:7100", "10.0.0.3:7100"}},
		{"flag beats env", append(base, "--cluster.peers=n1:1,n2:2,n3:3"), []string{"DKV_CLUSTER_PEERS=x:1"}, []string{"n1:1", "n2:2", "n3:3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := load(t, tc.args, tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(cfg.Cluster.Peers, tc.want) {
				t.Errorf("peers = %q, want %q", cfg.Cluster.Peers, tc.want)
			}
		})
	}
}
