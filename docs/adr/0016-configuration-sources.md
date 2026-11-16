# ADR 0016: Configuration sources and precedence

Status: Accepted (Phase 1)

## Context

The project rules forbid hardcoded cluster topology, ports and paths: everything
must come from `internal/config`. Nodes run as local processes, in Docker
Compose and on EC2, and each environment prefers a different way to pass
settings. The project owner chose YAML files overridable by command-line
flags and `DKV_*` environment variables.

## Decision

- Sources, lowest to highest priority: built-in defaults, a YAML file (named
  by `--config` or `DKV_CONFIG`), `DKV_*` environment variables, flags.
- Each setting is declared once in a table keyed by its YAML path; the env
  var and flag names are derived from it (`server.grpc_addr` →
  `DKV_SERVER_GRPC_ADDR`, `--server.grpc-addr`). A test fails if a Config
  field has no table entry, so the three sources cannot drift apart.
- Strictness: unknown YAML fields, unknown `DKV_*` variables, unknown flags
  and positional arguments are errors.
- Defaults never contain addresses, ports or paths; `server.grpc_addr` is
  required.
- `Load` takes args and environ as parameters and never touches globals, so
  tests are hermetic. `Validate` is pure and reports every problem at once.
- `dkvctl` reads its server address from `DKVCTL_ADDR`, outside the `DKV_`
  namespace, so a shell configured for the client does not trip the server's
  unknown-variable check.

## Alternatives considered

- **A configuration library (Viper, koanf).** Less code, but Viper pulls in
  many dependencies and its key handling is case-insensitive and loose about
  unknown keys; the plan prefers the standard library. The table-driven
  loader is about 150 lines. The standard library has no YAML parser, so
  the loader uses `go.yaml.in/yaml/v3`, the YAML organization's maintained
  successor to the archived `gopkg.in/yaml.v3` (strict decoding with
  `KnownFields`, `time.Duration` support). It is outside the plan's
  dependency table, but it parses config files only and does no core work.
- **Flags only.** Simple, but long command lines in Compose and Terraform
  and no way to keep a reviewed file per node.
- **Lenient parsing (ignore unknown keys).** Friendlier to version skew, but
  a typo such as `grpc_adr` silently runs a node with the wrong settings,
  which is worse for a database.

## Consequences

- Every new setting needs one table entry; flags and env vars come for free.
- Strict `DKV_*` checking means unrelated tools must not use that prefix.
- Rolling upgrades that add settings must ship new binaries before new
  config files, since old binaries reject unknown keys.
