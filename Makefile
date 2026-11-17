# Tool versions. CI installs exactly these; `make tools` installs them locally.
GOLANGCI_LINT_VERSION      := v2.15.0
BUF_VERSION                := v1.75.0
PROTOC_GEN_GO_VERSION      := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

.PHONY: all build check fmt fmt-check vet lint test test-short test-integration sim crash storage-gate bench fuzz docker-build compose-up compose-down compose-gate proto proto-check tools clean

all: check build

## build: compile dkv-server and dkvctl into bin/
build:
	go build -o bin/dkv-server ./cmd/dkv-server
	go build -o bin/dkvctl ./cmd/dkvctl

## check: everything CI runs: formatting, vet, lint, race-enabled tests
check: fmt-check vet lint test

## fmt: rewrite Go files with gofmt
fmt:
	gofmt -s -w .

fmt-check:
	@out="$$(gofmt -s -l .)"; if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

lint:
	golangci-lint run ./...

## test: all tests with the race detector
test:
	go test -race -count=1 ./...

## test-short: skip slow tests (the e2e binary test)
test-short:
	go test -race -count=1 -short ./...

## sim: deterministic Raft simulation (Phase 6 gate: SEEDS=10000)
SEEDS ?= 1000
sim:
	go test -count=1 -v -run '^(TestSimulation|TestReplayIsIdentical)$$' ./internal/sim -seeds=$(SEEDS)

## test-integration: in-process multi-node clusters
test-integration:
	go test -race -count=1 -v ./test/integration/...

## crash: kill -9 harness (Phase 2 gate: at least 100 cycles, no race detector)
CYCLES ?= 150
crash:
	go test -count=1 -v -run '^TestKill9Cycles$$' ./test/crash -cycles=$(CYCLES)

## storage-gate: Phase 3 gate at full size (make check runs smaller versions under -race)
storage-gate:
	go test -count=1 -v -run '^(TestModelMillionOps|TestCrashDuringFlushAndCompaction)$$' ./internal/storage/lsm -model-ops=1000000
	go test -count=1 -v -run '^(TestBloomFalsePositiveRate|TestTableFilterRate)$$' ./internal/storage/sstable

## bench: storage microbenchmarks
bench:
	go test -run '^$$' -bench . -benchmem ./internal/storage/sstable ./internal/storage/lsm

## fuzz: run every fuzz target for FUZZTIME each
FUZZTIME ?= 30s
fuzz:
	go test -run='^$$' -fuzz='^FuzzRecover$$' -fuzztime=$(FUZZTIME) ./internal/storage/wal
	go test -run='^$$' -fuzz='^FuzzDecodeBatch$$' -fuzztime=$(FUZZTIME) ./internal/storage/lsm
	go test -run='^$$' -fuzz='^FuzzOpenAndScan$$' -fuzztime=$(FUZZTIME) ./internal/storage/sstable

## docker-build: build the node image (dkv:dev)
docker-build:
	docker build -f deploy/docker/Dockerfile -t dkv:dev .

## compose-up / compose-down: three-node cluster in Docker Compose
compose-up:
	docker compose -f deploy/compose/docker-compose.yml up -d --build
compose-down:
	docker compose -f deploy/compose/docker-compose.yml down -v --remove-orphans

## compose-gate: Phase 4 gate in Compose (needs Docker)
compose-gate:
	deploy/compose/gate.sh

## proto: lint the protobuf API and regenerate gen/
proto:
	buf lint
	buf generate

## proto-check: fail if gen/ is out of date with proto/
proto-check: proto
	@git diff --exit-code -- gen/ || (echo "gen/ is stale: run make proto and commit"; exit 1)
	@test -z "$$(git ls-files --others --exclude-standard -- gen/)" || (echo "untracked files in gen/: run make proto and commit"; exit 1)

## tools: install pinned development tools into GOBIN
tools:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

clean:
	go clean
	rm -f bin/dkv-server bin/dkvctl
