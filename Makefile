.PHONY: check test test-integration sim chaos proto build lint vet fmt

# Build the server and CLI binaries
build:
	go build -o bin/dkv-server ./cmd/dkv-server
	go build -o bin/dkvctl ./cmd/dkvctl

# Run all checks: format, vet, lint, test with race detector
check: fmt vet lint test

# Format code
fmt:
	gofmt -s -w .

# Go vet
vet:
	go vet ./...

# Golangci-lint
lint:
	golangci-lint run ./...

# Run all tests with race detector
test:
	go test -race -count=1 ./...

# Run integration tests (in-process multi-node)
test-integration:
	go test -race -count=1 -tags=integration ./test/integration/...

# Run deterministic Raft/cluster simulation
# Usage: make sim SEEDS=1000
sim:
	go test -race -count=1 -run=TestSim -v ./internal/sim/... -seeds=$(SEEDS)

# Run chaos tests (Docker fault scenarios)
chaos:
	go test -race -count=1 -tags=chaos -timeout=30m ./test/chaos/...

# Generate protobuf code
proto:
	buf generate

# Download and verify dependencies
deps:
	go mod download
	go mod verify

# Tidy dependencies
tidy:
	go mod tidy

# Install development tools
tools:
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Clean build artifacts
clean:
	rm -rf bin/
	rm -f coverage.out