// Package server implements the public gRPC API of a dkv node.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
)

// grpcOverheadBytes is headroom above the largest key plus value for the
// rest of a request message.
const grpcOverheadBytes = 64 << 10

// Server serves the KV API over gRPC.
type Server struct {
	cfg  config.Server
	log  *slog.Logger
	grpc *grpc.Server
}

// New returns a server for backend that answers cluster status from
// status. It does not listen until Run.
func New(cfg *config.Config, backend Backend, status StatusSource, log *slog.Logger) *Server {
	s := &Server{cfg: cfg.Server, log: log}
	s.grpc = grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.Limits.MaxKeyBytes+cfg.Limits.MaxValueBytes+grpcOverheadBytes),
		grpc.ChainUnaryInterceptor(s.logRequests),
	)
	dkvv1.RegisterKVServiceServer(s.grpc, &kvService{backend: backend, limits: cfg.Limits})
	dkvv1.RegisterAdminServiceServer(s.grpc, &adminService{status: status})
	return s
}

// Run serves on lis until ctx is cancelled or serving fails. On
// cancellation it stops accepting requests, waits up to the configured
// shutdown timeout for in-flight ones, then closes remaining connections.
// Run closes lis and returns only after every server goroutine has exited.
func (s *Server) Run(ctx context.Context, lis net.Listener) error {
	s.log.Info("serving KV API", "addr", lis.Addr().String())
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.grpc.Serve(lis) }()

	select {
	case err := <-serveErr:
		// Serve failed on its own; close any connections it had accepted so
		// their goroutines do not outlive Run.
		s.grpc.Stop()
		return fmt.Errorf("grpc serve: %w", err)
	case <-ctx.Done():
	}

	s.log.Info("shutting down", "timeout", s.cfg.ShutdownTimeout)
	stopped := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()
	timer := time.NewTimer(s.cfg.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-stopped:
	case <-timer.C:
		s.log.Warn("graceful shutdown timed out; closing connections")
		s.grpc.Stop()
		<-stopped
	}
	// ErrServerStopped means the stop won a race with Serve starting up.
	if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("grpc serve: %w", err)
	}
	s.log.Info("stopped")
	return nil
}

func (s *Server) logRequests(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	code := status.Code(err)
	level := slog.LevelDebug
	if code == codes.Internal || code == codes.Unknown {
		level = slog.LevelError
	}
	attrs := []any{"method", info.FullMethod, "code", code.String(), "duration", time.Since(start)}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	s.log.Log(ctx, level, "request", attrs...)
	return resp, err
}

type kvService struct {
	dkvv1.UnimplementedKVServiceServer
	backend Backend
	limits  config.Limits
}

func (k *kvService) checkKey(key []byte) error {
	if len(key) == 0 {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if len(key) > k.limits.MaxKeyBytes {
		return status.Errorf(codes.InvalidArgument, "key is %d bytes; the limit is %d", len(key), k.limits.MaxKeyBytes)
	}
	return nil
}

func (k *kvService) Put(ctx context.Context, req *dkvv1.PutRequest) (*dkvv1.PutResponse, error) {
	if err := k.checkKey(req.GetKey()); err != nil {
		return nil, err
	}
	if n := len(req.GetValue()); n > k.limits.MaxValueBytes {
		return nil, status.Errorf(codes.InvalidArgument, "value is %d bytes; the limit is %d", n, k.limits.MaxValueBytes)
	}
	ver, err := k.backend.Put(ctx, req.GetKey(), req.GetValue())
	if err != nil {
		return nil, toStatus(err)
	}
	return &dkvv1.PutResponse{Version: ver}, nil
}

func (k *kvService) Get(ctx context.Context, req *dkvv1.GetRequest) (*dkvv1.GetResponse, error) {
	if err := k.checkKey(req.GetKey()); err != nil {
		return nil, err
	}
	v, ver, err := k.backend.Get(ctx, req.GetKey())
	if errors.Is(err, ErrNotFound) {
		return &dkvv1.GetResponse{Found: false}, nil
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &dkvv1.GetResponse{Found: true, Value: v, Version: ver}, nil
}

func (k *kvService) Delete(ctx context.Context, req *dkvv1.DeleteRequest) (*dkvv1.DeleteResponse, error) {
	if err := k.checkKey(req.GetKey()); err != nil {
		return nil, err
	}
	found, ver, err := k.backend.Delete(ctx, req.GetKey())
	if err != nil {
		return nil, toStatus(err)
	}
	return &dkvv1.DeleteResponse{Found: found, Version: ver}, nil
}

// toStatus maps backend errors to gRPC status errors.
func toStatus(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
