package client_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/server/servertest"
	"github.com/naveenk2608/distributed-database/pkg/client"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestClientRoundTrip(t *testing.T) {
	node := servertest.Start(t, nil)
	c, err := client.New(node.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, _, err := c.Get(ctx, []byte("k")); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Get missing key: %v, want ErrNotFound", err)
	}
	ver, err := c.Put(ctx, []byte("k"), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	v, gotVer, err := c.Get(ctx, []byte("k"))
	if err != nil || string(v) != "v" || gotVer != ver {
		t.Fatalf("Get = %q, %d, %v; want v at %d", v, gotVer, err, ver)
	}
	found, delVer, err := c.Delete(ctx, []byte("k"))
	if err != nil || !found || delVer <= ver {
		t.Fatalf("Delete = %v, %d, %v", found, delVer, err)
	}
	found, _, err = c.Delete(ctx, []byte("k"))
	if err != nil || found {
		t.Fatalf("second Delete = %v, %v; want found=false", found, err)
	}
	if _, _, err := c.Get(ctx, []byte("k")); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Get after Delete: %v, want ErrNotFound", err)
	}
}

func TestClientErrorsKeepStatusCode(t *testing.T) {
	node := servertest.Start(t, nil)
	c, err := client.New(node.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_, err = c.Put(context.Background(), nil, []byte("v"))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Put with empty key: %v; want InvalidArgument", err)
	}
}

// TestLargeValueRoundTrip stores and reads back a value larger than gRPC's
// default 4 MiB message limit, which the server allows when
// limits.max_value_bytes is raised.
func TestLargeValueRoundTrip(t *testing.T) {
	node := servertest.Start(t, func(c *config.Config) { c.Limits.MaxValueBytes = 6 << 20 })
	c, err := client.New(node.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	val := bytes.Repeat([]byte("0123456789"), (6<<20)/10)
	if _, err := c.Put(ctx, []byte("big"), val); err != nil {
		t.Fatalf("Put %d bytes: %v", len(val), err)
	}
	got, _, err := c.Get(ctx, []byte("big"))
	if err != nil || !bytes.Equal(got, val) {
		t.Fatalf("Get big value: %d bytes, %v; want %d bytes", len(got), err, len(val))
	}
	// Just over the value limit: the server reads the request and rejects it.
	if _, err := c.Put(ctx, []byte("bigger"), make([]byte, 6<<20+1)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Put 1 byte over the limit: %v; want InvalidArgument", err)
	}
	// Far over: gRPC refuses to read the message at all.
	if _, err := c.Put(ctx, []byte("huge"), make([]byte, 7<<20)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Put 1 MiB over the limit: %v; want ResourceExhausted", err)
	}
}

// TestShardMapsAsksNodesAtOnce: a live node between two silent ones (they
// accept TCP connections but never speak gRPC). Each node has its own
// timeout and they are asked at the same time, so the call takes one
// timeout, not two, however long ctx allows, and the live node's map comes
// back although a silent node comes first.
func TestShardMapsAsksNodesAtOnce(t *testing.T) {
	node := servertest.Start(t, nil)
	var silent []string
	for range 2 {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lis.Close() }()
		silent = append(silent, lis.Addr().String())
	}
	c, err := client.New(silent[0], node.Addr, silent[1])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const perNode = 1500 * time.Millisecond
	start := time.Now()
	maps, errs := c.ShardMaps(ctx, perNode)
	elapsed := time.Since(start)
	if len(maps) != 3 || len(errs) != 3 {
		t.Fatalf("ShardMaps returned %d maps and %d errors for 3 nodes", len(maps), len(errs))
	}
	if errs[1] != nil || len(maps[1].GetShards()) != 1 {
		t.Errorf("live node: map %v, error %v; want its one shard", maps[1], errs[1])
	}
	for _, i := range []int{0, 2} {
		if maps[i] != nil || status.Code(errs[i]) != codes.DeadlineExceeded {
			t.Errorf("silent node %d: map %v, error %v; want DeadlineExceeded", i, maps[i], errs[i])
		}
	}
	if elapsed >= 2*perNode {
		t.Errorf("ShardMaps took %v; asked one after another, the two silent nodes take at least %v", elapsed, 2*perNode)
	}
}

// TestClientUnresponsiveServer points the client at a listener that accepts
// TCP connections but never speaks gRPC. Unlike dialing a stopped server's
// old port, nothing else can be listening there.
func TestClientUnresponsiveServer(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lis.Close() }()
	c, err := client.New(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := c.Put(ctx, []byte("k"), nil); status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Put to unresponsive server: %v; want Unavailable or DeadlineExceeded", err)
	}
}
