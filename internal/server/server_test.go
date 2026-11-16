package server_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"log/slog"
	"net"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
	"github.com/naveenk2608/distributed-database/internal/server"
	"github.com/naveenk2608/distributed-database/internal/server/servertest"
	"github.com/naveenk2608/distributed-database/internal/storage/lsm"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func dial(t *testing.T, addr string) dkvv1.KVServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return dkvv1.NewKVServiceClient(conn)
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestPutGetDelete(t *testing.T) {
	node := servertest.Start(t, nil)
	kv := dial(t, node.Addr)
	c := ctx(t)

	get, err := kv.Get(c, &dkvv1.GetRequest{Key: []byte("k")})
	if err != nil || get.GetFound() {
		t.Fatalf("Get on empty store = %v, %v; want found=false", get, err)
	}

	put1, err := kv.Put(c, &dkvv1.PutRequest{Key: []byte("k"), Value: []byte("v1")})
	if err != nil {
		t.Fatal(err)
	}
	put2, err := kv.Put(c, &dkvv1.PutRequest{Key: []byte("k"), Value: []byte("v2")})
	if err != nil {
		t.Fatal(err)
	}
	if put2.GetVersion() <= put1.GetVersion() {
		t.Errorf("versions did not increase: %d then %d", put1.GetVersion(), put2.GetVersion())
	}

	get, err = kv.Get(c, &dkvv1.GetRequest{Key: []byte("k")})
	if err != nil || !get.GetFound() || string(get.GetValue()) != "v2" || get.GetVersion() != put2.GetVersion() {
		t.Fatalf("Get = %v, %v; want v2 at version %d", get, err, put2.GetVersion())
	}

	del, err := kv.Delete(c, &dkvv1.DeleteRequest{Key: []byte("k")})
	if err != nil || !del.GetFound() || del.GetVersion() <= put2.GetVersion() {
		t.Fatalf("Delete = %v, %v; want found=true with a newer version", del, err)
	}
	get, err = kv.Get(c, &dkvv1.GetRequest{Key: []byte("k")})
	if err != nil || get.GetFound() {
		t.Fatalf("Get after Delete = %v, %v; want found=false", get, err)
	}

	again, err := kv.Delete(c, &dkvv1.DeleteRequest{Key: []byte("k")})
	if err != nil || again.GetFound() || again.GetVersion() <= del.GetVersion() {
		t.Fatalf("second Delete = %v, %v; want found=false with a newer version", again, err)
	}
}

func TestEmptyValueIsAValue(t *testing.T) {
	node := servertest.Start(t, nil)
	kv := dial(t, node.Addr)
	if _, err := kv.Put(ctx(t), &dkvv1.PutRequest{Key: []byte("k")}); err != nil {
		t.Fatal(err)
	}
	get, err := kv.Get(ctx(t), &dkvv1.GetRequest{Key: []byte("k")})
	if err != nil || !get.GetFound() || len(get.GetValue()) != 0 {
		t.Fatalf("Get = %v, %v; want found with empty value", get, err)
	}
}

func TestBinaryKeysAndValues(t *testing.T) {
	node := servertest.Start(t, nil)
	kv := dial(t, node.Addr)
	key := []byte{0, 0xff, '\n', 0}
	val := bytes.Repeat([]byte{0, 1, 0xfe}, 100)
	if _, err := kv.Put(ctx(t), &dkvv1.PutRequest{Key: key, Value: val}); err != nil {
		t.Fatal(err)
	}
	get, err := kv.Get(ctx(t), &dkvv1.GetRequest{Key: key})
	if err != nil || !bytes.Equal(get.GetValue(), val) {
		t.Fatalf("binary round trip failed: %v", err)
	}
}

func TestRequestLimits(t *testing.T) {
	node := servertest.Start(t, func(c *config.Config) {
		c.Limits.MaxKeyBytes = 8
		c.Limits.MaxValueBytes = 16
	})
	kv := dial(t, node.Addr)
	c := ctx(t)
	ok8, long9 := []byte("12345678"), []byte("123456789")
	ok16, long17 := bytes.Repeat([]byte("v"), 16), bytes.Repeat([]byte("v"), 17)

	for _, tc := range []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"put at limits", func() error { _, err := kv.Put(c, &dkvv1.PutRequest{Key: ok8, Value: ok16}); return err }, codes.OK},
		{"put empty key", func() error { _, err := kv.Put(c, &dkvv1.PutRequest{Value: ok16}); return err }, codes.InvalidArgument},
		{"put long key", func() error { _, err := kv.Put(c, &dkvv1.PutRequest{Key: long9}); return err }, codes.InvalidArgument},
		{"put long value", func() error { _, err := kv.Put(c, &dkvv1.PutRequest{Key: ok8, Value: long17}); return err }, codes.InvalidArgument},
		{"get empty key", func() error { _, err := kv.Get(c, &dkvv1.GetRequest{}); return err }, codes.InvalidArgument},
		{"get long key", func() error { _, err := kv.Get(c, &dkvv1.GetRequest{Key: long9}); return err }, codes.InvalidArgument},
		{"delete empty key", func() error { _, err := kv.Delete(c, &dkvv1.DeleteRequest{}); return err }, codes.InvalidArgument},
		{"delete long key", func() error { _, err := kv.Delete(c, &dkvv1.DeleteRequest{Key: long9}); return err }, codes.InvalidArgument},
	} {
		if got := status.Code(tc.call()); got != tc.want {
			t.Errorf("%s: code %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestClosedEngineIsUnavailable(t *testing.T) {
	node := servertest.Start(t, nil)
	kv := dial(t, node.Addr)
	if err := node.DB.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := kv.Put(ctx(t), &dkvv1.PutRequest{Key: []byte("k")})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("Put on closed engine: %v, want Unavailable", err)
	}
	_, err = kv.Get(ctx(t), &dkvv1.GetRequest{Key: []byte("k")})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("Get on closed engine: %v, want Unavailable", err)
	}
	_, err = kv.Delete(ctx(t), &dkvv1.DeleteRequest{Key: []byte("k")})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("Delete on closed engine: %v, want Unavailable", err)
	}
}

func TestStopRefusesNewRequests(t *testing.T) {
	node := servertest.Start(t, nil)
	kv := dial(t, node.Addr)
	if _, err := kv.Put(ctx(t), &dkvv1.PutRequest{Key: []byte("k")}); err != nil {
		t.Fatal(err)
	}
	node.Stop()
	_, err := kv.Get(ctx(t), &dkvv1.GetRequest{Key: []byte("k")})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("Get after Stop: %v, want Unavailable", err)
	}
}

// TestConcurrentClients runs one writer per key and a reader on each key.
// Each writer records the version of every value it writes. Readers check
// that every value they see was written by the writer at exactly that
// version, and that a reader never sees a key go backwards. Finally every
// key must hold its writer's last value.
func TestConcurrentClients(t *testing.T) {
	node := servertest.Start(t, nil)
	kv := dial(t, node.Addr)
	c := ctx(t)
	const writers, perWriter = 8, 50
	// versions[w][i] is the version of write i by writer w. Readers record
	// what they saw and are checked against versions after all goroutines end.
	versions := make([][]uint64, writers)
	type seen struct {
		value   int
		version uint64
	}
	observed := make([][]seen, writers)
	var wg sync.WaitGroup
	for w := range writers {
		versions[w] = make([]uint64, perWriter)
		key := []byte(fmt.Sprintf("w%d", w))
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				r, err := kv.Put(c, &dkvv1.PutRequest{Key: key, Value: []byte(fmt.Sprint(i))})
				if err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				versions[w][i] = r.GetVersion()
			}
		}()
		go func() {
			defer wg.Done()
			for range perWriter {
				r, err := kv.Get(c, &dkvv1.GetRequest{Key: key})
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if !r.GetFound() {
					continue
				}
				var i int
				if _, err := fmt.Sscan(string(r.GetValue()), &i); err != nil || i < 0 || i >= perWriter {
					t.Errorf("%s: value %q was never written", key, r.GetValue())
					return
				}
				observed[w] = append(observed[w], seen{i, r.GetVersion()})
			}
		}()
	}
	wg.Wait()
	for w := range writers {
		for j, o := range observed[w] {
			if o.version != versions[w][o.value] {
				t.Errorf("w%d: read value %d at version %d, but it was written at version %d",
					w, o.value, o.version, versions[w][o.value])
			}
			if j > 0 && o.value < observed[w][j-1].value {
				t.Errorf("w%d: reader saw value %d after %d", w, o.value, observed[w][j-1].value)
			}
		}
		r, err := kv.Get(c, &dkvv1.GetRequest{Key: []byte(fmt.Sprintf("w%d", w))})
		if err != nil || string(r.GetValue()) != fmt.Sprint(perWriter-1) || r.GetVersion() != versions[w][perWriter-1] {
			t.Errorf("w%d = %q@%d, %v; want %d@%d", w, r.GetValue(), r.GetVersion(), err, perWriter-1, versions[w][perWriter-1])
		}
		for i := 1; i < perWriter; i++ {
			if versions[w][i] <= versions[w][i-1] {
				t.Errorf("w%d: write %d got version %d, not above %d", w, i, versions[w][i], versions[w][i-1])
			}
		}
	}
}

// TestRunReturnsServeError checks that Run reports a listener failure and
// leaves no goroutines behind (goleak runs after the package's tests).
func TestRunReturnsServeError(t *testing.T) {
	cfg := config.Default()
	cfg.Server.GRPCAddr = "127.0.0.1:0"
	lis, err := net.Listen("tcp", cfg.Server.GRPCAddr)
	if err != nil {
		t.Fatal(err)
	}
	_ = lis.Close() // Accept now fails permanently
	db := lsm.OpenInMemory()
	defer func() { _ = db.Close() }()
	srv := server.New(&cfg, server.NewStandaloneBackend(db), server.StaticStatus{}, slog.New(slog.DiscardHandler))
	if err := srv.Run(context.Background(), lis); err == nil {
		t.Fatal("Run on a closed listener returned nil")
	}
}
