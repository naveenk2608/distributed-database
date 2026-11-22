package console

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	dkvv1 "github.com/naveenk2608/distributed-database/gen/dkv/v1"
	"github.com/naveenk2608/distributed-database/internal/config"
)

// Modes of a fakeNode.
const (
	answer = iota // serve the admin API
	hang          // never answer: the poll times out
	fail          // answer 500
	hangUp        // the connection closes without an answer
)

// fakeNode is the console's HTTP transport, answering as one node's admin
// HTTP API would (a test double at the transport seam). An answer takes no
// time, so only a hanging node meets the poll's deadline.
type fakeNode struct {
	mode    atomic.Int32
	started int64
}

func (f *fakeNode) RoundTrip(r *http.Request) (*http.Response, error) {
	switch f.mode.Load() {
	case hang:
		<-r.Context().Done()
		return nil, r.Context().Err()
	case fail:
		return f.respond(r, http.StatusInternalServerError, []byte("broken")), nil
	case hangUp:
		return nil, io.ErrUnexpectedEOF
	}
	var m proto.Message
	switch r.URL.Path {
	case "/v1/stats":
		m = &dkvv1.StatsResponse{NodeId: "n1", StartedUnixNano: f.started, UnixNano: time.Now().UnixNano()}
	case "/v1/status":
		m = &dkvv1.ClusterStatusResponse{}
	case "/v1/events":
		m = &dkvv1.EventsResponse{}
	default:
		return f.respond(r, http.StatusNotFound, nil), nil
	}
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil, err
	}
	return f.respond(r, http.StatusOK, b), nil
}

func (f *fakeNode) respond(r *http.Request, code int, body []byte) *http.Response {
	return &http.Response{StatusCode: code, Status: fmt.Sprintf("%d %s", code, http.StatusText(code)),
		Body: io.NopCloser(bytes.NewReader(body)), Header: http.Header{}, Request: r}
}

// pollFake returns a console polling one fake node, with polls that time
// out after 100 ms and a node shown down after three timed-out polls.
func pollFake(t *testing.T) (*Console, *fakeNode) {
	t.Helper()
	f := &fakeNode{started: time.Now().UnixNano()}
	cfg := config.DefaultConsole().Console
	cfg.PollInterval, cfg.PollTimeout, cfg.DownAfterMisses = time.Second, 100*time.Millisecond, 3
	cfg.Nodes = []string{"node-1:7200"}
	c := New(cfg, nil, slog.New(slog.DiscardHandler))
	c.http = &http.Client{Timeout: cfg.PollTimeout, Transport: f}
	return c, f
}

// shown polls once and returns the node as the overview shows it.
func shown(t *testing.T, c *Console) NodeView {
	t.Helper()
	c.PollOnce(context.Background())
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.overview.Nodes) != 1 {
		t.Fatalf("overview has %d nodes; want 1", len(c.overview.Nodes))
	}
	return c.overview.Nodes[0]
}

// TestTimedOutPollsKeepANodeUp: a node that was up stays up, showing its
// last answer, while fewer than console.down_after_misses polls in a row
// time out, and shows down at the last of them; an answer resets the count.
// A node never seen up shows down after its first failed poll.
func TestTimedOutPollsKeepANodeUp(t *testing.T) {
	c, f := pollFake(t)
	up := shown(t, c)
	if !up.Up || up.NodeID != "n1" || !up.StartedAt.Equal(time.Unix(0, f.started)) {
		t.Fatalf("answering node shown as %+v", up)
	}
	f.mode.Store(hang)
	for miss := 1; miss <= 2; miss++ {
		if n := shown(t, c); !n.Up || n.Error != "" || !n.StartedAt.Equal(up.StartedAt) || n.LastSeen != up.LastSeen {
			t.Fatalf("after %d timed-out polls of 3: %+v; want it up with its last answer", miss, n)
		}
	}
	if n := shown(t, c); n.Up || n.Error == "" || !n.StartedAt.IsZero() {
		t.Fatalf("after 3 timed-out polls of 3: %+v; want it down, with the error and no numbers", n)
	}

	f.mode.Store(answer)
	if n := shown(t, c); !n.Up {
		t.Fatalf("answering again: %+v", n)
	}
	f.mode.Store(hang)
	for miss := 1; miss <= 2; miss++ {
		if n := shown(t, c); !n.Up {
			t.Fatalf("after an answer and %d timed-out polls: %+v; the count should have started again", miss, n)
		}
	}

	never, f2 := pollFake(t)
	f2.mode.Store(hang)
	if n := shown(t, never); n.Up || n.Error == "" {
		t.Fatalf("a node that never answered, after a timed-out poll: %+v; want it down", n)
	}
}

// TestFailedPollShowsNodeDownAtOnce: an error status, or a connection
// closed without an answer, shows an up node as down after one poll.
func TestFailedPollShowsNodeDownAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode int32
	}{{"error status", fail}, {"connection closed", hangUp}} {
		t.Run(tc.name, func(t *testing.T) {
			c, f := pollFake(t)
			if n := shown(t, c); !n.Up {
				t.Fatalf("answering node shown as %+v", n)
			}
			f.mode.Store(tc.mode)
			if n := shown(t, c); n.Up || n.Error == "" {
				t.Fatalf("after one failed poll: %+v; want it down with the error", n)
			}
		})
	}
}

// TestOnlyTimeouts: which poll errors count as missed polls.
func TestOnlyTimeouts(t *testing.T) {
	refused := &url.Error{Op: "Get", URL: "http://a/v1/stats", Err: &net.OpError{Op: "dial", Net: "tcp",
		Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}}
	deadline := &url.Error{Op: "Get", URL: "http://a/v1/stats", Err: context.DeadlineExceeded}
	dialTimeout := &net.OpError{Op: "dial", Net: "tcp", Err: timeoutErr{}}
	for _, tc := range []struct {
		name string
		errs []error
		want bool
	}{
		{"no error", []error{nil, nil}, false},
		{"deadline", []error{deadline}, true},
		{"dial timeout", []error{dialTimeout}, true},
		{"deadline and success", []error{nil, deadline}, true},
		{"refused", []error{refused}, false},
		{"refused and deadline", []error{deadline, refused}, false},
		{"unknown host", []error{&net.DNSError{Err: "no such host", Name: "n2", IsNotFound: true}}, false},
	} {
		if got := onlyTimeouts(tc.errs); got != tc.want {
			t.Errorf("%s: onlyTimeouts = %v; want %v", tc.name, got, tc.want)
		}
	}
}

// timeoutErr is a net.Error that timed out.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }
