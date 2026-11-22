// Package porttest gives tests loopback TCP addresses that nothing else
// takes from under them.
//
// A port found by listening on port 0 and closing the listener comes from
// the kernel's ephemeral range, which every outgoing connection also draws
// its local port from, so it can be taken before the test binds it again.
// On a machine with a small range (4,096 ports on the WSL2 machine these
// tests were written on) that happens often enough to fail tests.
//
// Servers in tests should therefore listen on 127.0.0.1:0 and report the
// address they got, or be handed a listener that is already open. When an
// address has to be chosen before it is bound (a cluster's peer list, or a
// node restarted on the address it had), Pick and Listen choose a port
// outside the ephemeral range, where only another deliberate listener can
// collide, and the caller retries when one does (InUse).
package porttest

import (
	"errors"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// lowest is the lowest port chosen: below it are well-known and commonly
// registered ports that development services listen on.
const lowest = 10000

// attempts bounds the ports tried before giving up.
const attempts = 100

// EphemeralRange returns the kernel's range for the local ports of outgoing
// connections: /proc/sys/net/ipv4/ip_local_port_range on Linux, otherwise
// the IANA dynamic range (49152–65535).
func EphemeralRange() (lo, hi int) {
	if b, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range"); err == nil {
		if f := strings.Fields(string(b)); len(f) == 2 {
			l, err1 := strconv.Atoi(f[0])
			h, err2 := strconv.Atoi(f[1])
			if err1 == nil && err2 == nil && 0 < l && l <= h && h <= 65535 {
				return l, h
			}
		}
	}
	return 49152, 65535
}

// randomPort returns a random port of at least lowest that is outside the
// ephemeral range.
func randomPort() int {
	lo, hi := EphemeralRange()
	type span struct{ from, to int }
	var spans []span
	if lowest <= lo-1 {
		spans = append(spans, span{lowest, lo - 1})
	}
	if from := max(hi+1, lowest); from <= 65535 {
		spans = append(spans, span{from, 65535})
	}
	total := 0
	for _, s := range spans {
		total += s.to - s.from + 1
	}
	if total == 0 { // the ephemeral range covers everything above lowest
		return lowest + rand.IntN(65536-lowest)
	}
	n := rand.IntN(total)
	for _, s := range spans {
		size := s.to - s.from + 1
		if n < size {
			return s.from + n
		}
		n -= size
	}
	panic("porttest: port index outside every span")
}

// InUse reports whether err says the address is already in use.
func InUse(err error) bool { return errors.Is(err, syscall.EADDRINUSE) }

// Listen binds a loopback port outside the ephemeral range, trying another
// port when one is in use, and returns the open listener. A server handed
// it can be restarted on the same address with Relisten.
func Listen(t testing.TB) net.Listener {
	t.Helper()
	var last error
	for range attempts {
		l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(randomPort())))
		if err == nil {
			return l
		}
		if !InUse(err) {
			t.Fatalf("porttest: listen: %v", err)
		}
		last = err
	}
	t.Fatalf("porttest: no free port in %d attempts: %v", attempts, last)
	return nil
}

// Pick returns a loopback address whose port is outside the ephemeral
// range and was free when Pick checked it, for a process that binds it
// itself. Another listener may still take it first; the caller then
// retries with a new Pick (InUse tells it the bind failed that way).
func Pick(t testing.TB) string {
	t.Helper()
	l := Listen(t)
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("porttest: %v", err)
	}
	return addr
}

// Relisten binds addr again, for a server restarted on the address it had,
// retrying while the port is in use for up to timeout: the old listener's
// port can stay busy for a moment after it closes.
func Relisten(t testing.TB, addr string, timeout time.Duration) net.Listener {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		l, err := net.Listen("tcp", addr)
		if err == nil {
			return l
		}
		if !InUse(err) || time.Now().After(deadline) {
			t.Fatalf("porttest: rebind %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Refusing returns the address of a listener that accepts each connection
// and closes it at once, like a node whose process is gone. Unlike the
// address of a closed listener, nothing else can start listening there.
// It stops when the test ends.
func Refusing(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("porttest: %v", err)
	}
	done := make(chan struct{})
	go func() { // owned by the test; ends when the listener closes
		defer close(done)
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() {
		_ = l.Close()
		<-done
	})
	return l.Addr().String()
}

// Silent returns the address of a listener that never accepts: the kernel
// completes TCP connections to it, and nothing ever answers, like a hung
// process or a stopped container behind a port proxy. It stops when the
// test ends.
func Silent(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("porttest: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l.Addr().String()
}
