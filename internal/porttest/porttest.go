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
// collide, and the caller retries when one does (InUse). They also stay
// below 49152: the IANA dynamic range is where Windows and macOS take the
// local ports of their own connections, and under WSL's mirrored
// networking Linux shares the Windows host's ports.
//
// A server that a test stops and starts again on the same address should
// not release the address at all: Keep holds its listening socket for the
// whole test and lends it to each start. Releasing and binding again would
// leave a window in which something else can take the port, and under
// mirrored networking a connection to a loopback port outside Linux's
// ephemeral range that nothing in Linux listens on goes to the Windows
// host, which drops it without an answer, so callers of a stopped server
// would hang instead of being refused.
package porttest

import (
	"errors"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// lowest is the lowest port chosen: below it are well-known and commonly
// registered ports that development services listen on.
const lowest = 10000

// dynamicLow is the start of the IANA dynamic range (49152–65535), which
// Windows and macOS use for their connections' local ports.
const dynamicLow = 49152

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

// randomPort returns a random port of at least lowest and below
// dynamicLow that is outside the ephemeral range.
func randomPort() int {
	lo, hi := EphemeralRange()
	type span struct{ from, to int }
	var spans []span
	if to := min(lo-1, dynamicLow-1); lowest <= to {
		spans = append(spans, span{lowest, to})
	}
	if from := max(hi+1, lowest); from <= dynamicLow-1 {
		spans = append(spans, span{from, dynamicLow - 1})
	}
	total := 0
	for _, s := range spans {
		total += s.to - s.from + 1
	}
	if total == 0 { // the ephemeral range covers every port we would choose
		return lowest + rand.IntN(dynamicLow-lowest)
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

// Kept is a listening socket held for a server that a test stops and
// starts again on the same address. Each start borrows the socket (Lend);
// the server's Close of the loan ends the loan, not the socket. Between
// loans the socket closes every connection it accepts, as a stopped
// process's port refuses them on a typical host, so callers fail at once.
type Kept struct {
	l        *net.TCPListener
	mu       sync.Mutex
	refusing func() // stops the refusing loop; nil while lent
	closed   bool   // the test ended; the socket closes when the loan does
}

// Keep holds l, a TCP listener, for the rest of the test, refusing
// connections until the first Lend. The socket is closed when the test
// ends, or when a loan still open then ends.
func Keep(t testing.TB, l net.Listener) *Kept {
	t.Helper()
	tl, ok := l.(*net.TCPListener)
	if !ok {
		t.Fatalf("porttest: Keep needs a TCP listener, got %T", l)
	}
	k := &Kept{l: tl}
	k.refusing = refuse(deadlineAcceptor{tl})
	t.Cleanup(func() {
		k.mu.Lock()
		defer k.mu.Unlock()
		k.closed = true
		if k.refusing != nil {
			k.refusing()
			k.refusing = nil
			_ = k.l.Close()
		}
	})
	return k
}

// Addr is the kept socket's address.
func (k *Kept) Addr() net.Addr { return k.l.Addr() }

// Lend stops refusing and returns a listener on the kept socket for one
// start of the server. Lend must not be called again before the loan is
// closed.
func (k *Kept) Lend() net.Listener {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.refusing != nil {
		k.refusing()
		k.refusing = nil
	}
	ln := &loan{k: k}
	ln.idle = sync.NewCond(&ln.mu)
	return ln
}

// loan is one server's use of a Kept socket.
type loan struct {
	k         *Kept
	mu        sync.Mutex
	idle      *sync.Cond // signalled when an Accept returns
	ended     bool
	accepting int
}

func (ln *loan) Addr() net.Addr { return ln.k.l.Addr() }

func (ln *loan) Accept() (net.Conn, error) {
	for {
		ln.mu.Lock()
		if ln.ended {
			ln.mu.Unlock()
			return nil, net.ErrClosed
		}
		ln.accepting++
		ln.mu.Unlock()
		conn, err := ln.k.l.Accept()
		ln.mu.Lock()
		ln.accepting--
		ended := ln.ended
		ln.idle.Broadcast()
		ln.mu.Unlock()
		switch {
		case ended:
			if conn != nil {
				_ = conn.Close() // accepted as the loan ended: refused
			}
			return nil, net.ErrClosed
		case err == nil:
			return conn, nil
		case !timedOut(err):
			return nil, err
		}
		// A deadline set to end an earlier loan or refusing loop: try again.
	}
}

// Close ends the loan: a pending Accept returns net.ErrClosed, and the
// socket goes back to refusing connections (or closes, if the test ended).
func (ln *loan) Close() error {
	ln.mu.Lock()
	if ln.ended {
		ln.mu.Unlock()
		return nil
	}
	ln.ended = true
	_ = ln.k.l.SetDeadline(time.Unix(1, 0)) // wakes a pending Accept
	for ln.accepting > 0 {
		ln.idle.Wait()
	}
	ln.mu.Unlock()
	_ = ln.k.l.SetDeadline(time.Time{})
	k := ln.k
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return k.l.Close()
	}
	k.refusing = refuse(deadlineAcceptor{k.l})
	return nil
}

// acceptor is what refuse accepts connections from.
type acceptor interface {
	Accept() (net.Conn, error)
	stop()   // makes a pending and every later Accept fail
	resume() // undoes stop, once no Accept is pending
}

// closer is an acceptor whose stop closes the listener for good.
type closer struct{ net.Listener }

func (c closer) stop()   { _ = c.Close() }
func (c closer) resume() {}

// deadlineAcceptor stops a kept socket's Accept with a deadline, leaving
// the socket open.
type deadlineAcceptor struct{ l *net.TCPListener }

func (d deadlineAcceptor) Accept() (net.Conn, error) { return d.l.Accept() }
func (d deadlineAcceptor) stop()                     { _ = d.l.SetDeadline(time.Unix(1, 0)) }
func (d deadlineAcceptor) resume()                   { _ = d.l.SetDeadline(time.Time{}) }

func timedOut(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// refuse accepts and closes connections from a until the returned
// function, which can be called more than once, stops a and waits.
func refuse(a acceptor) func() {
	done := make(chan struct{})
	var stopping atomic.Bool
	go func() { // owned by the returned function; ends when it stops a
		defer close(done)
		for {
			conn, err := a.Accept()
			if err == nil {
				_ = conn.Close()
				continue
			}
			if stopping.Load() || !timedOut(err) {
				return
			}
		}
	}()
	return sync.OnceFunc(func() {
		stopping.Store(true)
		a.stop()
		<-done
		a.resume()
	})
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
	t.Cleanup(refuse(closer{l}))
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
