package porttest

import (
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func port(t *testing.T, addr string) int {
	t.Helper()
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestPortsStayOutOfTheEphemeralRange: Listen and Pick never return a port
// that an outgoing connection could be given, here or (under mirrored
// networking) on a Windows host, nor one below lowest.
func TestPortsStayOutOfTheEphemeralRange(t *testing.T) {
	lo, hi := EphemeralRange()
	t.Logf("ephemeral range %d–%d", lo, hi)
	bad := func(p int) bool { return p < lowest || p >= dynamicLow || lo <= p && p <= hi }
	for range 200 {
		l := Listen(t)
		p := port(t, l.Addr().String())
		_ = l.Close()
		if bad(p) {
			t.Fatalf("Listen gave port %d (ephemeral %d–%d, allowed %d–%d)", p, lo, hi, lowest, dynamicLow-1)
		}
		if p := port(t, Pick(t)); bad(p) {
			t.Fatalf("Pick gave port %d (ephemeral %d–%d, allowed %d–%d)", p, lo, hi, lowest, dynamicLow-1)
		}
	}
}

// TestInUse: binding an address in use is recognised.
func TestInUse(t *testing.T) {
	l := Listen(t)
	defer func() { _ = l.Close() }()
	if _, err := net.Listen("tcp", l.Addr().String()); !InUse(err) {
		t.Fatalf("second listener on %s: %v; want EADDRINUSE", l.Addr(), err)
	}
	if InUse(errors.New("other")) {
		t.Fatal("InUse accepted an unrelated error")
	}
}

// refused reports whether a connection to addr is closed without an
// answer, as a kept socket between loans closes it.
func refused(t *testing.T, addr string) bool {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return true
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(make([]byte, 1))
	return err != nil && !timedOut(err)
}

// TestKeep: a kept socket refuses connections between loans and serves
// each loan; closing a loan ends a pending Accept without releasing the
// address, which nothing else can bind meanwhile.
func TestKeep(t *testing.T) {
	l := Listen(t)
	addr := l.Addr().String()
	k := Keep(t, l)
	if k.Addr().String() != addr || !refused(t, addr) {
		t.Fatalf("a kept socket before its first loan does not refuse connections at %s", addr)
	}
	for round := range 3 {
		ln := k.Lend()
		accepted := make(chan error, 1)
		go func() { // ends when the loan's Accept returns
			conn, err := ln.Accept()
			if err == nil {
				_, err = conn.Write([]byte("x"))
				_ = conn.Close()
			}
			accepted <- err
		}()
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		b := make([]byte, 1)
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(conn, b); err != nil || b[0] != 'x' {
			t.Fatalf("round %d: the loan did not serve the connection: %q, %v", round, b, err)
		}
		_ = conn.Close()
		if err := <-accepted; err != nil {
			t.Fatalf("round %d: Accept: %v", round, err)
		}

		pending := make(chan error, 1)
		go func() { // ends when Close ends the loan
			_, err := ln.Accept()
			pending <- err
		}()
		time.Sleep(20 * time.Millisecond) // let it block in Accept
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-pending; !errors.Is(err, net.ErrClosed) {
			t.Fatalf("round %d: Accept pending at Close: %v; want net.ErrClosed", round, err)
		}
		if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("round %d: Accept after Close: %v; want net.ErrClosed", round, err)
		}
		if !refused(t, addr) {
			t.Fatalf("round %d: between loans a connection was not refused", round)
		}
		if _, err := net.Listen("tcp", addr); !InUse(err) {
			t.Fatalf("round %d: listening on a kept address: %v; want EADDRINUSE", round, err)
		}
	}
}

// TestRefusingAndSilent: a refusing listener closes connections at once; a
// silent one completes them and never answers.
func TestRefusingAndSilent(t *testing.T) {
	conn, err := net.DialTimeout("tcp", Refusing(t), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(make([]byte, 1))
	if err == nil || timedOut(err) {
		t.Fatalf("read from a refusing listener: %v; want EOF or reset", err)
	}
	_ = conn.Close()

	conn, err = net.DialTimeout("tcp", Silent(t), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err := conn.Read(make([]byte, 1)); errors.Is(err, io.EOF) || !timedOut(err) {
		t.Fatalf("read from a silent listener: %v; want a timeout", err)
	}
}

// TestKeepClosesWithItsLastLoan: a loan still open when the test ends
// keeps the socket until the loan closes; then the address is free.
func TestKeepClosesWithItsLastLoan(t *testing.T) {
	var ln net.Listener
	var addr string
	t.Run("lend", func(t *testing.T) {
		l := Listen(t) // outside the ephemeral range: nothing takes it once freed
		addr = l.Addr().String()
		ln = Keep(t, l).Lend()
	})
	if _, err := net.Listen("tcp", addr); !InUse(err) {
		t.Fatalf("the socket closed while lent: %v", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("binding %s after the last loan closed: %v", addr, err)
	}
	_ = l.Close()
}
