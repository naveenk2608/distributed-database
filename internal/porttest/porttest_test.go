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
// that an outgoing connection could be given, nor one below lowest.
func TestPortsStayOutOfTheEphemeralRange(t *testing.T) {
	lo, hi := EphemeralRange()
	t.Logf("ephemeral range %d–%d", lo, hi)
	for range 200 {
		l := Listen(t)
		p := port(t, l.Addr().String())
		_ = l.Close()
		if p < lowest || lo <= p && p <= hi {
			t.Fatalf("Listen gave port %d (ephemeral %d–%d, lowest %d)", p, lo, hi, lowest)
		}
		if p := port(t, Pick(t)); p < lowest || lo <= p && p <= hi {
			t.Fatalf("Pick gave port %d (ephemeral %d–%d, lowest %d)", p, lo, hi, lowest)
		}
	}
}

// TestRelistenAndInUse: a closed listener's address can be bound again,
// and binding an address in use is recognised.
func TestRelistenAndInUse(t *testing.T) {
	l := Listen(t)
	addr := l.Addr().String()
	if _, err := net.Listen("tcp", addr); !InUse(err) {
		t.Fatalf("second listener on %s: %v; want EADDRINUSE", addr, err)
	}
	_ = l.Close()
	l2 := Relisten(t, addr, 5*time.Second)
	if l2.Addr().String() != addr {
		t.Fatalf("Relisten bound %s, want %s", l2.Addr(), addr)
	}
	_ = l2.Close()
	if InUse(errors.New("other")) {
		t.Fatal("InUse accepted an unrelated error")
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

func timedOut(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
