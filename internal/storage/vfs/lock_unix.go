//go:build unix

package vfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// lockFile takes a non-blocking exclusive flock on name. The kernel releases
// flock locks when the process exits for any reason, including SIGKILL, so a
// crashed engine never leaves a stale lock behind.
func lockFile(name string) (io.Closer, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close() // the flock error is the one worth reporting
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, name)
		}
		return nil, fmt.Errorf("vfs: lock %s: %w", name, err)
	}
	return f, nil // closing the descriptor releases the lock
}
