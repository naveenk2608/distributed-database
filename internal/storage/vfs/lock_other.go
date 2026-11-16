//go:build !unix

package vfs

import (
	"errors"
	"io"
)

func lockFile(string) (io.Closer, error) {
	return nil, errors.New("vfs: file locking is only implemented on unix")
}
