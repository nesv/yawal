// +build !windows

package wal

import (
	"os"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// checkDirPerms checks to see if name exists, is a directory, and that we
// have read and write permissions to it.
func checkDirPerms(name string) error {
	// Try to stat the path. If we can't get any info from it, this
	// usually means the path doesn't exit, or that we do not have
	// read access.
	fi, err := os.Stat(name)
	if err != nil {
		return errors.Wrap(err, "stat")
	}

	// Make sure the path refers to a directory.
	if !fi.IsDir() {
		return errors.Errorf("%s is not a directory", name)
	}

	// Can we write to the directory?
	if err := unix.Access(name, unix.W_OK); err != nil {
		return errors.Wrap(err, "check write permissions")
	}

	return nil
}
