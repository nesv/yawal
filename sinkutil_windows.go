// +build windows

package wal

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

func checkDirPerms(name string) error {
	fi, err := os.Stat(name)
	if err != nil {
		return errors.Wrap(err, "stat")
	}

	if !fi.IsDir() {
		return errors.Errorf("%s is not a directory", name)
	}

	// Attempt to write a file, and remove it before returning.
	testFile := filepath.Join(name, "yawalwrchk")
	if err := os.Create(testFile); err != nil {
		return errors.Wrap(err, "no write perms?")
	}
	os.Remove(testFile)
	return nil
}
