// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package execimage

import (
	"crypto"
	_ "crypto/sha256" // Needed for crypto.SHA256
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/sync/once"
)

var (
	digester     = digest.Digester(crypto.SHA256)
	binaryDigest digest.Digest
	digestOnce   once.Task
)

// ExecPath returns an absolute path to the executable of the current running process.
func ExecPath() (string, error) {
	// TODO(marius): use /proc/self/exe on Linux
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("evalsymlinks: %s %v", path, err)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("absolute: %s %v", path, err)
	}
	return path, nil
}

// ImageDigest returns the digest of the executable of the current running process.
func ImageDigest() (digest.Digest, error) {
	err := digestOnce.Do(func() error {
		path, err := ExecPath()
		if err != nil {
			return err
		}
		r, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("image digest: %s %v", path, err)
		}
		dw := digester.NewWriter()
		if _, err := io.Copy(dw, r); err != nil {
			return fmt.Errorf("image digest: %s %v", path, err)
		}
		binaryDigest = dw.Digest()
		if err := r.Close(); err != nil {
			return fmt.Errorf("image digest: %s %v", path, err)
		}
		return nil
	})
	return binaryDigest, err
}

// InstallImage reads a new image from its argument and replaces the current
// process with it. As a consequence, all state held by the caller is lost
// (pending requests, if any, etc) so its up to the caller to manage this interaction.
func InstallImage(exec io.ReadCloser, prefix string) error {
	f, err := ioutil.TempFile("", prefix)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, exec); err != nil {
		return err
	}
	if err := exec.Close(); err != nil {
		return err
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(path, 0755); err != nil {
		return err
	}
	log.Printf("exec %s %s", path, strings.Join(os.Args, " "))
	return syscall.Exec(path, os.Args, os.Environ())
}
