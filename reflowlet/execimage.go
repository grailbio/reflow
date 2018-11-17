// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflowlet

import (
	"crypto"
	_ "crypto/sha256" // Needed for crypto.SHA256
	"io"
	"os"
	"path/filepath"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/errors"
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
		return "", errors.E("evalsymlinks: "+path, err)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", errors.E("absolute: "+path, err)
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
			return errors.E("image digest: "+path, err)
		}
		dw := digester.NewWriter()
		if _, err := io.Copy(dw, r); err != nil {
			return errors.E("image digest: "+path, err)
		}
		binaryDigest = dw.Digest()
		if err := r.Close(); err != nil {
			return errors.E("image digest: "+path, err)
		}
		return nil
	})
	return binaryDigest, err
}
