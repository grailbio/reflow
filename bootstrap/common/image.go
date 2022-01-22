// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package common

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"syscall"
)

const ExecImageErrPrefix = "bootstrap execimage"

// Image is an image binary to be installed and executed on the bootstrap.
type Image struct {
	Path string   `json:"path"`
	Name string   `json:"name"`
	Args []string `json:"args"`
}

// InstallImage reads a new image from its argument and replaces the current
// process with it. As a consequence, all state held by the caller is lost
// (pending requests, if any, etc) so its up to the caller to manage this interaction.
func InstallImage(image io.ReadCloser, args []string, imageName string) error {
	f, err := ioutil.TempFile("", imageName)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, image); err != nil {
		return err
	}
	if err := image.Close(); err != nil {
		return err
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(path, 0755); err != nil {
		return err
	}
	cmd := append([]string{path}, args...)
	if err = syscall.Exec(path, cmd, os.Environ()); err != nil {
		return fmt.Errorf("InstallImage %s: %v", strings.Join(cmd, " "), err)
	}
	return nil
}
