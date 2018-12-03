// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package testutil provides functionality commonly used by tests.
package testutil

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
)

// Testing is a subset of testing.TB that allows an instance of testing.T to
// be passed without having to import testing into this non-test package.
type Testing interface {
	FailNow()
	Logf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

// TempDir is like ioutil.TempDir but intended for use from within tests.
// In particular, it will t.Fatal if it fails and returns a function that
// can be defer'ed by the caller to remove the newly created directory.
func TempDir(t Testing, dir, prefix string) (name string, cleanup func()) {
	d, err := ioutil.TempDir(dir, prefix)
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		t.Fatalf("%s:%d: TempDir(%v, %v): %v", filepath.Base(file), line, dir, prefix, err)
	}
	return d, func() {
		if err := os.RemoveAll(d); err != nil {
			t.Logf("TempDir RemoveAll %v: %s", d, err)
		}
	}
}

// CreateDirectoryTree creates a directory tree for use in tests. Parent
// specifies the root directory, depth the number of directory levels and
// fanout the number of directories at each level; files is the number of
// files to create at each level.
// Directories are named d0..n.
// Files are named f0..n and the contents of each file are its own name.
func CreateDirectoryTree(t Testing, parent string, depth, fanout, files int) {
	if depth > 0 && fanout == 0 {
		t.Fatalf("doesn't make sense to ask for more than one directory with zero fanout")
	}
	for f := 0; f < files; f++ {
		d := fmt.Sprintf("f%d", f)
		n := filepath.Join(parent, d)
		if err := ioutil.WriteFile(n, []byte(d), 0666); err != nil {
			t.Fatalf("failed to write %v: %v", n, err)
		}
	}
	if depth == 0 {
		return
	}
	for f := 0; f < fanout; f++ {
		path := filepath.Join(parent, fmt.Sprintf("d%d", f))
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatalf("failed to mkdir %v: %v", path, err)
		}
		CreateDirectoryTree(t, path, depth-1, fanout, files)
	}
}

// ListRecursively recursively lists the files and directories starting at
// parent.
func ListRecursively(t Testing, parent string) (dirs []string, files []string) {
	d, err := os.Open(parent)
	if err != nil {
		t.Fatalf("failed to open %v: %v", parent, err)
	}
	defer d.Close() // nolint: errcheck
	dirs = append(dirs, parent)

	entries, err := d.Readdir(-1)
	if err != nil {
		t.Fatalf("failed to readdir %v: %v", parent, err)
	}
	for _, fi := range entries {
		if fi.IsDir() {
			d, f := ListRecursively(t, filepath.Join(parent, fi.Name()))
			dirs = append(dirs, d...)
			files = append(files, f...)
		} else {
			files = append(files, filepath.Join(parent, fi.Name()))
		}
	}
	return
}
