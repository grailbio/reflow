// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package testutil_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grailbio/testutil"
)

func TestTempDir(t *testing.T) {
	var dummy testutil.Testing = t
	_ = dummy
	mytest := &testutil.MockTB{}
	testutil.TempDir(mytest, "./does-not-exist", "doesn-not-exist-")
	if got, want := strings.Contains(mytest.Result, "no such file or directory"), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	dir, cleanup := testutil.TempDir(t, "", "my-prefix-")
	f, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !f.IsDir() {
		cleanup()
		t.Fatalf("%v is not a directory", dir)
	}
	cleanup()
	_, err = os.Stat(dir)
	if err == nil || !os.IsNotExist(err) {
		t.Fatalf("%v exists!", dir)
	}
}

func expectedCounts(depth, fanout, files int) (ndirs, nfiles int) {
	ndirs = 1
	if depth == 0 {
		return
	}
	exp := 1
	for i := 0; i < depth; i++ {
		exp *= fanout
		ndirs += exp
	}
	nfiles = ndirs * files
	return
}

func TestCreateDirTree(t *testing.T) {
	tmpdir, cleanup := testutil.TempDir(t, "", "create-tree-")
	defer cleanup()

	for i, c := range []struct{ depth, fanout, files int }{
		{0, 0, 0},
		{1, 1, 0},
		{2, 1, 0},
		{2, 3, 2},
	} {
		parent := filepath.Join(tmpdir, fmt.Sprintf("%d", i))
		if err := os.Mkdir(parent, 0777); err != nil {
			t.Fatalf("failed to mkdir %v: %v", parent, err)
		}
		testutil.CreateDirectoryTree(t, parent, c.depth, c.fanout, c.files)
		edirs, efiles := expectedCounts(c.depth, c.fanout, c.files)
		dirs, files := testutil.ListRecursively(t, parent)
		if got, want := len(dirs), edirs; got != want {
			t.Logf("dirs: %v\nfiles %v\n", dirs, files)
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
		if got, want := len(files), efiles; got != want {
			t.Logf("dirs: %v\nfiles %v\n", dirs, files)
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
		for _, n := range files {
			buf, err := ioutil.ReadFile(n)
			if err != nil {
				t.Fatalf("failed to read %v: %v", n, err)
			}
			if got, want := string(buf), filepath.Base(n); got != want {
				t.Logf("name: %v\ncontent %v\n", n, string(buf))
				t.Errorf("%d: got %v, want %v", i, got, want)
			}
		}
	}
}
