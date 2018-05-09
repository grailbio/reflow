// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package walker

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/grailbio/testutil"
)

func TestWalker(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	testutil.CreateDirectoryTree(t, dir, 2, 2, 10)
	dirlist, filelist := testutil.ListRecursively(t, dir)
	dirs := map[string]bool{}
	for _, dir := range dirlist {
		dirs[dir] = true
	}
	files := map[string]bool{}
	for _, file := range filelist {
		files[file] = true
	}
	var w Walker
	w.Init(dir)
	for w.Scan() {
		if w.Info().IsDir() {
			if !dirs[w.Path()] {
				t.Fatalf("unexpected directory %q", w.Path())
			}
			delete(dirs, w.Path())
		} else {
			if !files[w.Path()] {
				t.Fatalf("unexpected file %q", w.Path())
			}
			delete(files, w.Path())
		}
	}
	if err := w.Err(); err != nil {
		t.Fatal(err)
	}
	if got, want := dirs, map[string]bool{}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := files, map[string]bool{}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestWalkerSymlinks(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	p := func(p ...string) string {
		p = append([]string{dir}, p...)
		return filepath.Join(p...)
	}
	if err := os.MkdirAll(p("dir"), 0777); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(p("dir", "file"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := os.Symlink(p("dir"), p("link")); err != nil {
		t.Fatal(err)
	}
	var w Walker
	w.Init(dir)
	var paths []string
	for w.Scan() {
		paths = append(paths, w.Relpath())
	}
	sort.Strings(paths)
	if got, want := paths, []string{".", "dir", "dir/file", "link", "link/file"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
