// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package walker

import (
	"os"
	"path/filepath"
	"sort"
)

// Walker walks a recursive directory hierarchy, exposing a scanner-like interface.
type Walker struct {
	root string
	err  error
	path string
	info os.FileInfo
	todo []string
}

// Init initializes a walker to walk from a root path.
func (w *Walker) Init(root string) {
	w.root = root
	w.todo = append(w.todo, w.root)
}

// Scan advances the walker to the next entry in the hierarchy.
// It returns false either when the scan stops because we have
// reached the end of the input or else because there was error.
// After Scan returns, the Err method returns any error that occured
// during scanning.
func (w *Walker) Scan() bool {
again:
	if len(w.todo) == 0 || w.err != nil {
		return false
	}
	w.path, w.todo = w.todo[0], w.todo[1:]
	w.info, w.err = os.Stat(w.path)
	if os.IsNotExist(w.err) {
		w.err = nil
		goto again
	} else if w.err != nil {
		return false
	}
	if w.info.IsDir() {
		var paths []string
		paths, w.err = readDirNames(w.path)
		if w.err != nil {
			return false
		}
		for i := range paths {
			paths[i] = filepath.Join(w.path, paths[i])
		}
		w.todo = append(paths, w.todo...)
	}
	return true
}

// Path returns the most recent path that was scanned.
func (w *Walker) Path() string {
	return w.path
}

// Relpath returns the most recent path that was scanned, relative to
// the scan root directory.
func (w *Walker) Relpath() string {
	path, err := filepath.Rel(w.root, w.Path())
	if err != nil {
		panic("bad path")
	}
	return path
}

// Info returns the os.FileInfo for the most recent path scanned.
func (w *Walker) Info() os.FileInfo {
	return w.info
}

// Err returns the first error that occured while scanning.
func (w *Walker) Err() error {
	return w.err
}

// readDirNames reads the directory named by dirname and returns
// a sorted list of directory entries.
func readDirNames(dirname string) ([]string, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}
