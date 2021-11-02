// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package testutil

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/repository"
)

// The following are useful constructors for testing.

// File returns a file object representing the given contents.
func File(contents string) reflow.File {
	return reflow.File{
		ID:   reflow.Digester.FromString(contents),
		Size: int64(len(contents)),
	}
}

func fileContents(files ...string) map[string]string {
	fc := make(map[string]string, len(files))
	for _, file := range files {
		var path, contents string
		parts := strings.SplitN(file, ":", 2)
		switch len(parts) {
		case 1:
			path = file
			contents = file
		case 2:
			path = parts[0]
			contents = parts[1]
		}
		fc[path] = contents
	}
	return fc
}

// Files returns a value comprising the given files with contents derived from
// their names.
func Files(files ...string) reflow.Fileset {
	var v reflow.Fileset
	v.Map = map[string]reflow.File{}
	for path, contents := range fileContents(files...) {
		v.Map[path] = File(contents)
	}
	return v
}

// WriteFiles writes the provided files into the repository r and
// returns a Fileset as in Files.
func WriteFiles(r reflow.Repository, files ...string) reflow.Fileset {
	for _, contents := range fileContents(files...) {
		_, err := r.Put(context.Background(), bytes.NewReader([]byte(contents)))
		if err != nil {
			panic(fmt.Sprintf("unexpected error writing to repository: %v", err))
		}
	}
	return Files(files...)
}

// WriteFile writes the provided contents into repository r and returns
// the corresponding Reflow file.
func WriteFile(r reflow.Repository, content string) reflow.File {
	d, err := r.Put(context.Background(), bytes.NewReader([]byte(content)))
	if err != nil {
		panic(fmt.Sprintf("unexpected error writing to repository: %v", err))
	}
	return reflow.File{ID: d, Size: int64(len(content))}
}

// WriteCache writes the provided files into the eval's repository and registers
// a Fileset cache assoc.
func WriteCache(e *flow.Eval, key digest.Digest, files ...string) {
	WriteCacheFileset(e, key, WriteFiles(e.Repository, files...))
}

// WriteCacheFileset writes the provided fileset into the eval's repository and registers
// a Fileset cache assoc.
func WriteCacheFileset(e *flow.Eval, key digest.Digest, fs reflow.Fileset) {
	fsid, err := repository.Marshal(context.Background(), e.Repository, &fs)
	if err != nil {
		unexpected(err)
	}
	if err := e.Assoc.Store(context.Background(), assoc.FilesetV2, key, fsid); err != nil {
		unexpected(err)
	}
}

// List constructs a list value.
func List(values ...reflow.Fileset) reflow.Fileset {
	return reflow.Fileset{List: values}
}
