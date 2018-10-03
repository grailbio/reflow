// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package blobrepo

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob/testblob"
)

const bucket = "test"

func newTestRepository(t *testing.T) *Repository {
	t.Helper()
	store := testblob.New("test")
	bucket, err := store.Bucket(context.Background(), "repo")
	if err != nil {
		t.Fatal(err)
	}
	return &Repository{Bucket: bucket}
}

func TestRepository(t *testing.T) {
	ctx := context.Background()
	r := newTestRepository(t)
	const content = "hello, world"
	id, err := r.Put(ctx, bytes.NewReader([]byte(content)))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := id, reflow.Digester.FromString(content); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	file, err := r.Stat(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := file, (reflow.File{ID: id, Size: int64(len(content))}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	rc, err := r.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), content; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestPutGetFile(t *testing.T) {
	ctx := context.Background()
	r := newTestRepository(t)
	const content = "another piece of content"
	file := reflow.File{
		ID:   reflow.Digester.FromString(content),
		Size: int64(len(content)),
	}
	if err := r.PutFile(ctx, file, bytes.NewReader([]byte(content))); err != nil {
		t.Fatal(err)
	}
	f, err := ioutil.TempFile("", "s3test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	n, err := r.GetFile(ctx, file.ID, f)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := n, file.Size; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), content; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAbbrevGet(t *testing.T) {
	ctx := context.Background()
	r := newTestRepository(t)
	content := []byte("hello world")
	id, err := r.Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	id.Truncate(4)
	if !id.IsAbbrev() {
		panic("!id.IsAbbrev")
	}
	rc, err := r.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	p, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := p, content; bytes.Compare(got, want) != 0 {
		t.Errorf("got %v, want %v", got, want)
	}
}
