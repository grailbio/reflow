// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit integration

package server

import (
	"context"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/local/testutil"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/pool/client"
	"github.com/grailbio/reflow/rest"
)

func TestClientServer(t *testing.T) {
	p, cleanup := testutil.NewTestPoolOrSkip(t)
	defer cleanup()
	srv := httptest.NewServer(rest.Handler(NewNode(p), log.Std))
	defer srv.Close()
	clientPool, err := client.New(srv.URL+"/v1/", srv.Client(), log.Std)
	if err != nil {
		t.Fatal(err)
	}
	testutil.TestPool(t, clientPool)
}

var (
	testFile   = reflow.File{Size: 123, Source: "test://test/test", ETag: "xyz"}
	testDigest = reflow.Digester.Rand(rand.New(rand.NewSource(0)))
)

type testAlloc struct {
	pool.Alloc
	files map[digest.Digest]bool
}

func (*testAlloc) Inspect(ctx context.Context) (pool.AllocInspect, error) {
	return pool.AllocInspect{}, nil
}

func (t *testAlloc) Load(ctx context.Context, repo *url.URL, fs reflow.Fileset) (reflow.Fileset, error) {
	if fs.N() != 1 || !fs.Map["."].Equal(testFile) {
		return reflow.Fileset{}, errors.New("unexpected fileset")
	}
	file := fs.Map["."]
	file.ID = testDigest
	fs.Map["."] = file
	t.files[testDigest] = true
	return fs, nil
}

func (t *testAlloc) Unload(ctx context.Context, fs reflow.Fileset) error {
	file := fs.Map["."]
	if _, ok := t.files[file.ID]; !ok {
		return errors.New(fmt.Sprintf("%v not loaded", file.ID))
	}
	delete(t.files, file.ID)
	return nil
}

type testPool struct {
	pool.Pool
	testalloc pool.Alloc
}

func (t *testPool) Alloc(ctx context.Context, id string) (pool.Alloc, error) {
	if id == "testalloc" {
		if t.testalloc != nil {
			return t.testalloc, nil
		}
		t.testalloc = &testAlloc{files: make(map[digest.Digest]bool)}
		return t.testalloc, nil
	}
	return nil, errors.E(errors.NotExist)
}

func TestClientServerLoad(t *testing.T) {
	srv := httptest.NewServer(rest.Handler(NewNode(&testPool{}), log.Std))
	defer srv.Close()
	clientPool, err := client.New(srv.URL+"/v1/", srv.Client(), log.Std)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	alloc, err := clientPool.Alloc(ctx, "testalloc")
	if err != nil {
		t.Fatal(err)
	}
	fs, err := alloc.Load(ctx, nil, reflow.Fileset{Map: map[string]reflow.File{".": testFile}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fs.N(), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	resolved := testFile
	resolved.ID = testDigest
	expect := reflow.Fileset{Map: map[string]reflow.File{".": resolved}}
	if got, want := fs, expect; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	err = alloc.Unload(ctx, fs)
	if err != nil {
		t.Fatal(err)
	}
	err = alloc.Unload(ctx, fs)
	if expected := errors.New(fmt.Sprintf("%v not loaded", testDigest)); err.Error() != expected.Error() {
		t.Fatal("expected: ", expected, " got:", err)
	}
}
