// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit integration

package server

import (
	"context"
	"math/rand"
	"net/http/httptest"
	"testing"

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
}

func (*testAlloc) Inspect(ctx context.Context) (pool.AllocInspect, error) {
	return pool.AllocInspect{}, nil
}

func (*testAlloc) Load(ctx context.Context, fs reflow.Fileset) (reflow.Fileset, error) {
	if fs.N() != 1 || fs.Map["."] != testFile {
		return reflow.Fileset{}, errors.New("unexpected fileset")
	}
	file := fs.Map["."]
	file.ID = testDigest
	fs.Map["."] = file
	return fs, nil
}

type testPool struct {
	pool.Pool
}

func (t *testPool) Alloc(ctx context.Context, id string) (pool.Alloc, error) {
	if id == "testalloc" {
		return new(testAlloc), nil
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
	fs, err := alloc.Load(ctx, reflow.Fileset{Map: map[string]reflow.File{".": testFile}})
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
}
