// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit integration

package server

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
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
	rtestutil "github.com/grailbio/reflow/test/testutil"
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
	files    map[digest.Digest]bool
	executor rtestutil.Executor
}

func (*testAlloc) Inspect(ctx context.Context) (pool.AllocInspect, error) {
	return pool.AllocInspect{}, nil
}

func (t *testAlloc) Load(ctx context.Context, repo *url.URL, fs reflow.Fileset) (reflow.Fileset, error) {
	file, err := fs.File()
	if err != nil {
		return reflow.Fileset{}, err
	}
	if !file.Equal(testFile) {
		return reflow.Fileset{}, errors.New(fmt.Sprintf("mismatch %s != %s", file.Digest(), testFile.Digest()))
	}
	file.ID = testDigest
	fs.Map["."] = file
	t.files[testDigest] = true
	return fs, nil
}

// VerifyIntegrity verifies the integrity of the given set of files
func (t *testAlloc) VerifyIntegrity(ctx context.Context, fs reflow.Fileset) error {
	// TODO(swami): Implement
	return nil
}

func (t *testAlloc) Unload(ctx context.Context, fs reflow.Fileset) error {
	file, _ := fs.File()
	if _, ok := t.files[file.ID]; !ok {
		return errors.New(fmt.Sprintf("%v not loaded", file.ID))
	}
	delete(t.files, file.ID)
	return nil
}

func (t *testAlloc) Get(ctx context.Context, id digest.Digest) (reflow.Exec, error) {
	return t.executor.Get(ctx, id)
}

func (t *testAlloc) Put(ctx context.Context, id digest.Digest, config reflow.ExecConfig) (reflow.Exec, error) {
	return t.executor.Put(ctx, id, config)
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
		testalloc := &testAlloc{files: make(map[digest.Digest]bool)}
		testalloc.executor.Init()
		t.testalloc = testalloc
		exec, err := t.testalloc.Put(ctx, reflow.Digester.FromString("testexec"), reflow.ExecConfig{})
		exec.(*rtestutil.Exec).Ok(reflow.Result{})
		if err != nil {
			return nil, err
		}
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

func TestEndToEnd(t *testing.T) {
	srv := httptest.NewServer(rest.Handler(NewNode(&testPool{}), nil))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{"logs", "stdout", "stderr"} {
		client := rest.NewClient(nil, u, nil)
		ctx := context.Background()
		path := fmt.Sprintf("/v1/allocs/testalloc/execs/%s/%s?follow=true", reflow.Digester.FromString("testexec"), output)
		call := client.Call("GET", path)
		code, err := call.Do(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := code, http.StatusOK; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		var m string
		if err := call.Unmarshal(&m); err != nil {
			t.Fatal(err)
		}
		if got, want := m, "following"; got != want {
			t.Errorf("got %s, want %s", got, want)
		}
		call.Close()
	}
}
