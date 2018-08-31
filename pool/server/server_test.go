// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit integration

package server

import (
	"context"
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

var testFile = reflow.File{Size: 123, Source: "test://test/test", ETag: "xyz"}

type testAlloc struct {
	pool.Alloc
}

func (*testAlloc) Inspect(ctx context.Context) (pool.AllocInspect, error) {
	return pool.AllocInspect{}, nil
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
