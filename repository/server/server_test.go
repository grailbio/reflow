// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package server

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/bloomlive"
	"github.com/grailbio/reflow/repository/client"
	"github.com/grailbio/reflow/repository/file"
	. "github.com/grailbio/reflow/repository/testutil"
	"github.com/grailbio/reflow/rest"
	"github.com/willf/bloom"
	"grail.com/testutil"
)

const maxBlobSize = 1 << 20

func newFileRepository(t *testing.T) (*file.Repository, func()) {
	objects, cleanup := testutil.TempDir(t, "", "test-")
	return &file.Repository{Root: objects}, cleanup
}

func newBlob() ([]byte, digest.Digest) {
	b := make([]byte, rand.Int()%maxBlobSize)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b, reflow.Digester.FromBytes(b)
}

func TestClientServer(t *testing.T) {
	expect := NewExpectRepository(t, "http://srv")
	expectNode := Node{expect}
	srv := httptest.NewServer(rest.Handler(expectNode, nil))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	repo := client.Client{rest.NewClient(nil, u, nil)}
	ctx := context.Background()

	const hello = "hello, world!"
	id := reflow.Digester.FromString(hello)
	expect.Expect(RepositoryCall{
		T:               CallGet,
		ArgID:           id,
		ReplyReadCloser: ioutil.NopCloser(bytes.NewReader([]byte(hello))),
	})
	expect.Expect(RepositoryCall{
		T:        CallGet,
		ArgID:    id,
		ReplyErr: errors.Errorf("get %v", id),
	})

	rc, err := repo.Get(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	b, err := ioutil.ReadAll(rc)
	rc.Close()
	if err != nil {
		log.Fatal(err)
	}
	if got, want := string(b), hello; got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	rc, err = repo.Get(ctx, id)
	if err == nil {
		t.Error("expected error")
	}

	expect.Complete()
}

func TestClientServerFile(t *testing.T) {
	filerepo, cleanup := newFileRepository(t)
	defer cleanup()
	srv := httptest.NewServer(rest.Handler(Node{filerepo}, nil))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	repo := client.Client{rest.NewClient(nil, u, nil)}
	ctx := context.Background()

	b1, id1 := newBlob()
	id, err := repo.Put(ctx, ioutil.NopCloser(bytes.NewReader(b1)))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := id, id1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	rc, err := repo.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := b, b1; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	id2 := reflow.Digester.FromString("hello, world")
	_, err = repo.Get(ctx, id2)
	if !errors.Is(errors.NotExist, err) {
		t.Fatalf("expected NotExist, got %v", err)
	}
}

func TestClientServerCollect(t *testing.T) {
	filerepo, cleanup := newFileRepository(t)
	defer cleanup()
	srv := httptest.NewServer(rest.Handler(Node{filerepo}, nil))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	repo := client.Client{rest.NewClient(nil, u, nil)}
	ctx := context.Background()

	b1, id1 := newBlob()
	_, err = repo.Put(ctx, ioutil.NopCloser(bytes.NewReader(b1)))
	if err != nil {
		t.Fatal(err)
	}
	b2, id2 := newBlob()
	_, err = repo.Put(ctx, ioutil.NopCloser(bytes.NewReader(b2)))
	if err != nil {
		t.Fatal(err)
	}
	live := bloom.New(64, 2)
	live.Add(id1.Bytes())
	if err := repo.Collect(ctx, bloomlive.New(live)); err != nil {
		t.Fatal(err)
	}
	_, err = repo.Get(ctx, id2)
	if !errors.Is(errors.NotExist, err) {
		t.Errorf("expected NotExist, got %v", err)
	}
	body, err := repo.Get(ctx, id1)
	if err != nil {
		t.Error(err)
	} else {
		body.Close()
	}
}
