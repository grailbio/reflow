// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package repository

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"testing"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	. "github.com/grailbio/reflow/repository/testutil"
)

type nilRepository struct{}

func (nilRepository) Stat(context.Context, digest.Digest) (reflow.File, error) {
	panic("not implemented")
}

func (nilRepository) Get(context.Context, digest.Digest) (io.ReadCloser, error) {
	panic("not implemented")
}

func (nilRepository) Put(context.Context, io.Reader) (digest.Digest, error) {
	panic("not implemented")
}

func (nilRepository) WriteTo(context.Context, digest.Digest, *url.URL) error {
	panic("not implemented")
}

func (nilRepository) ReadFrom(context.Context, digest.Digest, *url.URL) error {
	panic("not implemented")
}

func (nilRepository) Collect(context.Context, reflow.Liveset) error {
	panic("not implemented")
}

func (nilRepository) URL() *url.URL {
	panic("not implemented")
}

func TestDial(t *testing.T) {
	var repo nilRepository
	const scheme = "testscheme"
	RegisterScheme(scheme, func(u *url.URL) (reflow.Repository, error) {
		if got, want := u.Scheme, "testscheme"; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		return repo, nil
	})
	defer UnregisterScheme(scheme)
	r, err := Dial(scheme + "://foobar")
	if err != nil {
		t.Fatal(err)
	}
	rr, ok := r.(nilRepository)
	if !ok {
		t.Fatal("expected nilRepository")
	}
	if got, want := rr, repo; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestTransfer(t *testing.T) {
	dst := NewExpectRepository(t, "dst://foobar")
	src := NewExpectRepository(t, "src://foobar")
	id := reflow.Digester.FromString("hello, world!")
	dst.Expect(RepositoryCall{T: CallReadFrom, ArgID: id, ArgURL: *src.URL()})
	if err := Transfer(context.Background(), dst, src, id); err != nil {
		t.Fatal(err)
	}
	src.Complete()
	dst.Complete()

	uerr := errors.E(errors.NotSupported)

	dst.Expect(RepositoryCall{
		T:        CallReadFrom,
		ArgID:    id,
		ArgURL:   *src.URL(),
		ReplyErr: uerr,
	})
	src.Expect(RepositoryCall{
		T:      CallWriteTo,
		ArgID:  id,
		ArgURL: *dst.URL(),
	})
	if err := Transfer(context.Background(), dst, src, id); err != nil {
		t.Fatal(err)
	}
	if err := src.Complete(); err != nil {
		t.Error(err)
	}
	if err := dst.Complete(); err != nil {
		t.Error(err)
	}

	const body = "hello, world!"
	dst.Expect(RepositoryCall{
		T:        CallReadFrom,
		ArgID:    id,
		ArgURL:   *src.URL(),
		ReplyErr: uerr,
	})
	src.Expect(RepositoryCall{
		T:        CallWriteTo,
		ArgID:    id,
		ArgURL:   *dst.URL(),
		ReplyErr: uerr,
	})
	src.Expect(RepositoryCall{
		T:               CallGet,
		ArgID:           id,
		ReplyReadCloser: ioutil.NopCloser(bytes.NewReader([]byte(body))),
	})
	dst.Expect(RepositoryCall{
		T:        CallPut,
		ArgBytes: []byte(body),
		ReplyID:  reflow.Digester.FromString(body),
	})
	if err := Transfer(context.Background(), dst, src, id); err != nil {
		t.Fatal(err)
	}
	if err := src.Complete(); err != nil {
		t.Error(err)
	}
	if err := dst.Complete(); err != nil {
		t.Error(err)
	}
}
