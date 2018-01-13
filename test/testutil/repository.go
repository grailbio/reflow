// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package testutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"reflect"
	"sync"
	"testing"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
)

//go:generate stringer -type=RepositoryCallKind

// InmemoryRepository is an in-memory repository used for testing.
type inmemoryRepository struct {
	mu    sync.Mutex
	files map[digest.Digest][]byte
}

// NewInmemoryRepository returns a new repository that stores objects
// in memory.
func NewInmemoryRepository() reflow.Repository {
	return &inmemoryRepository{
		files: map[digest.Digest][]byte{},
	}
}

func (r *inmemoryRepository) get(k digest.Digest) []byte {
	r.mu.Lock()
	b := r.files[k]
	r.mu.Unlock()
	return b
}

func (r *inmemoryRepository) set(k digest.Digest, b []byte) {
	r.mu.Lock()
	r.files[k] = b
	r.mu.Unlock()
}

// Stat returns metadata for the blob named by id.
func (r *inmemoryRepository) Stat(_ context.Context, id digest.Digest) (reflow.File, error) {
	b := r.get(id)
	if b == nil {
		return reflow.File{}, errors.E("stat", id, errors.NotExist)
	}
	return reflow.File{ID: id, Size: int64(len(b))}, nil
}

// Get returns the blob named by id.
func (r *inmemoryRepository) Get(_ context.Context, id digest.Digest) (io.ReadCloser, error) {
	b := r.get(id)
	if b == nil {
		return nil, errors.E("get", id, errors.NotExist)
	}
	return ioutil.NopCloser(bytes.NewReader(b)), nil
}

// Put installs the blob rd and returns its digest.
func (r *inmemoryRepository) Put(_ context.Context, rd io.Reader) (digest.Digest, error) {
	b, err := ioutil.ReadAll(rd)
	if err != nil {
		return digest.Digest{}, err
	}
	id := reflow.Digester.FromBytes(b)
	r.set(id, b)
	return id, nil
}

// Collect removes any object not in the liveset.
func (r *inmemoryRepository) Collect(_ context.Context, live reflow.Liveset) error {
	r.mu.Lock()
	for k := range r.files {
		if !live.Contains(k) {
			delete(r.files, k)
		}
	}
	r.mu.Unlock()
	return nil
}

// WriteTo is not supported.
func (r *inmemoryRepository) WriteTo(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("writeto", id, u.String(), errors.NotSupported)
}

// ReadFrom is not supported.
func (r *inmemoryRepository) ReadFrom(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("readfrom", id, u.String(), errors.NotSupported)
}

// URL returns a nil URL.
func (r *inmemoryRepository) URL() *url.URL {
	return nil
}

// RepositoryCallKind indicates the type of repository call.
type RepositoryCallKind int

// The concrete types of repository calls.
const (
	RepositoryGet RepositoryCallKind = iota
	RepositoryPut
	RepositoryWriteTo
	RepositoryReadFrom
)

// RepositoryCall describes a single call to a Repository: its
// expected arguments, and a reply. Repository calls are used with an
// ExpectRepository.
type RepositoryCall struct {
	Kind         RepositoryCallKind
	ArgID        digest.Digest
	ArgURL       url.URL
	ArgBytes     []byte
	ArgReadError error

	ReplyID         digest.Digest
	ReplyErr        error
	ReplyReadCloser io.ReadCloser
}

// ExpectRepository is a Repository implementation used for
// testing; it takes a script of expected calls and replies. Violations
// are reported to the testing.T instance.
type ExpectRepository struct {
	*testing.T                  // the testing instance to use
	RepoURL    *url.URL         // the repository URL
	calls      []RepositoryCall // the script of repository calls
}

// NewExpectRepository creates a new testing repository.
func NewExpectRepository(t *testing.T, rawurl string) *ExpectRepository {
	u, err := url.Parse(rawurl)
	if err != nil {
		t.Fatal(err)
	}
	return &ExpectRepository{T: t, RepoURL: u}
}

// Expect adds a call to the repository's script.
func (r *ExpectRepository) Expect(call RepositoryCall) {
	r.calls = append(r.calls, call)
}

// Complete should be called when testing is finished; it verifies
// that the entire script has been exhausted.
func (r *ExpectRepository) Complete() error {
	if n := len(r.calls); n != 0 {
		return fmt.Errorf("finished with %d calls remaining", n)
	}
	return nil
}

func (r *ExpectRepository) call(c *RepositoryCall) error {
	if len(r.calls) == 0 {
		return fmt.Errorf("unexpected call %v", c)
	}
	expect := r.calls[0]
	r.calls = r.calls[1:]
	if got, want := c.Kind, expect.Kind; got != want {
		return fmt.Errorf("got %v, want %v", got, want)
	}
	if got, want := c.ArgID, expect.ArgID; got != want {
		return fmt.Errorf("got %v, want %v", got, want)
	}
	if got, want := c.ArgURL, expect.ArgURL; got != want {
		return fmt.Errorf("got %v, want %v", got, want)
	}
	if got, want := c.ArgBytes, expect.ArgBytes; !reflect.DeepEqual(got, want) {
		return fmt.Errorf("got %v, want %v", got, want)
	}
	*c = expect
	return nil
}

// Stat always returns a NotExist error.
func (r *ExpectRepository) Stat(_ context.Context, id digest.Digest) (reflow.File, error) {
	return reflow.File{}, errors.E("stat", id, errors.NotExist)
}

// Get implements the repository's Get call.
func (r *ExpectRepository) Get(_ context.Context, id digest.Digest) (io.ReadCloser, error) {
	call := RepositoryCall{Kind: RepositoryGet, ArgID: id}
	if err := r.call(&call); err != nil {
		return nil, err
	}
	return call.ReplyReadCloser, call.ReplyErr
}

// Put implements the repository's Put call.
func (r *ExpectRepository) Put(_ context.Context, body io.Reader) (digest.Digest, error) {
	call := RepositoryCall{Kind: RepositoryPut}
	call.ArgBytes, call.ArgReadError = ioutil.ReadAll(body)
	if err := r.call(&call); err != nil {
		return digest.Digest{}, err
	}
	return call.ReplyID, call.ReplyErr
}

// WriteTo implements the repository's WriteTo call.
func (r *ExpectRepository) WriteTo(_ context.Context, id digest.Digest, u *url.URL) error {
	call := RepositoryCall{Kind: RepositoryWriteTo, ArgID: id, ArgURL: *u}
	if err := r.call(&call); err != nil {
		return err
	}
	return call.ReplyErr
}

// ReadFrom implements the repository's ReadFrom call.
func (r *ExpectRepository) ReadFrom(_ context.Context, id digest.Digest, u *url.URL) error {
	call := RepositoryCall{Kind: RepositoryReadFrom, ArgID: id, ArgURL: *u}
	if err := r.call(&call); err != nil {
		return err
	}
	return call.ReplyErr
}

// Collect is not supported for the expect repository.
func (*ExpectRepository) Collect(context.Context, reflow.Liveset) error {
	return errors.E("collect", errors.NotSupported)
}

// URL returns the repository's URL.
func (r *ExpectRepository) URL() *url.URL { return r.RepoURL }

// panicRepository is an unimplemented Repository.
type panicRepository struct{}

func (*panicRepository) Stat(context.Context, digest.Digest) (reflow.File, error) {
	panic("not implemented")
}
func (*panicRepository) Get(context.Context, digest.Digest) (io.ReadCloser, error) {
	panic("not implemented")
}
func (*panicRepository) Put(context.Context, io.Reader) (digest.Digest, error) {
	panic("not implemented")
}
func (*panicRepository) WriteTo(context.Context, digest.Digest, *url.URL) error {
	panic("not implemented")
}
func (*panicRepository) ReadFrom(context.Context, digest.Digest, *url.URL) error {
	panic("not implemented")
}
func (*panicRepository) Collect(context.Context, reflow.Liveset) error {
	panic("not implemented")
}
func (*panicRepository) URL() *url.URL { panic("not implemented") }

// PanicRepository is an unimplemented repository.
// It panics on each call.
var PanicRepository reflow.Repository = &panicRepository{}
