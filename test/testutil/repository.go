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
	"log"
	"math/rand"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/liveset"
)

//go:generate stringer -type=RepositoryCallKind

// InmemoryRepository is an in-memory repository used for testing.
type InmemoryRepository struct {
	mu    sync.Mutex
	files map[digest.Digest][]byte
	url   *url.URL
}

var (
	inmemoryReposMapOnce sync.Once
	inmemoryReposMu      sync.Mutex
	inmemoryReposMap     map[string]*InmemoryRepository
)

// NewInmemoryRepository returns a new repository that stores objects
// in memory.
func NewInmemoryRepository(name string) *InmemoryRepository {
	inmemoryReposMapOnce.Do(func() {
		inmemoryReposMap = make(map[string]*InmemoryRepository)
	})
	if name == "" {
		name = fmt.Sprintf("%d", rand.Int63())
	}
	url, err := url.Parse(fmt.Sprint("inmemory://", name))
	if err != nil {
		log.Printf("url parse: %v", err)
		return nil
	}
	repo := &InmemoryRepository{
		files: map[digest.Digest][]byte{},
		url:   url,
	}
	inmemoryReposMu.Lock()
	inmemoryReposMap[name] = repo
	inmemoryReposMu.Unlock()
	return repo
}

func GetInMemoryRepository(repo *url.URL) *InmemoryRepository {
	inmemoryReposMu.Lock()
	defer inmemoryReposMu.Unlock()
	return inmemoryReposMap[repo.Host]
}

func (r *InmemoryRepository) get(k digest.Digest) []byte {
	r.mu.Lock()
	b := r.files[k]
	r.mu.Unlock()
	return b
}

func (r *InmemoryRepository) set(k digest.Digest, b []byte) {
	r.mu.Lock()
	r.files[k] = b
	r.mu.Unlock()
}

// Delete removes the key id from this repository.
func (r *InmemoryRepository) Delete(_ context.Context, id digest.Digest) {
	r.mu.Lock()
	delete(r.files, id)
	r.mu.Unlock()
}

// Stat returns metadata for the blob named by id.
func (r *InmemoryRepository) Stat(_ context.Context, id digest.Digest) (reflow.File, error) {
	b := r.get(id)
	if b == nil {
		return reflow.File{}, errors.E("stat", id, errors.NotExist)
	}
	return reflow.File{ID: id, Size: int64(len(b))}, nil
}

// Get returns the blob named by id.
func (r *InmemoryRepository) Get(_ context.Context, id digest.Digest) (io.ReadCloser, error) {
	b := r.get(id)
	if b == nil {
		return nil, errors.E("get", id, errors.NotExist)
	}
	return ioutil.NopCloser(bytes.NewReader(b)), nil
}

// Put installs the blob rd and returns its digest.
func (r *InmemoryRepository) Put(_ context.Context, rd io.Reader) (digest.Digest, error) {
	b, err := ioutil.ReadAll(rd)
	if err != nil {
		return digest.Digest{}, err
	}
	id := reflow.Digester.FromBytes(b)
	r.set(id, b)
	return id, nil
}

// CollectWithThreshold removes from this repository any objects not in the
// Liveset and whose creation times are not more recent than the
// threshold time.
func (r *InmemoryRepository) CollectWithThreshold(ctx context.Context, live liveset.Liveset, dead liveset.Liveset, threshold time.Time, dryRun bool) error {
	return errors.E("collectwiththreshold", errors.NotSupported)
}

// Collect removes any object not in the liveset.
func (r *InmemoryRepository) Collect(_ context.Context, live liveset.Liveset) error {
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
func (r *InmemoryRepository) WriteTo(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("writeto", id, u.String(), errors.NotSupported)
}

// ReadFrom is not supported.
func (r *InmemoryRepository) ReadFrom(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("readfrom", id, u.String(), errors.NotSupported)
}

// URL returns a nil URL.
func (r *InmemoryRepository) URL() *url.URL {
	return r.url
}

func (r *InmemoryRepository) RawFiles() map[digest.Digest][]byte {
	return r.files
}

// InmemoryRepository is an in-memory repository used for testing which also implements scheduler.blobLocator.
type InmemoryLocatorRepository struct {
	*InmemoryRepository
	locations map[digest.Digest]string
}

// NewInmemoryLocatorRepository returns a new repository that stores objects
// in memory.
func NewInmemoryLocatorRepository() *InmemoryLocatorRepository {
	return &InmemoryLocatorRepository{
		InmemoryRepository: NewInmemoryRepository(""),
		locations:          map[digest.Digest]string{},
	}
}

func (r *InmemoryLocatorRepository) SetLocation(k digest.Digest, loc string) {
	r.mu.Lock()
	r.locations[k] = loc
	r.mu.Unlock()
}

// Implement scheduler.blobLocator
func (r *InmemoryLocatorRepository) Location(ctx context.Context, id digest.Digest) (string, error) {
	loc := r.locations[id]
	if loc == "" {
		return "", errors.E(errors.NotExist, fmt.Errorf("unknown %v", id))
	}
	return loc, nil
}

// RepositoryCallKind indicates the type of repository call.
type RepositoryCallKind int

// The concrete types of repository calls.
const (
	RepositoryGet RepositoryCallKind = iota
	RepositoryPut
	RepositoryStat
	RepositoryWriteTo
	RepositoryReadFrom
	RepositoryGetFile
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
	ReplyFile       reflow.File
	ReplyErr        error
	ReplyReadCloser io.ReadCloser
	ReplyN          int64
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

// CollectWithThreshold removes from this repository any objects not in the
// Liveset and whose creation times are not more recent than the
// threshold time.
func (r *ExpectRepository) CollectWithThreshold(ctx context.Context, live liveset.Liveset, dead liveset.Liveset, threshold time.Time, dryRun bool) error {
	return errors.E("collectwiththreshold", errors.NotSupported)
}

// Collect is not supported for the expect repository.
func (*ExpectRepository) Collect(context.Context, liveset.Liveset) error {
	return errors.E("collect", errors.NotSupported)
}

// URL returns the repository's URL.
func (r *ExpectRepository) URL() *url.URL { return r.RepoURL }

// ExpectGetFilerRepository is an ExpectRepository which also implements GetFile.
type ExpectGetFilerRepository struct {
	*ExpectRepository
}

// GetFile implements the repository's GetFile call.
func (r *ExpectGetFilerRepository) GetFile(_ context.Context, id digest.Digest, w io.WriterAt) (int64, error) {
	call := RepositoryCall{Kind: RepositoryGetFile, ArgID: id}
	if err := r.call(&call); err != nil {
		return 0, err
	}
	return call.ReplyN, call.ReplyErr
}

type repositoryCallKey struct {
	Kind RepositoryCallKind
	Arg  digest.Digest
}

// WaitRepository is a repository implementation for testing
// that lets the caller rendeszvous calls so that concurrency
// can be controlled.
type WaitRepository struct {
	url   *url.URL
	mu    sync.Mutex
	calls map[repositoryCallKey]chan RepositoryCall
}

// NewWaitRepository creates a new WaitRespository with the
// provided URL. If the URL is invalid, NewWaitRepository will
// panic.
func NewWaitRepository(rawurl string) *WaitRepository {
	w := &WaitRepository{
		calls: make(map[repositoryCallKey]chan RepositoryCall),
	}
	var err error
	w.url, err = url.Parse(rawurl)
	if err != nil {
		panic(err)
	}
	return w
}

// CollectWithThreshold removes from this repository any objects not in the
// Liveset and whose creation times are not more recent than the
// threshold time.
func (r *WaitRepository) CollectWithThreshold(ctx context.Context, live liveset.Liveset, dead liveset.Liveset, threshold time.Time, dryRun bool) error {
	panic("not implemented")
}

// Collect is not supported by WaitRepository.
func (r *WaitRepository) Collect(context.Context, liveset.Liveset) error {
	panic("not implemented")
}

// Stat waits for another caller to rendeszvous and then returns the caller's reply.
func (w *WaitRepository) Stat(ctx context.Context, id digest.Digest) (reflow.File, error) {
	select {
	case reply := <-w.Call(RepositoryStat, id):
		return reply.ReplyFile, reply.ReplyErr
	case <-ctx.Done():
		return reflow.File{}, ctx.Err()
	}
}

// Get waits for another caller to rendeszvous and then returns the caller's reply.
func (w *WaitRepository) Get(ctx context.Context, id digest.Digest) (io.ReadCloser, error) {
	select {
	case reply := <-w.Call(RepositoryGet, id):
		return reply.ReplyReadCloser, reply.ReplyErr
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Put reads the contents of r, waits for another caller to rendeszvous
// and then returns the caller's reply.
func (w *WaitRepository) Put(ctx context.Context, r io.Reader) (digest.Digest, error) {
	dw := reflow.Digester.NewWriter()
	if _, err := io.Copy(dw, r); err != nil {
		return digest.Digest{}, err
	}
	id := dw.Digest()
	select {
	case reply := <-w.Call(RepositoryPut, id):
		return id, reply.ReplyErr
	case <-ctx.Done():
		return digest.Digest{}, ctx.Err()
	}
}

// WriteTo is not supported by WaitRepository.
func (w *WaitRepository) WriteTo(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("writeto", id, u.String(), errors.NotSupported)
}

// ReadFrom is not supported by WaitRepository.
func (w *WaitRepository) ReadFrom(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("readfrom", id, u.String(), errors.NotSupported)
}

// URL returns the repository's URL.
func (w *WaitRepository) URL() *url.URL {
	return w.url
}

// Call rendeszvous with another caller. The returned channel
// is used to reply to the call.
func (w *WaitRepository) Call(kind RepositoryCallKind, digest digest.Digest) chan RepositoryCall {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := repositoryCallKey{kind, digest}
	call := w.calls[key]
	if call == nil {
		call = make(chan RepositoryCall)
		w.calls[key] = call
	}
	return call
}
