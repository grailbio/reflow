package testutil

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"sync"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"grail.com/lib/digest"
)

// Inmemory is an in-memory repository used for testing.
type Inmemory struct {
	mu    sync.Mutex
	files map[digest.Digest][]byte
}

// NewInmemory returns a new, empty Inmemory.
func NewInmemory() *Inmemory {
	return &Inmemory{
		files: map[digest.Digest][]byte{},
	}
}

func (r *Inmemory) get(k digest.Digest) []byte {
	r.mu.Lock()
	b := r.files[k]
	r.mu.Unlock()
	return b
}

func (r *Inmemory) set(k digest.Digest, b []byte) {
	r.mu.Lock()
	r.files[k] = b
	r.mu.Unlock()
}

// Stat returns metadata for the blob named by id.
func (r *Inmemory) Stat(_ context.Context, id digest.Digest) (reflow.File, error) {
	b := r.get(id)
	if b == nil {
		return reflow.File{}, errors.E("stat", id, errors.NotExist)
	}
	return reflow.File{ID: id, Size: int64(len(b))}, nil
}

// Get returns the blob named by id.
func (r *Inmemory) Get(_ context.Context, id digest.Digest) (io.ReadCloser, error) {
	b := r.get(id)
	if b == nil {
		return nil, errors.E("get", id, errors.NotExist)
	}
	return ioutil.NopCloser(bytes.NewReader(b)), nil
}

// Put installs the blob rd and returns its digest.
func (r *Inmemory) Put(_ context.Context, rd io.Reader) (digest.Digest, error) {
	b, err := ioutil.ReadAll(rd)
	if err != nil {
		return digest.Digest{}, err
	}
	id := reflow.Digester.FromBytes(b)
	r.set(id, b)
	return id, nil
}

// Collect removes any object not in the liveset.
func (r *Inmemory) Collect(_ context.Context, live reflow.Liveset) error {
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
func (r *Inmemory) WriteTo(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("writeto", id, u.String(), errors.NotSupported)
}

// ReadFrom is not supported.
func (r *Inmemory) ReadFrom(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("readfrom", id, u.String(), errors.NotSupported)
}

// URL returns a nil URL.
func (r *Inmemory) URL() *url.URL {
	return nil
}
