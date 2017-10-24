package test

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sort"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
)

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

// TestRepository is an unimplemented repository.
var TestRepository reflow.Repository = &panicRepository{}

// Transferer is a transfer manager for testing. It lets the test
// code rendezvous with the caller. In this way, Transferer provides
// both a "mock" transfer manager implementation and also permits
// concurrency control for the tester.
type Transferer struct {
	mu        sync.Mutex
	transfers map[digest.Digest]chan error
}

// Init initializes a Transferer.
func (t *Transferer) Init() {
	t.transfers = map[digest.Digest]chan error{}
}

func (t *Transferer) transfer(dst, src reflow.Repository, files ...reflow.File) chan error {
	dw := reflow.Digester.NewWriter()
	fmt.Fprintf(dw, "%p%p", dst, src)
	sort.Slice(files, func(i, j int) bool {
		return files[i].ID.Less(files[j].ID)
	})
	for i := range files {
		fmt.Fprintf(dw, "%s%d", files[i].ID, files[i].Size)
	}
	d := dw.Digest()

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.transfers[d] == nil {
		t.transfers[d] = make(chan error)
	}
	return t.transfers[d]
}

// Ok rendezvous the call named by the destination repository,
// source repository and fileset with succcess.
func (t *Transferer) Ok(dst, src reflow.Repository, files ...reflow.File) {
	t.transfer(dst, src, files...) <- nil
}

// Err rendezvous the call named by the destination repository,
// source repository and fileset with failure.
func (t *Transferer) Err(err error, dst, src reflow.Repository, files ...reflow.File) {
	t.transfer(dst, src, files...) <- err
}

// Transfer waits for the tester to rendezvous, returning its result.
func (t *Transferer) Transfer(ctx context.Context, dst, src reflow.Repository, files ...reflow.File) error {
	select {
	case err := <-t.transfer(dst, src, files...):
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
