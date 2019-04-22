// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package test

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/liveset"
)

//go:generate stringer -type=callType

type reporter []string

func (r *reporter) Errorf(format string, args ...interface{}) {
	*r = append(*r, fmt.Sprintf(format, args...))
}

type callType int

const (
	callStat callType = iota
	callGet
	callPut
	callWriteTo
	callReadFrom
	callCollect
)

type call struct {
	Type       callType
	ID         digest.Digest
	Reader     io.Reader
	ReadCloser io.ReadCloser
	URL        *url.URL
	File       reflow.File
	Live       liveset.Liveset
	Err        error

	admitc chan struct{}
}

func (c *call) Equal(m *call) bool {
	if c.Type != m.Type || c.ID != m.ID || ((c.URL == nil) != (m.URL == nil)) || !c.File.Equal(m.File) || c.Err != m.Err {
		return false
	}
	if c.URL != nil && *c.URL != *m.URL {
		return false
	}
	return true
}

func (c *call) Admit() {
	close(c.admitc)
}

type testRepository struct {
	Repository reflow.Repository
	Check      func(r *reporter, calls []*call)

	reporter reporter
	mu       sync.Mutex
	pending  []*call
}

func (r *testRepository) enter(c *call) (exit func()) {
	r.mu.Lock()
	r.pending = append(r.pending, c)
	r.Check(&r.reporter, r.pending)
	r.mu.Unlock()
	return func() { r.exit(c) }
}

func (r *testRepository) exit(c *call) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.pending {
		if r.pending[i] == c {
			r.pending = append(r.pending[:i], r.pending[i+1:]...)
			return
		}
	}
	panic("missing call")
}

func (r *testRepository) Report(t *testing.T) {
	for _, e := range r.reporter {
		t.Error(e)
	}
}

func (r *testRepository) Stat(ctx context.Context, id digest.Digest) (reflow.File, error) {
	exit := r.enter(&call{Type: callStat, ID: id})
	defer exit()
	return r.Repository.Stat(ctx, id)
}

func (r *testRepository) Get(ctx context.Context, id digest.Digest) (io.ReadCloser, error) {
	exit := r.enter(&call{Type: callGet, ID: id})
	defer exit()
	return r.Repository.Get(ctx, id)
}

func (r *testRepository) Put(ctx context.Context, rd io.Reader) (digest.Digest, error) {
	exit := r.enter(&call{Type: callPut})
	defer exit()
	return r.Repository.Put(ctx, rd)
}

func (r *testRepository) WriteTo(ctx context.Context, id digest.Digest, u *url.URL) error {
	exit := r.enter(&call{Type: callWriteTo, ID: id, URL: u})
	defer exit()
	return r.Repository.WriteTo(ctx, id, u)
}

func (r *testRepository) ReadFrom(ctx context.Context, id digest.Digest, u *url.URL) error {
	exit := r.enter(&call{Type: callReadFrom, ID: id, URL: u})
	defer exit()
	return r.Repository.ReadFrom(ctx, id, u)

}

func (r *testRepository) CollectWithThreshold(ctx context.Context, live liveset.Liveset, dead liveset.Liveset, threshold time.Time, dryRun bool) error {
	return errors.E("collectwiththreshold", errors.NotSupported)
}

func (r *testRepository) Collect(ctx context.Context, live liveset.Liveset) error {
	exit := r.enter(&call{Type: callCollect, Live: live})
	defer exit()
	return r.Repository.Collect(ctx, live)
}

func (r *testRepository) URL() *url.URL {
	return r.Repository.URL()
}
