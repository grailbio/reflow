// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package blobrepo

import (
	"context"
	"net/url"
	"sync"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/repository"
)

var (
	mu  sync.RWMutex
	mux = make(blob.Mux)
)

// Register registers a blob store implementation used to dial
// repositories for the provided scheme.
func Register(scheme string, store blob.Store) {
	mu.Lock()
	mux[scheme] = store
	mu.Unlock()
	repository.RegisterScheme(scheme, Dial)
}

// Dial dials a blob repository. The URL must have the form:
//
//	type://bucket/prefix
//
// TODO(marius): we should support shipping authentication
// information in the URL also.
func Dial(u *url.URL) (reflow.Repository, error) {
	mu.RLock()
	bucket, prefix, err := mux.Bucket(context.Background(), u.String())
	mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return &Repository{bucket, prefix}, nil
}
