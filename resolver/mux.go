// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package resolver provides utility functions to define resolvers
// used within evaluation in Reflow.
package resolver

import (
	"context"
	"net/url"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
)

// Resolver resolves fileset URLs into reference filesets.
type Resolver interface {
	Resolve(ctx context.Context, url string) (reflow.Fileset, error)
}

// Func is an adapter to allow the use of ordinary functions as a
// Resolver.
type Func func(ctx context.Context, url string) (reflow.Fileset, error)

// Resolve implements Resolver.
func (f Func) Resolve(ctx context.Context, url string) (reflow.Fileset, error) {
	return f(ctx, url)
}

// Mux is a multiplexing resolver. Keys in the underlying
// map are URL schemes, each mapping to a resolver.
type Mux map[string]Resolver

// HandleFunc adds the provided function as a handler for the given scheme.
func (m Mux) HandleFunc(scheme string, r func(ctx context.Context, url string) (reflow.Fileset, error)) {
	m[scheme] = Func(r)
}

// Resolve implements Resolver.
func (m Mux) Resolve(ctx context.Context, rawurl string) (reflow.Fileset, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return reflow.Fileset{}, err
	}
	r := m[u.Scheme]
	if r == nil {
		return reflow.Fileset{}, errors.E("resolve", rawurl, errors.NotSupported)
	}
	return r.Resolve(ctx, rawurl)
}
