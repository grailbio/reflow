// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package repository provides common ways to dial reflow.Repository
// implementations; it also provides some common utilities for
// working with repositories.
package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
)

var (
	mu       sync.Mutex
	diallers = map[string]func(*url.URL) (reflow.Repository, error){}
)

// RegisterScheme associates a dialler with a URL scheme.
func RegisterScheme(scheme string, dial func(*url.URL) (reflow.Repository, error)) {
	mu.Lock()
	diallers[scheme] = dial
	mu.Unlock()
}

// UnregisterScheme is used for testing.
func UnregisterScheme(scheme string) {
	mu.Lock()
	delete(diallers, scheme)
	mu.Unlock()
}

// Dial attempts to connect to the repository named by the given URL.
// The URL's scheme must be registered with RegisterScheme.
func Dial(rawurl string) (reflow.Repository, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	mu.Lock()
	dial := diallers[u.Scheme]
	mu.Unlock()
	if dial == nil {
		return nil, errors.E("dial", rawurl, errors.NotSupported, errors.Errorf("unknown scheme %q", u.Scheme))
	}
	return dial(u)
}

// Transfer attempts to transfer an object from one repository to
// another. It attempts to achieve this via direct transfer, but
// falling back to copying when necessary.
//
// BUG(marius): Transfer (or the underyling repositories) should ensure
// that progress is made.
func Transfer(ctx context.Context, dst, src reflow.Repository, id digest.Digest) error {
	if u := src.URL(); u != nil {
		err := dst.ReadFrom(ctx, id, u)
		switch {
		case err == nil:
			return nil
		case errors.Is(errors.NotSupported, err):
		default:
			return err
		}
	}
	if u := dst.URL(); u != nil {
		err := src.WriteTo(ctx, id, u)
		switch {
		case err == nil:
			return nil
		case errors.Is(errors.NotSupported, err):
		default:
			return err
		}
	}
	log.Printf("local transfer %v %v %v", dst.URL(), src.URL(), id)
	return transferLocal(ctx, dst, src, id)
}

func transferLocal(ctx context.Context, dst, src reflow.Repository, id digest.Digest) error {
	rc, err := src.Get(ctx, id)
	if err != nil {
		return err
	}
	defer rc.Close()
	dgst, err := dst.Put(ctx, rc)
	if err != nil {
		return err
	}
	if dgst != id {
		return errors.Errorf("transfer %v: wrong digest %s", id, dgst)
	}
	return nil
}

// Marshal marshals the value v and stores it in the provided
// repository. The digest of the contents of the marshaled content is
// returned.
func Marshal(ctx context.Context, repo reflow.Repository, v interface{}) (digest.Digest, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return digest.Digest{}, err
	}
	return repo.Put(ctx, bytes.NewReader(b))
}

// Unmarshal unmarshals the value named by digest k into v.
// If the value does not exist in repository, an error is returned.
func Unmarshal(ctx context.Context, repo reflow.Repository, k digest.Digest, v interface{}) error {
	rc, err := repo.Get(ctx, k)
	if err != nil {
		return err
	}
	defer rc.Close()
	return json.NewDecoder(rc).Decode(v)
}
