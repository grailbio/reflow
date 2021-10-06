// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package repository provides common ways to dial reflow.Repository
// implementations; it also provides some common utilities for
// working with repositories.
package repository

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
)

var (
	mu       sync.Mutex
	diallers = map[string]func(*url.URL) (reflow.Repository, error){}
	retrier  = retry.MaxRetries(retry.Backoff(20*time.Millisecond, 100*time.Millisecond, 1.5), 3)
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

// Transfer attempts to transfer an object from src to dst repository with retries on errors.
func Transfer(ctx context.Context, dst, src reflow.Repository, id digest.Digest) (err error) {
	for retries := 0; ; retries++ {
		if err = doTransfer(ctx, dst, src, id); !retryTransfer(err) {
			break
		}
		log.Printf("transfer %v from %s -> %s: %v", id, repoName(src), repoName(dst), err)
		if rerr := retry.Wait(ctx, retrier, retries); rerr != nil {
			break
		}
	}
	return
}

// retryTransfer returns whether the given error is retryable in the context of a transfer.
func retryTransfer(err error) bool {
	if err == nil {
		return false
	}
	if errors.Transient(err) {
		return true
	}
	if errors.Is(errors.Integrity, err) {
		return true
	}
	return false
}

func repoName(r reflow.Repository) string {
	if u := r.URL(); u != nil {
		return fmt.Sprintf("%v", u)
	}
	return fmt.Sprintf("%T", r)
}

// doTransfer attempts to transfer an object from one repository to
// another. It attempts to achieve this via direct transfer, but
// falling back to copying when necessary.
//
// BUG(marius): Transfer (or the underyling repositories) should ensure
// that progress is made.
func doTransfer(ctx context.Context, dst, src reflow.Repository, id digest.Digest) error {
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
	r, w := io.Pipe()
	bw := bufio.NewWriter(w)
	go func() {
		var err error
		if _, ok := v.(*reflow.Fileset); ok {
			err = v.(*reflow.Fileset).Write(bw, reflow.FilesetMarshalFmtJSON)
			if err == nil {
				err = bw.Flush()
			}
		} else {
			e := json.NewEncoder(w)
			err = e.Encode(v)
		}
		_ = w.CloseWithError(err)
	}()
	return repo.Put(ctx, r)
}

// Unmarshal unmarshals the value named by digest k into v.
// If the value does not exist in repository, an error is returned.
func Unmarshal(ctx context.Context, repo reflow.Repository, k digest.Digest, v interface{}) error {
	rc, err := repo.Get(ctx, k)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	if fs, ok := v.(*reflow.Fileset); ok {
		return fs.Read(rc, reflow.FilesetMarshalFmtJSON)
	}
	return json.NewDecoder(rc).Decode(v)
}
