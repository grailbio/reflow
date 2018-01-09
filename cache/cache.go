// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package cache

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository"
)

// An Assoc is an associative array mapping digests to other digests.
type Assoc interface {
	// Map associates the digest v with the key digest k.
	Map(k, v digest.Digest) error

	// Unmap removes the mapping of key k.
	Unmap(k digest.Digest) error

	// Lookup returns the digest associated with key digest k. Lookup
	// returns an error flagged errors.NotExist when no such mapping
	// exists.
	Lookup(k digest.Digest) (digest.Digest, error)
}

// Cache assembles a reflow.Cache from a Repository, Assoc, and a
// Transferer. It also applies concurrency limits to calls to the Cache's
// underlying repository.
//
// Cache implements reflow.Cache.
type Cache struct {
	// Repo stores the cache's objects: both file objects and
	// serialized values are stored here.
	Repo reflow.Repository

	// Assoc maintains the association between cache keys and the
	// ID of their associated Value object as stored in Repository.
	Assoc Assoc

	// Transferer is used to managed transfers between repositories.
	Transferer reflow.Transferer

	// LookupLim limits the number of concurrent assoc lookups.
	// No lookup limits are applied when LookupLim is nil.
	LookupLim *limiter.Limiter

	// WriteLim limits the number of concurrent (value) writes to Repository.
	// No write limits are applied when WriteLim is nil.
	WriteLim *limiter.Limiter
}

// Repository returns this cache's remote repository.
func (c *Cache) Repository() reflow.Repository {
	return c.Repo
}

// Write stores the Value v, whose file objects exist in Repository
// repo, under the key id.
func (c *Cache) Write(ctx context.Context, id digest.Digest, v reflow.Fileset, repo reflow.Repository) error {
	if repo != nil {
		if err := c.Transferer.Transfer(ctx, c.Repo, repo, v.Files()...); err != nil {
			return err
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if c.WriteLim != nil {
		if err := c.WriteLim.Acquire(ctx, 1); err != nil {
			return err
		}
		defer c.WriteLim.Release(1)
	}
	d, err := c.Repo.Put(ctx, bytes.NewReader(b))
	if err != nil {
		return err
	}
	return c.Assoc.Map(id, d)
}

// Lookup returns the value associated with a (digest) key. Lookup
// returns an error flagged errors.NotExist when there is no such
// value. Lookup also checks that the objects are available in the
// cache's repository; an errors.NotExist error is returned if any
// object is missing.
func (c *Cache) Lookup(ctx context.Context, id digest.Digest) (reflow.Fileset, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if c.LookupLim != nil {
		if err := c.LookupLim.Acquire(ctx, 1); err != nil {
			return reflow.Fileset{}, err
		}
		defer c.LookupLim.Release(1)
	}
	d, err := c.Assoc.Lookup(id)
	if err != nil {
		return reflow.Fileset{}, err
	}
	rc, err := c.Repo.Get(ctx, d)
	if err != nil {
		return reflow.Fileset{}, err
	}
	var v reflow.Fileset
	err = json.NewDecoder(rc).Decode(&v)
	rc.Close()
	if err != nil {
		return reflow.Fileset{}, err
	}
	// Also check that the objects exist.
	missing, err := repository.Missing(ctx, c.Repo, v.Files()...)
	if err != nil {
		return reflow.Fileset{}, err
	}
	if len(missing) > 0 {
		var total int64
		for _, file := range missing {
			total += file.Size
		}
		return reflow.Fileset{}, errors.E(
			errors.NotExist, "cache.Lookup",
			errors.Errorf("missing %d files (%s)", len(missing), data.Size(total)))
	}
	return v, err
}

// Delete removes the cache key id from this cache's Assoc.
func (c *Cache) Delete(ctx context.Context, id digest.Digest) error {
	return c.Assoc.Unmap(id)
}

// Transfer transmits the file objects associated with value v
// (usually retrieved by Lookup) to the repository dst.
func (c *Cache) Transfer(ctx context.Context, dst reflow.Repository, v reflow.Fileset) error {
	return c.Transferer.Transfer(ctx, dst, c.Repo, v.Files()...)
}

// NeedTransfer returns the file objects in v that are missing from repository dst.
func (c *Cache) NeedTransfer(ctx context.Context, dst reflow.Repository, v reflow.Fileset) ([]reflow.File, error) {
	return c.Transferer.NeedTransfer(ctx, dst, v.Files()...)
}
