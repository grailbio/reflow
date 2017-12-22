// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package test

import (
	"context"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository"
)

// Cache is a implementation of reflow.Cache that stores values
// (but not objects) in memory.
type Cache struct {
	mu   sync.Mutex
	vmap map[digest.Digest]reflow.Fileset
}

// Init initializes (or re-initializes) a Cache.
func (c *Cache) Init() {
	c.vmap = map[digest.Digest]reflow.Fileset{}
}

// Value returns the value stored for flow f.
func (c *Cache) Value(f *reflow.Flow) reflow.Fileset {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.vmap[f.Digest()]
}

// Exists tells whether a value has been stored for flow f.
func (c *Cache) Exists(f *reflow.Flow) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.vmap[f.Digest()]
	return ok
}

// ExistsAll tells whether a value has been stored for flow f,
// for all of its cache keys.
func (c *Cache) ExistsAll(f *reflow.Flow) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range f.CacheKeys() {
		_, ok := c.vmap[key]
		if !ok {
			return false
		}
	}
	return true
}

// Lookup returns the value stored at id, or else an errors.NotExist.
func (c *Cache) Lookup(ctx context.Context, id digest.Digest) (reflow.Fileset, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.vmap[id]
	if !ok {
		return reflow.Fileset{}, errors.E("lookup", id, errors.NotExist)
	}
	return v, nil
}

// Delete removes the key id from the cache.
func (c *Cache) Delete(ctx context.Context, id digest.Digest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.vmap, id)
	return nil
}

// Transfer is not implemented in Cache.
func (c *Cache) Transfer(ctx context.Context, dst reflow.Repository, v reflow.Fileset) error {
	return errors.E("transfer", errors.NotSupported)
}

// NeedTransfer returns the file objects in v that are missing from repository dst.
func (c *Cache) NeedTransfer(ctx context.Context, dst reflow.Repository, v reflow.Fileset) ([]reflow.File, error) {
	return repository.Missing(ctx, dst, v.Files()...)
}

// Write stores the value v at key id.
func (c *Cache) Write(ctx context.Context, id digest.Digest, v reflow.Fileset, repo reflow.Repository) error {
	c.mu.Lock()
	c.vmap[id] = v
	c.mu.Unlock()
	return nil
}

type cacheValue struct {
	v   reflow.Fileset
	hit bool
}

// WaitCache implements a reflow.Cache used for testing.
// WaitCaches rendezvous callers, acting as a concurrency
// control mechanism for tests.
type WaitCache struct {
	mu    sync.Mutex
	chans map[digest.Digest]chan cacheValue
}

// Init (re-) initializes a WaitCache.
func (c *WaitCache) Init() {
	c.chans = map[digest.Digest]chan cacheValue{}
}

// val returns the cacheValue chan for a digest.
func (c *WaitCache) val(id digest.Digest) chan cacheValue {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.chans[id] == nil {
		c.chans[id] = make(chan cacheValue)
	}
	return c.chans[id]
}

// Hit sets the value of flow f to v. Hit returns when the value
// has been consumed by the code under test.
func (c *WaitCache) Hit(f *reflow.Flow, v reflow.Fileset) {
	c.cacheReply(f, cacheValue{v: v, hit: true})
}

// Miss sets the value of flow f to a cache miss. Miss returns when
// it has been consumed by the code under test.
func (c *WaitCache) Miss(f *reflow.Flow) {
	c.cacheReply(f, cacheValue{})
}

func (c *WaitCache) cacheReply(f *reflow.Flow, v cacheValue) {
	// TODO(marius): we should probably watch for mutations on this flow node
	// and expand the key set as they become available.
	switch keys := f.CacheKeys(); len(keys) {
	case 1:
		c.val(keys[0]) <- v
	case 2:
		select {
		case c.val(keys[0]) <- v:
			if !v.hit {
				c.val(keys[1]) <- v
			}
		case c.val(keys[1]) <- v:
			if !v.hit {
				c.val(keys[0]) <- v
			}
		}
	default:
		panic("can only handle up to 2 cache keys")
	}
}

// Lookup implements cache lookups. Lookup returns when the value
// for id has been set (through Hit or Miss) or when the context is done.
func (c *WaitCache) Lookup(ctx context.Context, id digest.Digest) (reflow.Fileset, error) {
	select {
	case cv := <-c.val(id):
		if !cv.hit {
			return reflow.Fileset{}, errors.E("lookup", id, errors.NotExist)
		}
		return cv.v, nil
	case <-ctx.Done():
		return reflow.Fileset{}, ctx.Err()
	}
}

// Delete is not implemented in WaitCache, and will panic.
func (c *WaitCache) Delete(ctx context.Context, id digest.Digest) error {
	panic("delete is not implemented in WaitCache")
}

// Transfer always returns (immediate) success.
func (c *WaitCache) Transfer(ctx context.Context, dst reflow.Repository, v reflow.Fileset) error {
	return nil
}

// NeedTransfer returns the file objects in v that are missing from repository dst.
func (c *WaitCache) NeedTransfer(ctx context.Context, dst reflow.Repository, v reflow.Fileset) ([]reflow.File, error) {
	return repository.Missing(ctx, dst, v.Files()...)
}

// Write always returns (immediate) success.
func (c *WaitCache) Write(ctx context.Context, id digest.Digest, v reflow.Fileset, repo reflow.Repository) error {
	return nil
}
