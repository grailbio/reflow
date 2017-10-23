package cache

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/grailbio/reflow"
	"grail.com/lib/digest"
	"grail.com/lib/limiter"
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
	// Repository stores the cache's objects: both file objects and
	// serialized values are stored here.
	Repository reflow.Repository

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

// Write stores the Value v, whose file objects exist in Repository
// repo, under the key id.
func (c *Cache) Write(ctx context.Context, id digest.Digest, v reflow.Fileset, repo reflow.Repository) error {
	if err := c.Transferer.Transfer(ctx, c.Repository, repo, v.Files()...); err != nil {
		return err
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
	d, err := c.Repository.Put(ctx, bytes.NewReader(b))
	if err != nil {
		return err
	}
	return c.Assoc.Map(id, d)
}

// Lookup returns the value associated with a (digest) key. Lookup
// returns an error flagged errors.NotExist when there is no such
// value.
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
	rc, err := c.Repository.Get(ctx, d)
	if err != nil {
		return reflow.Fileset{}, err
	}
	var v reflow.Fileset
	err = json.NewDecoder(rc).Decode(&v)
	rc.Close()
	return v, err
}

// Delete removes the cache key id from this cache's Assoc.
func (c *Cache) Delete(ctx context.Context, id digest.Digest) error {
	return c.Assoc.Unmap(id)
}

// Transfer transmits the file objects associated with value v
// (usually retrieved by Lookup) to the repository dst.
func (c *Cache) Transfer(ctx context.Context, dst reflow.Repository, v reflow.Fileset) error {
	return c.Transferer.Transfer(ctx, dst, c.Repository, v.Files()...)
}
