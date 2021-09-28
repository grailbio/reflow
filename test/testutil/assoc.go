// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/liveset"
)

type assocKey struct {
	assoc.Kind
	digest.Digest
}

type InmemoryAssoc struct {
	mu     sync.Mutex
	assocs map[assocKey]digest.Digest
}

// NewInmemoryAssoc returns a new assoc.Assoc
// that stores its mapping in memory.
func NewInmemoryAssoc() *InmemoryAssoc {
	return &InmemoryAssoc{
		assocs: make(map[assocKey]digest.Digest),
	}
}

func (a *InmemoryAssoc) Store(ctx context.Context, kind assoc.Kind, k, v digest.Digest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := assocKey{kind, k}
	if v.IsZero() {
		delete(a.assocs, key)
	} else {
		a.assocs[key] = v
	}
	return nil
}

func (a *InmemoryAssoc) Get(ctx context.Context, kind assoc.Kind, k digest.Digest) (digest.Digest, digest.Digest, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := assocKey{kind, k}
	v, ok := a.assocs[key]
	if !ok {
		return k, digest.Digest{}, errors.E(errors.NotExist, errors.New("key does not exist"))
	}
	return k, v, nil
}

func (a *InmemoryAssoc) BatchGet(ctx context.Context, batch assoc.Batch) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for k := range batch {
		v := a.assocs[assocKey{k.Kind, k.Digest}]
		batch[k] = assoc.Result{Digest: v}
	}
	return nil
}

// CollectWithThreshold removes from this assoc any objects whose keys are not in the
// liveset and which have not been accessed more recently than the liveset's
// threshold time.
func (a *InmemoryAssoc) CollectWithThreshold(context.Context, liveset.Liveset, liveset.Liveset, time.Time, int64, bool) error {
	return errors.E("collect", errors.NotSupported)
}

// Count returns an estimate of the number of associations in this mapping.
func (a *InmemoryAssoc) Count(ctx context.Context) (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return int64(len(a.assocs)), nil
}

// Scan calls the handler function for every association in the mapping.
// Note that the handler function may be called asynchronously from multiple threads.
func (a *InmemoryAssoc) Scan(ctx context.Context, kind assoc.Kind, handler assoc.MappingHandler) error {
	return errors.E("scan", errors.NotSupported)
}

// Delete deletes the key k unconditionally from the provided assoc.
func (a *InmemoryAssoc) Delete(ctx context.Context, k digest.Digest) error {
	return errors.E("delete", errors.NotSupported)
}

func (a *InmemoryAssoc) RawAssocs() map[assocKey]digest.Digest {
	return a.assocs
}
