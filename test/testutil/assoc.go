// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package testutil

import (
	"context"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
)

type assocKey struct {
	assoc.Kind
	digest.Digest
}

type inmemoryAssoc struct {
	mu     sync.Mutex
	assocs map[assocKey]digest.Digest
}

// NewInmemoryAssoc returns a new assoc.Assoc
// that stores its mapping in memory.
func NewInmemoryAssoc() assoc.Assoc {
	return &inmemoryAssoc{
		assocs: make(map[assocKey]digest.Digest),
	}
}

func (a *inmemoryAssoc) Put(ctx context.Context, kind assoc.Kind, expect digest.Digest, k digest.Digest, v digest.Digest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := assocKey{kind, k}
	if !expect.IsZero() && a.assocs[key] != expect {
		return errors.E(errors.Precondition, errors.Errorf("expected value %v, have %v", expect, a.assocs[key]))
	}
	if v.IsZero() {
		delete(a.assocs, key)
	} else {
		a.assocs[key] = v
	}
	return nil
}

func (a *inmemoryAssoc) Get(ctx context.Context, kind assoc.Kind, k digest.Digest) (digest.Digest, error) {
	key := assocKey{kind, k}
	v, ok := a.assocs[key]
	if !ok {
		return digest.Digest{}, errors.E(errors.NotExist, errors.New("key does not exist"))
	}
	return v, nil
}
