// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package assoc defines data types for associative maps used within
// Reflow.
package assoc

import (
	"context"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/liveset"
)

// Kind describes the kind of mapping.
type Kind int

const (
	// Fileset maps fileset values.
	Fileset Kind = iota
)

// MappingHandler is an interface for handling a mapping while scanning.
type MappingHandler interface {
	// HandleMapping handles a scanned association.
	HandleMapping(k, v digest.Digest, lastAccessTime time.Time)
}

// MappingHandlerFunc is a convenience type to avoid having to declare a struct
// to implement the MappingHandler interface.
type MappingHandlerFunc func(k, v digest.Digest, lastAccessTime time.Time)

// HandleMapping implements the MappingHandler interface.
func (h MappingHandlerFunc) HandleMapping(k, v digest.Digest, lastAccessTime time.Time) {
	h(k, v, lastAccessTime)
}

// An Assoc is an associative array mapping digests to other digests.
// Mappings are also assigned a kind, and can thus be expanded to
// store multiple types of mapping for each key.
type Assoc interface {
	// Store unconditionally stores the association k, v.
	// Zero values indicate that the association is to be deleted.
	Store(ctx context.Context, kind Kind, k, v digest.Digest) error

	// Get returns the digest associated with key digest k and the
	// provided kind. Get returns an errors.NotExist when no such
	// mapping exists. Get expands the provided key when it is abbreviated,
	// and returns the expanded key when appropriate.
	Get(ctx context.Context, kind Kind, k digest.Digest) (kexp, v digest.Digest, err error)

	// CollectWithThreshold removes from this assoc any objects whose keys are not in the
	// liveset and which have not been accessed more recently than the threshold time.
	CollectWithThreshold(context.Context, liveset.Liveset, time.Time, bool) error

	// Count returns an estimate of the number of associations in this mapping.
	Count(ctx context.Context) (int64, error)

	// Scan calls the handler function for every association in the mapping.
	// Note that the handler function may be called asynchronously from multiple threads.
	Scan(ctx context.Context, handler MappingHandler) error
}

// Delete deletes the key k unconditionally from the provided assoc.
func Delete(ctx context.Context, assoc Assoc, kind Kind, k digest.Digest) error {
	return assoc.Store(ctx, kind, k, digest.Digest{})
}
