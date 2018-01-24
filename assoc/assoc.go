// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package assoc defines data types for associative maps used within
// Reflow.
package assoc

import (
	"context"

	"github.com/grailbio/base/digest"
)

// Kind describes the kind of mapping.
type Kind int

const (
	// Fileset maps fileset values.
	Fileset Kind = iota
)

// An Assoc is an associative array mapping digests to other digests.
// Mappings are also assigned a kind, and can thus be expanded to
// store multiple types of mapping for each key.
type Assoc interface {
	// Put associates the digest v with the key digest k of the provided
	// kind. If expect is nonzero, Put performs a compare-and-set,
	// erroring with errors.Precondition if the expected current value
	// was not equal to expect. Zero values indicate that the association
	// is to be deleted.
	Put(ctx context.Context, kind Kind, expect, k, v digest.Digest) error

	// Get returns the digest associated with key digest k and the
	// provided kind. Get returns an errors.NotExist when no such
	// mapping exists. Get expands the provided key when it is abbreviated,
	// and returns the expanded key when appropriate.
	Get(ctx context.Context, kind Kind, k digest.Digest) (kexp, v digest.Digest, err error)
}

// Delete deletes the key k unconditionally from the provided assoc.
func Delete(ctx context.Context, assoc Assoc, kind Kind, k digest.Digest) error {
	return assoc.Put(ctx, kind, digest.Digest{}, k, digest.Digest{})
}
