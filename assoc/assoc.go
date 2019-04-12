// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package assoc defines data types for associative maps used within
// Reflow.
package assoc

import (
	"context"
	"fmt"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/liveset"
)

//go:generate stringer -type=Kind

// Kind describes the kind of mapping.
type Kind int

const (
	// Fileset maps fileset values.
	Fileset Kind = iota
	// ExecInspect maps exec inspect info (profile, cmd, etc)
	ExecInspect
	// Logs maps exec logs files (stdout, stderr).
	Logs
	// Bundle stores the program source, args, image names.
	Bundle
)

// MappingHandler is an interface for handling a mapping while scanning.
type MappingHandler interface {
	// HandleMapping handles a scanned association.
	HandleMapping(k, v digest.Digest, kind Kind, lastAccessTime time.Time, labels []string)
}

// MappingHandlerFunc is a convenience type to avoid having to declare a struct
// to implement the MappingHandler interface.
type MappingHandlerFunc func(k, v digest.Digest, kind Kind, lastAccessTime time.Time, labels []string)

// HandleMapping implements the MappingHandler interface.
func (h MappingHandlerFunc) HandleMapping(k, v digest.Digest, kind Kind, lastAccessTime time.Time, labels []string) {
	h(k, v, kind, lastAccessTime, labels)
}

// Key is the (key, kind) pair which uniquely identifies a value in the assoc.
type Key struct {
	// Kind is the mapping kind.
	Kind
	// Digest is the key digest.
	digest.Digest
}

// String returns the key (digest, kind) as a string.
func (k Key) String() string {
	return fmt.Sprintf("%s:%s", k.Digest, k.Kind)
}

// Batch is map of keys, fetched together.
type Batch map[Key]Result

// Add adds a key to the set if it doesn't exist already.
func (b Batch) Add(keys ...Key) {
	for _, key := range keys {
		if _, ok := b[key]; !ok {
			b[key] = Result{}
		}
	}
}

// Found returns if the key is present in the result set.
func (b Batch) Found(key Key) bool {
	if v, ok := b[key]; ok {
		return !v.IsZero() && v.Error == nil
	}
	return false
}

// Result is set of a key, kind and its value.
type Result struct {
	// Digest is the result of a lookup.
	digest.Digest
	// Err is the error during lookup, if any.
	Error error
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

	// BatchGet fetches a batch of keys. The result or error is set for each key.
	// Global errors (such as context errors or unrecoverable system errors)
	// are returned. Errors that can be attributable to a single fetch is returned
	// as that key's error.
	BatchGet(ctx context.Context, batch Batch) error

	// CollectWithThreshold removes from this assoc any objects whose keys are not in the
	// liveset and is either in the dead set or its creation times are not more recent than the threshold time.
	CollectWithThreshold(ctx context.Context, live, dead liveset.Liveset, kind Kind, threshold time.Time, rate int64, dryrun bool) error

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
