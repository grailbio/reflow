// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"context"

	"github.com/grailbio/base/digest"
)

// A Cache stores Values and their associated File objects for later
// retrieval. Caches may be temporary: objects are not guaranteed
// to persist.
type Cache interface {
	// Lookup returns the value associated with a (digest) key.
	// Lookup returns an error flagged errors.NotExist when there
	// is no such value.
	//
	// Lookup should also check to make sure that the objects
	// actually exist, and provide a reasonable guarantee that they'll
	// be available for transfer.
	//
	// TODO(marius): allow the caller to maintain a lease on the desired
	// objects so that garbage collection can (safely) be run
	// concurrently with flows. This isn't a correctness concern (the
	// flows may be restarted), but rather one of efficiency.
	Lookup(context.Context, digest.Digest) (Fileset, error)

	// Transfer transmits the file objects associated with value v
	// (usually retrieved by Lookup) to the repository dst. Transfer
	// should be used in place of direct (cache) repository access since
	// it may apply additional policies (e.g., rate limiting, etc.)
	Transfer(ctx context.Context, dst Repository, v Fileset) error

	// NeedTransfer returns the set of files in the Fileset v that are absent
	// in the provided repository.
	NeedTransfer(ctx context.Context, dst Repository, v Fileset) ([]File, error)

	// Write stores the Value v, whose file objects exist in Repository repo,
	// under the key id. If the repository is nil no objects are transferred.
	Write(ctx context.Context, id digest.Digest, v Fileset, repo Repository) error

	// Delete removes the value named by id from this cache.
	Delete(ctx context.Context, id digest.Digest) error

	// Repository returns this cache's underlying repository. It should
	// not be used for data transfer during the course of evaluation; see
	// Transfer.
	Repository() Repository
}
