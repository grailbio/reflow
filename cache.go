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
	Lookup(context.Context, digest.Digest) (Fileset, error)

	// Transfer transmits the file objects associated with value v
	// (usually retrieved by Lookup) to the repository dst.
	Transfer(ctx context.Context, dst Repository, v Fileset) error

	// Write stores the Value v, whose file objects exist in Repository repo,
	// under the key id.
	Write(ctx context.Context, id digest.Digest, v Fileset, repo Repository) error

	// Delete removes the value named by id from this cache.
	Delete(ctx context.Context, id digest.Digest) error
}
