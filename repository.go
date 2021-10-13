// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/liveset"
)

// Repository defines an interface used for servicing blobs of
// data that are named-by-hash.
type Repository interface {
	// Collect removes from this repository any objects not in the
	// Liveset
	Collect(context.Context, liveset.Liveset) error

	// CollectWithThreshold removes from this repository any objects not in the live set and
	// is either in the dead set or its creation times are not more recent than the threshold time.
	CollectWithThreshold(ctx context.Context, live liveset.Liveset, dead liveset.Liveset, threshold time.Time, dryrun bool) error

	// Stat returns the File metadata for the blob with the given digest.
	// It returns errors.NotExist if the blob does not exist in this
	// repository.
	Stat(context.Context, digest.Digest) (File, error)

	// Get streams the blob named by the given Digest.
	// If it does not exist in this repository, an error with code
	// errors.NotFound will be returned.
	Get(context.Context, digest.Digest) (io.ReadCloser, error)

	// Put streams a blob to the repository and returns its
	// digest when completed.
	Put(context.Context, io.Reader) (digest.Digest, error)

	// WriteTo writes a blob identified by a Digest directly to a
	// foreign repository named by a URL. If the repository is
	// unable to write directly to the foreign repository, an error
	// with flag errors.NotSupported is returned.
	WriteTo(context.Context, digest.Digest, *url.URL) error

	// ReadFrom reads a blob identified by a Digest directly from a
	// foreign repository named by a URL. If the repository is
	// unable to read directly from the foreign repository, an error
	// with flag errors.NotSupported is returned.
	ReadFrom(context.Context, digest.Digest, *url.URL) error

	// URL returns the URL of this repository, or nil if it does not
	// have one. The returned URL may be used for direct transfers via
	// WriteTo or ReadFrom.
	URL() *url.URL
}

// Transferer defines an interface used for management of transfers
// between multiple repositories.
type Transferer interface {
	// Transfer transfers a set of files from the src to the dst
	// repository. A transfer manager may apply policies (e.g., rate
	// limits and concurrency limits) to these transfers.
	Transfer(ctx context.Context, dst, src Repository, files ...File) error

	NeedTransfer(ctx context.Context, dst Repository, files ...File) ([]File, error)
}

// RepoObjectRef is a reference to an object in a particular Repository.
type RepoObjectRef struct {
	// RepoURL is the URL of the repository where this object is located
	RepoURL *url.URL
	// Digest is the reference to this object in the repository where it is located
	Digest digest.Digest
}

func (ror RepoObjectRef) String() string {
	return fmt.Sprintf("digest: %s (repo: %s)", ror.Digest, ror.RepoURL)
}
