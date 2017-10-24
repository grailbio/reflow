// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"context"
	"io"
	"net/url"

	"github.com/grailbio/base/digest"
)

// A Liveset contains a possibly approximate judgement about live
// objects.
type Liveset interface {
	// Contains returns true if the given object definitely is in the
	// set; it may rarely return true when the object does not.
	Contains(digest.Digest) bool
}

// Repository defines an interface used for servicing blobs of
// data that are named-by-hash.
type Repository interface {
	// Collect removes from this repository any objects not in the
	// Liveset
	Collect(context.Context, Liveset) error

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
}
