// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package blob implements a set of generic interfaces used to
// implement blob storage implementations such as S3, GCS, and local
// file system implementations. Package blob also contains a blob
// storage multiplexer that can be used to retrieve buckets from
// several underlying implementations.
package blob

import (
	"context"
	"io"

	"github.com/grailbio/reflow"
)

// Store represents a storage system, from which buckets can
// retrieved.
type Store interface {
	Bucket(ctx context.Context, name string) (Bucket, error)
}

// A Bucket is a single Namespace of keys from which files can be
// retrieved. Buckets support efficient prefix scans as well as both
// streaming and "direct" (concurrent) downloads.
type Bucket interface {
	// File retrieves file metadata for the provided key.
	File(ctx context.Context, key string) (reflow.File, error)

	// Scan returns a scanner for the provided prefix, which is
	// then used to retrieve all keys (in order) with this prefix.
	Scan(prefix string) Scanner

	// Download downloads the provided key into the provided writer. The
	// underlying implementation may perform concurrent downloading,
	// writing to arbitrary offsets in the writer, potentially leaving
	// holes on error on incomplete downloads. If the provided ETag is
	// nonempty, then it is taken as a precondition for fetching.
	// If the provided size is non-zero, it is used as a hint to manage
	// concurrency.
	Download(ctx context.Context, key, etag string, size int64, w io.WriterAt) (int64, error)

	// Get returns a (streaming) reader for the contents at the provided
	// key. The returned reflow.File represents the known metadata of
	// the file object. If the provided ETag is nonempty, then it is taken
	// as a precondition for fetching.
	Get(ctx context.Context, key, etag string) (io.ReadCloser, reflow.File, error)

	// Put streams the provided body to the provided key. Put overwrites
	// any existing object at the same key.
	// If the provided size is non-zero, it is used as a hint to manage
	// concurrency.
	Put(ctx context.Context, key string, size int64, body io.Reader) error

	// Snapshot returns an un-loaded Reflow fileset representing the
	// contents of the provided prefix. This may then later be used to
	// load and verify the contents of the returned fileset.
	Snapshot(ctx context.Context, prefix string) (reflow.Fileset, error)

	// Copy copies key src to key dst in this bucket.
	Copy(ctx context.Context, src, dst string) error

	// Delete removes the provided keys.
	Delete(ctx context.Context, keys ...string) error

	// Location returns a URL indicating the location of the bucket.
	// A key location can generally be derived by appending the
	// key to the bucket's location.
	Location() string
}

// A Scanner scans keys in a bucket. Scanners are provided by
// Bucket implementations. Scanning commences after the first
// call to Scan.
type Scanner interface {
	// Scan forwards the scanner to the next entry, returning a boolean
	// indicating whether the operation was successful. When Scan
	// returns false, the caller should check Err() to distinguish
	// between errors and complete scans.
	Scan(ctx context.Context) bool

	// Err returns the error, if any, that occurred while scanning.
	Err() error

	// File returns the currently scanned file metadata.
	File() reflow.File

	// Key returns the current key.
	Key() string
}
