// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package blob

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
)

// Mux multiplexes a number of blob store implementations. Mux
// implements bucket operations based on blob store URLs. URLs
// that are passed into Mux are intepreted as:
//
//	store://bucket/key
type Mux map[string]Store

// Bucket parses the provided URL, looks up its implementation, and
// returns the store's Bucket and the prefix implied by the URL. A
// errors.NotSupported is returned if there is no implementation for
// the requested scheme.
func (m Mux) Bucket(ctx context.Context, rawurl string) (Bucket, string, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, "", err
	}
	store, ok := m[u.Scheme]
	if !ok {
		return nil, "", errors.E(errors.NotSupported, "blob.Bucket", rawurl,
			errors.Errorf("no implementation for scheme %s", u.Scheme))
	}
	bucket, err := store.Bucket(ctx, u.Host)
	if err != nil {
		return nil, "", err
	}
	return bucket, strings.TrimPrefix(rawurl, bucket.Location()), err
}

// File returns file metadata for the provided URL.
func (m Mux) File(ctx context.Context, url string) (reflow.File, error) {
	bucket, key, err := m.Bucket(ctx, url)
	if err != nil {
		return reflow.File{}, err
	}
	return bucket.File(ctx, key)
}

// Scan returns a scanner for the provided URL (which represents a
// prefix). If withMetadata is true, the scanner is configured to
// make a best-effort attempt to fetch each object's metadata.
func (m Mux) Scan(ctx context.Context, url string, withMetadata bool) (Scanner, error) {
	bucket, prefix, err := m.Bucket(ctx, url)
	if err != nil {
		return nil, err
	}
	return bucket.Scan(prefix, withMetadata), nil
}

// Download downloads the object named by the provided URL to the
// provided io.WriterAt. If the provided etag is nonempty, then it is
// checked as a precondition on downloading the object. Download may
// download multiple chunks concurrently.
func (m Mux) Download(ctx context.Context, url, etag string, size int64, w io.WriterAt) (int64, error) {
	bucket, key, err := m.Bucket(ctx, url)
	if err != nil {
		return -1, err
	}
	return bucket.Download(ctx, key, etag, size, w)
}

// Get returns a (streaming) reader of the object named by the
// provided URL. If the provided etag is nonempty, then it is checked
// as a precondition on streaming the object.
func (m Mux) Get(ctx context.Context, url, etag string) (io.ReadCloser, reflow.File, error) {
	bucket, key, err := m.Bucket(ctx, url)
	if err != nil {
		return nil, reflow.File{}, err
	}
	return bucket.Get(ctx, key, etag)
}

// Put stores the contents of the provided io.Reader at the provided URL and attaches the given contentHash.
func (m Mux) Put(ctx context.Context, url string, size int64, body io.Reader, contentHash string) error {
	bucket, key, err := m.Bucket(ctx, url)
	if err != nil {
		return err
	}
	return bucket.Put(ctx, key, size, body, contentHash)
}

// CanTransfer returns whether contents of object in srcurl can be transferred to dsturl.
// If not supported, then the error corresponds to the reason why.
func (m Mux) CanTransfer(ctx context.Context, dsturl, srcurl string) (bool, error) {
	srcB, _, err := m.Bucket(ctx, srcurl)
	if err != nil {
		return false, err
	}
	dstB, _, err := m.Bucket(ctx, dsturl)
	if err != nil {
		return false, err
	}
	var srcScheme, dstScheme string
	if u, err := url.Parse(srcurl); err == nil {
		srcScheme = u.Scheme
	}
	if u, err := url.Parse(dsturl); err == nil {
		dstScheme = u.Scheme
	}
	switch {
	case srcScheme != "" && dstScheme != "" && srcScheme != dstScheme:
		return false, errors.E(errors.NotSupported, errors.Errorf("mux.Transfer %s -> %s", srcScheme, dstScheme))
	case reflect.TypeOf(srcB) != reflect.TypeOf(dstB):
		return false, errors.E(errors.NotSupported, errors.Errorf("mux.Transfer %T -> %T)", srcB, dstB))
	}
	return true, nil
}

// NeedTransfer returns whether src needs to be transferred to the location
// of dst. It expects both src and dst to be reference files, and it only
// determines that a transfer is unnecessary if the objects have the same ETag
// or ContentHash.
func (m Mux) NeedTransfer(ctx context.Context, dst, src reflow.File) (bool, error) {
	if src.Size != 0 && dst.Size != 0 && src.Size != dst.Size {
		return true, nil
	}
	// An ETag mismatch doesn't necessarily mean that the objects have different
	// contents. E.g. the ETag of an S3 object uploaded via multipart copy is
	// not a digest of the object data (https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html).
	if src.ETag != "" && src.ETag == dst.ETag {
		return false, nil
	}
	// A zero ContentHash doesn't necessarily mean that the field is missing
	// from the object's metadata.
	if src.ContentHash.IsZero() {
		var err error
		src, err = m.File(ctx, src.Source)
		if err != nil {
			return false, err
		}
	}
	if dst.ContentHash.IsZero() {
		var err error
		dst, err = m.File(ctx, dst.Source)
		if err != nil {
			return false, err
		}
	}
	if !src.ContentHash.IsZero() && !dst.ContentHash.IsZero() {
		return src.ContentHash != dst.ContentHash, nil
	}
	return true, nil
}

// Transfer transfers the contents of object in srcurl to dsturl.
// errors.NotSupported is returned if the transfer is not possible.
func (m Mux) Transfer(ctx context.Context, dsturl, srcurl string) error {
	srcB, src, err := m.Bucket(ctx, srcurl)
	if err != nil {
		return err
	}
	dstB, dst, err := m.Bucket(ctx, dsturl)
	if err != nil {
		return err
	}
	return dstB.CopyFrom(ctx, srcB, src, dst)
}

// Snapshot returns an un-loaded Reflow fileset representing the contents
// of the provided URL.
func (m Mux) Snapshot(ctx context.Context, url string) (reflow.Fileset, error) {
	bucket, prefix, err := m.Bucket(ctx, url)
	if err != nil {
		return reflow.Fileset{}, err
	}
	fs, err := bucket.Snapshot(ctx, prefix)
	if err == nil {
		setAssertions(&fs)
	}
	return fs, err
}

// Generate implements the AssertionGenerator interface for the blob namespace.
func (m Mux) Generate(ctx context.Context, key reflow.AssertionKey) (*reflow.Assertions, error) {
	if key.Namespace != reflow.BlobAssertionsNamespace {
		return nil, fmt.Errorf("unsupported namespace: %v", key.Namespace)
	}
	f, err := m.File(ctx, key.Subject)
	if err != nil {
		return nil, err
	}
	return Assertions(f), nil
}

// setAssertions sets the assertions for each file in the given fileset.
func setAssertions(fileset *reflow.Fileset) {
	for _, fs := range fileset.List {
		setAssertions(&fs)
	}
	for k := range fileset.Map {
		file := fileset.Map[k]
		file.Assertions = Assertions(file)
		fileset.Map[k] = file
	}
}

// Assertions returns assertions for a blob file.
func Assertions(f reflow.File) *reflow.Assertions {
	if f.Source == "" {
		return nil
	}
	gk := reflow.AssertionKey{Subject: f.Source, Namespace: reflow.BlobAssertionsNamespace}
	m := make(map[string]string, 3)
	if f.ETag != "" {
		m[reflow.BlobAssertionPropertyETag] = f.ETag
	}
	if !f.LastModified.IsZero() {
		m[reflow.BlobAssertionPropertyLastModified] = f.LastModified.String()
	}
	if f.Size > 0 {
		m[reflow.BlobAssertionPropertySize] = strconv.FormatInt(f.Size, 10)
	}
	return reflow.AssertionsFromEntry(gk, m)
}
