// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package s3blob implements the blob interfaces for S3.
package s3blob

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/grailbio/base/admit"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/sync/ctxsync"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/s3walker"
	"github.com/grailbio/reflow/log"
)

const (
	s3minpartsize     = 100 << 20
	s3concurrency     = 20
	defaultS3MinLimit = 2000
	defaultS3MaxLimit = 10000
	defaultMaxRetries = 3
)

// DefaultRegion is the region used for s3 requests if a bucket's
// region is undiscoverable (e.g., lacking permissions for the
// GetBucketLocation API call.)
//
// Amazon generally defaults to us-east-1 when regions are unspecified
// (or undiscoverable), but this can be overridden if a different default is
// desired.
var DefaultRegion = "us-east-1"

// Store implements blob.Store for S3. Buckets in store correspond
// exactly with buckets in S3. Store manages region discovery and
// session maintenance so that S3 access can be treated uniformly
// across regions.
type Store struct {
	sess *session.Session

	mu      sync.Mutex
	cond    *ctxsync.Cond
	buckets map[string]*Bucket
}

// New returns a new store that uses the provided session for API
// access.
func New(sess *session.Session) *Store {
	s := &Store{
		sess:    sess,
		buckets: make(map[string]*Bucket),
	}
	s.cond = ctxsync.NewCond(&s.mu)
	return s
}

// Bucket returns the s3 bucket with the provided name. An
// errors.NotExist error is returned if the bucket does not exist.
func (s *Store) Bucket(ctx context.Context, bucket string) (blob.Bucket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		b, ok := s.buckets[bucket]
		switch {
		case ok && b != nil:
			return b, nil
		case ok:
			if err := s.cond.Wait(ctx); err != nil {
				return nil, err
			}
		default:
			s.buckets[bucket] = nil
			s.mu.Unlock()
			b, err := s.newBucket(ctx, bucket)
			s.mu.Lock()
			if err != nil {
				if err != ctx.Err() {
					log.Printf("s3blob: failed to create bucket for %s: %v", bucket, err)
				}
				delete(s.buckets, bucket)
			} else {
				s.buckets[bucket] = b
			}
			s.cond.Broadcast()
			return b, err
		}
	}
}

func (s *Store) newBucket(ctx context.Context, bucket string) (*Bucket, error) {
	region, err := s3manager.GetBucketRegion(ctx, s.sess, bucket, DefaultRegion)
	if err != nil {
		if err == ctx.Err() {
			return nil, err
		}
		if kind(err) == errors.NotExist {
			return nil, errors.E("s3blob.bucket", bucket, errors.NotExist, err)
		}
		log.Printf("s3blob: unable to determine region for bucket %s: %v", bucket, err)
		region = DefaultRegion
	}
	config := aws.Config{
		MaxRetries: aws.Int(10),
		Region:     aws.String(region),
	}
	return NewBucket(bucket, s3.New(s.sess, &config)), nil
}

// NewS3Policy returns a default admit.RetryPolicy useful for S3 operations.
func newS3Policy() admit.RetryPolicy {
	rp := retry.MaxTries(retry.Jitter(retry.Backoff(500*time.Millisecond, time.Minute, 1.5), 0.5), defaultMaxRetries)
	return admit.WithVarExport(admit.ControllerWithRetry(defaultS3MinLimit, defaultS3MaxLimit, rp), "s3ops")
}

// Bucket represents an s3 bucket; it implements blob.Bucket.
type Bucket struct {
	bucket string
	client s3iface.S3API
	policy admit.RetryPolicy
}

// NewBucket returns a new S3 bucket that uses the provided client
// for SDK calls. NewBucket is primarily intended for testing.
func NewBucket(name string, client s3iface.S3API) *Bucket {
	return &Bucket{name, client, newS3Policy()}
}

// File returns metadata for the provided key.
func (b *Bucket) File(ctx context.Context, key string) (reflow.File, error) {
	var resp *s3.HeadObjectOutput
	err := admit.Do(ctx, b.policy, 1, func() error {
		var err error
		resp, err = b.client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(key),
		})
		if kind(err) == errors.ResourcesExhausted {
			return admit.ErrOverCapacity
		}
		return err
	})
	if err != nil {
		// The S3 API presents inconsistent error codes between GetObject
		// and HeadObject. It seems that GetObject returns "NoSuchKey" for
		// a missing object, while HeadObject returns a body-less HTTP 404
		// error, which is then assigned the fallback HTTP error code
		// NotFound by the SDK.
		return reflow.File{}, errors.E("s3blob.file", b.bucket, key, kind(err), err)
	}
	// TODO(marius): support ID if x-sha256 is present.
	return reflow.File{
		Source: fmt.Sprintf("s3://%s/%s", b.bucket, key),
		ETag:   aws.StringValue(resp.ETag),
		Size:   *resp.ContentLength,
	}, nil
}

// Scanner implements blob.Scanner.
type scanner struct {
	*s3walker.S3Walker
	bucket string
}

func (s *scanner) File() reflow.File {
	return reflow.File{
		ETag:         aws.StringValue(s.Object().ETag),
		Size:         aws.Int64Value(s.Object().Size),
		Source:       fmt.Sprintf("s3://%s/%s", s.bucket, s.Key()),
		LastModified: aws.TimeValue(s.Object().LastModified),
	}
}

func (s *scanner) Key() string {
	return *s.Object().Key
}

// Scan returns a scanner that iterates over all objects in the
// provided prefix.
func (b *Bucket) Scan(prefix string) blob.Scanner {
	return &scanner{
		S3Walker: &s3walker.S3Walker{S3: b.client, Bucket: b.bucket, Prefix: prefix},
		bucket:   b.bucket,
	}
}

// MaxS3Ops returns the optimal s3concurrency for parallel data transfers
// based on the file size.  Returns a value from [1, s3concurrency]
func maxS3Ops(size int64) int {
	if size == 0 {
		return s3concurrency
	}
	c := (size + s3minpartsize - 1) / s3minpartsize
	if c > s3concurrency {
		c = s3concurrency
	}
	return int(c)
}

// Download downloads the object named by the provided key. Download
// uses the AWS SDK's download manager, performing concurrent
// downloads to the provided io.WriterAt.
func (b *Bucket) Download(ctx context.Context, key, etag string, size int64, w io.WriterAt) (int64, error) {
	var n int64
	s3concurrency := maxS3Ops(size)
	err := admit.Retry(ctx, b.policy, s3concurrency, func() error {
		var err error
		d := s3manager.NewDownloaderWithClient(b.client, func(d *s3manager.Downloader) {
			d.PartSize = s3minpartsize
			d.Concurrency = s3concurrency
		})
		n, err = d.DownloadWithContext(ctx, w, b.getObjectInput(key, etag))
		if kind(err) == errors.ResourcesExhausted {
			err = admit.ErrOverCapacity
		}
		return err
	})
	if err != nil {
		err = errors.E("s3blob.download", b.bucket, key, kind(err), err)
	}
	return n, err
}

// Get retrieves the object at the provided key.
func (b *Bucket) Get(ctx context.Context, key, etag string) (io.ReadCloser, reflow.File, error) {
	resp, err := b.client.GetObject(b.getObjectInput(key, etag))
	if err != nil {
		return nil, reflow.File{}, errors.E("s3blob.get", b.bucket, key, kind(err), err)
	}
	return resp.Body, reflow.File{
		Source:       fmt.Sprintf("s3://%s/%s", b.bucket, key),
		ETag:         aws.StringValue(resp.ETag),
		Size:         *resp.ContentLength,
		LastModified: aws.TimeValue(resp.LastModified),
	}, nil
}

// Put stores the contents of the provided io.Reader at the provided key.
func (b *Bucket) Put(ctx context.Context, key string, size int64, body io.Reader) error {
	s3concurrency := maxS3Ops(size)
	err := admit.Retry(ctx, b.policy, s3concurrency, func() error {
		up := s3manager.NewUploaderWithClient(b.client, func(u *s3manager.Uploader) {
			u.PartSize = s3minpartsize
			u.Concurrency = s3concurrency
		})
		_, err := up.UploadWithContext(ctx, &s3manager.UploadInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(key),
			Body:   body,
		})
		if kind(err) == errors.ResourcesExhausted {
			return admit.ErrOverCapacity
		}
		return err
	})
	if err != nil {
		return errors.E("s3blob.put", b.bucket, key, kind(err), err)
	}
	return nil
}

// Snapshot returns an un-loaded Reflow fileset of the contents at the
// provided prefix.
func (b *Bucket) Snapshot(ctx context.Context, prefix string) (reflow.Fileset, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		head, err := b.client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(prefix),
		})
		if err != nil {
			return reflow.Fileset{}, errors.E("s3blob.snapshot", b.bucket, prefix, kind(err), err)
		}
		if head.ContentLength == nil || head.ETag == nil {
			return reflow.Fileset{}, errors.E("s3blob.snapshot", b.bucket, prefix, errors.Invalid, errors.New("incomplete metadata"))
		}
		file := reflow.File{
			Source:       fmt.Sprintf("s3://%s/%s", b.bucket, prefix),
			ETag:         *head.ETag,
			Size:         *head.ContentLength,
			LastModified: aws.TimeValue(head.LastModified),
		}
		return reflow.Fileset{Map: map[string]reflow.File{".": file}}, nil
	}

	var (
		dir     = reflow.Fileset{Map: make(map[string]reflow.File)}
		nprefix = len(prefix)
	)
	scan := b.Scan(prefix)
	for scan.Scan(ctx) {
		key := scan.Key()
		// Skip "directories".
		if strings.HasSuffix(key, "/") {
			continue
		}
		file := scan.File()
		if file.ETag == "" {
			return reflow.Fileset{}, errors.E("s3blob.Snapshot", b.bucket, prefix, errors.Invalid, errors.New("incomplete metadata"))
		}
		dir.Map[key[nprefix:]] = file
	}
	return dir, scan.Err()
}

// Copy copies the key src to the key dst. This is done directly without
// streaming the data through the client.
func (b *Bucket) Copy(ctx context.Context, src, dst string) error {
	_, err := b.client.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(b.bucket),
		Key:        aws.String(dst),
		CopySource: aws.String(path.Join(b.bucket + "/" + src)),
	})
	if err != nil {
		err = errors.E("s3blob.Copy", b.bucket, src, dst, kind(err), err)
	}
	return err
}

// Delete removes the provided keys in bulk.
func (b *Bucket) Delete(ctx context.Context, keys ...string) error {
	var del s3.Delete
	for _, key := range keys {
		del.Objects = append(del.Objects, &s3.ObjectIdentifier{Key: aws.String(key)})
	}
	_, err := b.client.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(b.bucket),
		Delete: &del,
	})
	return err
}

// Location returns the s3 URL of this bucket, e.g., s3://grail-reflow/.
func (b *Bucket) Location() string {
	return "s3://" + b.bucket + "/"
}

func (b *Bucket) getObjectInput(key, etag string) *s3.GetObjectInput {
	in := &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}
	if etag != "" {
		in.IfMatch = aws.String(etag)
	}
	return in
}

// kind interprets an S3 API error into a Reflow error kind.
func kind(err error) errors.Kind {
	aerr, ok := err.(awserr.Error)
	if !ok {
		return errors.Other
	}
	// The underlying error was an S3 error. Try to classify it.
	// Best guess based on Amazon's descriptions:
	switch aerr.Code() {
	// Code NotFound is not documented, but it's what the API actually returns.
	case "NoSuchBucket", "NoSuchKey", "NoSuchVersion", "NotFound":
		return errors.NotExist
	case "AccessDenied":
		return errors.NotAllowed
	case "InvalidRequest", "InvalidArgument", "EntityTooSmall", "EntityTooLarge", "KeyTooLong", "MethodNotAllowed":
		return errors.Fatal
	case "ExpiredToken", "AccountProblem", "ServiceUnavailable", "TokenRefreshRequired", "OperationAborted":
		return errors.Unavailable
	case "PreconditionFailed":
		return errors.Precondition
	case "SlowDown":
		return errors.ResourcesExhausted
	}
	return errors.Other
}
