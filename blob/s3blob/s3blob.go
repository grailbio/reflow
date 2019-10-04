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

	"github.com/grailbio/base/digest"

	"github.com/grailbio/base/traverse"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
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
	// awsContentSha256Key is the header used to store the sha256 of a file's content.
	awsContentSha256Key = "Content-Sha256"

	s3minpartsize     = 5 << 20
	s3concurrency     = 100
	defaultS3MinLimit = 500
	defaultS3MaxLimit = 2000
	defaultMaxRetries = 3

	// minBPS defines the lowest acceptable transfer rate.
	minBPS = 1 << 20
	// minTimeout defines the smallest acceptable timeout.
	// This helps to give wiggle room for small data transfers.
	minTimeout = 60 * time.Second
	// unknownSizeTimeout defines timeout if the size is unknown.
	unknownSizeTimeout = 5 * time.Minute

	// metaTimeout is used for metadata operations.
	metaTimeout = 30 * time.Second

	// defaultS3ObjectCopySizeLimit is the max size of object for a single PUT Object Copy request.
	// As per AWS: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html
	// the max size allowed is 5GB, but we use a smaller size here to speed up large file copies.
	defaultS3ObjectCopySizeLimit = 256 << 20 // 256MiB

	// defaultS3MultipartCopyPartSize is the max size of each part when doing a multi-part copy.
	// Note: Though we can do parts of size up to defaultS3ObjectCopySizeLimit, for large files
	// using smaller size parts (concurrently) is much faster.
	defaultS3MultipartCopyPartSize = 128 << 20 // 128MiB

	// s3MultipartCopyConcurrencyLimit is the number of concurrent parts to do during a multi-part copy.
	s3MultipartCopyConcurrencyLimit = 100
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
					log.Printf("s3blob.Bucket: failed to create bucket for %s: %v", bucket, err)
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
			return nil, errors.E("s3blob.newBucket", bucket, errors.NotExist, err)
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

// NewS3RetryPolicy returns a default retry.Policy useful for S3 operations.
func newS3RetryPolicy() retry.Policy {
	return retry.MaxTries(retry.Jitter(retry.Backoff(1*time.Second, time.Minute, 2), 0.25), defaultMaxRetries)
}

// NewS3AdmitPolicy returns a default admit.RetryPolicy useful for S3 operations.
func newS3AdmitPolicy() admit.RetryPolicy {
	rp := retry.MaxTries(retry.Jitter(retry.Backoff(500*time.Millisecond, time.Minute, 1.5), 0.5), defaultMaxRetries)
	c := admit.ControllerWithRetry(defaultS3MinLimit, defaultS3MaxLimit, rp)
	admit.EnableVarExport(c, "s3ops")
	return c
}

// Bucket represents an s3 bucket; it implements blob.Bucket.
type Bucket struct {
	bucket   string
	client   s3iface.S3API
	admitter admit.RetryPolicy
	retrier  retry.Policy

	// s3ObjectCopySizeLimit is the max size of object for a single PUT Object Copy request.
	s3ObjectCopySizeLimit int64
	// s3MultipartCopyPartSize is the max size of each part when doing a multi-part copy.
	s3MultipartCopyPartSize int64
}

// NewBucket returns a new S3 bucket that uses the provided client
// for SDK calls. NewBucket is primarily intended for testing.
func NewBucket(name string, client s3iface.S3API) *Bucket {
	return &Bucket{name, client, newS3AdmitPolicy(), newS3RetryPolicy(),
		defaultS3ObjectCopySizeLimit, defaultS3MultipartCopyPartSize}
}

// File returns metadata for the provided key.
func (b *Bucket) File(ctx context.Context, key string) (reflow.File, error) {
	var resp *s3.HeadObjectOutput
	var err error
	for retries := 0; ; retries++ {
		err = admit.Retry(ctx, b.admitter, 1, func() error {
			var err error
			ctx, cancel := context.WithTimeout(ctx, metaTimeout)
			defer cancel()
			resp, err = b.client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(b.bucket),
				Key:    aws.String(key),
			})
			err = ctxErr(ctx, err)
			if kind(err) == errors.ResourcesExhausted {
				log.Printf("s3blob.File: %s/%s: %v (over capacity)\n", b.bucket, key, err)
				return admit.ErrOverCapacity
			}
			return err
		})
		if !retryable(err) {
			break
		}
		log.Printf("s3blob.File: %s/%s (attempt %d): %v\n", b.bucket, key, retries, err)
		if err = retry.Wait(ctx, b.retrier, retries); err != nil {
			break
		}
	}
	if err != nil {
		// The S3 API presents inconsistent error codes between GetObject
		// and HeadObject. It seems that GetObject returns "NoSuchKey" for
		// a missing object, while HeadObject returns a body-less HTTP 404
		// error, which is then assigned the fallback HTTP error code
		// NotFound by the SDK.
		return reflow.File{}, errors.E("s3blob.File", b.bucket, key, kind(err), err)
	}
	return reflow.File{
		Source:       fmt.Sprintf("s3://%s/%s", b.bucket, key),
		ETag:         aws.StringValue(resp.ETag),
		LastModified: aws.TimeValue(resp.LastModified),
		Size:         *resp.ContentLength,
		ContentHash:  getContentHash(resp.Metadata),
	}, nil
}

// getContentHash gets the ContentHash (if possible) from the given S3 metadata map.
func getContentHash(metadata map[string]*string) digest.Digest {
	if metadata == nil {
		return digest.Digest{}
	}
	sha256, ok := metadata[awsContentSha256Key]
	if !ok || *sha256 == "" {
		return digest.Digest{}
	}
	d, err := reflow.Digester.Parse(*sha256)
	if err != nil {
		return digest.Digest{}
	}
	return d
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
		ContentHash:  getContentHash(s.Metadata()),
	}
}

func (s *scanner) Key() string {
	return *s.Object().Key
}

// Scan returns a scanner that iterates over all objects in the
// provided prefix.
func (b *Bucket) Scan(prefix string) blob.Scanner {
	return &scanner{
		S3Walker: &s3walker.S3Walker{S3: b.client, Bucket: b.bucket, Prefix: prefix, Policy: b.admitter},
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

// ctxErr will return the context's error (if any) or other.
// Since AWS wraps context errors, we override any other errors with that
// to easily determine if the error was ours (ie context canceled or timed out)
func ctxErr(ctx context.Context, other error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return other
}

func timeoutPolicy(size int64) retry.Policy {
	baseTimeout := time.Duration(size/minBPS) * time.Second
	if size == 0 {
		baseTimeout = unknownSizeTimeout
	}
	if baseTimeout < minTimeout {
		baseTimeout = minTimeout
	}
	return retry.Backoff(baseTimeout, 3*baseTimeout, 1.5)
}

func timeout(policy retry.Policy, retries int) time.Duration {
	_, timeout := policy.Retry(retries)
	return timeout
}

// retryable returns whether an error is retryable.
func retryable(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(awserr.Error); ok {
		return kind(err) == errors.Temporary
	}
	// Not an AWS error, so attempt to recover as reflow error
	kind := errors.Recover(err).Kind
	return kind == errors.Timeout || kind == errors.Temporary
}

// Download downloads the object named by the provided key. Download
// uses the AWS SDK's download manager, performing concurrent
// downloads to the provided io.WriterAt.
func (b *Bucket) Download(ctx context.Context, key, etag string, size int64, w io.WriterAt) (int64, error) {
	// Determine size if unspecified
	if size == 0 {
		if rf, err := b.File(ctx, key); err == nil {
			size = rf.Size
		}
	}
	var n int64
	s3concurrency := maxS3Ops(size)
	var err error
	policy := timeoutPolicy(size)
	for retries := 0; ; retries++ {
		err = admit.Retry(ctx, b.admitter, s3concurrency, func() error {
			var err error
			d := s3manager.NewDownloaderWithClient(b.client, func(d *s3manager.Downloader) {
				d.PartSize = s3minpartsize
				d.Concurrency = s3concurrency
			})
			ctx, cancel := context.WithTimeout(ctx, timeout(policy, retries))
			defer cancel()
			n, err = d.DownloadWithContext(ctx, w, b.getObjectInput(key, etag))
			err = ctxErr(ctx, err)
			if kind(err) == errors.ResourcesExhausted {
				log.Printf("s3blob.Download: %s/%s: %v (over capacity)\n", b.bucket, key, err)
				err = admit.ErrOverCapacity
			}
			return err
		})
		if !retryable(err) {
			break
		}
		log.Printf("s3blob.Download: %s/%s (attempt %d): %v\n", b.bucket, key, retries, err)
		if err = retry.Wait(ctx, b.retrier, retries); err != nil {
			break
		}
	}
	if err != nil && kind(err) != errors.Canceled {
		err = errors.E("s3blob.Download", b.bucket, key, kind(err), err)
	}
	return n, err
}

// Get retrieves the object at the provided key.
func (b *Bucket) Get(ctx context.Context, key, etag string) (io.ReadCloser, reflow.File, error) {
	resp, err := b.client.GetObject(b.getObjectInput(key, etag))
	if err != nil {
		return nil, reflow.File{}, errors.E("s3blob.Get", b.bucket, key, kind(err), err)
	}
	return resp.Body, reflow.File{
		Source:       fmt.Sprintf("s3://%s/%s", b.bucket, key),
		ETag:         aws.StringValue(resp.ETag),
		Size:         *resp.ContentLength,
		LastModified: aws.TimeValue(resp.LastModified),
		ContentHash:  getContentHash(resp.Metadata),
	}, nil
}

// Put stores the contents of the provided io.Reader at the provided key
// and attaches the given contentHash to the object's metadata.
func (b *Bucket) Put(ctx context.Context, key string, size int64, body io.Reader, contentHash string) error {
	s3concurrency := maxS3Ops(size)
	var err error
	policy := timeoutPolicy(size)
	for retries := 0; ; retries++ {
		err = admit.Retry(ctx, b.admitter, s3concurrency, func() error {
			var err error
			up := s3manager.NewUploaderWithClient(b.client, func(u *s3manager.Uploader) {
				u.PartSize = s3minpartsize
				u.Concurrency = s3concurrency
			})
			ctx, cancel := context.WithTimeout(ctx, timeout(policy, retries))
			defer cancel()
			input := &s3manager.UploadInput{
				Bucket: aws.String(b.bucket),
				Key:    aws.String(key),
				Body:   body,
			}
			if contentHash != "" {
				input.Metadata = map[string]*string{awsContentSha256Key: aws.String(contentHash)}
			}
			_, err = up.UploadWithContext(ctx, input)
			err = ctxErr(ctx, err)
			if kind(err) == errors.ResourcesExhausted {
				log.Printf("s3blob.Put: %s/%s: %v (over capacity)\n", b.bucket, key, err)
				return admit.ErrOverCapacity
			}
			return err
		})
		if !retryable(err) {
			break
		}
		log.Printf("s3blob.Put: %s/%s (attempt %d): %v\n", b.bucket, key, retries, err)
		if err = retry.Wait(ctx, b.retrier, retries); err != nil {
			break
		}
	}
	if err != nil && kind(err) != errors.Canceled {
		err = errors.E("s3blob.Put", b.bucket, key, kind(err), err)
	}
	return err
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
			return reflow.Fileset{}, errors.E("s3blob.Snapshot", b.bucket, prefix, kind(err), err)
		}
		if head.ContentLength == nil || head.ETag == nil {
			return reflow.Fileset{}, errors.E("s3blob.Snapshot", b.bucket, prefix, errors.Invalid, errors.New("incomplete metadata"))
		}
		file := reflow.File{
			Source:       fmt.Sprintf("s3://%s/%s", b.bucket, prefix),
			ETag:         *head.ETag,
			Size:         *head.ContentLength,
			LastModified: aws.TimeValue(head.LastModified),
			ContentHash:  getContentHash(head.Metadata),
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
// If a non-empty contentHash is provided, it is stored in the object's metadata.
func (b *Bucket) Copy(ctx context.Context, src, dst string, contentHash string) error {
	err := b.copyObject(ctx, dst, b, src, contentHash)
	if err != nil {
		err = errors.E("s3blob.Copy", b.bucket, src, dst, kind(err), err)
	}
	return err
}

// CopyFrom copies from bucket src and key srcKey into this bucket.
// This is done directly without streaming the data through the client.
func (b *Bucket) CopyFrom(ctx context.Context, srcBucket blob.Bucket, src, dst string) error {
	srcB, ok := srcBucket.(*Bucket)
	if !ok {
		return errors.E(errors.NotSupported, "s3blob.CopyFrom", srcBucket.Location())
	}
	err := b.copyObject(ctx, dst, srcB, src, "")
	if err != nil {
		err = errors.E("s3blob.CopyFrom", b.Location(), dst, srcBucket.Location(), src, err)
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

// copyObject copies to this bucket and key from the given src bucket and srcKey.
// Since AWS doesn't allow copying files larger than defaultS3ObjectCopySizeLimit
// in a single operation, this does multi-part copy object in those cases.
// A non-empty contentHash will be added to destination object's metadata but
// only if not set in src's metadata (ie, src's contentHash if present takes precedence)
func (b *Bucket) copyObject(ctx context.Context, key string, src *Bucket, srcKey string, contentHash string) error {
	srcUrl, dstUrl := path.Join(src.bucket, srcKey), path.Join(b.bucket, key)
	srcFile, err := src.File(ctx, srcKey)
	if err != nil {
		return err
	}
	if srcFile.Size <= b.s3ObjectCopySizeLimit {
		// Do single copy
		input := &s3.CopyObjectInput{
			Bucket:     aws.String(b.bucket),
			Key:        aws.String(key),
			CopySource: aws.String(srcUrl),
		}
		// We set metadata only if the src file doesn't already have it and we are provided one.
		if srcFile.ContentHash.IsZero() && contentHash != "" {
			input.Metadata = map[string]*string{awsContentSha256Key: aws.String(contentHash)}
		}
		_, err = b.client.CopyObjectWithContext(ctx, input)
		return err
	}
	// Do a multi-part copy
	numParts := (srcFile.Size + b.s3MultipartCopyPartSize - 1) / b.s3MultipartCopyPartSize
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}
	// For a multi-part copy, metadata isn't transferred from src because technically we are creating a new object,
	// and then merely telling S3 to fill its parts by copying from another existing object.
	// This means, we must always set Metadata in the request.
	// TODO(swami): Copy all of src's metadata, not just the hash.
	if !srcFile.ContentHash.IsZero() {
		input.Metadata = map[string]*string{awsContentSha256Key: aws.String(srcFile.ContentHash.Hex())}
	} else if contentHash != "" {
		input.Metadata = map[string]*string{awsContentSha256Key: aws.String(contentHash)}
	}
	createOut, err := b.client.CreateMultipartUploadWithContext(ctx, input)
	if err != nil {
		return errors.E(fmt.Sprintf("CreateMultipartUpload: %s -> %s", srcUrl, dstUrl), err)
	}
	completedParts := make([]*s3.CompletedPart, numParts)
	err = traverse.Limit(s3MultipartCopyConcurrencyLimit).Each(int(numParts), func(ti int) error {
		i := int64(ti)
		firstByte := i * b.s3MultipartCopyPartSize
		lastByte := firstByte + b.s3MultipartCopyPartSize - 1
		if lastByte >= srcFile.Size {
			lastByte = srcFile.Size - 1
		}
		var err error
		var uploadOut *s3.UploadPartCopyOutput
		for retries := 0; ; retries++ {
			uploadOut, err = b.client.UploadPartCopyWithContext(ctx, &s3.UploadPartCopyInput{
				Bucket:          aws.String(b.bucket),
				Key:             aws.String(key),
				CopySource:      aws.String(srcUrl),
				UploadId:        createOut.UploadId,
				PartNumber:      aws.Int64(i + 1),
				CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", firstByte, lastByte)),
			})
			if err == nil || !retryable(err) {
				break
			}
			log.Debugf("s3blob.copyObject: attempt (%d) (part %d/%d): %s -> %s\n%v\n", retries, i, numParts, srcUrl, dstUrl, err)
			if err = retry.Wait(ctx, b.retrier, retries); err != nil {
				break
			}
		}
		if err == nil {
			completedParts[i] = &s3.CompletedPart{ETag: uploadOut.CopyPartResult.ETag, PartNumber: aws.Int64(i + 1)}
			log.Debugf("s3blob.copyObject: done (part %d/%d): %s -> %s", i, numParts, srcUrl, dstUrl)
			return nil
		}
		return errors.E(fmt.Sprintf("upload part copy (part %d/%d) %s -> %s", i, numParts, srcUrl, dstUrl), kind(err), err)
	})
	if err == nil {
		// Complete the multi-part copy
		for retries := 0; ; retries++ {
			_, err = b.client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
				Bucket:          aws.String(b.bucket),
				Key:             aws.String(key),
				UploadId:        createOut.UploadId,
				MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
			})
			if err == nil || kind(err) != errors.Temporary {
				break
			}
			log.Debugf("s3blob.copyObject complete upload: attempt (%d): %s -> %s\n%v\n", retries, srcUrl, dstUrl, err)
			if err = retry.Wait(ctx, b.retrier, retries); err != nil {
				break
			}
		}
		if err == nil {
			log.Debugf("s3blob.copyObject: done (all %d parts): %s -> %s", numParts, srcUrl, dstUrl)
			return nil
		}
		err = errors.E(fmt.Sprintf("complete multipart upload %s -> %s", srcUrl, dstUrl), kind(err), err)
	}
	// Abort the multi-part copy
	if _, er := b.client.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(b.bucket),
		Key:      aws.String(key),
		UploadId: createOut.UploadId,
	}); er != nil {
		err = errors.E(fmt.Sprintf("abort multipart copy %v", er), err)
	}
	return err
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
	if request.IsErrorThrottle(err) {
		return errors.ResourcesExhausted
	}
	if request.IsErrorRetryable(err) {
		return errors.Temporary
	}
	if aerr.Code() == request.CanceledErrorCode {
		return errors.Canceled
	}
	// The underlying error was an S3 error. Try to classify it.
	// Best guess based on Amazon's descriptions:
	switch aerr.Code() {
	// Code NotFound is not documented, but it's what the API actually returns.
	case s3.ErrCodeNoSuchBucket, s3.ErrCodeNoSuchKey, "NoSuchVersion", "NotFound":
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
	case "BadRequest":
		return errors.Temporary
	}
	return errors.Other
}
