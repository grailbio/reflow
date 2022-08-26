// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package s3blob implements the blob interfaces for S3.
package s3blob

import (
	"context"
	"fmt"
	"io"
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
	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	baseerrors "github.com/grailbio/base/errors"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/s3util"
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

	s3minpartsize = 5 << 20
	// The maximum number of parts allowed by S3 for multi-part operations.
	s3MaxParts        = 10000
	s3concurrency     = 100
	defaultS3MinLimit = 500
	defaultMaxRetries = 3

	// See: https://docs.google.com/document/d/1Nl3UyQXTRusXDu8tIt_s9N6JxXu1vhuKeBYssyaKcGU
	// defaultS3AIMDDecFactor is the default decrease factor for the AIMD-based admission controller policy.
	defaultS3AIMDDecFactor = 10
	// defaultS3HeadLatencyLimit is the max acceptable latency for S3 HeadObject calls.
	defaultS3HeadLatencyLimit = 300 * time.Millisecond

	// minBPS defines the lowest acceptable transfer rate.
	minBPS = 1 << 20
	// preferredBPS defines the preferred transfer rate.
	preferredBPS = 10 << 20
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

	// s3DeleteKeyLimit is the max number of keys supported by AWS per batch delete operation.
	// As per AWS: https://docs.aws.amazon.com/AmazonS3/latest/dev/DeletingObjects.html
	s3DeleteKeyLimit = 1000
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
		Endpoint:   aws.String(fmt.Sprintf("s3.%s.amazonaws.com", region)),
	}
	return NewBucket(bucket, s3.New(s.sess, &config)), nil
}

// NewS3RetryPolicy returns a default retry.Policy useful for S3 operations.
func newS3RetryPolicy() retry.Policy {
	return retry.MaxRetries(retry.Jitter(retry.Backoff(2*time.Second, time.Minute, 4), 0.25), defaultMaxRetries)
}

// NewS3AimdPolicy returns a default admit.RetryPolicy backed by an AIMD admission controller.
func newS3AimdPolicy(varname string) admit.RetryPolicy {
	rp := retry.MaxRetries(retry.Jitter(retry.Backoff(500*time.Millisecond, time.Minute, 1.5), 0.5), defaultMaxRetries)
	c := admit.AIMDWithRetry(defaultS3MinLimit, defaultS3AIMDDecFactor, rp)
	admit.EnableVarExport(c, varname)
	return c
}

// Bucket represents an s3 bucket; it implements blob.Bucket.
type Bucket struct {
	bucket       string
	client       s3iface.S3API
	admitter     admit.RetryPolicy
	fileAdmitter admit.RetryPolicy
	retrier      retry.Policy

	// s3ObjectCopySizeLimit is the max size of object for a single PUT Object Copy request.
	s3ObjectCopySizeLimit int64
	// s3MultipartCopyPartSize is the max size of each part when doing a multi-part copy.
	s3MultipartCopyPartSize int64
}

// NewBucket returns a new S3 bucket that uses the provided client
// for SDK calls. NewBucket is primarily intended for testing.
func NewBucket(name string, client s3iface.S3API) *Bucket {
	return &Bucket{
		name, client,
		newS3AimdPolicy("s3data"),
		newS3AimdPolicy("s3head"),
		newS3RetryPolicy(),
		defaultS3ObjectCopySizeLimit,
		defaultS3MultipartCopyPartSize,
	}
}

// File returns metadata for the provided key.
func (b *Bucket) File(ctx context.Context, key string) (reflow.File, error) {
	var resp *s3.HeadObjectOutput
	var err error
	for retries := 0; ; retries++ {
		err = admit.Retry(ctx, b.fileAdmitter, 1, func() (admit.CapacityStatus, error) {
			ctx, cancel := context.WithTimeout(ctx, metaTimeout)
			defer cancel()
			start := time.Now()
			resp, err = b.client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(b.bucket),
				Key:    aws.String(key),
			})
			dur := time.Since(start)
			err = s3util.CtxErr(ctx, err)
			if kind(err) == errors.ResourcesExhausted {
				log.Printf("s3blob.File: %s/%s: %v (over capacity)\n", b.bucket, key, err)
				return admit.OverNeedRetry, err
			}
			if dur > defaultS3HeadLatencyLimit {
				return admit.OverNoRetry, err
			}
			return admit.Within, err
		})
		if !retryable(ctx, err) {
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
		Size:         aws.Int64Value(resp.ContentLength),
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
// provided prefix. If withMetadata is true, the scanner is configured
// to make a best-effort attempt to fetch each object's metadata.
func (b *Bucket) Scan(prefix string, withMetadata bool) blob.Scanner {
	walker := &s3walker.S3Walker{
		S3:      b.client,
		Bucket:  b.bucket,
		Prefix:  prefix,
		Policy:  b.fileAdmitter,
		Retrier: b.retrier,
	}
	if withMetadata {
		walker = walker.WithMetadata()
	}
	return &scanner{
		S3Walker: walker,
		bucket:   b.bucket,
	}
}

// S3TransferParams returns the optimal (part size, concurrency) for parallel data transfers
// based on the file size.
// Returns part size in MiB and concurrency in the range [1, s3concurrency]
func s3TransferParams(size int64) (int64, int) {
	if size == 0 {
		return s3minpartsize, s3concurrency
	}
	partSize := int64(s3minpartsize)
	if size > s3minpartsize*s3MaxParts {
		// For the given size, the default part size would result in too many parts
		// so we compute a larger part size to (safely) fall within the max allowed parts.
		partSzB := size / s3MaxParts
		partSzMiB := int64((data.Size(partSzB) + 5*data.MiB).Count(data.MiB))
		partSize = partSzMiB * data.MiB.Bytes()
	}
	c := (size + partSize - 1) / partSize
	if c > s3concurrency {
		c = s3concurrency
	}
	return partSize, int(c)
}

// transferDuration estimates the time duration it would take to transfer
// data of the given size at the given rate.
func transferDuration(size int64, rate int) time.Duration {
	if size == 0 {
		return unknownSizeTimeout
	}
	return time.Duration(size/int64(rate)) * time.Second
}

func timeoutPolicy(timeout time.Duration) retry.Policy {
	if timeout < minTimeout {
		timeout = minTimeout
	}
	return retry.Backoff(timeout, 3*timeout, 1.5)
}

func timeout(policy retry.Policy, retries int) time.Duration {
	_, timeout := policy.Retry(retries)
	return timeout
}

// retryable returns whether an error is retryable based on the request context
func retryable(ctx context.Context, err error) bool {
	kinds := []errors.Kind{errors.Temporary, errors.Timeout}
	kinds = append(kinds, errors.GetRetryableKinds(ctx)...)
	return isAnyOf(err, kinds...)
}

// isAnyOf returns whether an error is any of the given kinds.
func isAnyOf(err error, kinds ...errors.Kind) bool {
	if err == nil {
		return false
	}
	kind := kind(err)
	for _, k := range kinds {
		if kind == k {
			return true
		}
	}
	return false
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
	var (
		n                         int64
		err                       error
		s3partsize, s3concurrency = s3TransferParams(size)
		policy                    = timeoutPolicy(transferDuration(size, minBPS))
		preferredDur              = transferDuration(size, preferredBPS)
	)
	for retries := 0; ; retries++ {
		err = admit.Retry(ctx, b.admitter, s3concurrency, func() (admit.CapacityStatus, error) {
			d := s3manager.NewDownloaderWithClient(b.client, func(d *s3manager.Downloader) {
				d.PartSize = s3partsize
				d.Concurrency = s3concurrency
			})
			ctx, cancel := context.WithTimeout(ctx, timeout(policy, retries))
			defer cancel()
			start := time.Now()
			n, err = d.DownloadWithContext(ctx, w, b.getObjectInput(key, etag))
			dur := time.Since(start)
			err = s3util.CtxErr(ctx, err)
			if kind(err) == errors.ResourcesExhausted {
				log.Printf("s3blob.Download: %s/%s: %v (over capacity)\n", b.bucket, key, err)
				return admit.OverNeedRetry, err
			}
			if dur > preferredDur {
				return admit.OverNoRetry, err
			}
			return admit.Within, err
		})
		if !retryable(ctx, err) {
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
	var (
		err                       error
		s3partsize, s3concurrency = s3TransferParams(size)
		policy                    = timeoutPolicy(transferDuration(size, minBPS))
		preferredDur              = transferDuration(size, preferredBPS)
	)
	for retries := 0; ; retries++ {
		err = admit.Retry(ctx, b.admitter, s3concurrency, func() (admit.CapacityStatus, error) {
			up := s3manager.NewUploaderWithClient(b.client, func(u *s3manager.Uploader) {
				u.PartSize = s3partsize
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
			start := time.Now()
			_, err = up.UploadWithContext(ctx, input)
			dur := time.Since(start)
			err = s3util.CtxErr(ctx, err)
			if kind(err) == errors.ResourcesExhausted {
				log.Printf("s3blob.Put: %s/%s: %v (over capacity)\n", b.bucket, key, err)
				return admit.OverNeedRetry, err
			}
			if dur > preferredDur {
				return admit.OverNoRetry, err
			}
			return admit.Within, err
		})
		if !retryable(ctx, err) {
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
		file, err := b.File(ctx, prefix)
		if err != nil {
			return reflow.Fileset{}, errors.E("s3blob.Snapshot", b.bucket, prefix, err)
		}
		if file.ETag == "" || file.Size == 0 {
			return reflow.Fileset{}, errors.E("s3blob.Snapshot", b.bucket, prefix, errors.Invalid, errors.New("incomplete metadata"))
		}
		return reflow.Fileset{Map: map[string]reflow.File{".": file}}, nil
	}

	var (
		dir     = reflow.Fileset{Map: make(map[string]reflow.File)}
		nprefix = len(prefix)
	)
	const withMetadata = true
	scan := b.Scan(prefix, withMetadata)
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
		err = errors.E("s3blob.CopyFrom", b.bucket, b.Location(), dst, srcBucket.Location(), src, err)
	}
	return err
}

// Delete removes the provided keys in bulk.
func (b *Bucket) Delete(ctx context.Context, keys ...string) (err error) {
	var failed []string
	numiters := len(keys) / s3DeleteKeyLimit
	if (len(keys) % s3DeleteKeyLimit) > 0 {
		numiters++
	}
	for i := 0; i < numiters; i++ {
		delkeys := keys[i*s3DeleteKeyLimit:]
		if len(delkeys) > s3DeleteKeyLimit {
			delkeys = delkeys[:s3DeleteKeyLimit]
		}
		var del s3.Delete
		for _, key := range delkeys {
			del.Objects = append(del.Objects, &s3.ObjectIdentifier{Key: aws.String(key)})
		}
		var out *s3.DeleteObjectsOutput
		out, err = b.client.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(b.bucket),
			Delete: &del,
		})
		if err != nil {
			break
		}
		for i, e := range out.Errors {
			if e != nil {
				failed = append(failed, delkeys[i])
			}
		}
	}
	if err != nil {
		return err
	}
	if len(failed) > 0 {
		return fmt.Errorf("failed to delete: %s", strings.Join(failed, ","))
	}
	return nil
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
	srcUrl, dstUrl := src.Location()+srcKey, b.Location()+key
	srcFile, err := src.File(ctx, srcKey)
	if err != nil {
		return err
	}
	metadata := make(map[string]*string)

	// We set metadata if the src file already has it.
	// If not, then we set it only if we are provided one.
	if srcContentHash := srcFile.ContentHash; !srcContentHash.IsZero() {
		metadata[awsContentSha256Key] = aws.String(srcContentHash.Hex())
	} else if contentHash != "" {
		metadata[awsContentSha256Key] = aws.String(contentHash)
	}

	copier := s3util.NewCopierWithParams(b.client, b.retrier, b.s3ObjectCopySizeLimit, b.s3MultipartCopyPartSize, log.Std)
	err = copier.Copy(ctx, srcUrl, dstUrl, srcFile.Size, metadata)
	if err != nil {
		// convert from base to reflow error.
		baseErr := baseerrors.Recover(err)
		err = errors.E(baseErr.Message, errors.BaseToReflow(baseErr.Kind, baseErr.Severity), baseErr.Err)
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

// kind interprets any error into a Reflow error kind.
func kind(err error) errors.Kind {
	if aerr, ok := err.(awserr.Error); ok {
		k, s := s3util.KindAndSeverity(aerr)
		return errors.BaseToReflow(k, s)
	}
	if re := errors.Recover(err); re != nil {
		return re.Kind
	}
	return errors.Other
}
