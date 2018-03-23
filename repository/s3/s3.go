// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package s3 implements an S3-backed repository. Objects are stored
// within a prefix in a bucket.
package s3

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/url"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/s3walker"
	"github.com/grailbio/reflow/liveset"
	"github.com/grailbio/reflow/log"
)

const (
	objectsPath = "objects"
	uploadsPath = "uploads"

	s3minpartsize      = 100 << 20
	s3maxpartsize      = 2 * s3minpartsize
	s3concurrency      = 20
	s3maxdeleteobjects = 1000
)

// Repository implements an S3-backed Repository. Objects are stored
// in the given bucket under the given prefix, followed by "objects":
//
//	s3://bucket/<prefix>/objects/sha256:<hex>...
//
// In-progress uploads are stored under "uploads":
//
//	s3://bucket/<prefix>/uploads/<hex>
type Repository struct {
	Client s3iface.S3API
	Bucket string
	Prefix string
}

// String returns the repository URL.
func (r *Repository) String() string {
	return fmt.Sprintf("s3r://%s/%s", r.Bucket, r.Prefix)
}

func (r *Repository) ShortString() string {
	s := "s3:" + r.Bucket
	if r.Prefix != "" {
		s += "/" + r.Prefix
	}
	return s
}

// Stat queries the repository for object metadata.
func (r *Repository) Stat(ctx context.Context, id digest.Digest) (reflow.File, error) {
	resp, err := r.Client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(r.Bucket),
		Key:    aws.String(path.Join(r.Prefix, objectsPath, id.String())),
	})
	if err != nil {
		// The S3 API presents inconsistent error codes between GetObject
		// and HeadObject. It seems that GetObject returns "NoSuchKey" for
		// a missing object, while HeadObject returns a body-less HTTP 404
		// error, which is then assigned the fallback HTTP error code
		// NotFound by the SDK.
		if err, ok := err.(awserr.Error); ok && (err.Code() == "NotFound" || err.Code() == "NoSuchKey" || err.Code() == "NoSuchBucket") {
			return reflow.File{}, errors.E("stat", r.URL().String(), id, errors.NotExist, err)
		}
		return reflow.File{}, err
	}
	if resp.ContentLength == nil {
		return reflow.File{}, errors.Errorf("stat %v %v: missing content length", r.URL(), id)
	}
	return reflow.File{ID: id, Size: *resp.ContentLength}, nil
}

// Get retrieves an object from the repository.
func (r *Repository) Get(ctx context.Context, id digest.Digest) (io.ReadCloser, error) {
	// TODO(marius): use s3manager.Downloader here. Note that this gets complicated since
	// since it requires a io.WriterAt, and thus loses compositionality (e.g., streaming
	// an s3 read over http...). We can recover this by implementing a buffer that streams
	// reads out of concurrent WriteAts. This implies a memory penalty, but it shouldn't
	// be too bad, modulo straggler chunks, since the AWS APIs retrieves the chunks serially.
	resp, err := r.Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(r.Bucket),
		Key:    aws.String(path.Join(r.Prefix, objectsPath, id.String())),
	})
	if err != nil {
		if err, ok := err.(awserr.Error); ok && err.Code() == "NoSuchKey" || err.Code() == "NoSuchBucket" {
			return nil, errors.E("open", r.URL().String(), id, errors.NotExist)
		}
		return nil, err
	}
	return resp.Body, nil
}

// GetFile retrieves an object from the repository directly to the a io.WriterAt.
// This uses the S3 download manager to download chunks concurrently.
func (r *Repository) GetFile(ctx context.Context, id digest.Digest, w io.WriterAt) (int64, error) {
	d := s3manager.NewDownloaderWithClient(r.Client, func(d *s3manager.Downloader) {
		d.Concurrency = s3concurrency
		d.PartSize = s3minpartsize
	})
	return d.Download(w, &s3.GetObjectInput{
		Bucket: aws.String(r.Bucket),
		Key:    aws.String(path.Join(r.Prefix, objectsPath, id.String())),
	})
}

// Put installs an object into the repository; its digest ID is returned.
func (r *Repository) Put(ctx context.Context, body io.Reader) (digest.Digest, error) {
	dw := reflow.Digester.NewWriter()
	up := s3manager.NewUploaderWithClient(r.Client, func(u *s3manager.Uploader) {
		u.PartSize = s3minpartsize
		u.Concurrency = s3concurrency
	})
	uploadKey := path.Join(r.Prefix, uploadsPath, newID())
	_, err := up.Upload(&s3manager.UploadInput{
		Bucket: aws.String(r.Bucket),
		Key:    aws.String(uploadKey),
		Body:   io.TeeReader(body, dw),
	})
	if err != nil {
		return digest.Digest{}, err
	}
	defer func() {
		r.Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(r.Bucket),
			Key:    aws.String(uploadKey),
		})
	}()
	id := dw.Digest()
	_, err = r.Client.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(r.Bucket),
		Key:        aws.String(path.Join(r.Prefix, objectsPath, id.String())),
		CopySource: aws.String(path.Join(r.Bucket, uploadKey)),
	})
	return id, err
}

// PutFile installs a file into the repository. PutFile uses the S3 upload manager
// directly.
func (r *Repository) PutFile(ctx context.Context, file reflow.File, body io.Reader) error {
	_, err := r.Stat(ctx, file.ID)
	// TODO: check that the sizes match, etc.
	if err == nil {
		return nil
	}
	up := s3manager.NewUploaderWithClient(r.Client, func(u *s3manager.Uploader) {
		u.PartSize = int64(file.Size / s3manager.MaxUploadParts)
		if u.PartSize < s3minpartsize {
			u.PartSize = s3minpartsize
		}
		if n := int64(file.Size / u.PartSize); n > s3concurrency {
			// Note that if this is set too high, the uploader will adjust it.
			u.PartSize = s3maxpartsize
		}
		u.Concurrency = s3concurrency
	})
	uploadKey := path.Join(r.Prefix, objectsPath, file.ID.String())
	_, err = up.Upload(&s3manager.UploadInput{
		Bucket: aws.String(r.Bucket),
		Key:    aws.String(uploadKey),
		Body:   body,
	})
	return err
}

// WriteTo is unsupported by the S3 repository.
//
// TODO(marius): we can support other s3r here by performing CopyObjects.
func (r *Repository) WriteTo(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("writeto", r.URL().String(), id, u.String(), errors.NotSupported)
}

// ReadFrom is unsupported by the S3 repository.
//
// TODO(marius): we can support other s3r here by performing CopyObjects.
func (r *Repository) ReadFrom(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("readfrom", r.URL().String(), id, u.String(), errors.NotSupported)
}

// Collect is not supported on S3.
func (r *Repository) Collect(ctx context.Context, live liveset.Liveset) error {
	return errors.E("collect", errors.NotSupported)
}

// CollectWithThreshold removes from this repository any objects which are not in the
// liveset and which have not been accessed more recently than the liveset's
// threshold time
func (r *Repository) CollectWithThreshold(ctx context.Context, live liveset.Liveset, threshold time.Time, dryRun bool) error {
	log.Debug("Collecting repository")
	objectsCheckedCount := int64(0)
	liveObjectsCount := int64(0)
	invalidObjectsCount := int64(0)
	afterThresholdCount := int64(0)
	collectedCount := int64(0)
	totalBytesCollected := int64(0)
	start := time.Now()

	// The objects we want to delete
	deleteObjects := s3.Delete{
		Objects: make([]*s3.ObjectIdentifier, 0, s3maxdeleteobjects),
	}

	w := &s3walker.S3Walker{S3: r.Client, Bucket: r.Bucket, Prefix: path.Join(r.Prefix, objectsPath)}
	for w.Scan(ctx) {
		objectsCheckedCount++
		if objectsCheckedCount%10000 == 0 {
			// This can take a long time, we want to know it's doing something
			log.Debugf("Checking object %d in repository", objectsCheckedCount)
		}

		key, modified, size := aws.StringValue(w.Object().Key), aws.TimeValue(w.Object().LastModified), aws.Int64Value(w.Object().Size)
		digest, err := reflow.Digester.Parse(path.Base(key))
		if err != nil {
			invalidObjectsCount++
			log.Errorf("Invalid s3 entry %v", w.Object())
			continue
		}

		if live.Contains(digest) {
			liveObjectsCount++
		} else if modified.After(threshold) {
			afterThresholdCount++
		} else {
			if !dryRun {
				// Stick this on our delete queue
				deleteObjects.Objects = append(deleteObjects.Objects, &s3.ObjectIdentifier{Key: w.Object().Key})

				// If we reach enough objects do a batch delete on them
				if len(deleteObjects.Objects) == cap(deleteObjects.Objects) {
					_, err = r.Client.DeleteObjectsWithContext(ctx,
						&s3.DeleteObjectsInput{Bucket: aws.String(r.Bucket), Delete: &deleteObjects})
					if err != nil {
						log.Errorf("error calling DeleteObjectsWithContext (%s)", err)
					}
					deleteObjects.Objects = deleteObjects.Objects[:0]
				}
			}
			collectedCount++
			totalBytesCollected += size
		}
	}

	if len(deleteObjects.Objects) > 0 && !dryRun {
		_, err := r.Client.DeleteObjectsWithContext(ctx,
			&s3.DeleteObjectsInput{Bucket: aws.String(r.Bucket), Delete: &deleteObjects})
		if err != nil {
			log.Errorf("error calling DeleteObjectsWithContext (%s)", err)
		}
	}

	// Print what happened
	log.Debugf("Time to collect %s: %s", r.Bucket, time.Since(start))
	log.Debugf("Checked %d objects, %d were live, %d were after the threshold, %d were invalid.",
		objectsCheckedCount, liveObjectsCount, afterThresholdCount, invalidObjectsCount)
	action := "would have been"
	if !dryRun {
		action = "were"
	}
	log.Printf("%d of %d objects (%.2f%%) and %d bytes %s collected",
		collectedCount, objectsCheckedCount, float64(collectedCount)/float64(objectsCheckedCount)*100, totalBytesCollected, action)

	return w.Err()
}

// URL returns the URL for this repository. It is of the form:
//
//	s3r://bucket/prefix
func (r *Repository) URL() *url.URL {
	return &url.URL{
		Scheme: "s3r",
		Host:   r.Bucket,
		Path:   r.Prefix,
	}
}

// newID returns a new, randomly generated hexadecimal
// identifier of length 16.
func newID() string {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", b[:])
}
