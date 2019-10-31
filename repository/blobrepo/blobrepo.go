// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package blobrepo implements a generic reflow.Repository on top of
// a blob.Bucket.
package blobrepo

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/url"
	"path"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/liveset"
	"github.com/grailbio/reflow/log"
)

const (
	objectsPath = "objects"
	uploadsPath = "uploads"
)

// Repository implements an blob-backed Repository. Objects are stored
// in the given bucket under the given prefix, followed by "objects":
//
//	type://bucket/<prefix>/objects/sha256:<hex>...
//
// In-progress uploads are stored under "uploads":
//
//	type://bucket/<prefix>/uploads/<hex>
//
// Prefix may be empty.
type Repository struct {
	Bucket blob.Bucket
	Prefix string
}

// String returns the repository URL.
func (r *Repository) String() string {
	return r.Bucket.Location() + r.Prefix
}

// Stat queries the repository for object metadata.
func (r *Repository) Stat(ctx context.Context, id digest.Digest) (reflow.File, error) {
	id, err := r.resolve(ctx, id)
	if err != nil {
		return reflow.File{}, err
	}
	file, err := r.Bucket.File(ctx, path.Join(r.Prefix, objectsPath, id.String()))
	if err == nil {
		file.ID = id
	}
	return file, err
}

// Location returns the location of this object.
func (r *Repository) Location(ctx context.Context, id digest.Digest) (string, error) {
	id, err := r.resolve(ctx, id)
	if err != nil {
		return "", err
	}
	var src string
	file, err := r.Bucket.File(ctx, path.Join(r.Prefix, objectsPath, id.String()))
	if err == nil {
		src = file.Source
	}
	return src, err
}

// Get retrieves an object from the repository.
func (r *Repository) Get(ctx context.Context, id digest.Digest) (io.ReadCloser, error) {
	id, err := r.resolve(ctx, id)
	if err != nil {
		return nil, err
	}
	rc, _, err := r.Bucket.Get(ctx, path.Join(r.Prefix, objectsPath, id.String()), "")
	return rc, err
}

// GetFile retrieves an object from the repository directly to the a io.WriterAt.
// This uses the S3 download manager to download chunks concurrently.
func (r *Repository) GetFile(ctx context.Context, id digest.Digest, w io.WriterAt) (int64, error) {
	id, err := r.resolve(ctx, id)
	if err != nil {
		return 0, err
	}
	return r.Bucket.Download(ctx, path.Join(r.Prefix, objectsPath, id.String()), "", 0, w)
}

// Put installs an object into the repository; its digest ID is returned.
func (r *Repository) Put(ctx context.Context, body io.Reader) (digest.Digest, error) {
	dw := reflow.Digester.NewWriter()
	uploadKey := path.Join(r.Prefix, uploadsPath, newID())
	err := r.Bucket.Put(ctx, uploadKey, 0, io.TeeReader(body, dw), "")
	if err != nil {
		return digest.Digest{}, err
	}
	defer r.Bucket.Delete(ctx, uploadKey)
	id := dw.Digest()
	return id, r.Bucket.Copy(ctx, uploadKey, path.Join(r.Prefix, objectsPath, id.String()), id.Hex())
}

// PutFile installs a file into the repository. PutFile uses the S3 upload manager
// directly.
func (r *Repository) PutFile(ctx context.Context, file reflow.File, body io.Reader) error {
	// TODO: check that the sizes match, etc.
	if _, err := r.Stat(ctx, file.ID); err == nil {
		return nil
	}
	key := path.Join(r.Prefix, objectsPath, file.ID.String())
	return r.Bucket.Put(ctx, key, file.Size, body, file.ID.Hex())
}

// WriteTo is unsupported by the blob repository.
//
// TODO(marius): we can support other s3r here by performing CopyObjects.
func (r *Repository) WriteTo(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("writeto", r.URL().String(), id, u.String(), errors.NotSupported)
}

// ReadFrom is unsupported by the blob repository.
//
// TODO(marius): we can support other s3r here by performing CopyObjects.
func (r *Repository) ReadFrom(_ context.Context, id digest.Digest, u *url.URL) error {
	return errors.E("readfrom", r.URL().String(), id, u.String(), errors.NotSupported)
}

// Collect is not supported on S3.
func (r *Repository) Collect(ctx context.Context, live liveset.Liveset) error {
	return errors.E("collect", errors.NotSupported)
}

// Resolve resolves the appropriate ID if it is abbreviated.
// Unabbreviated IDs are returned immediately.
func (r *Repository) resolve(ctx context.Context, id digest.Digest) (digest.Digest, error) {
	if !id.IsAbbrev() {
		return id, nil
	}
	var (
		abbrev = id.ShortString(id.NPrefix())
		prefix = path.Join(r.Prefix, objectsPath, abbrev)
		scan   = r.Bucket.Scan(prefix)
	)
	if !scan.Scan(ctx) {
		return id, errors.E("blobrepo.resolve", id, errors.NotExist)
	}
	key := scan.Key()

	if scan.Scan(ctx) {
		return id, errors.E("blobrepo.resolve", id,
			errors.Errorf("abbreviated id %s not unique", abbrev))
	}
	if err := scan.Err(); err != nil {
		return id, errors.E("blobrepo.resolve", id, err)
	}
	_, name := path.Split(key)
	return reflow.Digester.Parse(name)
}

const (
	deleteMaxTries   = 6
	deleteMaxObjects = 1000
)

var deletePolicy = retry.MaxTries(retry.Backoff(2*time.Second, 30*time.Second, 2), deleteMaxTries)

// delete deletes objects from the repository's bucket, with exponential backoff.
func (r *Repository) delete(ctx context.Context, keys []string) error {
	var err error
	for try := 0; ; try++ {
		err = r.Bucket.Delete(ctx, keys...)
		if err == nil {
			break
		}
		err = retry.Wait(ctx, deletePolicy, try)
	}
	return err
}

// CollectWithThreshold removes from this repository any objects which are not in the
// liveset and which have not been accessed more recently than the liveset's
// threshold time
func (r *Repository) CollectWithThreshold(ctx context.Context, live liveset.Liveset, dead liveset.Liveset, threshold time.Time, dryRun bool) error {
	var (
		objectsCheckedCount int64
		liveObjectsCount    int64
		invalidObjectsCount int64
		afterThresholdCount int64
		collectedCount      int64
		totalBytesCollected int64
		start               = time.Now()
		todo                = make([]string, 0, deleteMaxObjects)
	)

	scan := r.Bucket.Scan(path.Join(r.Prefix, objectsPath))
	for scan.Scan(ctx) {
		objectsCheckedCount++
		if objectsCheckedCount%10000 == 0 {
			// This can take a long time, we want to know it's doing something
			log.Debugf("checking object %d in repository", objectsCheckedCount)
		}
		var (
			file = scan.File()
			key  = scan.Key()
		)
		digest, err := reflow.Digester.Parse(path.Base(key))
		if err != nil {
			invalidObjectsCount++
			log.Errorf("invalid s3 entry %v (%s)", key, file)
			continue
		}
		if live.Contains(digest) {
			liveObjectsCount++
		} else if !dead.Contains(digest) && file.LastModified.After(threshold) {
			afterThresholdCount++
		} else {
			if !dryRun {
				// Stick this on our delete queue
				todo = append(todo, key)

				// If we reach enough objects do a batch delete on them
				if len(todo) == cap(todo) {
					err = r.delete(ctx, todo)
					if err != nil {
						log.Errorf("error deleting objects: %v", err)
					}
					todo = todo[:0]
				}
			}
			collectedCount++
			totalBytesCollected += file.Size
		}
	}

	if len(todo) > 0 && !dryRun {
		err := r.delete(ctx, todo)
		if err != nil {
			log.Errorf("error deleting objects: %v", err)
		}
	}

	// Print what happened
	log.Debugf("time to collect %s: %s", r.Bucket, time.Since(start))
	log.Debugf("checked %d objects, %d were live, %d were after the threshold, %d were invalid.",
		objectsCheckedCount, liveObjectsCount, afterThresholdCount, invalidObjectsCount)
	action := "would have been"
	if !dryRun {
		action = "were"
	}
	log.Printf("%d of %d objects (%.2f%%) and %d bytes %s collected",
		collectedCount, objectsCheckedCount, float64(collectedCount)/float64(objectsCheckedCount)*100, totalBytesCollected, action)

	return scan.Err()
}

// URL returns the URL for this repository. It is of the form:
//
//	<type>://bucket/prefix
func (r *Repository) URL() *url.URL {
	u, err := url.Parse(r.Bucket.Location() + "/" + r.Prefix)
	if err != nil {
		panic(err)
	}
	return u
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
