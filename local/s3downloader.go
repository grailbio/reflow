// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"context"
	"io/ioutil"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/grailbio/base/data"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/base/sync/ctxsync"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository/file"
	"github.com/grailbio/reflow/s3/s3client"
)

// S3downloader manages concurrent downloads from S3. It is also
// responsible for resolving buckets to per-region clients.
type s3downloader struct {
	client        s3client.Client
	tempdir       string
	digestLimiter *limiter.Limiter

	mu      sync.Mutex
	cond    *ctxsync.Cond
	buckets map[string]*s3manager.Downloader
}

// NewS3downloader initializes and then returns a downloader
// provided a client, IO limiter, and temporary download directory.
func newS3downloader(client s3client.Client, digestLimiter *limiter.Limiter, tempdir string) *s3downloader {
	d := &s3downloader{
		tempdir:       tempdir,
		client:        client,
		digestLimiter: digestLimiter,
		buckets:       make(map[string]*s3manager.Downloader),
	}
	d.cond = ctxsync.NewCond(&d.mu)
	return d
}

func (d *s3downloader) get(ctx context.Context, bucket string) (*s3manager.Downloader, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for {
		dl, ok := d.buckets[bucket]
		switch {
		case ok && dl != nil:
			return dl, nil
		case ok:
			if err := d.cond.Wait(ctx); err != nil {
				return nil, err
			}
		default:
			d.buckets[bucket] = nil
			d.mu.Unlock()
			dl, err := d.downloader(ctx, bucket)
			d.mu.Lock()
			if err != nil {
				delete(d.buckets, bucket)
			} else {
				d.buckets[bucket] = dl
			}
			d.cond.Broadcast()
			return dl, err
		}
	}
}

func (d *s3downloader) downloader(ctx context.Context, bucket string) (*s3manager.Downloader, error) {
	const (
		// The default file concurrency is 100.
		// 10*100 = 1000 maximum TCP streams, for the pool.
		// This isn't a global limit, and isn't ideal for downloads
		// that have a large number of small files.
		//
		// TODO(marius): use the repository limiter here, with an
		// additional restriction to limit TCP streams so that we
		// don't run out of file descriptors.
		chunkConcurrency = 20

		partSize = 100 << 20
	)

	config := &aws.Config{
		MaxRetries: aws.Int(10),
		Region:     aws.String(s3client.DefaultRegion),
	}
	client := d.client.New(config)
	rep, err := client.GetBucketLocationWithContext(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err == nil {
		region := aws.StringValue(rep.LocationConstraint)
		if region == "" {
			// This is a bit of an AWS wart: if the region is empty,
			// it means us-east-1; however, the API does not accept
			// an empty region.
			region = "us-east-1"
		}
		// TODO(marius): write to supplied logger?
		log.Printf("discovered region %s for %s", region, bucket)
		config.Region = aws.String(region)
	} else if err == ctx.Err() {
		return nil, ctx.Err()
	} else {
		log.Errorf("could not discover region for bucket %s: %v", bucket, err)
	}
	client = d.client.New(config)
	return s3manager.NewDownloaderWithClient(client, func(d *s3manager.Downloader) {
		d.Concurrency = chunkConcurrency
		d.PartSize = partSize
	}), nil
}

// Download downloads the provided bucket/key into the provided repository. Download
// checks that the downloaded sizes and etags match. Empty etags are not checked.
func (d *s3downloader) Download(ctx context.Context, repo *file.Repository, bucket, key string, size int64, etag string) (reflow.File, error) {
	dl, err := d.get(ctx, bucket)
	if err != nil {
		return reflow.File{}, err
	}
	fetchingFiles.Add(1)
	defer fetchingFiles.Add(-1)
	temp, err := ioutil.TempFile(d.tempdir, "")
	if err != nil {
		return reflow.File{}, err
	}
	defer func() {
		if err := os.Remove(temp.Name()); err != nil {
			log.Errorf("failed to remove file %q: %v", temp.Name(), err)
		}
	}()
	var w bytewatch
	w.Reset()
	log.Printf("download s3://%s/%s (%s) to %s", bucket, key, data.Size(size), temp.Name())
	downloadingFiles.Add(1)
	req := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if etag != "" {
		req.IfMatch = aws.String(etag)
	}
	_, err = dl.DownloadWithContext(ctx, temp, req)
	downloadingFiles.Add(-1)
	if err != nil {
		// TODO(marius): special propagation of "412 precondition failed" errors?
		// (i.e., when ETags are mismatched).
		// e.FileLimiter.Release(1)
		log.Printf("download s3://%s/%s: %v", bucket, key, err)
		return reflow.File{}, err
	}
	if err := temp.Close(); err != nil {
		return reflow.File{}, err
	}
	dur, bps := w.Lap(size)
	log.Printf("done s3://%s/%s in %s (%s/s)", bucket, key, dur, data.Size(bps))

	// We only admit the next file upload once we have secured
	// a spot for digesting. This way backpressure propagates
	// and the bottleneck limits total throughput.
	if err := d.digestLimiter.Acquire(ctx, 1); err != nil {
		return reflow.File{}, err
	}
	defer d.digestLimiter.Release(1)
	w.Reset()
	digestingFiles.Add(1)
	file, err := repo.Install(temp.Name())
	digestingFiles.Add(-1)
	if err != nil {
		log.Errorf("install s3://%s/%s: %v", bucket, key, err)
		return file, err
	}
	if file.Size != size {
		err = errors.E(errors.Integrity,
			errors.Errorf("expected size %d does not match actual size %d", size, file.Size))
	}
	dur, bps = w.Lap(size)
	log.Printf("installed s3://%s/%s (%s) to %v in %s (%s/s)", bucket, key, file, temp, dur, data.Size(bps))
	return file, err
}
