// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package s3client

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/s3walker"
	"github.com/grailbio/reflow/resolver"
)

func Resolver(s3client Client) resolver.Resolver {
	return resolver.Func(func(ctx context.Context, rawurl string) (reflow.Fileset, error) {
		u, err := url.Parse(rawurl)
		if err != nil {
			return reflow.Fileset{}, err
		}
		bucket, path := u.Host, strings.TrimPrefix(u.Path, "/")

		config := &aws.Config{
			MaxRetries: aws.Int(10),
			Region:     aws.String("us-west-2"),
		}
		client := s3client.New(config)
		loc, err := client.GetBucketLocationWithContext(ctx, &s3.GetBucketLocationInput{
			Bucket: aws.String(bucket),
		})
		if err == nil {
			region := aws.StringValue(loc.LocationConstraint)
			if region == "" {
				// This is a bit of an AWS wart: if the region is empty,
				// it means us-east-1; however, the API does not accept
				// an empty region.
				region = "us-east-1"
			}
			config.Region = aws.String(region)
		} else {
			config.Region = aws.String(DefaultRegion)
		}
		client = s3client.New(config)

		// TODO(marius): include checksum if the object has a x-content-sha256 header?

		if path != "" && !strings.HasSuffix(path, "/") {
			head, err := client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(path),
			})
			if err != nil {
				return reflow.Fileset{}, errors.E("HeadObject", path, err, ErrKind(err))
			}
			if head.ContentLength == nil || head.ETag == nil {
				return reflow.Fileset{}, errors.E(errors.Invalid, errors.Errorf("s3://%s/%s: incomplete metadata", bucket, path))
			}
			file := reflow.File{
				Source: fmt.Sprintf("s3://%s/%s", bucket, path),
				ETag:   *head.ETag,
				Size:   *head.ContentLength,
			}
			return reflow.Fileset{Map: map[string]reflow.File{".": file}}, nil
		}

		var (
			w       = &s3walker.S3Walker{S3: client, Bucket: bucket, Prefix: path}
			dir     = reflow.Fileset{Map: make(map[string]reflow.File)}
			nprefix = len(path)
		)
		for w.Scan(ctx) {
			key := aws.StringValue(w.Object().Key)
			if len(key) < nprefix {
				continue
			}
			// Skip "directories".
			if strings.HasSuffix(key, "/") {
				continue
			}
			if w.Object().ETag == nil || w.Object().Size == nil {
				return reflow.Fileset{}, errors.E(errors.Invalid, errors.Errorf("s3://%s/%s: incomplete metadata", bucket, path))
			}

			dir.Map[key[nprefix:]] = reflow.File{
				Source: fmt.Sprintf("s3://%s/%s", bucket, key),
				ETag:   *w.Object().ETag,
				Size:   *w.Object().Size,
			}
		}
		return dir, nil
	})
}
