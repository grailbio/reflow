// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package s3

import (
	"net/url"
	"sync"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository"
)

var (
	mu            sync.Mutex
	clients       = map[string]s3iface.S3API{}
	defaultClient s3iface.S3API
)

func init() {
	repository.RegisterScheme("s3r", Dial)
}

// SetClient sets the default s3 client to use for dialling repositories.
// If non-nil, it is used when there is not a more specific per-bucket client.
func SetClient(client s3iface.S3API) {
	defaultClient = client
}

// Dial dials an s3 repository. The URL must have the form:
//
//	s3r://bucket/prefix
//
// TODO(marius): we should support shipping authentication
// information in the URL also.
func Dial(u *url.URL) (reflow.Repository, error) {
	if u.Scheme != "s3r" {
		return nil, errors.E("dial", u.String(), errors.NotSupported, errors.Errorf("unknown scheme %v", u.Scheme))
	}
	bucket := u.Host
	mu.Lock()
	client := clients[bucket]
	mu.Unlock()
	if client == nil {
		client = defaultClient
	}
	if client == nil {
		return nil, errors.E("dial", u.String(), errors.NotSupported, "bucket %s not registered for dialing", bucket)
	}
	return &Repository{
		Client: client,
		Bucket: bucket,
		Prefix: u.Path,
	}, nil
}
