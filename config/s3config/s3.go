// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package s3config defines a configuration provider named "s3"
// which can be used to configure S3-based caches.
package s3config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/config/dynamodbconfig"
	reflows3 "github.com/grailbio/reflow/repository/s3"
)

func init() {
	// Provided for backwards compatibility.
	// Prefer using repository and assoc separately.
	config.Register(config.Cache, "s3", "bucket,table", "configure a cache using an S3 bucket and DynamoDB table (legacy)",
		func(cfg config.Config, arg string) (config.Config, error) {
			parts := strings.Split(arg, ",")
			if n := len(parts); n != 2 {
				return nil, fmt.Errorf("cache: s3: expected 2 arguments, got %d", n)
			}
			cfg = &repository{Config: cfg, Bucket: parts[0]}
			cfg = &dynamodbconfig.Assoc{Config: cfg, Table: parts[1]}
			cfg = &cacheMode{Config: cfg, Mode: reflow.CacheRead | reflow.CacheWrite}
			return cfg, nil
		},
	)
	config.Register(config.Repository, "s3", "bucket", "configure a repository using an S3 bucket",
		func(cfg config.Config, arg string) (config.Config, error) {
			if arg == "" {
				return nil, errors.New("bucket name not provided")
			}
			return &repository{cfg, arg}, nil
		},
	)
}

type repository struct {
	config.Config
	Bucket string
}

// Repository returns a new repository instance as configured by this
// S3 repository configuration.
func (r *repository) Repository() (reflow.Repository, error) {
	sess, err := r.AWS()
	if err != nil {
		return nil, err
	}
	// Set the default client for dialing here.
	// TODO(marius): this should be done outside of the specific configs.
	reflows3.SetClient(s3.New(sess))
	return &reflows3.Repository{
		Bucket: r.Bucket,
		Client: s3.New(sess),
	}, nil
}

type cacheMode struct {
	config.Config
	Mode reflow.CacheMode
}

func (m *cacheMode) CacheMode() reflow.CacheMode {
	return m.Mode
}
