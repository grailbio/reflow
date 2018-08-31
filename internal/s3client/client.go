// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package s3client contains s3 client abstractions for use within
// Reflow.
package s3client

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// DefaultRegion is the region used for s3 requests if a bucket's
// region is undiscoverable (e.g., lacking permissions for the
// GetBucketLocation API call.)
//
// Amazon generally defaults to us-east-1 when regions are unspecified
// (or undiscoverable), but this can be overridden if a different default is
// desired.
var DefaultRegion = "us-east-2"

// Client defines an abstract constructor for S3 clients, permitting
// callers to construct customized S3 clients (e.g., specifying which
// region to use).
type Client interface {
	// New creates a new S3 client with a config.
	New(*aws.Config) s3iface.S3API
}

// Config defines a Client that constructs clients
// from a base config together with the one supplied by the
// user's call to New.
type Config struct {
	*aws.Config
}

// New constructs a new S3 client with a configuration based
// on the merged config of the one supplied by the user and
// a base config.
func (c *Config) New(user *aws.Config) s3iface.S3API {
	config := new(aws.Config)
	config.MergeIn(c.Config, user)
	return s3.New(session.New(config))
}

// Static is a Client that ignores the user's supplied configuration.
type Static struct {
	Client s3iface.S3API
}

// New ignores the provided user config and returns the statically
// configured client.
func (s *Static) New(user *aws.Config) s3iface.S3API {
	return s.Client
}
