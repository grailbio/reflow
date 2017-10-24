// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"net/http"

	"github.com/aws/aws-sdk-go/service/s3"
	repositoryhttp "github.com/grailbio/reflow/repository/http"
	reflows3 "github.com/grailbio/reflow/repository/s3"
	"github.com/grailbio/reflow/runner"
	"golang.org/x/net/http2"
)

// Cluster returns a configured cluster and sets up repository
// credentials so that remote repositories can be dialed.
//
// TODO(marius): handle this more elegantly, perhaps by avoiding
// such global registration altogether. The current way of doing this
// also ties the binary to specific implementations (e.g., s3), which
// should be avoided.
func (c *Cmd) cluster() runner.Cluster {
	cluster, err := c.Config.Cluster()
	if err != nil {
		c.Fatal(err)
	}
	sess, err := c.Config.AWS()
	if err != nil {
		c.Fatal(err)
	}
	clientConfig, _, err := c.Config.HTTPS()
	if err != nil {
		c.Fatal(err)
	}
	reflows3.SetClient(s3.New(sess))
	transport := &http.Transport{TLSClientConfig: clientConfig}
	http2.ConfigureTransport(transport)
	repositoryhttp.HTTPClient = &http.Client{Transport: transport}
	return cluster
}
