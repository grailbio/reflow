// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"fmt"
	"net/http"

	"github.com/grailbio/base/status"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/runner"
	"golang.org/x/net/http2"
)

type needer interface {
	Need() reflow.Resources
}

// Cluster returns a configured cluster and sets up repository
// credentials so that remote repositories can be dialed.
//
// Cluster also registers an http handler to export the cluster's
// additional resource needs, if the cluster exports this.
//
// TODO(marius): handle this more elegantly, perhaps by avoiding
// such global registration altogether. The current way of doing this
// also ties the binary to specific implementations (e.g., s3), which
// should be avoided.
func (c *Cmd) Cluster(status *status.Status) runner.Cluster {
	cluster, err := clusterInstance(c.Config, status)
	if err != nil {
		c.Fatalf("Cluster: %v", err)
	}
	if ec, ok := cluster.(*ec2cluster.Cluster); ok {
		if err = ec.VerifyAndInit(); err != nil {
			c.Fatal(fmt.Errorf("clusterInstance VerifyAndInit: %v", err))
		}
	} else {
		c.Log.Printf("not a ec2cluster - %s %T", cluster.GetName(), cluster)
	}
	return cluster
}

func (c *Cmd) httpClient() (*http.Client, error) {
	var ca tls.Certs
	err := c.Config.Instance(&ca)
	if err != nil {
		c.Fatal(err)
	}
	clientConfig, _, err := ca.HTTPS()
	if err != nil {
		c.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: clientConfig}
	if err := http2.ConfigureTransport(transport); err != nil {
		c.Fatal(err)
	}
	return &http.Client{Transport: transport}, nil
}
