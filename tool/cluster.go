// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/internal/status"
	repositoryhttp "github.com/grailbio/reflow/repository/http"
	reflows3 "github.com/grailbio/reflow/repository/s3"
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
func (c *Cmd) cluster(status *status.Group) runner.Cluster {
	cluster, err := c.Config.Cluster()
	if err != nil {
		c.Fatal(err)
	}
	if ec2cluster, ok := cluster.(*ec2cluster.Cluster); ok {
		ec2cluster.Status = status
	} else {
		log.Print("not a ec2cluster!")
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
	if n, ok := cluster.(needer); ok {
		http.HandleFunc("/clusterneed", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				http.Error(w, "bad method", http.StatusMethodNotAllowed)
				return
			}
			need := n.Need()
			enc := json.NewEncoder(w)
			if err := enc.Encode(need); err != nil {
				http.Error(w, fmt.Sprintf("internal error: %v", err), http.StatusInternalServerError)
				return
			}
		})
	}
	return cluster
}
