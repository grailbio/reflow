// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/status"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository/blobrepo"
	repositoryhttp "github.com/grailbio/reflow/repository/http"
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
func (c *Cmd) Cluster(status *status.Group) runner.Cluster {
	var cluster runner.Cluster
	err := c.Config.Instance(&cluster)
	if err != nil {
		c.Fatal(err)
	}
	var ec *ec2cluster.Cluster
	if err := c.Config.Instance(&ec); err == nil {
		ec.Status = status
		ec.Configuration = c.Config
	} else {
		log.Printf("not a ec2cluster! : %v", err)
	}
	var sess *session.Session
	err = c.Config.Instance(&sess)
	if err != nil {
		c.Fatal(err)
	}
	blobrepo.Register("s3", s3blob.New(sess))
	repositoryhttp.HTTPClient, err = c.httpClient()
	if err != nil {
		c.Fatal(err)
	}
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
