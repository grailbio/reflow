// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"fmt"

	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/runtime"
)

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
func (c *Cmd) Cluster() runner.Cluster {
	cluster, err := runtime.ClusterInstance(c.Config)
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
