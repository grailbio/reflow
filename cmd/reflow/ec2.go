// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/tool"
)

// securityGroup is the name of reflow's security group.
const securityGroup = "reflow"

func setupEC2(c *tool.Cmd, ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("setup-ec2", flag.ExitOnError)
	// TODO(pgopal) - fix this.
	// sshkey := flags.String("sshkey", os.ExpandEnv("$HOME/.ssh/id_rsa.pub"), "install this public SSH key on EC2 nodes")
	help := `Setup-ec2 modifies Reflow's configuration to use Reflow's cluster
manager to compute on an EC2 cluster. 

Reflow is configured to launch new instances in the default VPC of 
the user's AWS account. A new security group named "reflow" is 
provisioned if necessary. The security group permits the following
ingress traffic:

	port 9000 source 0.0.0.0/0 9000
	port 22 source 0.0.0.0/0 9000
	
The former port is used for reflowlet RPC; the latter to permit users
to SSH into the EC2 instances for debugging.

The cluster is configured to install the user's SSH keys (flag
-sshkey, $HOME/.ssh/id_rsa.pub by default).

The resulting configuration can be examined with "reflow config".`
	c.Parse(flags, args, help, "setup-ec2")
	if flags.NArg() != 0 {
		flags.Usage()
	}

	b, err := ioutil.ReadFile(c.ConfigFile)
	if err != nil && !os.IsNotExist(err) {
		c.Fatal(err)
	}
	config, err := c.Schema.Unmarshal(b)
	if err != nil {
		c.Fatal(err)
	}
	pkgPath := "ec2cluster"
	if v, ok := config.Keys[infra.Cluster]; ok {
		if v.(string) != pkgPath {
			c.Fatalf("cluster already setup: %v", v)
		}
		c.Fatal("cluster already set up")
	}

	if _, ok := config.Keys["tls"]; !ok {
		path := filepath.Join(filepath.Dir(c.ConfigFile), "reflow.pem")
		c.SchemaKeys["tls"] = fmt.Sprintf("tls,file=%v", path)
	}
	c.SchemaKeys[infra.Cluster] = pkgPath
	c.Config, err = c.Schema.Make(c.SchemaKeys)
	if err != nil {
		c.Fatal(err)
	}
	if err = c.Config.Setup(); err != nil {
		c.Fatal(err)
	}
	b, err = c.Config.Marshal(true)
	if err != nil {
		c.Fatal(err)
	}
	if err := ioutil.WriteFile(c.ConfigFile, b, 0666); err != nil {
		c.Fatal(err)
	}
}
