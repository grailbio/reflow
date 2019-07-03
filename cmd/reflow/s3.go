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
	"strings"

	"github.com/grailbio/reflow/tool"
)

func setupS3Repository(c *tool.Cmd, ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("setup-s3-repository", flag.ExitOnError)
	help := `Setup-s3-repository provisions a bucket in AWS's S3 storage service
and modifies Reflow's configuration to use this S3 bucket as its
object repository.

The resulting configuration can be examined with "reflow config"`
	c.Parse(flags, args, help, "setup-s3-repository s3bucket")
	if flags.NArg() != 1 {
		flags.Usage()
	}
	bucket := flags.Arg(0)

	b, err := ioutil.ReadFile(c.ConfigFile)
	if err != nil && !os.IsNotExist(err) {
		c.Fatal(err)
	}
	config, err := c.Schema.Unmarshal(b)
	if err != nil {
		c.Fatal(err)
	}
	pkgPath := "github.com/grailbio/reflow/repository/s3.Repository"
	keys := config.Keys
	if v, ok := keys["repository"]; ok {
		parts := strings.Split(v.(string), ",")
		if len(parts) != 2 || parts[0] != pkgPath || parts[1] != bucket {
			c.Fatalf("repository already setup: %v", v)
		}
		c.Log.Printf("repository already set up; updating schemas")
	}
	c.SchemaKeys["repository"] = fmt.Sprintf("%s,bucket=%v", pkgPath, bucket)
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
