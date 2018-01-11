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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/reflow/config"
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
	base := make(config.Base)
	if err := config.Unmarshal(b, base.Keys()); err != nil {
		c.Fatal(err)
	}
	v, _ := base[config.Repository].(string)
	if v != "" {
		c.Fatalf("repository already set up: %v", v)
	}
	sess, err := c.Config.AWS()
	if err != nil {
		c.Fatal(err)
	}
	region, err := c.Config.AWSRegion()
	if err != nil {
		c.Fatal(err)
	}
	c.Log.Printf("creating s3 bucket %s", bucket)
	_, err = s3.New(sess).CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(region),
		},
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				c.Fatalf("s3 bucket %s is already owned by someone else", bucket)
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				c.Log.Printf("s3 bucket %s already exists; not created", bucket)
			default:
				c.Fatal(err)
			}
		} else {
			c.Fatal(err)
		}
	} else {
		c.Log.Printf("created s3 bucket %s", bucket)
	}
	base[config.Repository] = fmt.Sprintf("s3,%s", bucket)
	b, err = config.Marshal(base)
	if err != nil {
		c.Fatal(err)
	}
	if err := ioutil.WriteFile(c.ConfigFile, b, 0666); err != nil {
		c.Fatal(err)
	}
}
