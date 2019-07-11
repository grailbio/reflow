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

	_ "github.com/grailbio/reflow/assoc/dydbassoc"
	"github.com/grailbio/reflow/tool"
)

// Default provisioned capacities for DynamoDB.
const (
	writecap = 10
	readcap  = 20
)

func setupDynamoDBAssoc(c *tool.Cmd, ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("setup-dynamodb-assoc", flag.ExitOnError)
	help := `Setup-dynamodb-assoc provisions a table in AWS's DynamoDB service and
modifies Reflow's configuration to use this table as its assoc.

By default the DynamoDB table is configured with a provisoned
capacity of 10 writes/sec and 20 reads/sec. This can be 
modified through the AWS console after configuration.

The resulting configuration can be examined with "reflow config"`
	c.Parse(flags, args, help, "setup-dynamodb-assoc tablename")
	if flags.NArg() != 1 {
		flags.Usage()
	}
	table := flags.Arg(0)

	b, err := ioutil.ReadFile(c.ConfigFile)
	if err != nil && !os.IsNotExist(err) {
		c.Fatal(err)
	}
	config, err := c.Schema.Unmarshal(b)
	if err != nil {
		c.Fatal(err)
	}
	name := "dynamodbassoc"
	keys := config.Keys
	if v, ok := keys["assoc"]; ok {
		parts := strings.Split(v.(string), ",")
		if len(parts) != 2 || parts[0] != name || parts[1] != table {
			c.Fatalf("assoc already setup: %v", v)
		}
		c.Log.Printf("assoc already set up; updating schemas")
	}
	c.SchemaKeys["assoc"] = fmt.Sprintf("%s,table=%v", name, table)
	c.Config, err = c.Schema.Make(c.SchemaKeys)
	if err != nil {
		c.Fatal(err)
	}
	if err = c.Config.Setup(); err != nil {
		c.Fatal(err)
	}
	b, err = c.Config.Marshal(false)
	if err != nil {
		c.Fatal(err)
	}
	if err := ioutil.WriteFile(c.ConfigFile, b, 0666); err != nil {
		c.Fatal(err)
	}
}
