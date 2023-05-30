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

	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/assoc/dydbassoc"
	"github.com/grailbio/reflow/taskdb/dynamodbtask"
	"github.com/grailbio/reflow/tool"
)

func setupDynamoDBAssoc(c *tool.Cmd, _ context.Context, args ...string) {
	flags := flag.NewFlagSet("setup-dynamodb-assoc", flag.ExitOnError)
	help := `Setup-dynamodb-assoc provisions a table in AWS's DynamoDB service and
modifies Reflow's configuration to use this table as its assoc.

The resulting configuration can be examined with "reflow config"`
	c.Parse(flags, args, help, "setup-dynamodb-assoc <tablename>")
	if flags.NArg() != 1 {
		flags.Usage()
	}
	table := flags.Arg(0)

	config := readConfig(c)

	name := dydbassoc.ProviderName
	keys := config.Keys
	if v, ok := keys["assoc"]; ok {
		parts := strings.Split(v.(string), ",")
		if len(parts) != 2 || parts[0] != name || parts[1] != table {
			c.Fatalf("assoc already setup: %v", v)
		}
		c.Log.Printf("assoc already set up; updating schemas")
	}
	c.SchemaKeys["assoc"] = fmt.Sprintf("%s,table=%v", name, table)
	setupAndSaveConfig(c)
}

func setupTaskDB(c *tool.Cmd, _ context.Context, args ...string) {
	flags := flag.NewFlagSet("setup-taskdb", flag.ExitOnError)
	help := `Setup-taskdb sets up Reflow's configuration to use a TaskDB.
Setup-taskdb provisions a table in AWS's DynamoDB service and a bucket in AWS's S3 storage service
and modifies Reflow's configuration to use these two for the TaskDB.

The resulting configuration can be examined with "reflow config"`
	c.Parse(flags, args, help, "setup-taskdb <tablename> <s3bucket>")
	if flags.NArg() != 2 {
		flags.Usage()
	}

	table, bucket := flags.Arg(0), flags.Arg(1)

	config := readConfig(c)

	name := dynamodbtask.ProviderName
	keys := config.Keys
	if v, ok := keys["taskdb"]; ok {
		parts := strings.Split(v.(string), ",")
		switch {
		case len(parts) == 1 && parts[0] == "noptaskdb":
			c.Printf("replacing noptaskdb with %s\n", name)
		case parts[0] != name:
			c.Fatalf("taskdb configured, but unexpected provider type '%s' (must be '%s')", parts[0], name)
		case len(parts) != 3:
			c.Printf("taskdb configured, but setup incomplete: %v", v)
			tableCfg, bucketCfg := parts[1], parts[2]
			if tableParts := strings.Split(tableCfg, "="); len(tableParts) != 2 {
				c.Fatalf("taskdb table misconfiguration (must be `table=<table_name>`): %s", tableCfg)
			} else if tableParts[1] != table {
				c.Fatalf("taskdb configured, but mismatched table '%s' (vs '%s')", tableParts[1], table)
			}
			if bucketParts := strings.Split(bucketCfg, "="); len(bucketParts) != 2 {
				c.Fatalf("taskdb bucket misconfiguration (must be `bucket=<bucket>`): %s", bucketCfg)
			} else if bucketParts[1] != bucket {
				c.Fatalf("taskdb configured, but mismatched bucket '%s' (vs '%s')", bucketParts[1], bucketParts)
			}
			c.Log.Printf("taskdb already set up; updating schemas (if necessary)")
		}
	}
	c.SchemaKeys["taskdb"] = fmt.Sprintf("%s,table=%s,bucket=%s", name, table, bucket)
	setupAndSaveConfig(c)
}

func readConfig(c *tool.Cmd) infra.Config {
	b, err := ioutil.ReadFile(c.ConfigFile)
	if err != nil && !os.IsNotExist(err) {
		c.Fatal(err)
	}
	config, err := c.Schema.Unmarshal(b)
	if err != nil {
		c.Fatal(err)
	}
	return config
}

func setupAndSaveConfig(c *tool.Cmd) {
	var err error
	c.Config, err = c.Schema.Make(c.SchemaKeys)
	if err != nil {
		c.Fatal(err)
	}
	if err = c.Config.Setup(); err != nil {
		c.Fatal(err)
	}
	b, err := c.Config.Marshal(false)
	if err != nil {
		c.Fatal(err)
	}
	if err := ioutil.WriteFile(c.ConfigFile, b, 0666); err != nil {
		c.Fatal(err)
	}
}
