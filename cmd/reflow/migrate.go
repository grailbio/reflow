// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"strings"

	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/tool"
)

func migrate(c *tool.Cmd, ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("migrate", flag.ExitOnError)
	help := `Migrate forward-migrates Reflow's configuration and
underlying services.

Migrations are as follows:

reflow0.6:
 *	Convert cache configuration from the monolithic "cache" key to the
	split "repository" and "assoc" keys, configured separately.
 *	Add abbreviation indices to DynamoDB assocs to permit abbreviated
	lookups to command reflow info, others.`
	c.Parse(flags, args, help, "migrate")
	if flags.NArg() != 0 {
		flags.Usage()
	}
	b, err := ioutil.ReadFile(c.ConfigFile)
	if err != nil && !os.IsNotExist(err) {
		c.Fatal(err)
	}
	base := make(config.Base)
	if err := config.Unmarshal(b, base.Keys()); err != nil {
		c.Fatal(err)
	}
	// cache: s3,bucket,table =>
	//	assoc: dynamodb,table
	//	repository: s3,bucket
	v, _ := base[config.Cache].(string)
	parts := strings.SplitN(v, ",", 3)
	if len(parts) == 3 && parts[0] == "s3" {
		delete(base, config.Cache)
		base[config.Repository] = "s3," + parts[1]
		base[config.Assoc] = "dynamodb," + parts[2]
		c.Log.Print("migrated from cache: to assoc: and repository:")
		b, err = config.Marshal(base)
		if err != nil {
			c.Fatal(err)
		}
		if err := ioutil.WriteFile(c.ConfigFile, b, 0666); err != nil {
			c.Fatal(err)
		}
	}
	assoc, _ := base[config.Assoc].(string)
	if prefix := "dynamodb,"; strings.HasPrefix(assoc, prefix) {
		assoc = strings.TrimPrefix(assoc, prefix)
		// Migrate the underlying assoc to add indices.
		configureDynamoDBAssoc(c, ctx, assoc)
	}
}
