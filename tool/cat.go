// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"io"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
)

func (c *Cmd) cat(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("cat", flag.ExitOnError)
	help := "Cat copies files from Reflow's repository"
	c.Parse(flags, args, help, "cat files...")
	if flags.NArg() == 0 {
		flags.Usage()
	}
	repo, err := c.Config.Repository()
	if err != nil {
		c.Fatal(err)
	}
	var ids []digest.Digest
	for _, arg := range flags.Args() {
		id, err := reflow.Digester.Parse(arg)
		if err != nil {
			c.Fatalf("parse %s: %v", id, err)
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		_, err := repo.Stat(ctx, id)
		if err != nil {
			c.Fatalf("stat %s: %v", id.Hex(), err)
		}
	}
	for _, id := range ids {
		rc, err := repo.Get(ctx, id)
		if err != nil {
			c.Fatalf("get %s: %v", id, err)
		}
		_, err = io.Copy(c.Stdout, rc)
		if err != nil {
			c.Fatalf("read %s: %v", id, err)
		}
		rc.Close()
	}
}
