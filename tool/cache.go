// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"bufio"
	"context"
	"flag"
	"os"

	"github.com/grailbio/reflow"
)

func (c *Cmd) rmcache(ctx context.Context, args ...string) {
	var (
		flags = flag.NewFlagSet("cacherm", flag.ExitOnError)
		help  = `Rmcache removes items from cache. 
Items are digests read from the standard input.`
	)
	c.Parse(flags, args, help, "cacherm")
	if flags.NArg() != 0 {
		flags.Usage()
	}

	cache, err := c.Config.Cache()
	if err != nil {
		c.Fatal(err)
	}

	var n int
	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		id, err := reflow.Digester.Parse(scan.Text())
		if err != nil {
			c.Log.Errorf("failed to parse %s: %v; skipping", scan.Text(), err)
			continue
		}
		// TODO(marius): parallelize this for large jobs.
		if err := cache.Delete(ctx, id); err != nil {
			c.Log.Errorf("failed to delete %s: %v", id, err)
		}
		c.Log.Debugf("removed key %v", id)
		n++
	}
	c.Log.Debugf("removed %d keys", n)
}
