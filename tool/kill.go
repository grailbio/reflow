// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
)

func (c *Cmd) kill(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("kill", flag.ExitOnError)
	help := "Kill terminates and frees allocs."
	c.Parse(flags, args, help, "kill allocs...")
	if flags.NArg() == 0 {
		flags.Usage()
	}
	cluster := c.cluster()
	for _, uri := range flags.Args() {
		u, err := parseURI(uri)
		if err != nil {
			c.Errorf("%s: %s\n", uri, err)
			continue
		}
		if u.Kind != allocURI {
			c.Errorf("%s: only allocs can be killed\n", uri)
			continue
		}
		alloc, err := cluster.Alloc(ctx, u.AllocID)
		if err != nil {
			c.Errorf("%s: %s\n", uri, err)
			continue
		}
		if err := alloc.Free(ctx); err != nil {
			c.Errorf("%s: %s\n", uri, err)
			continue
		}
	}
}
