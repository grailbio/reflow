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
	cluster := c.Cluster()
	for _, arg := range flags.Args() {
		n, err := parseName(arg)
		if err != nil {
			c.Printf("invalid argument '%s': %v", arg, err)
			continue
		}
		if n.Kind != allocName {
			c.Errorf("'%s' not an alloc\n", arg)
			continue
		}
		allocUri := allocURI(n)
		alloc, err := cluster.Alloc(ctx, allocUri)
		if err != nil {
			c.Errorf("alloc %s (from arg %s): %v\n", allocUri, arg, err)
			continue
		}
		if err := alloc.Free(ctx); err != nil {
			c.Errorf("%s: %s\n", arg, err)
			continue
		}
	}
}
