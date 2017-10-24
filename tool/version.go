// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"runtime"
)

func (c *Cmd) version(ctx context.Context, args ...string) {
	var (
		flags = flag.NewFlagSet("offers", flag.ExitOnError)
		help  = "Version displays this binary's version (datestamp) and git hash from which it was built."
	)
	c.Parse(flags, args, help, "version")
	if len(args) != 0 {
		flags.Usage()
	}
	if c.Version == "" {
		c.Version = "broken"
	}
	if c.Variant != "" {
		fmt.Printf("%s (%s, %s)\n", c.Version, c.Variant, runtime.Version())
	} else {
		fmt.Printf("%s (%s)\n", c.Version, runtime.Version())
	}
}
