// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"text/tabwriter"
)

func (c *Cmd) offers(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("offers", flag.ExitOnError)
	help := "Offers displays the currently available offers from the cluster."
	c.Parse(flags, args, help, "offers")
	if flags.NArg() != 0 {
		flags.Usage()
	}
	cluster := c.cluster(nil)
	offers, err := cluster.Offers(ctx)
	if err != nil {
		c.Fatal(err)
	}
	var tw tabwriter.Writer
	tw.Init(c.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()
	for _, offer := range offers {
		fmt.Fprintf(&tw, "%s\t%s\n", offer.ID(), offer.Available())
	}
}
