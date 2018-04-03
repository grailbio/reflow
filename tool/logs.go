// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"io"
)

func (c *Cmd) logs(ctx context.Context, args ...string) {
	var (
		flags      = flag.NewFlagSet("logs", flag.ExitOnError)
		stdoutFlag = flags.Bool("stdout", false, "display stdout instead of stderr")
		followFlag = flags.Bool("f", false, "follow the logs")
		help       = "Logs displays logs from execs."
	)
	c.Parse(flags, args, help, "logs exec")
	if flags.NArg() != 1 {
		flags.Usage()
	}

	arg := flags.Arg(0)
	n, err := parseName(arg)
	if err != nil {
		c.Fatal(err)
	}
	if n.Kind != execName {
		c.Fatal("%s: not an exec URI", arg)
	}
	cluster := c.Cluster(nil)
	alloc, err := cluster.Alloc(ctx, n.AllocID)
	if err != nil {
		c.Fatalf("alloc %s: %s", n.AllocID, err)
	}
	exec, err := alloc.Get(ctx, n.ID)
	if err != nil {
		c.Fatalf("%s: %s", n.ID, err)
	}
	rc, err := exec.Logs(ctx, *stdoutFlag, !*stdoutFlag, *followFlag)
	if err != nil {
		c.Fatalf("logs %s: %s", exec.URI(), err)
	}
	_, err = io.Copy(c.Stdout, rc)
	rc.Close()
	if err != nil {
		c.Fatal(err)
	}
}
