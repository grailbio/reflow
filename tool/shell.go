// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"bufio"
	"context"
	"flag"
	"io"
	"os"
)

func (c *Cmd) shell(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("shell", flag.ExitOnError)
	help := `Run a shell (/bin/bash) inside the container of a running exec.
The local standard input, output and error streams are attached.
The user may exit the terminal by typing 'exit'/'quit'`
	// TODO(pgopal) - Put the terminal in raw mode.
	c.Parse(flags, args, help, "shell exec")
	if flags.NArg() != 1 {
		flags.Usage()
	}
	arg := flags.Arg(0)
	n, err := parseName(arg)
	if err != nil {
		c.Fatalf("parse %s: %v", arg, err)
	}
	if n.Kind != execName {
		c.Fatalf("%s: not an exec URI", arg)
	}

	cluster := c.Cluster(nil)
	alloc, err := cluster.Alloc(ctx, n.AllocID)
	if err != nil {
		c.Fatalf("alloc %s: %s", n.AllocID, err)
	}
	e, err := alloc.Get(ctx, n.ID)
	if err != nil {
		c.Fatalf("%s: %s", n.ID, err)
	}
	sr, sw := io.Pipe()
	go func() {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			_, err := sw.Write([]byte(s.Text() + "\n"))
			if err != nil {
				c.Fatal("%s: %s", n.ID, err)
			}
		}
		sw.Close()
		if s.Err() != nil {
			c.Fatal("%s: %s", n.ID, s.Err())
		}
	}()

	rwc, err := e.Shell(ctx)
	if err != nil {
		c.Fatalf("%s: %s", n.ID, err)
	}
	go func() {
		io.Copy(rwc, sr)
	}()
	_, err = io.Copy(c.Stdout, rwc)
	if err != nil {
		c.Fatalf("%s: %s", n.ID, err)
	}
}
