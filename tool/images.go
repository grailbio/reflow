// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/types"
)

func (c *Cmd) images(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("images", flag.ExitOnError)
	help := "Images prints the paths of all Docker images used by a reflow program."
	c.Parse(flags, args, help, "images path")

	if flags.NArg() == 0 {
		flags.Usage()
	}
	programPath := flags.Arg(0)
	sess := syntax.NewSession(nil)
	m, err := sess.Open(programPath)
	if err != nil {
		c.Fatal(err)
	}

	programFlags, err := m.Flags(sess, sess.Values)
	if err != nil {
		c.Fatal(err)
	}
	programFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage of %s:\n", programPath)
		programFlags.PrintDefaults()
		c.Exit(2)
	}
	err = programFlags.Parse(flags.Args()[1:])
	if err != nil {
		c.Fatalf("error parsing module flags: %v", err)
	}

	env := sess.Values.Push()
	if err = m.FlagEnv(programFlags, env, types.NewEnv()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		programFlags.Usage()
	}
	_, err = m.Make(sess, env)
	if err != nil {
		c.Fatal(err)
	}

	images := sess.Images()
	sort.Strings(images)

	for _, image := range images {
		fmt.Println(image)
	}
}
