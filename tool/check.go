// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package tool

import (
	"context"
	"flag"

	"github.com/grailbio/reflow/syntax"
)

func (c *Cmd) check(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("check", flag.ExitOnError)
	warnErr := flags.Bool("w", false, "treat warnings as errors")
	help := `Check typechecks the provided modules and prints typechecking
warnings. If any errors are encountered during typechecking, check
exits with code 1.`
	c.Parse(flags, args, help, "check modules...")
	if flags.NArg() == 0 {
		flags.Usage()
	}
	sess := syntax.NewSession(nil)
	sess.Stdwarn = c.Stderr
	ok := true
	for i := 0; i < flags.NArg(); i++ {
		if _, err := sess.Open(flags.Arg(i)); err != nil {
			c.Errorln(err)
			ok = false
		}
	}
	if !ok || *warnErr && sess.NWarn() > 0 {
		c.Exit(1)
	}
}
