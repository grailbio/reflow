// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/grailbio/reflow/syntax"
	"v.io/x/lib/textutil"
)

func (c *Cmd) doc(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("doc", flag.ExitOnError)
	help := "Doc displays documentation for Reflow modules."
	c.Parse(flags, args, help, "doc path")

	if flags.NArg() != 1 {
		flags.Usage()
	}
	sess := syntax.NewSession()
	m, err := sess.Open(flags.Arg(0))
	if err != nil {
		c.Fatal(err)
	}
	if len(m.Params) > 0 {
		fmt.Println("Parameters")
		fmt.Println()
		for _, p := range m.Params {
			switch p.Kind {
			case syntax.DeclDeclare:
				fmt.Printf("val %s %s\n", p.Ident, p.Type)
				c.printdoc(p.Comment, "")
			case syntax.DeclAssign:
				// TODO: evaluate the parameter to show its value.
				fmt.Printf("val %s %s = <default>\n", p.Pat, p.Type)
				c.printdoc(p.Comment, "")
			}
		}
		fmt.Println()
	}

	fmt.Println("Declarations")
	fmt.Println()
	for _, f := range m.Type.Aliases {
		fmt.Printf("type %s %s\n", f.Name, f.T)
		c.printdoc(m.Docs[f.Name], "\n")
	}
	for _, f := range m.Type.Fields {
		fmt.Printf("val %s %s\n", f.Name, f.T)
		c.printdoc(m.Docs[f.Name], "\n")
	}
}

func (c *Cmd) printdoc(doc string, nl string) {
	if doc == "" {
		fmt.Printf("%s", nl)
		return
	}
	pw := textutil.PrefixLineWriter(os.Stderr, "    ")
	ww := textutil.NewUTF8WrapWriter(pw, 80)
	if _, err := io.WriteString(ww, doc); err != nil {
		c.Fatal(err)
	}
	ww.Flush()
	pw.Flush()
	fmt.Printf("%s", nl)
}
