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

	if flags.NArg() == 0 {
		fmt.Println("Reflow's system modules are:")
		for _, name := range syntax.Modules() {
			fmt.Printf("	$/%s\n", name)
		}
		return
	}
	if flags.NArg() != 1 {
		flags.Usage()
	}
	sess := syntax.NewSession()
	m, err := sess.Open(flags.Arg(0))
	if err != nil {
		c.Fatal(err)
	}
	if params := m.Params(); len(params) > 0 {
		fmt.Println("Parameters")
		fmt.Println()
		for _, p := range params {
			if p.Required {
				fmt.Printf("val %s %s (required)\n", p.Ident, p.Type)
			} else {
				fmt.Printf("val %s %s = %s\n", p.Ident, p.Type, p.Expr.Abbrev())
			}
			c.printdoc(p.Doc, "")
		}
		fmt.Println()
	}

	fmt.Println("Declarations")
	fmt.Println()
	for _, f := range m.Type().Aliases {
		fmt.Printf("type %s %s\n", f.Name, f.T)
		c.printdoc(m.Doc(f.Name), "\n")
	}
	for _, f := range m.Type().Fields {
		fmt.Printf("val %s %s\n", f.Name, f.T)
		c.printdoc(m.Doc(f.Name), "\n")
	}
}

func (c *Cmd) printdoc(doc string, nl string) {
	if doc == "" {
		fmt.Printf("%s", nl)
		return
	}
	pw := textutil.PrefixLineWriter(os.Stdout, "    ")
	ww := textutil.NewUTF8WrapWriter(pw, 80)
	if _, err := io.WriteString(ww, doc); err != nil {
		c.Fatal(err)
	}
	ww.Flush()
	pw.Flush()
	fmt.Printf("%s", nl)
}
