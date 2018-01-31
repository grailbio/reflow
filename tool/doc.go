// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"io"

	"github.com/grailbio/reflow/syntax"
	"v.io/x/lib/textutil"
)

func (c *Cmd) doc(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("doc", flag.ExitOnError)
	help := "Doc displays documentation for Reflow modules."
	c.Parse(flags, args, help, "doc path")

	if flags.NArg() == 0 {
		c.Println("Reflow's system modules are:")
		for _, name := range syntax.Modules() {
			c.Printf("	$/%s\n", name)
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
		c.Println("Parameters")
		c.Println()
		for _, p := range params {
			if p.Required {
				c.Printf("val %s %s (required)\n", p.Ident, p.Type)
			} else {
				c.Printf("val %s %s = %s\n", p.Ident, p.Type, p.Expr.Abbrev())
			}
			c.printdoc(p.Doc, "")
		}
		c.Println()
	}

	c.Println("Declarations")
	c.Println()
	for _, f := range m.Type().Aliases {
		c.Printf("type %s %s\n", f.Name, f.T)
		c.printdoc(m.Doc(f.Name), "\n")
	}
	for _, f := range m.Type().Fields {
		c.Printf("val %s %s\n", f.Name, f.T)
		c.printdoc(m.Doc(f.Name), "\n")
	}
}

func (c *Cmd) printdoc(doc string, nl string) {
	if doc == "" {
		c.Printf("%s", nl)
		return
	}
	pw := textutil.PrefixLineWriter(c.Stdout, "    ")
	ww := textutil.NewUTF8WrapWriter(pw, 80)
	if _, err := io.WriteString(ww, doc); err != nil {
		c.Fatal(err)
	}
	ww.Flush()
	pw.Flush()
	c.Printf("%s", nl)
}
