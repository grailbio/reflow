// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"sort"

	"v.io/x/lib/textutil"
)

func (c *Cmd) config(ctx context.Context, args ...string) {
	var (
		flags  = flag.NewFlagSet("config", flag.ExitOnError)
		header = `Config writes the current Reflow configuration to standard 
output.

Reflow's configuration is a YAML file with the follow toplevel
keys:

`
		footer = `A Reflow distribution may contain a builtin configuration that may be
modified and overriden:

	$ reflow config > myconfig
	<edit myconfig>
	$ reflow -config myconfig ...`
	)
	marshalFlag := flags.Bool("marshal", false, "marshal the configuration before displaying it")
	// Construct a help string from the available providers.
	b := new(bytes.Buffer)
	b.WriteString(header)

	var keys []string
	help := c.Config.Help()
	for key := range help {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := help[k]
		for _, p := range v {
			fmt.Fprintf(b, "%s: %s", k, p.Name)
			for _, arg := range p.Args {
				fmt.Fprintf(b, ",%s", arg.Name)
			}
			b.WriteString("\n")
			pw := textutil.PrefixLineWriter(b, "	")
			ww := textutil.NewUTF8WrapWriter(pw, 80)
			if _, err := io.WriteString(ww, p.Usage); err != nil {
				c.Fatal(err)
			}
			hw := textutil.NewUTF8WrapWriter(pw, 80)
			if len(p.Args) > 0 {
				io.WriteString(hw, "\n")
			}
			for _, arg := range p.Args {
				io.WriteString(hw, fmt.Sprintf("%s: %s", arg.Name, arg.Help))
				if arg.DefaultValue != "" {
					io.WriteString(hw, fmt.Sprintf(" (default %q)", arg.DefaultValue))
				}
				io.WriteString(hw, "\n")
			}
			ww.Flush()
			hw.Flush()
			pw.Flush()
		}
		b.WriteString("\n")
	}
	b.WriteString(footer)

	c.Parse(flags, args, b.String(), "config")

	if flags.NArg() != 0 {
		flags.Usage()
	}
	// Do not marshal the key for reflow version.
	delete(c.Config.Keys, "reflow")
	data, err := c.Config.Marshal(*marshalFlag)
	if err != nil {
		c.Fatal(err)
	}
	c.Stdout.Write(data)
	c.Println()
}
