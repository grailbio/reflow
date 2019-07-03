// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"sort"
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
	for key := range c.Schema {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		// TODO(pgopal) - Do we need to instantiate all the providers for the keys
		// and inspect their flag definitions here?
		fmt.Fprintf(b, "%s: %s", key, c.SchemaKeys[key])
		b.WriteString("\n")
	}
	b.WriteString(footer)

	c.Parse(flags, args, b.String(), "config")

	if flags.NArg() != 0 {
		flags.Usage()
	}
	var data []byte
	if *marshalFlag {
		var err error
		data, err = c.Config.Marshal(true)
		if err != nil {
			c.Fatal(err)
		}
	} else {
		var err error
		data, err = c.Config.Marshal(false)
		if err != nil {
			c.Fatal(err)
		}
	}
	c.Stdout.Write(data)
	c.Println()
}
