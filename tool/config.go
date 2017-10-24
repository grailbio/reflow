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
	"os"
	"sort"

	"github.com/grailbio/reflow/config"
	yaml "gopkg.in/yaml.v2"
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
	usages := config.Help()
	var keys []string
	for key := range usages {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		usages := usages[key]
		sort.Slice(usages, func(i, j int) bool {
			return usages[i].Kind < usages[j].Kind
		})
		for _, usage := range usages {
			fmt.Fprintf(b, "%s: %s", key, usage.Kind)
			if arg := usage.Arg; arg != "" {
				fmt.Fprintf(b, ",%s", arg)
			}
			b.WriteString("\n")
			pw := textutil.PrefixLineWriter(b, "	")
			ww := textutil.NewUTF8WrapWriter(pw, 80)
			if _, err := io.WriteString(ww, usage.Usage); err != nil {
				c.Fatal(err)
			}
			ww.Flush()
			pw.Flush()
		}
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
		data, err = config.Marshal(c.Config)
		if err != nil {
			c.Fatal(err)
		}
	} else {
		var err error
		data, err = yaml.Marshal(c.Config.Keys())
		if err != nil {
			c.Fatal(err)
		}
	}
	os.Stdout.Write(data)
	fmt.Println()
}
