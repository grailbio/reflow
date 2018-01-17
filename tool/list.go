// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/pool"
)

func (c *Cmd) list(ctx context.Context, args ...string) {
	var (
		flags     = flag.NewFlagSet("list", flag.ExitOnError)
		shortFlag = flags.Bool("n", false, "display entries only")
		allFlag   = flags.Bool("a", false, "recursively list all resources")
		help      = `List enumerates resources (allocs and execs) in a hierarchical fashion.

The columns displayed by list are:

	state   the state of an exec
	memory  the amount of reserved memory
	cpu     the number of reserved CPUs
	disk    the amount of reserved disk space
	ident   the exec's identifier, or the alloc's owner
	uri     the exec's or alloc's URI`
	)
	c.Parse(flags, args, help, "list [-a] [[-n] alloc]")
	args = flags.Args()
	cluster := c.cluster()
	var entries []interface{}

	if len(args) == 0 {
		allocs, err := cluster.Allocs(ctx)
		if err != nil {
			c.Fatal(err)
		}
		for _, alloc := range allocs {
			entries = append(entries, alloc)
			if *allFlag {
				execs, err := alloc.Execs(ctx)
				if err != nil {
					c.Fatal(err)
				}
				for _, exec := range execs {
					entries = append(entries, exec)
				}
			}
		}
	} else if *allFlag {
		flags.Usage()
	} else {
		for _, arg := range args {
			n, err := parseName(arg)
			if err != nil {
				c.Fatalf("invalid URI %s: %s", arg, err)
			}
			if n.Kind == idName {
				c.Errorf("%s: can only list allocs and execs", arg)
				continue
			}
			alloc, err := cluster.Alloc(ctx, n.AllocID)
			if err != nil {
				c.Fatalf("%s: %s", arg, err)
			}
			switch n.Kind {
			case allocName:
				execs, err := alloc.Execs(ctx)
				if err != nil {
					c.Fatalf("%s: %s", arg, err)
				}
				for _, exec := range execs {
					entries = append(entries, exec)
				}
			case execName:
				exec, err := alloc.Get(ctx, n.ID)
				if err != nil {
					c.Fatalf("%s: %s", arg, err)
				}
				entries = append(entries, exec)
			}
		}
	}
	if *shortFlag {
		for _, e := range entries {
			fmt.Println(sprintURI(e))
		}
		return
	}

	inspects := make([]interface{}, len(entries))
	err := traverse.Each(len(entries)).Do(func(i int) error {
		var err error
		switch entry := entries[i].(type) {
		case reflow.Exec:
			inspects[i], err = entry.Inspect(ctx)
		case pool.Alloc:
			inspects[i], err = entry.Inspect(ctx)
		default:
			panic("unknown entry type")
		}
		return err
	})
	if err != nil {
		c.Fatal(err)
	}
	var tw tabwriter.Writer
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()
	for i := range inspects {
		// TODO(marius): print creation times here
		// (these need to be propagated from the alloc).
		switch inspect := inspects[i].(type) {
		case reflow.ExecInspect:
			fmt.Fprintf(&tw, "%s\t%s\t%g\t%s\t%s\t%s\n",
				inspect.State,
				data.Size(inspect.Config.Resources["mem"]),
				inspect.Config.Resources["cpu"], data.Size(inspect.Config.Resources["disk"]),
				inspect.Config.Ident, sprintURI(entries[i]))
		case pool.AllocInspect:
			fmt.Fprintf(&tw, "%s\t%s\t%g\t%s\t%s\t%s\n",
				"", /*TODO(marius): print whether it's active or zombie*/
				data.Size(inspect.Resources["mem"]),
				inspect.Resources["cpu"], data.Size(inspect.Resources["disk"]),
				inspect.Meta.Owner, sprintURI(entries[i]))
		default:
			panic("unknown inspect type")
		}
	}
}

func sprintURI(x interface{}) string {
	switch x := x.(type) {
	case interface {
		URI() string
	}:
		return x.URI()
	case interface {
		ID() string
	}:
		return x.ID()
	default:
		panic(fmt.Sprintf("unknown entry type %T", x))
	}
}
