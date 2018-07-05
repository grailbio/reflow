package tool

import (
	"context"
	"flag"
	"os"

	"github.com/grailbio/reflow/assoc"
)

func (c *Cmd) rerun(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("rerun", flag.ExitOnError)
	help := `Rerun repeats the run named by the provided run ID or a reflow bundle archive`

	var config runConfig
	config.Flags(flags)

	c.Parse(flags, args, help, "rerun [-local] [flags] runid|bundle.zip [args]")
	if err := config.Err(); err != nil {
		c.Errorln(err)
		flags.Usage()
	}
	if flags.NArg() == 0 {
		flags.Usage()
	}
	a, err := c.Config.Assoc()
	if err != nil {
		c.Fatal(err)
	}
	spec, args := flags.Args()[0], flags.Args()[1:]
	var b *Bundle
	if _, err := os.Stat(spec); !os.IsNotExist(err) {
		if err != nil {
			c.Fatal(err)
		}
		b, err = ReadArchive(spec)
		if err != nil {
			c.Fatal(err)
		}
	} else {
		name, err := parseName(spec)
		if err != nil {
			c.Fatal(err)
		}
		if name.Kind != idName {
			c.Fatal("runid not a digest: %v", name.Kind)
		}
		_, v, err := a.Get(ctx, assoc.Bundle, name.ID)
		if err != nil {
			c.Fatal(err)
		}
		repo, err := c.Config.Repository()
		if err != nil {
			c.Fatal(err)
		}

		b, err = ReadBundle(ctx, v, repo)
		if err != nil {
			c.Fatal(err)
		}
	}
	er, err := c.EvalBundle(b, args)
	if err != nil {
		c.Fatal(err)
	}
	c.runCommon(ctx, config, er)
}
