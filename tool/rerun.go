package tool

import (
	"context"
	"flag"

	"github.com/grailbio/reflow/assoc"
)

func (c *Cmd) rerun(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("rerun", flag.ExitOnError)
	help := `Rerun repeats the run named by the provided run ID`

	var config runConfig
	config.Flags(flags)

	c.Parse(flags, args, help, "rerun [-local] [flags] runid")
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
	name, err := parseName(flags.Args()[0])
	if err != nil {
		c.Fatal(err)
	}
	if name.Kind != idName {
		c.Fatal("runId not a digest: %v", name.Kind)
	}
	_, v, err := a.Get(ctx, assoc.Bundle, name.ID)
	if err != nil {
		c.Fatal(err)
	}
	repo, err := c.Config.Repository()
	if err != nil {
		c.Fatal(err)
	}

	p, err := ReadBundle(ctx, v, repo)
	if err != nil {
		c.Fatal(err)
	}
	er, err := c.EvalBundle(p)
	if err != nil {
		c.Fatal(err)
	}
	c.runCommon(ctx, config, er)
}
