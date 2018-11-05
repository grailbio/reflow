package tool

import (
	"context"
	"flag"
	"os"
	"path/filepath"

	"github.com/grailbio/reflow/syntax"
)

func (c *Cmd) bundle(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("bundle", flag.ExitOnError)
	out := flags.String("o", "", "output path of bundle")
	nowarn := flags.Bool("nowarn", false, "suppress warnings")
	help := `Bundle type checks, then writes a self-contained Reflow bundle to a
file named by the basename of the provided Reflow module with the
suffix ".rfx".

Reflow bundles are interchangeable with other Reflow modules: they
may be run with command run or imported by other Reflow modules. Any
flags provided as arguments provide default values to the module's
parameters.`
	c.Parse(flags, args, help, "bundle [-o output] path [args]")
	if flags.NArg() == 0 {
		flags.Usage()
	}
	file, args := flags.Arg(0), flags.Args()[1:]
	if ext := filepath.Ext(file); ext != ".rf" {
		c.Fatalf("extension %s not supported for bundling", ext)
	}
	sess := syntax.NewSession(nil)
	if !*nowarn {
		sess.Stdwarn = c.Stderr
	}
	m, err := sess.Open(file)
	if err != nil {
		c.Fatal(err)
	}
	if err := m.InjectArgs(sess, args); err != nil {
		c.Fatal(err)
	}
	if *out == "" {
		*out = filepath.Base(file) + "x" // ".rfx"
	}
	f, err := os.Create(*out)
	if err != nil {
		c.Fatal(err)
	}
	if err := sess.Bundle().Write(f); err != nil {
		c.Fatal(err)
	}
	if err := f.Close(); err != nil {
		c.Fatal(err)
	}
}
