package tool

import (
	"context"
	"flag"
	"io"
	"os"
)

func (c *Cmd) logs(ctx context.Context, args ...string) {
	var (
		flags      = flag.NewFlagSet("logs", flag.ExitOnError)
		stdoutFlag = flags.Bool("stdout", false, "display stdout instead of stderr")
		help       = "Logs displays logs from execs."
	)
	c.Parse(flags, args, help, "logs exec")
	if flags.NArg() != 1 {
		flags.Usage()
	}

	u, err := parseURI(flags.Arg(0))
	if err != nil {
		c.Fatal(err)
	}
	if u.Kind != execURI {
		c.Fatal("URI is not an exec URI")
	}
	cluster := c.cluster()
	alloc, err := cluster.Alloc(ctx, u.AllocID)
	if err != nil {
		c.Fatalf("alloc %s: %s", u.AllocID, err)
	}
	exec, err := alloc.Get(ctx, u.ExecID)
	if err != nil {
		c.Fatalf("%s: %s", u.ExecID, err)
	}
	rc, err := exec.Logs(ctx, *stdoutFlag, !*stdoutFlag)
	if err != nil {
		c.Fatalf("logs %s: %s", exec.URI(), err)
	}
	_, err = io.Copy(os.Stdout, rc)
	rc.Close()
	if err != nil {
		c.Fatal(err)
	}
}
