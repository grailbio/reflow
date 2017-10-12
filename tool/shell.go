package tool

import (
	"bufio"
	"context"
	"flag"
	"io"
	"os"
)

func (c *Cmd) shell(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("shell", flag.ExitOnError)
	help := `Run a shell (/bin/bash) inside the container of a running exec.
The local standard input, output and error streams are attached.
The user may exit the terminal by typing 'exit'/'quit'`
	// TODO(pgopal) - Put the terminal in raw mode.
	c.Parse(flags, args, help, "shell exec")
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
	e, err := alloc.Get(ctx, u.ExecID)
	if err != nil {
		c.Fatalf("%s: %s", u.ExecID, err)
	}
	sr, sw := io.Pipe()
	go func() {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			_, err := sw.Write([]byte(s.Text() + "\n"))
			if err != nil {
				c.Fatal("%s: %s", u.ExecID, err)
			}
		}
		sw.Close()
		if s.Err() != nil {
			c.Fatal("%s: %s", u.ExecID, s.Err())
		}
	}()

	rwc, err := e.Shell(ctx)
	go func() {
		io.Copy(rwc, sr)
	}()
	_, err = io.Copy(os.Stdout, rwc)
	if err != nil {
		c.Fatalf("%s: %s", u.ExecID, err)
	}
}
