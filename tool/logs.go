// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"io"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/taskdb"
)

func (c *Cmd) logs(ctx context.Context, args ...string) {
	var (
		flags      = flag.NewFlagSet("logs", flag.ExitOnError)
		stdoutFlag = flags.Bool("stdout", false, "display stdout instead of stderr")
		followFlag = flags.Bool("f", false, "follow the logs")
		help       = "Logs displays logs from execs."
	)
	c.Parse(flags, args, help, "logs exec")
	if flags.NArg() != 1 {
		flags.Usage()
	}
	arg := flags.Arg(0)
	var tdb taskdb.TaskDB
	err := c.Config.Instance(&tdb)
	if err != nil || tdb == nil {
		n, err := parseName(arg)
		if err != nil {
			c.Fatal(err)
		}
		if n.Kind != execName {
			c.Fatalf("%s: not an exec URI", arg)
		}
		cluster := c.Cluster(nil)
		alloc, err := cluster.Alloc(ctx, n.AllocID)
		if err != nil {
			c.Fatalf("alloc %s: %s", n.AllocID, err)
		}
		exec, err := alloc.Get(ctx, n.ID)
		if err != nil {
			c.Fatalf("%s: %s", n.ID, err)
		}
		rc, err := exec.Logs(ctx, *stdoutFlag, !*stdoutFlag, *followFlag)
		if err != nil {
			c.Fatalf("logs %s: %s", exec.URI(), err)
		}
		_, err = io.Copy(c.Stdout, rc)
		rc.Close()
		if err != nil {
			c.Fatal(err)
		}
		return
	}

	d, err := reflow.Digester.Parse(arg)
	if err == nil {
		q := taskdb.Query{ID: d}
		tasks, err := tdb.Tasks(ctx, q)
		if err != nil {
			c.Fatal(err)
		}
		if len(tasks) == 0 {
			c.Fatal("no tasks matched id: %v", d.String())
		}
		if len(tasks) > 1 {
			c.Fatal("more than one task matched id: %v", d.String())
		}
		var repo reflow.Repository
		err = c.Config.Instance(&repo)
		if err != nil {
			log.Fatal("repository: ", err)
		}
		var rc io.ReadCloser
		if *stdoutFlag {
			rc, err = repo.Get(ctx, tasks[0].Stdout)
			if err != nil {
				log.Fatal("repository get stdout: ", err)
			}
		} else {
			rc, err = repo.Get(ctx, tasks[0].Stderr)
			if err != nil {
				log.Fatal("repository get stderr: ", err)
			}
		}
		if rc != nil {
			_, err = io.Copy(c.Stdout, rc)
			rc.Close()
			if err != nil {
				c.Fatal(err)
			}
		}
		return
	}
	n, err := parseName(arg)
	if err != nil {
		c.Fatal(err)
	}
	if n.Kind != execName {
		c.Fatalf("%s: not an exec URI", arg)
	}
	rc, err := c.execLogs(ctx, *stdoutFlag, *followFlag, n)
	if err != nil {
		c.Fatalf("logs %s: %s", arg, err)
	}
	_, err = io.Copy(c.Stdout, rc)
	rc.Close()
	if err != nil {
		c.Fatal(err)
	}
}
