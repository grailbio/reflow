// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"io"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/taskdb"
)

func (c *Cmd) cat(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("cat", flag.ExitOnError)
	taskdbRepoFlag := flags.Bool("taskdbrepo", false, "whether to use the taskdb repository instead of reflow repository")
	help := `Cat copies files from Reflow's repository (by default)
If the flag -taskdbrepo is provided, then Cat will use the configured TaskDB repository
`
	c.Parse(flags, args, help, "cat [-taskdbrepo] files...")
	if flags.NArg() == 0 {
		flags.Usage()
	}
	var repo reflow.Repository
	if *taskdbRepoFlag {
		var tdb taskdb.TaskDB
		c.must(c.Config.Instance(&tdb))
		repo = tdb.Repository()
	} else {
		c.must(c.Config.Instance(&repo))
	}
	var ids []digest.Digest
	for _, arg := range flags.Args() {
		id, err := reflow.Digester.Parse(arg)
		if err != nil {
			c.Fatalf("parse %s: %v", id, err)
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		_, err := repo.Stat(ctx, id)
		if err != nil {
			// TODO(swami): Should we just lookup all relevant repos instead of forcing the user to specify ?
			if errors.Is(errors.NotExist, err) {
				c.Fatalf("stat %s: %v\nDo you need to specify -taskdbrepo (you would if the reference was from taskdb)", id.Hex(), err)
			}
			c.Fatalf("stat %s: %v", id.Hex(), err)
		}
	}
	for _, id := range ids {
		rc, err := repo.Get(ctx, id)
		if err != nil {
			c.Fatalf("get %s: %v", id, err)
		}
		_, err = io.Copy(c.Stdout, rc)
		if err != nil {
			c.Fatalf("read %s: %v", id, err)
		}
		rc.Close()
	}
}
