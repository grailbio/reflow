// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/batch"
	"github.com/grailbio/reflow/cache"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/ctxwg"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/runner"
)

func (c *Cmd) batchrun(ctx context.Context, args ...string) {
	c.Fatal("command batchrunb has been renamed runbatch")
}

func (c *Cmd) runbatch(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("runbatch", flag.ExitOnError)
	help := `Runbatch runs the batch defined in this directory.
	
A batch is defined by a directory with a batch configuration file named
config.json, which stores a single JSON dictionary with two entries, 
defining the paths of the reflow program to be used and the run file 
that contains each run's parameter. For example, the config.json file

	{
		"program": "pipeline.reflow",
		"runs_file": "samples.csv"
	}

specifies that batch should run "pipeline.reflow" with the parameters
specified in each row of "samples.csv".

The runs file must contain a header naming its columns. Its first
column must be named "id" and contain the unique identifier of each
run. The other columns name the parameters to be used for each reflow
run. Unnamed columns are used as arguments to the run. For example,
if the above "pipeline.reflow" specified parameters "bam" and
"sample", then the following CSV defines that three runs are part of
the batch: bam=1.bam,sample=a; bam=2.bam,sample=b; and
bam=3.bam,sample=c.

	id,bam,sample
	1,1.bam,a
	2,2.bam,b
	3,3.bam,c`
	retryFlag := flags.Bool("retry", false, "retry failed runs")
	resetFlag := flags.Bool("reset", false, "reset failed runs")
	gcFlag := flags.Bool("gc", false, "enable runtime garbage collection")
	nocacheexternFlag := flags.Bool("nocacheextern", false, "don't cache extern ops")
	recomputeemptyFlag := flags.Bool("recomputeempty", false, "recompute empty cache values")

	c.Parse(flags, args, help, "runbatch [-retry] [-reset] [flags]")
	if flags.NArg() != 0 {
		flags.Usage()
	}
	user, err := c.Config.User()
	if err != nil {
		c.Fatal(err)
	}
	cluster := c.cluster()
	rcache, err := c.Config.Cache()
	if err != nil {
		c.Fatal(err)
	}

	transferer := &repository.Manager{
		Log:          c.Log.Tee(nil, "transferer: "),
		PendingBytes: repository.NewLimits(transferLimit),
		Stat:         repository.NewLimits(statLimit),
	}
	if cache, ok := rcache.(*cache.Cache); ok {
		transferer.PendingBytes.Set(cache.Repository.URL().String(), int(^uint(0)>>1))
		cache.Transferer = transferer
	}
	go transferer.Report(ctx, time.Minute)

	b := &batch.Batch{
		EvalConfig: reflow.EvalConfig{
			Log:            c.Log,
			Cache:          rcache,
			NoCacheExtern:  *nocacheexternFlag,
			RecomputeEmpty: *recomputeemptyFlag,
			Transferer:     transferer,
			GC:             *gcFlag,
		},
		Rundir:  c.rundir(),
		User:    user,
		Cluster: cluster,
	}
	b.Dir, err = os.Getwd()
	if err != nil {
		c.Fatal(err)
	}
	if err := b.Init(*resetFlag); err != nil {
		c.Fatal(err)
	}
	defer b.Close()
	if *retryFlag {
		for id, run := range b.Runs {
			var retry bool
			switch run.State.Phase {
			case runner.Init, runner.Eval:
				continue
			case runner.Done, runner.Retry:
				retry = run.State.Err != nil
			}
			if !retry {
				continue
			}
			c.Errorf("retrying run %v\n", id)
			run.State.Reset()
		}
	}
	var wg ctxwg.WaitGroup
	ctx, bgcancel := reflow.WithBackground(ctx, &wg)
	err = b.Run(ctx)
	if err != nil {
		c.Log.Errorf("batch failed with error %v", err)
	}
	c.Log.Debugf("waiting for cache writes to complete")
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	if err := wg.Wait(ctx); err != nil {
		c.Log.Errorf("some cache writes still pending: %v", err)
	}
	bgcancel()
	cancel()
	if err != nil {
		os.Exit(1)
	}
}

func (c *Cmd) batchinfo(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("batchinfo", flag.ExitOnError)
	help := `Batchinfo displays runtime information for the batch in the current directory.
See runbatch -help for information about Reflow's batching mechanism.`
	c.Parse(flags, args, help, "batchinfo")
	if flags.NArg() != 0 {
		flags.Usage()
	}

	var b batch.Batch
	b.Rundir = c.rundir()
	var err error
	b.Dir, err = os.Getwd()
	if err != nil {
		c.Fatal(err)
	}
	if err := b.Init(false); err != nil {
		c.Fatal(err)
	}
	defer b.Close()
	ids := make([]string, len(b.Runs))
	i := 0
	for id := range b.Runs {
		ids[i] = id
		i++
	}
	sort.Strings(ids)
	var tw tabwriter.Writer
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()

	for _, id := range ids {
		run := b.Runs[id]
		fmt.Fprintf(&tw, "run %s: %s\n", id, run.State.Name.Short())
		c.printRunInfo(ctx, &tw, run.State.Name)
		fmt.Fprintf(&tw, "\tlog:\t%s\n", filepath.Join(b.Dir, "log."+id))
	}
}

func (c *Cmd) listbatch(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("listbatch", flag.ExitOnError)
	help := `Listbatch lists runtime status for the batch in the current directory.
See runbatch -help for information about Reflow's batching mechanism.

The columns displayed by listbatch are:

	id    the batch run ID
	run   the run's name
	state the run's state`
	c.Parse(flags, args, help, "listbatch")
	if flags.NArg() != 0 {
		flags.Usage()
	}

	var b batch.Batch
	b.Rundir = c.rundir()
	var err error
	b.Dir, err = os.Getwd()
	if err != nil {
		c.Fatal(err)
	}
	if err := b.Init(false); err != nil {
		c.Fatal(err)
	}
	defer b.Close()
	ids := make([]string, len(b.Runs))
	i := 0
	for id := range b.Runs {
		ids[i] = id
		i++
	}
	sort.Strings(ids)
	var tw tabwriter.Writer
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()

	for _, id := range ids {
		run := b.Runs[id]
		var state string
		switch run.State.Phase {
		case runner.Init:
			state = "waiting"
		case runner.Eval:
			state = "running"
		case runner.Retry:
			state = "retrying"
		case runner.Done:
			if err := run.State.Err; err != nil {
				state = errors.Recover(err).ErrorSeparator(": ")
			} else {
				state = "done"
			}
		}
		fmt.Fprintf(&tw, "%s\t%s\t%s\n", id, run.State.Name.Short(), state)
	}
}
