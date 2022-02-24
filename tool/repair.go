// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/runtime"
	"golang.org/x/sync/errgroup"
)

func (c *Cmd) repair(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("repair", flag.ExitOnError)
	batch := flags.String("batch", "", "batch file to process")
	writebackConcurrency := flags.Int("writebackconcurrency", 20, "number of concurrent writeback threads")
	getConcurrency := flags.Int("getconcurrency", 50, "number of concurrent assoc gets")
	help := `Repair performs cache repair by cache-assisted pseudo-evaluation of
the provided reflow program. The program (evaluated with its arguments)
is evaluated by performing logical cache lookups in place of executor
evaluation. When values are missing and are immediately computable,
they are computed. Flow nodes that are successfully computed this way
are written back to the cache with all available keys. Repair is used to 
perform forward-migration of caching scheme, or back-filling when 
evaluations strategies change (e.g., bottomup vs. topdown evaluation).

Repair accepts command line arguments as in "reflow run" or parameters
supplied via a CSV batch file as in "reflow runbatch".`
	c.Parse(flags, args, help, "repair -batch samples.csv path | repair path [args]")
	if *writebackConcurrency <= 0 || *getConcurrency <= 0 {
		flags.Usage()
	}
	switch {
	case *batch != "" && flags.NArg() == 1:
	case *batch == "" && flags.NArg() > 0:
	default:
		flags.Usage()
	}

	var sess *session.Session
	c.must(c.Config.Instance(&sess))
	var assoc assoc.Assoc
	c.must(c.Config.Instance(&assoc))
	var repo reflow.Repository
	c.must(c.Config.Instance(&repo))
	config := flow.EvalConfig{
		Log:        c.Log,
		Repository: repo,
		Assoc:      assoc,
	}
	repair := flow.NewRepair(config)
	repair.GetLimit = limiter.New()
	repair.GetLimit.Release(*getConcurrency)
	repair.Go(ctx, *writebackConcurrency)
	if *batch != "" {
		f, err := os.Open(*batch)
		c.must(err)
		r := csv.NewReader(f)
		r.FieldsPerRecord = -1
		records, err := r.ReadAll()
		f.Close()
		c.must(err)
		lim := limiter.New()
		lim.Release(50)
		header, records := records[0], records[1:]
		program := flags.Arg(0)
		g, ctx := errgroup.WithContext(ctx)
		for i := range records {
			record := records[i]
			g.Go(func() error {
				if err := lim.Acquire(ctx, 1); err != nil {
					return err
				}
				defer lim.Release(1)
				args := []string{program}
				// This is ... not pretty, but it gets the job done.
				for i, key := range header {
					args = append(args, fmt.Sprintf("-%s=%s", key, record[i]))
				}
				e := runtime.Eval{
					InputArgs: args,
				}
				_, err := e.Run(false)
				if err != nil {
					return err
				}
				if err := e.ResolveImages(sess); err != nil {
					return err
				}
				c.Log.Printf("repair: %s", strings.Join(args, " "))
				// TODO(sbagaria): thread-safe append the resolved canonical images to repair.ImageMap.
				// Or instead of storing imageMap, store tool.ImageResolver instead in EvalConfig.
				repair.Do(ctx, e.Main())
				return nil
			})
		}
		c.must(g.Wait())
	} else {
		e := runtime.Eval{
			InputArgs: flags.Args(),
		}
		_, err := e.Run(false)
		c.must(err)
		c.must(e.ResolveImages(sess))
		repair.ImageMap = e.ImageMap
		repair.Do(ctx, e.Main())

	}
	c.must(repair.Done())
	c.Log.Printf("wrote %d new assoc entries", repair.NumWrites)
}
