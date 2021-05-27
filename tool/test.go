// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
	"github.com/grailbio/reflow/wg"
)

func (c *Cmd) test(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("test", flag.ExitOnError)
	verbose := flags.Bool("v", false, "print verbose test output")
	help := `Test runs the tests in the provided module, using the local Docker
daemon for external execution. (Equivalent to "reflow run -local".)
Every identifier with the prefix "Test", and whose type is a boolean
is evaluated by command test. Any failure is reported to the user,
and the program exits with a non-zero status if any tests fail.`
	c.Parse(flags, args, help, "test path [args]")
	if flags.NArg() == 0 {
		flags.Usage()
	}
	e := Eval{InputArgs: flags.Args()}
	c.must(e.Run())
	c.must(e.ResolveImages(c.Config))
	if !e.V1 {
		c.Fatal("reflow test is supported only for v1 reflows")
	}
	type test struct {
		Name string
		Val  values.T
	}
	var tests []test
	for name, val := range e.Module {
		if !strings.HasPrefix(name, "Test") {
			continue
		}
		if typ := e.Type.Field(name); typ.Kind != types.BoolKind {
			c.Errorf("non-boolean test %v: %v\n", name, typ)
			continue
		}
		tests = append(tests, test{name, val})
	}
	if len(tests) == 0 {
		c.Fatal("module contains no tests")
	}
	var (
		schedCancel context.CancelFunc
		wg          wg.WaitGroup
		start       = time.Now()
		nfail       int
	)
testloop:
	for _, test := range tests {
		testStart := time.Now()
		if *verbose {
			c.Printf("RUN %s\n", test.Name)
		}
		var ok bool
		switch val := test.Val.(type) {
		case *flow.Flow:
			var schedCtx context.Context
			schedCtx, schedCancel = context.WithCancel(ctx)
			evalConfig := flow.EvalConfig{
				Scheduler: c.makeTestScheduler(schedCtx, &wg),
				Log:       c.Log.Tee(nil, test.Name+": "),
				CacheMode: infra.CacheOff,
				ImageMap:  e.ImageMap,
			}
			eval := flow.NewEval(val, evalConfig)
			err := eval.Do(ctx)
			if err == nil {
				err = eval.Err()
			}
			if err != nil {
				nfail++
				c.Printf("ERROR %s (%s): %s\n", test.Name, time.Since(testStart), err)
				continue testloop
			}
			ok = eval.Value().(bool)
		case bool:
			ok = val
		default:
			panic("invalid test")
		}
		if !ok {
			nfail++
			c.Printf("FAIL %s (%s)\n", test.Name, time.Since(testStart))
		} else if *verbose {
			c.Printf("PASS %s (%s)\n", test.Name, time.Since(testStart))
		}
	}
	if schedCancel != nil {
		schedCancel()
	}
	if nfail == 0 {
		c.Printf("PASS (%s)\n", time.Since(start))
	} else {
		c.Printf("FAIL (%s)\n", time.Since(start))
		c.Exit(1)
	}
}

// makeTestScheduler makes a scheduler useful for the "test" tool.
// TODO(swami): Fix parameters used by this scheduler or consider removing this tool ?
// This scheduler still uses a shared (s3) repository which is probably not desirable
// for the use-cases of this tool.  Perhaps a file-based local repository would work better.

func (c *Cmd) makeTestScheduler(ctx context.Context, w *wg.WaitGroup) *sched.Scheduler {
	c.SchemaKeys[infra.Cluster] = fmt.Sprintf("localcluster,dir=%v", defaultFlowDir)
	c.SchemaKeys[infra.TaskDB] = ""
	cfg, err := c.Schema.Make(c.SchemaKeys)
	c.must(err)
	var cluster runner.Cluster
	c.must(cfg.Instance(&cluster))
	s, err := NewScheduler(ctx, cfg, w, cluster, c.Log, c.Status)
	c.must(err)
	s.MaxAllocIdleTime = 30 * time.Second
	return s
}
