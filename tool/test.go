// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/reflow/ec2authenticator"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/local"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
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
		executor *local.Executor
		start    = time.Now()
		nfail    int
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
			c.makeTestExecutor(&executor)
			evalConfig := flow.EvalConfig{
				Executor:  executor,
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
	if nfail == 0 {
		c.Printf("PASS (%s)\n", time.Since(start))
		c.Exit(1)
	} else {
		c.Printf("FAIL (%s)\n", time.Since(start))
	}
}

func (c *Cmd) makeTestExecutor(executor **local.Executor) {
	if *executor != nil {
		return
	}
	client, resources, err := dockerClient()
	c.must(err)
	var sess *session.Session
	c.must(c.Config.Instance(&sess))
	var creds *credentials.Credentials
	c.must(c.Config.Instance(&creds))
	*executor = &local.Executor{
		Client:        client,
		Dir:           defaultFlowDir,
		Authenticator: ec2authenticator.New(sess),
		AWSCreds:      creds,
		Log:           c.Log.Tee(nil, "executor: "),
	}
	(*executor).SetResources(resources)
	c.must((*executor).Start())
}
