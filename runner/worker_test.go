// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/internal/ctxwg"
	"github.com/grailbio/reflow/repository/testutil"
	"github.com/grailbio/reflow/test"
	"github.com/grailbio/reflow/test/flow"
)

func TestWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode because it sleeps")
	}
	intern := flow.Intern("internurl")
	exec1 := flow.Exec("image", "command1 %s", test.Resources, intern)
	exec2 := flow.Exec("image", "command2 %s", test.Resources, intern)
	exec3 := flow.Exec("image", "command3 %s", test.Resources, intern)
	merge := flow.Merge(exec1, exec2, exec3)

	var (
		tf    test.Transferer
		cache test.Cache
	)
	tf.Init()
	cache.Init()
	e := &test.Executor{Have: test.Resources}
	e.Init()
	e.Repo = testutil.NewInmemory()
	eval := reflow.NewEval(merge, reflow.EvalConfig{
		Executor:   e,
		Transferer: &tf,
		Cache:      &cache,
	})
	e2 := &test.Executor{Have: test.Resources.Scale(2)}
	e2.Init()
	e2.Repo = testutil.NewInmemory()
	const idleTime = 2 * time.Second
	w := &worker{
		Executor:    e2,
		Eval:        eval,
		MaxIdleTime: idleTime,
	}

	ctx := context.Background()
	rc := test.EvalAsync(ctx, eval)
	var wg ctxwg.WaitGroup
	wg.Add(1)
	go func() {
		w.Go(ctx)
		wg.Done()
	}()

	e.Ok(intern, test.WriteFiles(e.Repository(), "intern"))

	tf.Ok(e2.Repository(), e.Repository(), test.File("intern"))
	tf.Ok(e2.Repository(), e.Repository(), test.File("intern"))

	// Now wait for the main executor to grab a task, and figure out
	// which it is. We expect the auxilliary executor to grab the
	// remaining ones (concurrently).
	execs := []*reflow.Flow{exec1, exec2, exec3}
	main := e.WaitAny(execs...)
	var maini int
	for i, exec := range execs {
		if exec == main {
			maini = i
			continue
		}
		e2.Ok(exec, test.WriteFiles(e2.Repository(), fmt.Sprint(i+1)))
	}
	e.Ok(main, test.WriteFiles(e.Repository(), fmt.Sprint(maini+1)))
	for i, exec := range execs {
		if exec == main {
			continue
		}
		tf.Ok(e.Repository(), e2.Repository(), test.File(fmt.Sprint(i+1)))
	}

	ctx, cancel := context.WithTimeout(ctx, idleTime*time.Duration(2))
	defer cancel()
	// TODO(marius): figure out how to not rely on real time here;
	// this takes way too long and can fail even when there are no
	// bugs.
	if err := wg.Wait(ctx); err != nil {
		t.Errorf("idle worker failed to die: %v", err)
	}

	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got, want := r.Val, test.List(test.Files("1"), test.Files("2"), test.Files("3")); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	for _, expect := range []*reflow.Flow{exec1, exec2, exec3} {
		if !cache.Exists(expect) {
			t.Errorf("no cached value for %v", expect)
		}
	}
}
