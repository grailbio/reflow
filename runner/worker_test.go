// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"testing"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/internal/ctxwg"
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
	eval := &reflow.Eval{Executor: e, Transferer: &tf}
	eval.Init(merge)
	e2 := &test.Executor{Have: test.Resources.Scale(2)}
	e2.Init()
	const idleTime = 2 * time.Second
	w := &worker{
		Executor:    e2,
		Eval:        eval,
		MaxIdleTime: idleTime,
		Cache:       &cache,
	}

	ctx := context.Background()
	rc := test.EvalAsync(ctx, eval)
	var wg ctxwg.WaitGroup
	wg.Add(1)
	go func() {
		w.Go(ctx)
		wg.Done()
	}()

	e.Ok(intern, test.Files("intern"))
	tf.Ok(e2.Repository(), e.Repository(), test.Files("intern").Files()...)
	tf.Ok(e2.Repository(), e.Repository(), test.Files("intern").Files()...)
	e2.Ok(exec2, test.Files("2"))
	e2.Ok(exec3, test.Files("3"))
	tf.Ok(e.Repository(), e2.Repository(), test.Files("2").Files()...)
	tf.Ok(e.Repository(), e2.Repository(), test.Files("3").Files()...)
	e.Ok(exec1, test.Files("1"))

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
	if !cache.Exists(exec2) {
		t.Error("no cached value for exec2")
	}
	if !cache.Exists(exec3) {
		t.Error("no cached value for exec3")
	}
	// We did not give the main evaluator a cache.
	if cache.Exists(exec1) {
		t.Error("cached value for exec1")
	}
}
