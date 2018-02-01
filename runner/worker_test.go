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
	"github.com/grailbio/reflow/internal/wg"
	"github.com/grailbio/reflow/test/flow"
	"github.com/grailbio/reflow/test/testutil"
)

func TestWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode because it sleeps")
	}
	intern := flow.Intern("internurl")
	exec1 := flow.Exec("image", "command1 %s", testutil.Resources, intern)
	exec2 := flow.Exec("image", "command2 %s", testutil.Resources, intern)
	exec3 := flow.Exec("image", "command3 %s", testutil.Resources, intern)
	merge := flow.Merge(exec1, exec2, exec3)

	var tf testutil.WaitTransferer
	tf.Init()
	e := &testutil.Executor{Have: testutil.Resources}
	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	eval := reflow.NewEval(merge, reflow.EvalConfig{
		Executor:   e,
		Transferer: &tf,
	})
	e2 := &testutil.Executor{}
	e2.Have.Scale(testutil.Resources, 2)
	e2.Init()
	e2.Repo = testutil.NewInmemoryRepository()
	const idleTime = 2 * time.Second
	w := &worker{
		Executor:    e2,
		Eval:        eval,
		MaxIdleTime: idleTime,
	}

	ctx := context.Background()
	rc := testutil.EvalAsync(ctx, eval)
	var wg wg.WaitGroup
	wg.Add(1)
	go func() {
		w.Go(ctx)
		wg.Done()
	}()

	e.Ok(intern, testutil.WriteFiles(e.Repository(), "intern"))

	tf.Ok(e2.Repository(), e.Repository(), testutil.File("intern"))
	tf.Ok(e2.Repository(), e.Repository(), testutil.File("intern"))

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
		e2.Ok(exec, testutil.WriteFiles(e2.Repository(), fmt.Sprint(i+1)))
	}
	e.Ok(main, testutil.WriteFiles(e.Repository(), fmt.Sprint(maini+1)))
	for i, exec := range execs {
		if exec == main {
			continue
		}
		tf.Ok(e.Repository(), e2.Repository(), testutil.File(fmt.Sprint(i+1)))
	}

	// TODO(marius): figure out how to not rely on real time here;
	// this takes way too long and can fail even when there are no
	// bugs.
	select {
	case <-wg.C():
	case <-time.After(idleTime * time.Duration(2)):
		t.Error("idle worker failed to die")
	}

	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got, want := r.Val, testutil.List(testutil.Files("1"), testutil.Files("2"), testutil.Files("3")); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
