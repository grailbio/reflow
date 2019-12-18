// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package testutil

import (
	"context"
	"errors"
	"github.com/grailbio/reflow/values"
	"sync"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/flow"
)

// Resources is a convenient set of resources to use for testing.
var Resources = reflow.Resources{"mem": 500 << 20, "cpu": 1, "disk": 10}

// EvalResult stores the result of an asynchronous evaluation.
type EvalResult struct {
	Val reflow.Fileset
	Err error
}

// EvalAsync evaluates Flow f on Eval e asynchronously. It also
// ensures that any background tasks are completed before reporting
// completion. EvalAsync expects that e's toplevel flow returns a
// Fileset.
func EvalAsync(ctx context.Context, e *flow.Eval) <-chan EvalResult {
	c := make(chan EvalResult, 1)
	go func() {
		var wg sync.WaitGroup
		ctx, _ = flow.WithBackground(ctx, &wg)
		var r EvalResult
		r.Err = e.Do(ctx)
		if r.Err == nil {
			r.Err = e.Err()
		}
		if r.Err == nil {
			var ok bool
			r.Val, ok = e.Value().(reflow.Fileset)
			if !ok {
				r.Err = errors.New("flow did not return a fileset")
			}
		}
		wg.Wait()
		c <- r
	}()
	return c
}

// EvalFlowResult is the result of asynchronously evaluating a flow in EvalFlowAsync.
type EvalFlowResult struct {
	// Val is the value produced by the flow
	Val values.T
	// Err is the error produced by the flow, if any.
	Err error
}

// EvalFlowAsync evaluates a flow and returns the result as a value.
// This function is similar to EvalAsync. The only difference being that
// EvalAsync expects a fileset value whereas EvalFlowAsync doesn't restrict
// the value to any specific type.
// TODO(prasadgopal): move all tests to use EvalFlowAsync and let callers assert the type.
func EvalFlowAsync(ctx context.Context, e *flow.Eval) <-chan EvalFlowResult {
	c := make(chan EvalFlowResult, 1)
	go func() {
		var wg sync.WaitGroup
		ctx, _ = flow.WithBackground(ctx, &wg)
		var r EvalFlowResult
		r.Err = e.Do(ctx)
		if r.Err == nil {
			r.Err = e.Err()
		}
		if r.Err == nil {
			r.Val = e.Value()
		}
		wg.Wait()
		c <- r
	}()
	return c
}
