// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package test

import (
	"context"
	"sync"

	"github.com/grailbio/reflow"
)

// Resources is a convenient set of resources to use for testing.
var Resources = reflow.Resources{500 << 20, 1, 10}

// EvalResult stores the result of an asynchronous evaluation.
type EvalResult struct {
	Val reflow.Fileset
	Err error
}

// EvalAsync evaluates Flow f on Eval e asynchronously. It also
// ensures that any background tasks are completed before reporting
// completion.
func EvalAsync(ctx context.Context, e *reflow.Eval) <-chan EvalResult {
	c := make(chan EvalResult, 1)
	//	e.Logger = log.New(os.Stderr, "", 0)
	go func() {
		var wg sync.WaitGroup
		ctx, _ := reflow.WithBackground(ctx, &wg)
		var r EvalResult
		r.Err = e.Do(ctx)
		r.Val = e.Value().(reflow.Fileset)
		wg.Wait()
		c <- r
	}()
	return c
}
