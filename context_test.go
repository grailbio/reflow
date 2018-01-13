// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow_test

import (
	"context"
	"sync"
	"testing"

	"github.com/grailbio/reflow"
)

type nopWaitGroup struct{}

func (nopWaitGroup) Add(int) {}
func (nopWaitGroup) Done()   {}

func TestContext(t *testing.T) {
	ctx := context.Background()
	_ = reflow.Background(ctx) // nil ok.

	var wg1, wg2 sync.WaitGroup
	ctx1, _ := reflow.WithBackground(ctx, &wg1)
	ctx2, _ := reflow.WithBackground(ctx, &wg2)

	const N = 100
	var ctxs1, ctxs2 [N]reflow.Context
	for i := 0; i < N; i++ {
		ctxs1[i], ctxs2[i] = reflow.Background(ctx1), reflow.Background(ctx2)
	}
	done := make(chan bool)
	for i := 0; i < N; i++ {
		go func(i int) {
			ctxs1[i].Complete()
			wg2.Wait()
			done <- true
		}(i)
	}
	wg1.Wait()
	for i := 0; i < N; i++ {
		select {
		case <-done:
			t.Fatal("WaitGroup released too soon")
		default:
		}
		ctxs2[i].Complete()
	}
	for i := 0; i < N; i++ {
		<-done
	}
}

func TestContextCancel(t *testing.T) {
	ctx, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := reflow.WithBackground(ctx, nopWaitGroup{})
	bgctx := reflow.Background(ctx)

	if err := ctx.Err(); err != nil {
		t.Error(err)
	}
	cancelParent()
	if got, want := ctx.Err(), context.Canceled; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if err := bgctx.Err(); err != nil {
		t.Error(err)
	}
	cancel()
	if got, want := bgctx.Err(), context.Canceled; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
