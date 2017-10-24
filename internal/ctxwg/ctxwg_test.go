// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ctxwg

import (
	"context"
	"testing"
	"time"
)

const N = 16

func testInterlocked(t *testing.T, w1, w2 *WaitGroup) {
	w1.Add(N)
	w2.Add(N)
	done := make(chan bool)
	for i := 0; i < N; i++ {
		go func(i int) {
			w1.Done()
			w2.Wait(context.Background())
			done <- true
		}(i)
	}
	w1.Wait(context.Background())
	for i := 0; i < N; i++ {
		select {
		case <-done:
			t.Fatal("WaitGroup released too soon")
		default:
		}
		w2.Done()
	}
	for i := 0; i < N; i++ {
		<-done
	}
}

func TestWaitGroup(t *testing.T) {
	var w1, w2 WaitGroup
	testInterlocked(t, &w1, &w2)
}

func TestWaitGroupCancel(t *testing.T) {
	var w1, w2 WaitGroup
	w1.Add(1)
	go func() {
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		err := w1.Wait(ctx)
		if got, want := err, context.DeadlineExceeded; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		w1.Done()
	}()
	testInterlocked(t, &w1, &w2)
}
