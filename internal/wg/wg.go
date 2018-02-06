// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package wg implements a channel-enabled WaitGroup.
package wg

import "sync"

// A WaitGroup waits for a collection of goroutines to finish. The main
// goroutine calls Add to set the number of goroutines to wait for. Then each
// of the goroutines runs and calls Done when finished. At the same time, Wait
// can be used to block until all goroutines have finished.
// A WaitGroup must not be copied after first use.
//
// TODO(marius): this could be made more efficient by using atomics.
type WaitGroup struct {
	mu    sync.Mutex
	n     int
	waitc chan struct{}
}

// Add adds delta, which may be negative, to the WaitGroup counter. If the
// counter becomes zero, all goroutines blocked on Wait are released. If the
// counter goes negative, Add panics.
//
// Note that calls with a positive delta that occur when the counter is zero
// must happen before a Wait. Calls with a negative delta, or calls with a
// positive delta that start when the counter is greater than zero, may happen
// at any time. Typically this means the calls to Add should execute before the
// statement creating the goroutine or other event to be waited for. If a
// WaitGroup is reused to wait for several independent sets of events, new Add
// calls must happen after all previous Wait calls have returned. See the
// WaitGroup example.
func (w *WaitGroup) Add(delta int) {
	w.mu.Lock()
	w.n += delta
	if w.n < 0 {
		panic("negative waitgroup count")
	}
	var c chan struct{}
	if w.n == 0 {
		c = w.waitc
		w.waitc = nil
	}
	w.mu.Unlock()
	if c != nil {
		close(c)
	}
}

// Done decrements the WaitGroup counter.
func (w *WaitGroup) Done() {
	w.Add(-1)
}

// C returns a channel that is closed when the waitgroup count is 0.
func (w *WaitGroup) C() <-chan struct{} {
	w.mu.Lock()
	if w.n == 0 {
		w.mu.Unlock()
		c := make(chan struct{})
		close(c)
		return c
	}
	c := w.waitc
	if c == nil {
		c = make(chan struct{})
		w.waitc = c
	}
	w.mu.Unlock()
	return c
}

// N returns the current number of waiters.
func (w *WaitGroup) N() int {
	w.mu.Lock()
	n := w.n
	w.mu.Unlock()
	return n
}
