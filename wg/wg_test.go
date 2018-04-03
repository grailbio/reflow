// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package wg

import "testing"

const N = 16

func testInterlocked(t *testing.T, w1, w2 *WaitGroup) {
	w1.Add(N)
	w2.Add(N)
	done := make(chan bool)
	for i := 0; i < N; i++ {
		go func(i int) {
			w1.Done()
			<-w2.C()
			done <- true
		}(i)
	}
	<-w1.C()
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
