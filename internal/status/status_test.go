// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package status

import (
	"math/rand"
	"testing"
)

func TestStatus(t *testing.T) {
	var status Status
	if got, want := len(status.Groups()), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	c := status.Wait(-1)
	var v int
	select {
	case v = <-c:
	default:
		t.Fatal("expected update")
	}
	if got, want := v, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	c = status.Wait(v)
	select {
	case <-c:
		t.Fatal("unexpected update")
	default:
	}
	g := status.Group("test")
	select {
	case v = <-c:
	default:
		t.Fatal("expected update")
	}
	if got, want := v, 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	c = status.Wait(v)
	task := g.Start("hello world")
	select {
	case v = <-c:
	default:
		t.Fatal("expected update")
	}
	c = status.Wait(v)
	task.Print("an update")
	select {
	case v = <-c:
	default:
		t.Fatal("expected update")
	}
	groups := status.Groups()
	if got, want := len(groups), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := groups[0], g; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	g = groups[0]
	if got, want := g.Value().Title, "test"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	tasks := g.Tasks()
	if got, want := len(tasks), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	task = tasks[0]
	if got, want := task.Value().Title, "hello world"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := task.Value().Status, "an update"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestStatusManyWaiters(t *testing.T) {
	var (
		status   Status
		versions = rand.Perm(100)
		waiters  = make([]<-chan int, len(versions))
	)
	for _, v := range versions {
		waiters[v] = status.Wait(v)
	}
	for v := <-status.Wait(-1); v < len(versions); v++ {
		for w := v; w < len(versions); w++ {
			select {
			case <-waiters[w]:
				t.Errorf("unexpected ready waiter %v", w)
			default:
			}
		}
		status.notify()
		select {
		case <-waiters[v]:
		default:
			t.Errorf("expected waiter %v to be ready (version %d)", v, status.version)
		}
	}
}
