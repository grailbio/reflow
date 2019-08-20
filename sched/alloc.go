// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sched

import (
	"context"
	"fmt"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/pool"
)

// Allocq implements a priority queue of allocs, ordered by the
// scaled distance of available resources in the alloc.
type allocq []*alloc

// Len implements sort.Interface/heap.Interface.
func (q allocq) Len() int { return len(q) }

// Less implements sort.Interface/heap.Interface.
// We consider the alloc with the least amount of
// available resources the min alloc.
func (q allocq) Less(i, j int) bool {
	return q[i].Available.ScaledDistance(nil) < q[j].Available.ScaledDistance(nil)
}

// Swap implements heap.Interface/sort.Interface
func (q allocq) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index, q[j].index = i, j
}

// Push implements heap.Interface.
func (q *allocq) Push(x interface{}) {
	a := x.(*alloc)
	a.index = len(*q)
	*q = append(*q, a)
}

// Pop implements heap.Interface.
func (q *allocq) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	x.index = -1
	return x
}

// Alloc is the scheduler's representation of a Reflow alloc.
type alloc struct {
	// Alloc is the underlying alloc.
	pool.Alloc

	// Requirements is this alloc's scheduling requirements.
	// Used for internal bookeeping.
	Requirements reflow.Requirements
	// Available is the unused set of resources in this alloc.
	Available reflow.Resources

	// Context is the the context with which all calls beneath this
	// alloc should be made. It is canceled when the alloc dies.
	Context context.Context
	// Cancel is used to cancel the alloc's context.
	Cancel func()

	// Pending is the number of running tasks on this alloc.
	Pending int

	idleTime time.Time
	index    int
	// id is the alloc id. It is the same as Alloc.ID(). It is present here
	// so that we can retrieve the id to update the stats after the alloc dies.
	id string
}

// Init is called to initialize the alloc from its underlying Reflow alloc.
func (a *alloc) Init() {
	a.Available = a.Alloc.Resources()
	a.Pending = 0
	a.idleTime = time.Now()
	a.id = a.Alloc.ID()
}

func (a *alloc) String() string {
	return fmt.Sprintf("%s available %s", a.ID(), a.Available)
}

// Assign updates this alloc to account for the provided task
// assignment.
func (a *alloc) Assign(task *Task) {
	if task.alloc != nil {
		panic("sched: task already assigned")
	}
	task.alloc = a
	a.Pending++
	a.Available.Sub(a.Available, task.Config.Resources)
}

// Unassign updates this alloc to account for the completion of the
// the provided task assignment.
func (a *alloc) Unassign(task *Task) {
	if task.alloc != a {
		panic("sched: unassigned from wrong alloc")
	}
	a.Pending--
	a.Available.Add(a.Available, task.Config.Resources)
	if a.Pending == 0 {
		a.idleTime = time.Now()
	}
	task.alloc = nil
}

// IdleFor returns the time passed since the alloc had zero
// assigned tasks.
func (a *alloc) IdleFor() time.Duration {
	if a.Pending > 0 {
		return 0
	}
	return time.Since(a.idleTime)
}

func newAlloc() *alloc {
	return &alloc{index: -1}
}
