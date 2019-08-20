// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sched

import (
	"context"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/sync/ctxsync"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
)

// TaskState enumerates the possible states of a task.
type TaskState int

const (
	// TaskInit is the initial state of a Task. No work has yet been done.
	TaskInit TaskState = iota
	// TaskStaging indicates that the task is currently staging input
	// data.
	TaskStaging
	// TaskRunning indicates the task is currently executing.
	TaskRunning
	// TaskLost indicates that the task has transiently failed and will be
	// retried by the scheduler.
	TaskLost
	// TaskDone indicates the task has completed.
	TaskDone
)

func (s TaskState) String() string {
	switch s {
	case TaskInit:
		return "initializing"
	case TaskStaging:
		return "staging data"
	case TaskRunning:
		return "running"
	case TaskLost:
		return "lost"
	case TaskDone:
		return "done"
	default:
		return "unknown"
	}
}

// Task represents a schedulable unit of work. Tasks are submitted to
// a scheduler which in turn schedules them onto allocs. After
// submission, all coordination is performed through the task struct.
type Task struct {
	// ID is a caller-assigned identifier for the task.
	ID digest.Digest
	// Config is the task's exec config, which is passed on to the
	// alloc after scheduling.
	Config reflow.ExecConfig
	// Log receives any status log messages during task scheduling
	// and execution.
	Log *log.Logger

	// Err stores any task scheduling error. If Err != nil while the
	// task is TaskDone, then the task failed to run.
	Err error
	// Result stores the Reflow result returned by a successful
	// execution.
	Result reflow.Result
	// Inspect stores the Reflow inspect output after a successful
	// execution.
	Inspect reflow.ExecInspect

	// Exec is the exec which is running (or ran) the task. Exec is
	// set by the scheduler before the task enters TaskRunning state.
	Exec reflow.Exec

	// Priority is the task priority. Lower numbers indicate higher priority.
	// Higher priority tasks will get scheduler before any lower priority tasks.
	Priority int

	// RunID that created this task.
	RunID digest.Digest
	// TaskID is the unique identifier for this task
	TaskID digest.Digest

	mu   sync.Mutex
	cond *ctxsync.Cond

	state TaskState
	alloc *alloc
	index int
	stats *TaskStats
}

// NewTask returns a new, initialized task. The Task may be populated
// and then submitted to the scheduler.
func NewTask() *Task {
	task := new(Task)
	task.cond = ctxsync.NewCond(&task.mu)
	return task
}

// State returns the task's current state.
func (t *Task) State() TaskState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// Wait returns after the task's state is at least the provided state. Wait
// returns an error if the context was canceled while waiting.
func (t *Task) Wait(ctx context.Context, state TaskState) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var err error
	for t.state < state && err == nil {
		err = t.cond.Wait(ctx)
	}
	return err
}

func (t *Task) set(state TaskState) {
	t.mu.Lock()
	t.state = state
	t.stats.Update(t)
	t.cond.Broadcast()
	t.mu.Unlock()
}

// Taskq defines a priority queue of tasks, ordered by
// scaled resource distance.
type taskq []*Task

func (q taskq) Len() int { return len(q) }

func (q taskq) Less(i, j int) bool {
	if q[i].Priority != q[j].Priority {
		return q[i].Priority < q[j].Priority
	}
	return q[i].Config.Resources.ScaledDistance(nil) < q[j].Config.Resources.ScaledDistance(nil)
}

func (q taskq) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index, q[j].index = i, j
}

// Push implements heap.Interface.
func (q *taskq) Push(x interface{}) {
	t := x.(*Task)
	t.index = len(*q)
	*q = append(*q, t)
}

// Pop implements heap.Interface.
func (q *taskq) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	x.index = -1
	return x
}
