// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sched

import (
	"context"
	"sync"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/sync/ctxsync"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/taskdb"
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
	ID taskdb.TaskID
	// Config is the task's exec config, which is passed on to the
	// alloc after scheduling.
	Config reflow.ExecConfig
	// Repository to use for this task.
	// Repository is the repository from which dependent objects are
	// downloaded and to which result objects are uploaded.
	Repository reflow.Repository
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

	// InspectDigest Stores the digest returned from the exec when it is instructed to write its inspect data to the repo.
	InspectDigest digest.Digest

	// Exec is the exec which is running (or ran) the task. Exec is
	// set by the scheduler before the task enters TaskRunning state.
	Exec reflow.Exec

	// Priority is the task priority. Lower numbers indicate higher priority.
	// Higher priority tasks will get scheduler before any lower priority tasks.
	Priority int

	// PostUseChecksum indicates whether input filesets are checksummed after use.
	PostUseChecksum bool

	// ExpectedDuration is the duration the task is expected to take used only as a hint
	// by the scheduler for better scheduling.
	ExpectedDuration time.Duration

	// RunID that created this task.
	RunID taskdb.RunID
	// FlowID is the digest (flow.Digest) of the flow for which this task was created.
	FlowID digest.Digest

	// TaskDB is where the task row for this task is recorded and is set by the scheduler only after the task was attempted.
	TaskDB taskdb.TaskDB

	mu   sync.Mutex
	cond *ctxsync.Cond

	state TaskState
	alloc *alloc
	index int
	stats *TaskStats

	// attempt stores the (zero-based) current attempt number for this task.
	attempt int

	// nonDirectTransfer represents a task which cannot be executed as a direct transfer.
	nonDirectTransfer bool
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

// Attempt returns the task's current attempt index (zero-based).
func (t *Task) Attempt() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.attempt
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

// Reset resets the task's state to `TaskInit` and increases its attempt count.
func (t *Task) Reset() {
	mutate(t, func(target *Task) {
		target.state = TaskInit
		target.attempt++
	})
}

// Set sets the task's state to the given state.
func (t *Task) Set(state TaskState) {
	if state == TaskInit {
		panic("task state change to TaskInit must be done using Reset()")
	}
	mutate(t, func(target *Task) { target.state = state })
}

// mutate mutates the given task using the given mutator function.
func mutate(target *Task, mutator func(t *Task)) {
	target.mu.Lock()
	mutator(target)
	target.stats.Update(target)
	target.cond.Broadcast()
	target.mu.Unlock()
}

// TaskSet is a set of tasks.
type TaskSet map[*Task]bool

// newTaskSet returns a set of tasks.
func NewTaskSet(tasks ...*Task) TaskSet {
	set := make(TaskSet)
	for _, task := range tasks {
		set[task] = true
	}
	return set
}

// RemoveAll removes tasks from the taskSet.
func (s TaskSet) RemoveAll(tasks ...*Task) {
	for _, task := range tasks {
		delete(s, task)
	}
}

// Slice returns a slice containing the tasks in the taskSet.
func (s TaskSet) Slice() []*Task {
	var tasks = make([]*Task, 0, len(s))
	for task := range s {
		tasks = append(tasks, task)
	}
	return tasks
}

// Len returns the number of tasks in the taskSet.
func (s TaskSet) Len() int {
	return len(s)
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
