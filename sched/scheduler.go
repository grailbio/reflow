// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package sched implements task scheduling for Reflow.
//
// A unit of work is encapsulated by a Task, and is submitted to the
// scheduler. Multiple tasks may be submitted simultaneously (and may
// lead to better packing). Tasks are packed into a set of Reflow
// allocs; these are dynamically sourced from the provided cluster as
// needed. The scheduler attempts to pack tasks into as few allocs as
// possible so that they may be reclaimed when they become idle.
//
// The scheduler is given a Repository from which dependent objects
// are downloaded and to which results are uploaded. This repository
// is typically also the Reflow cache. A task fails if the necessary
// objects cannot be fetched, or if uploading fails.
//
// If an alloc's keepalive fails, its running tasks are marked as
// lost and rescheduled.
package sched

import (
	"container/heap"
	"context"
	"time"

	"github.com/grailbio/reflow"

	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"golang.org/x/sync/errgroup"
)

const numExecTries = 5

// Cluster is the scheduler's cluster interface.
type Cluster interface {
	// Allocate returns an alloc with at least req.Min resources, or an
	// error. The requirement's width is used as a hint to size allocs
	// efficiently.
	Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (pool.Alloc, error)
}

// A Scheduler is responsible for managing a set of tasks and allocs,
// assigning (and reassigning) tasks to appropriate allocs. Scheduler
// can manage large numbers of tasks and allocs efficiently.
type Scheduler struct {
	// Transferer is used to manage data movement between
	// allocs and the scheduler's repository.
	Transferer reflow.Transferer
	// Repository is the repository from which dependent objects are
	// downloaded and to which result objects are uploaded.
	Repository reflow.Repository
	// Cluster provides dynamic allocation of allocs.
	Cluster Cluster
	// Log logs scheduler actions.
	Log *log.Logger

	// MaxPendingAllocs is the maximum number outstanding
	// alloc requests.
	MaxPendingAllocs int
	// MaxAllocIdleTime is the time after which an idle alloc is
	// collected.
	MaxAllocIdleTime time.Duration

	// MinAlloc is the smallest resource allocation that is made by
	// the scheduler.
	MinAlloc reflow.Resources

	// Labels is the set of labels applied to newly created allocs.
	Labels pool.Labels

	submitc chan []*Task
}

// New returns a new Scheduler instance. The caller may customize its
// parameters before starting scheduling by invoking Scheduler.Do.
func New() *Scheduler {
	return &Scheduler{
		submitc:          make(chan []*Task),
		MaxPendingAllocs: 5,
		MaxAllocIdleTime: 5 * time.Minute,
		MinAlloc:         reflow.Resources{"cpu": 1, "mem": 1 << 30, "disk": 10 << 30},
	}
}

// Submit adds a set of tasks to the scheduler's todo list. The provided
// tasks are managed by the scheduler after this call. The scheduler
// manages a task until it reaches the TaskDone state.
func (s *Scheduler) Submit(tasks ...*Task) {
	for _, task := range tasks {
		task.Log.Debugf("scheduler: task submitted with %v", task.Config)
	}
	s.submitc <- tasks
}

// Do commences scheduling. The scheduler runs until the provided
// context is canceled, after which the context error is returned.
func (s *Scheduler) Do(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// We maintain a priority queue of runnable tasks, and priority
	// queues for live and pending live. The priority queues are
	// ordered by the resource measure (scaled distance). This leads to
	// a straightforward allocation strategy: we try to match tasks with
	// live in order, thus allocating the "smallest" runnable task
	// onto the "smallest" available alloc, progressively trying larger
	// live until we succeed. If we run out of live, we have to
	// allocate (or wait for pending allocations).
	//
	// Similarly, we maintain a set of pending live. When we fail to
	// assign all tasks onto running live, we attempt to assign the
	// remaining tasks onto the pending live. Any remaining tasks
	// represent needed resources for which we do not have any pending
	// live. Thus we craft an allocation requirement from the
	// remaining task list.
	var (
		live, pending allocq
		todo          taskq

		nrunning int

		notifyc = make(chan *alloc)
		deadc   = make(chan *alloc)
		returnc = make(chan *Task)

		tick = time.NewTicker(s.MaxAllocIdleTime / 2)
	)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			// After being canceled, we fail all pending tasks, and then drain
			// all tasks, current allocs, and pending allocs. (All of which
			// will be canceled by the same context cancellation.)
			//
			// We also cancel keepalives
			for _, task := range todo {
				task.Err = ctx.Err()
				task.set(TaskDone)
			}
			for ; nrunning > 0; nrunning-- {
				task := <-returnc
				switch task.State() {
				default:
					panic("illegal task state")
				case TaskLost:
					task.Err = ctx.Err()
					task.set(TaskDone)
				case TaskDone:
				}
			}
			for n := len(live); n > 0; n-- {
				<-deadc
			}
			for n := len(pending); n > 0; n-- {
				<-notifyc
			}
			return ctx.Err()
		case <-tick.C:
			for _, alloc := range live {
				if alloc.IdleFor() > s.MaxAllocIdleTime {
					alloc.Cancel()
				}
			}
		case tasks := <-s.submitc:
			for _, task := range tasks {
				heap.Push(&todo, task)
			}
		case task := <-returnc:
			nrunning--
			alloc := task.alloc
			alloc.Unassign(task)
			if alloc.index != -1 {
				heap.Fix(&live, alloc.index)
			}
			switch task.State() {
			default:
				panic("illegal task state")
			case TaskLost:
				task.set(TaskInit)
				heap.Push(&todo, task)
			case TaskDone:
				// In this case we're done, and we can forget about the task.
			}
		case alloc := <-notifyc:
			heap.Remove(&pending, alloc.index)
			if alloc.Alloc != nil {
				alloc.Init()
				heap.Push(&live, alloc)
			}
		case alloc := <-deadc:
			// The allocs tasks will be returned with state TaskLost.
			heap.Remove(&live, alloc.index)
		}

		assigned := s.assign(&todo, &live)
		for _, task := range assigned {
			task.Log.Debugf("scheduler: assigning task to alloc %v", task.alloc)
			nrunning++
			go s.run(task, returnc)
		}

		// At this point, we've scheduled everything we can onto the current
		// set of allocs. If we have more work, we'll need to try to create more
		// allocs.
		if len(todo) == 0 || len(pending) >= s.MaxPendingAllocs {
			continue
		}

		// We have more to do, and potential to allocate. We mock allocate remaining
		// tasks to pending allocs, and then allocate any remaining.
		assigned = s.assign(&todo, &pending)
		req := requirements(todo)
		for _, task := range assigned {
			task.alloc.Unassign(task)
			heap.Push(&todo, task)
		}
		if req.Equal(reflow.Requirements{}) {
			continue
		}

		req.Min.Max(s.MinAlloc, req.Min)
		alloc := newAlloc()
		alloc.Requirements = req
		alloc.Available = req.Min
		if req.Width > 1 {
			alloc.Available = nil
			alloc.Available.Scale(alloc.Available, float64(req.Width))
		}
		heap.Push(&pending, alloc)
		go s.allocate(ctx, alloc, notifyc, deadc)
	}
}

func (s *Scheduler) assign(tasks *taskq, allocs *allocq) (assigned []*Task) {
	var unassigned []*alloc
	for len(*tasks) > 0 && len(*allocs) > 0 {
		var (
			task  = (*tasks)[0]
			alloc = (*allocs)[0]
		)
		if !alloc.Available.Available(task.Config.Resources) {
			// We can't fit the smallest task in the smallest alloc.
			// Remove the alloc from consideration.
			heap.Pop(allocs)
			unassigned = append(unassigned, alloc)
			continue
		}
		heap.Pop(tasks)
		alloc.Assign(task)
		assigned = append(assigned, task)
		heap.Fix(allocs, 0)
	}
	for _, alloc := range unassigned {
		heap.Push(allocs, alloc)
	}
	return
}

func (s *Scheduler) allocate(ctx context.Context, alloc *alloc, notify, dead chan<- *alloc) {
	var err error
	alloc.Alloc, err = s.Cluster.Allocate(ctx, alloc.Requirements, s.Labels)
	if err != nil {
		// TODO: don't print errors that indicate resource exhaustion
		s.Log.Errorf("failed to allocate %s from cluster: %v", alloc.Requirements, err)
		notify <- alloc
		return
	}
	alloc.Context, alloc.Cancel = context.WithCancel(ctx)
	notify <- alloc
	err = pool.Keepalive(alloc.Context, nil, alloc.Alloc)
	alloc.Cancel()
	if err != nil && err == ctx.Err() {
		var cancel func()
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		_, err = alloc.Keepalive(ctx, 0)
		cancel()
	}
	if err != nil {
		s.Log.Errorf("alloc keepalive failed: %v", err)
	}
	dead <- alloc
}

type execState int

const (
	stateLoad execState = iota
	stateTransferIn
	statePut
	stateWait
	statePromote
	stateInspect
	stateResult
	stateTransferOut
	stateDone
)

func (e execState) String() string {
	switch e {
	default:
		panic("bad state")
	case stateLoad:
		return "loading"
	case stateTransferIn:
		return "transferring input"
	case statePut:
		return "submitting"
	case stateWait:
		return "waiting for completion"
	case statePromote:
		return "promoting objects"
	case stateInspect:
		return "retrieving diagnostic output"
	case stateResult:
		return "retrieving result"
	case stateTransferOut:
		return "transferring output"
	case stateDone:
		return "complete"
	}
}

func (s *Scheduler) run(task *Task, returnc chan<- *Task) {
	var (
		err   error
		alloc = task.alloc
		ctx   = alloc.Context
		x     reflow.Exec
		n     = 0
		state execState
	)
	// TODO(marius): we should distinguish between fatal and nonfatal errors.
	// The fatal ones are useless to retry.
	for n < numExecTries && state < stateDone {
		switch state {
		default:
			panic("bad state")
		case stateLoad:
			// TODO(marius): inbound transfers and loading shoud be concurrent.
			g, ctx := errgroup.WithContext(ctx)
			for i, arg := range task.Config.Args {
				if arg.Fileset == nil {
					continue
				}
				i, arg := i, arg
				g.Go(func() error {
					task.Log.Debugf("loading %v", *arg.Fileset)
					fs, err := alloc.Load(ctx, *arg.Fileset)
					if err != nil {
						return err
					}
					task.Log.Debugf("loaded %v", fs)
					task.Config.Args[i].Fileset = &fs
					return nil
				})
			}
			err = g.Wait()
		case stateTransferIn:
			task.set(TaskStaging)
			fs := reflow.Fileset{List: make([]reflow.Fileset, 0, len(task.Config.Args))}
			for _, arg := range task.Config.Args {
				if arg.Fileset != nil {
					fs.List = append(fs.List, *arg.Fileset)
				}
			}
			err = s.Transferer.Transfer(ctx, alloc.Repository(), s.Repository, fs.Files()...)
		case statePut:
			x, err = alloc.Put(ctx, task.ID, task.Config)
		case stateWait:
			task.Exec = x
			task.set(TaskRunning)
			err = x.Wait(ctx)
		case statePromote:
			err = x.Promote(ctx)
		case stateInspect:
			task.Inspect, err = x.Inspect(ctx)
		case stateResult:
			task.Result, err = x.Result(ctx)
		case stateTransferOut:
			files := task.Result.Fileset.Files()
			err = s.Transferer.Transfer(ctx, s.Repository, alloc.Repository(), files...)
		}
		if err == nil {
			task.Log.Debugf("scheduler: %s", state)
			n = 0
			state++
		} else if err == ctx.Err() {
			break
		} else {
			// TODO(marius): terminate early on NotSupported, Invalid
			task.Log.Debugf("scheduler: %s: %s; try %d", state, err, n+1)
			n++
		}
	}
	task.Err = err
	if err != nil && err == ctx.Err() {
		task.set(TaskLost)
	} else {
		task.set(TaskDone)
	}
	returnc <- task
}

func requirements(tasks []*Task) reflow.Requirements {
	// TODO(marius): We should revisit this requirements model and how
	// it interacts with the underlying cluster providers. Specifically,
	// this approach can be very conservative if we have a mix of tasks
	// that require different resource types or vary widely in
	// magnitudes across the same resource dimensions. This could even
	// lead to unattainable allocations that could theoretically be
	// satisfied by multiple allocs. Doing this properly requires
	// changing the interaction model between the scheduler and cluster
	// so that the cluster presents a "menu" of different alloc types,
	// and the scheduler is allowed to pick from this menu.
	var req reflow.Requirements
	for _, task := range tasks {
		req.AddParallel(task.Config.Resources)
	}
	return req
}
