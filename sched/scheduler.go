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
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/taskdb"
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

// blobLocator defines an interface for locating blobs.
type blobLocator interface {
	Location(ctx context.Context, id digest.Digest) (string, error)
}

// A Scheduler is responsible for managing a set of tasks and allocs,
// assigning (and reassigning) tasks to appropriate allocs. Scheduler
// can manage large numbers of tasks and allocs efficiently.
type Scheduler struct {
	// Transferer is used to manage data movement between
	// allocs and the scheduler's repository.
	Transferer reflow.Transferer
	// Mux is used to manage direct data transfers between blob stores (if supported)
	Mux blob.Mux
	// Repository is the repository from which dependent objects are
	// downloaded and to which result objects are uploaded.
	Repository reflow.Repository
	// Cluster provides dynamic allocation of allocs.
	Cluster Cluster
	// Log logs scheduler actions.
	Log *log.Logger
	// TaskDB is  the task reporting db.
	TaskDB taskdb.TaskDB

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

	// Stats is the scheduler stats.
	Stats *Stats

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
		Stats:            newStats(),
	}
}

// Submit adds a set of tasks to the scheduler's todo list. The provided
// tasks are managed by the scheduler after this call. The scheduler
// manages a task until it reaches the TaskDone state.
func (s *Scheduler) Submit(tasks ...*Task) {
	for _, task := range tasks {
		task.Log.Debugf("submitted with %v", task.Config)
	}
	s.submitc <- tasks
}

// ExportStats exports scheduler stats as expvars.
func (s *Scheduler) ExportStats() {
	s.Stats.publish()
}

func (s *Scheduler) configString() string {
	var b bytes.Buffer
	if s.Transferer != nil {
		_, _ = fmt.Fprintf(&b, "transferer %T", s.Transferer)
	}
	if s.Repository != nil {
		_, _ = fmt.Fprintf(&b, " repository %T", s.Repository)
	}
	if s.Cluster != nil {
		_, _ = fmt.Fprintf(&b, " cluster %T", s.Cluster)
	}
	if s.TaskDB != nil {
		_, _ = fmt.Fprintf(&b, " taskDB %T", s.TaskDB)
	}
	var schemes []string
	for s := range s.Mux {
		schemes = append(schemes, s)
	}
	sort.Strings(schemes)
	_, _ = fmt.Fprintf(&b, " blob.Mux[%s]", strings.Join(schemes, ", "))
	return b.String()
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

	s.Log.Debugf("starting with configuration: %s", s.configString())
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
			s.Stats.AddTasks(tasks)
			for _, task := range tasks {
				if task.Config.Type == "extern" && !task.nonDirectTransfer {
					go s.directTransfer(ctx, task)
					continue
				}
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
			s.Stats.ReturnTask(task, alloc)
			// Tasks failing due to network errors imply that the alloc is unusable.
			if errors.Is(errors.Net, task.Err) && alloc.index != -1 {
				heap.Remove(&live, alloc.index)
				alloc.index = -1
			}
		case alloc := <-notifyc:
			heap.Remove(&pending, alloc.index)
			if alloc.Alloc != nil {
				alloc.Init()
				heap.Push(&live, alloc)
				s.Stats.AddAlloc(alloc)
			}
		case alloc := <-deadc:
			// The allocs tasks will be returned with state TaskLost.
			if alloc.index != -1 {
				heap.Remove(&live, alloc.index)
			}
			s.Stats.MarkAllocDead(alloc)
		}

		assigned := s.assign(&todo, &live, s.Stats)
		for _, task := range assigned {
			task.Log.Debugf("assigning to alloc %v", task.alloc)
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
		assigned = s.assign(&todo, &pending, nil)
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
		heap.Push(&pending, alloc)
		go s.allocate(ctx, alloc, notifyc, deadc)
	}
}

func (s *Scheduler) assign(tasks *taskq, allocs *allocq, stats *Stats) (assigned []*Task) {
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
		if stats != nil {
			stats.AssignTask(task, alloc)
		}
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
	err = pool.Keepalive(alloc.Context, s.Log, alloc.Alloc)
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
	statePut
	stateWait
	statePromote
	stateInspect
	stateResult
	stateTransferOut
	stateUnload
	stateDone
)

func (e execState) String() string {
	switch e {
	default:
		panic("bad state")
	case stateLoad:
		return "loading"
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
	case stateUnload:
		return "unloading"
	case stateDone:
		return "complete"
	}
}

func (s *Scheduler) run(task *Task, returnc chan<- *Task) {
	var (
		err            error
		alloc          = task.alloc
		ctx            = alloc.Context
		x              reflow.Exec
		n              = 0
		state          execState
		tcancel        context.CancelFunc
		tctx           context.Context
		loadedData     sync.Map
		resultUnloaded bool
	)
	// TODO(marius): we should distinguish between fatal and nonfatal errors.
	// The fatal ones are useless to retry.
	for n < numExecTries && state < stateDone {
		switch state {
		default:
			panic("bad state")
		case stateLoad:
			for i, arg := range task.Config.Args {
				if arg.Fileset == nil {
					continue
				}
				loadedData.Store(i, false)
			}
			g, gctx := errgroup.WithContext(ctx)
			loadedData.Range(func(key, value interface{}) bool {
				if value.(bool) {
					return true
				}
				i := key.(int)
				arg := task.Config.Args[i]
				g.Go(func() error {
					task.Log.Debugf("loading %s", (*arg.Fileset).Short())
					fs, lerr := alloc.Load(gctx, s.Repository.URL(), *arg.Fileset)
					if lerr != nil {
						return lerr
					}
					task.Log.Debugf("loaded %s", fs.Short())
					task.Config.Args[i].Fileset = &fs
					loadedData.Store(i, true)
					return nil
				})
				return true
			})
			err = g.Wait()
		case statePut:
			x, err = alloc.Put(ctx, digest.Digest(task.ID), task.Config)
		case stateWait:
			if s.TaskDB != nil {
				tctx, tcancel = context.WithCancel(ctx)
				if taskdbErr := s.TaskDB.CreateTask(tctx, task.ID, task.RunID, task.FlowID, taskdb.NewImgCmdID(task.Config.Image, task.Config.Cmd), task.Config.Ident, x.URI()); taskdbErr != nil {
					task.Log.Errorf("taskdb createtask: %v", taskdbErr)
				} else {
					go func() { _ = taskdb.KeepTaskAlive(tctx, s.TaskDB, task.ID) }()
				}
			}
			task.Exec = x
			task.set(TaskRunning)
			err = x.Wait(ctx)
			if s.TaskDB != nil {
				if taskdbErr := s.TaskDB.SetTaskResult(tctx, task.ID, x.ID()); taskdbErr != nil {
					task.Log.Errorf("taskdb settaskresult: %v", taskdbErr)
				}
				tcancel()
			}
		case statePromote:
			err = x.Promote(ctx)
		case stateInspect:
			task.Inspect, err = x.Inspect(ctx)
		case stateResult:
			task.Result, err = x.Result(ctx)
		case stateTransferOut:
			files := task.Result.Fileset.Files()
			err = s.Transferer.Transfer(ctx, s.Repository, alloc.Repository(), files...)
		case stateUnload:
			g, gctx := errgroup.WithContext(ctx)
			loadedData.Range(func(key, value interface{}) bool {
				i := key.(int)
				fs := *task.Config.Args[i].Fileset
				g.Go(func() error {
					task.Log.Debugf("unloading %v", fs.Short())
					uerr := alloc.Unload(gctx, fs)
					if uerr != nil {
						return uerr
					}
					task.Log.Debugf("unloaded %v", fs.Short())
					loadedData.Delete(i)
					return nil
				})
				return true
			})
			// Extern loads the files to be externed and gets unloaded above. Extern's result
			// fileset includes files that were externed and not necessarily any new data
			// that was produced. Hence we don't need to unload the result.
			if task.Config.Type != "extern" && !resultUnloaded {
				g.Go(func() error {
					fs := task.Result.Fileset
					task.Log.Debugf("unloading %v", fs.Short())
					uerr := alloc.Unload(gctx, fs)
					if uerr != nil {
						return uerr
					}
					task.Log.Debugf("unloaded %v", fs.Short())
					resultUnloaded = true
					return nil
				})
			}
			err = g.Wait()
		}
		if err == nil {
			task.Log.Debugf("%s", state)
			n = 0
			state++
		} else if err == ctx.Err() {
			break
		} else {
			// TODO(marius): terminate early on NotSupported, Invalid
			task.Log.Debugf("%s: %s; try %d", state, err, n+1)
			n++
		}
	}
	task.Err = err
	if err != nil && (err == ctx.Err() || errors.Restartable(err)) {
		task.set(TaskLost)
	} else {
		task.set(TaskDone)
	}
	returnc <- task
}

func (s *Scheduler) directTransfer(ctx context.Context, task *Task) {
	taskLogger := task.Log.Tee(nil, "direct transfer: ")
	if s.TaskDB != nil {
		if err := s.TaskDB.CreateTask(ctx, task.ID, task.RunID, task.FlowID, taskdb.NewImgCmdID(task.Config.Image, task.Config.Cmd), task.Config.Ident, "local"); err != nil {
			taskLogger.Errorf("taskdb createtask: %v", err)
		} else {
			tctx, tcancel := context.WithCancel(ctx)
			defer tcancel()
			go func() { _ = taskdb.KeepTaskAlive(tctx, s.TaskDB, task.ID) }()
		}
	}
	task.set(TaskRunning)
	task.Err = s.doDirectTransfer(ctx, task)
	if task.Err != nil && errors.Is(errors.NotSupported, task.Err) {
		taskLogger.Debugf("switching to non-direct %v", task.Err)
		task.nonDirectTransfer = true
		task.set(TaskLost)
		s.submitc <- []*Task{task}
		return
	}
	if task.Err != nil {
		taskLogger.Error(task.Err)
	}
	task.set(TaskDone)
	if s.TaskDB != nil && task.Result.Err == nil {
		if err := s.TaskDB.SetTaskResult(ctx, task.ID, task.Result.Fileset.Digest()); err != nil {
			taskLogger.Errorf("taskdb settaskresult: %v", err)
		}
	}
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

// doDirectTransfer attempts to do a direct transfer for externs.
// Direct transfers are supported only if the scheduler's Repository
// and the destination repository are both blob stores.
func (s *Scheduler) doDirectTransfer(ctx context.Context, task *Task) error {
	taskLogger := task.Log.Tee(nil, "direct transfer: ")
	if task.Config.Type != "extern" {
		panic("direct transfers only supported for extern")
	}
	if len(task.Config.Args) != 1 {
		return errors.E(errors.Precondition,
			errors.Errorf("unexpected args (must be 1, but was %d): %v", len(task.Config.Args), task.Config.Args))
	}
	// Check if the scheduler's repository supports blobLocator.
	fileLocator, ok := s.Repository.(blobLocator)
	if !ok {
		return errors.E(errors.NotSupported, errors.New("scheduler repository does not support locating blobs"))
	}
	// Check if the destination is a blob store.
	if _, _, err := s.Mux.Bucket(ctx, task.Config.URL); err != nil {
		return err
	}

	fs := task.Config.Args[0].Fileset.Pullup()
	for _, f := range fs.Files() {
		if f.IsRef() {
			return errors.E(errors.NotSupported, errors.New("unresolved files not supported"))
		}
	}
	task.mu.Lock()
	task.Result.Fileset.Map = map[string]reflow.File{}
	task.mu.Unlock()

	extUrl := strings.TrimSuffix(task.Config.URL, "/")

	g, ctx := errgroup.WithContext(ctx)
	for k, v := range fs.Map {
		filename, file := k, v
		g.Go(func() error {
			srcUrl, err := fileLocator.Location(ctx, file.ID)
			if err != nil {
				return err
			}
			dstUrl := extUrl + "/" + filename
			if filename == "." {
				dstUrl = extUrl
			}
			start := time.Now()
			if err = s.Mux.Transfer(ctx, dstUrl, srcUrl); err != nil {
				return errors.E(fmt.Sprintf("scheduler direct transfer: %s -> %s", srcUrl, dstUrl), err)
			}
			dur := time.Since(start).Round(time.Second)
			if dur < 1 {
				dur += time.Second
			}
			taskLogger.Debugf("completed %s -> %s (%s) in %s (%s/s) ", srcUrl, dstUrl, data.Size(file.Size), dur, data.Size(file.Size/int64(dur.Seconds())))
			task.mu.Lock()
			task.Result.Fileset.Map[filename] = file
			task.mu.Unlock()
			return err
		})
	}
	task.Result.Err = errors.Recover(g.Wait())
	return nil
}
