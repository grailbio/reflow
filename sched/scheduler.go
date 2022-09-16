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
	"github.com/grailbio/reflow/metrics"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/sched/internal"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/trace"
	"golang.org/x/sync/errgroup"
)

const (
	numExecTries        = 5
	defaultDrainTimeout = 50 * time.Millisecond
)

var allocateTraceId = reflow.Digester.FromString("allocate")

// Cluster is the scheduler's cluster interface.
type Cluster interface {
	// Allocate returns an alloc with at least req.Min resources, or an
	// error. The requirement's width is used as a hint to size allocs
	// efficiently.
	Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (pool.Alloc, error)

	// CanAllocate returns whether this cluster can allocate the given amount of resources.
	CanAllocate(reflow.Resources) (bool, error)
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
	// Cluster provides dynamic allocation of allocs.
	Cluster Cluster
	// Log logs scheduler actions.
	Log *log.Logger
	// TaskDB is the task reporting db.
	TaskDB taskdb.TaskDB

	// MaxPendingAllocs is the maximum number outstanding
	// alloc requests.
	MaxPendingAllocs int
	// MaxAllocIdleTime is the time after which an idle alloc is
	// collected.
	MaxAllocIdleTime time.Duration

	// DrainTimeout is the duration to wait to see if more tasks have been submitted
	// so that we can combine the requirements of those tasks together to make larger allocs.
	DrainTimeout time.Duration

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
		DrainTimeout:     defaultDrainTimeout,
		MinAlloc:         reflow.Resources{"cpu": 1, "mem": 1 << 30, "disk": 1 << 30},
		Stats:            newStats(),
	}
}

// Submit adds a set of tasks to the scheduler's todo list. The provided
// tasks are managed by the scheduler after this call. The scheduler
// manages a task until it reaches the TaskDone state.
func (s *Scheduler) Submit(tasks ...*Task) {
	for _, task := range tasks {
		if task.Repository == nil {
			panic(fmt.Sprintf("scheduler Submit task (flow %s) with no repository", task.FlowID.Short()))
		}
		s.Log.Debugf("task (flow %s) submitted with %v", task.FlowID.Short(), task.Config)
	}
	tasksCopy := append([]*Task{}, tasks...)
	s.submitc <- tasksCopy
}

// ExportStats exports scheduler stats as expvars.
func (s *Scheduler) ExportStats() {
	s.Stats.Publish()
}

func (s *Scheduler) configString() string {
	var b bytes.Buffer
	if s.Transferer != nil {
		_, _ = fmt.Fprintf(&b, "transferer %T", s.Transferer)
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
				task.Set(TaskDone)
			}
			for ; nrunning > 0; nrunning-- {
				task := <-returnc
				switch task.State() {
				default:
					panic("illegal task state")
				case TaskLost:
					task.Err = ctx.Err()
					task.Set(TaskDone)
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
			tasks = append(tasks, s.drain()...)
			for _, task := range tasks {
				// All accepted tasks must be initialized with an ID.
				task.Init()
			}
			s.Stats.AddTasks(tasks)
			for _, task := range tasks {
				if task.Config.Type == "extern" && !task.nonDirectTransfer {
					go s.directTransfer(ctx, task)
					continue
				}
				if ok, err := s.Cluster.CanAllocate(task.Config.Resources); !ok {
					task.Err = err
					task.Set(TaskDone)
					continue
				}
				metrics.GetTasksSubmittedCountCounter(ctx).Inc()
				metrics.GetTasksSubmittedSizeCounter(ctx).Add(task.Config.ScaledDistance(nil))
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
				old := task.ID().IDShort()
				// Reset the task (which also assigns it a new task identifier)
				task.Reset()
				task.Log.Printf("task %s (flow %s) has been lost, will retry (attempt %d) as task %s", old, task.FlowID.Short(), 1+task.Attempt(), task.ID().IDShort())
				heap.Push(&todo, task)
			case TaskDone:
				// In this case we're done, and we can forget about the task.
			}
			s.Stats.ReturnTask(task, alloc)
			// Network errors imply that the alloc is unreachable.
			// Context cancelled errors indicate that the alloc's context is done and therefore unusable.
			// While in both these cases, the alloc's keepalive mechanism will eventually mark it as dead,
			// we do it early here to immediately avoid scheduling tasks on it.
			if (errors.Is(errors.Canceled, task.Err) || errors.Is(errors.Net, task.Err)) && alloc.index != -1 {
				heap.Remove(&live, alloc.index)
				alloc.index = -1
			}
		case alloc := <-notifyc:
			heap.Remove(&pending, alloc.index)
			if alloc.Alloc != nil {
				alloc.Init(ctx, s.Log)
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
			task.Log.Printf("task %s (flow %s) assigning to alloc %v", task.ID().IDShort(), task.FlowID.Short(), task.alloc)
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
		// tasks to pending allocs, and then allocate any remaining (if any).
		assigned = s.assign(&todo, &pending, nil)
		var (
			req reflow.Requirements
			// needMore tells whether any tasks remain after mock allocation.
			// This is needed in addition to `req` because if all tasks have empty resources
			// we end up getting empty requirements, but we should trigger at least one allocation.
			needMore bool
		)
		if len(todo) > 0 {
			req = requirements(todo)
			needMore = true
		}
		for _, task := range assigned {
			task.alloc.Unassign(task)
			heap.Push(&todo, task)
		}
		if req.Equal(reflow.Requirements{}) && !needMore {
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

// drain drains the task submission channel if a valid DrainTimeout is set.
// Draining is done by waiting upto DrainTimeout (since the last set of tasks were received) for new tasks.
func (s *Scheduler) drain() (tasks []*Task) {
	if s.DrainTimeout == 0 {
		return
	}
	var (
		drained bool
		t       = time.NewTimer(s.DrainTimeout)
	)
	for !drained {
		select {
		case more := <-s.submitc:
			tasks = append(tasks, more...)
			// Stop timer and explicitly drain if necessary
			if !t.Stop() {
				<-t.C
			}
		case <-t.C:
			drained = true
		}
		t.Reset(s.DrainTimeout)
	}
	t.Stop()
	return
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
	allocReqCtx, endAllocReqTrace := trace.Start(ctx, trace.AllocReq, allocateTraceId, "allocating resources")
	alloc.Alloc, err = s.Cluster.Allocate(allocReqCtx, alloc.Requirements, s.Labels)
	if err != nil {
		msg := fmt.Sprintf("failed to allocate %s from cluster: %v", alloc.Requirements, err)
		s.Log.Errorf(msg)
		if allocReqCtx.Err() != nil {
			// to avoid polluting the trace, only emit the span if it wasn't due to context cancellation
			trace.Note(allocReqCtx, "error", msg)
			endAllocReqTrace()
		}
		notify <- alloc
		return
	}
	trace.Note(allocReqCtx, "allocID", alloc.Alloc.ID())
	endAllocReqTrace()

	metrics.GetAllocsStartedCountCounter(ctx).Inc()
	metrics.GetAllocsStartedSizeCounter(ctx).Add(alloc.Resources().ScaledDistance(nil))
	metrics.GetAllocsLiveSizeGauge(ctx).Add(alloc.Resources().ScaledDistance(nil))
	for _, r := range reflow.ResourcesKeys {
		metrics.GetAllocsLiveResourcesGauge(ctx, r).Add(alloc.Resources()[r])
	}

	alloc.Context, alloc.Cancel = context.WithCancel(ctx)
	var endAllocLifespanTrace func()
	alloc.Context, endAllocLifespanTrace = trace.Start(alloc.Context, trace.AllocLifespan, reflow.Digester.FromString(alloc.Alloc.ID()), "alloc: "+alloc.Alloc.ID())
	notify <- alloc
	err = pool.Keepalive(alloc.Context, s.Log, alloc.Alloc)
	alloc.Cancel()
	endAllocLifespanTrace()

	metrics.GetAllocsCompletedCountCounter(ctx).Inc()
	metrics.GetAllocsCompletedSizeCounter(ctx).Add(alloc.Resources().ScaledDistance(nil))
	metrics.GetAllocsLiveSizeGauge(ctx).Sub(alloc.Resources().ScaledDistance(nil))
	for _, r := range reflow.ResourcesKeys {
		metrics.GetAllocsLiveResourcesGauge(ctx, r).Sub(alloc.Resources()[r])
	}

	switch {
	case err == nil:
		s.Log.Debugf("alloc %s keepalive stopped (this should never happen!)", alloc.id)
	case alloc.Context.Err() != nil:
		// Alloc.Context error (which means we cancelled the alloc's context)
		s.Log.Debugf("alloc %s keepalive stopped (alloc.Context): %v", alloc.id, alloc.Context.Err())
	case ctx.Err() != nil:
		// parent ctx error
		s.Log.Debugf("alloc %s keepalive stopped (scheduler Context): %v", alloc.id, ctx)
	default:
		// Unexpected keepalive error
		s.Log.Errorf("alloc %s keepalive failed: %v", alloc.id, err)
	}
	// Always free the alloc since it is no longer usable anyway.
	var cancel func()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = alloc.Free(ctx); err != nil {
		s.Log.Errorf("alloc %s free failed: %v", alloc.id, err)
	}
	dead <- alloc
}

func (s *Scheduler) run(task *Task, returnc chan<- *Task) {
	var (
		err            error
		alloc          = task.alloc
		ctx            = alloc.Context
		x              reflow.Exec
		attempt        = 0
		state          internal.ExecState
		tcancel        context.CancelFunc
		tctx           context.Context
		loadedData     sync.Map // map[int]bool - where int is the index of task.Config.Args.
		resultUnloaded bool
	)
	task.TaskDB = s.TaskDB

	taskLogger := task.Log.Tee(nil, fmt.Sprintf("scheduler task %s (flow %s): ", task.ID().IDShort(), task.FlowID.Short()))

	metrics.GetTasksStartedCountCounter(ctx).Inc()
	metrics.GetTasksStartedSizeCounter(ctx).Add(task.Config.ScaledDistance(nil))
	metrics.GetTasksInProgressSizeGauge(ctx).Add(task.Config.ScaledDistance(nil))
	for _, r := range reflow.ResourcesKeys {
		metrics.GetTasksInProgressResourcesGauge(ctx, r).Add(task.Config.Resources[r])
	}
	defer func() {
		metrics.GetTasksCompletedCountCounter(ctx).Inc()
		metrics.GetTasksCompletedSizeCounter(ctx).Add(task.Config.ScaledDistance(nil))
		metrics.GetTasksInProgressSizeGauge(ctx).Sub(task.Config.ScaledDistance(nil))
		for _, r := range reflow.ResourcesKeys {
			metrics.GetTasksInProgressResourcesGauge(ctx, r).Sub(task.Config.Resources[r])
		}
	}()

	defer func() {
		if tcancel == nil {
			return
		}
		// Use background context for setting task completion status.
		if taskdbErr := s.TaskDB.SetTaskComplete(context.Background(), task.ID(), err, time.Now()); taskdbErr != nil {
			taskLogger.Errorf("taskdb settaskcomplete: %v", taskdbErr)
		}
		tcancel()
	}()
	// Save the original fileset. In cases, where we fail, we need to restore the original fileset,
	// since after the load all the interned files are technically resolved w.r.t. the current alloc.
	// If we get reassigned to a new alloc, that will not be true anymore, and hence we need to resolve
	// the files all over again.
	savedArgs := append([]reflow.Arg{}, task.Config.Args...)
	ctx, endTrace := trace.Start(ctx, state.TraceKind(), task.FlowID, fmt.Sprintf("%s_%s %s", task.Config.Ident, task.FlowID.Short(), state.String()))
	for attempt < numExecTries && state < internal.StateDone {
		taskLogger.Debugf("%s (try %d): started", state, attempt)
		switch state {
		default:
			panic("bad state")
		case internal.StateLoad:
			task.Set(TaskStaging)
			if s.TaskDB != nil && tctx == nil {
				// disable govet check due to https://github.com/golang/go/issues/29587
				tctx, tcancel = context.WithCancel(ctx) //nolint: govet
				tdbtask := taskdb.Task{
					ID:        task.ID(),
					RunID:     task.RunID,
					FlowID:    task.FlowID,
					ImgCmdID:  taskdb.NewImgCmdID(task.Config.Image, task.Config.Cmd),
					Ident:     task.Config.Ident,
					Attempt:   task.Attempt(),
					Resources: task.Config.Resources,
					AllocID:   alloc.taskdbAllocID,
				}
				if taskdbErr := s.TaskDB.CreateTask(tctx, tdbtask); taskdbErr != nil {
					taskLogger.Errorf("taskdb createtask: %v", taskdbErr)
				} else {
					go func() { _ = taskdb.KeepTaskAlive(tctx, s.TaskDB, task.ID()) }()
				}
			}
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
					taskLogger.Debugf("loading %s", (*arg.Fileset).Short())
					fs, lerr := alloc.Load(gctx, task.Repository.URL(), *arg.Fileset)
					if lerr != nil {
						return lerr
					}
					taskLogger.Debugf("loaded %s", fs.Short())
					task.Config.Args[i].Fileset = &fs
					loadedData.Store(i, true)
					return nil
				})
				return true
			})
			err = g.Wait()
		case internal.StatePut:
			x, err = alloc.Put(ctx, digest.Digest(task.ID()), task.Config)
		case internal.StateWait:
			if s.TaskDB != nil {
				if taskdbErr := s.TaskDB.SetTaskUri(tctx, task.ID(), x.URI()); taskdbErr != nil {
					taskLogger.Errorf("taskdb settaskuri: %v", taskdbErr)
				}
			}
			task.Exec = x
			task.Set(TaskRunning)
			err = x.Wait(ctx)
			if s.TaskDB != nil {
				// TODO(swami): Fix this so that the task result points to the result fileset.
				if taskdbErr := s.TaskDB.SetTaskResult(tctx, task.ID(), x.ID()); taskdbErr != nil {
					taskLogger.Errorf("taskdb settaskresult: %v", taskdbErr)
				}
			}
		case internal.StateVerify:
			g, gctx := errgroup.WithContext(ctx)
			loadedData.Range(func(key, value interface{}) bool {
				i := key.(int)
				fs := *task.Config.Args[i].Fileset
				g.Go(func() error {
					taskLogger.Debugf("verifying %v", fs.Short())
					uerr := alloc.VerifyIntegrity(gctx, fs)
					if uerr == nil {
						taskLogger.Debugf("verified %v", fs.Short())
					} else {
						taskLogger.Debugf("verify %v: %s", fs.Short(), uerr)
					}
					return uerr
				})
				return true
			})
			err = g.Wait()
		case internal.StatePromote:
			err = x.Promote(ctx)
		case internal.StateInspect:
			taskDBRepoUrl := s.TaskDB.Repository().URL()
			taskLogger.Debugf("retrieving inspect (and saving to repo %s)", taskDBRepoUrl)
			var resp reflow.InspectResponse
			resp, err = x.Inspect(ctx, taskDBRepoUrl)
			if err == nil {
				task.RunInfo = *resp.RunInfo
			}
			if d := task.RunInfo.InspectDigest.Digest; err == nil && !d.IsZero() {
				taskLogger.Debugf("saved inspect to repo %s: %s", taskDBRepoUrl, d.Short())
			}
		case internal.StateResult:
			task.Result, err = x.Result(ctx)
			if err == nil && task.Config.Type == "extern" {
				// If files are 'extern'ed without using direct transfer, the result fileset contains
				// the same files as the input, but are missing assertions. The assertions do however
				// exist in the original fileset (`savedArgs` and not the one modified after load)
				task.Result.Fileset.MapAssertionsByFile(savedArgs[0].Fileset.Files())
			}
		case internal.StateTransferOut:
			files := task.Result.Fileset.Files()
			err = s.Transferer.Transfer(ctx, task.Repository, alloc.Repository(), files...)
		case internal.StateUnload:
			err = unload(ctx, task, taskLogger, &loadedData, alloc, &resultUnloaded)
		}
		next, nextIsRetry, msg := state.Next(ctx, err, task.PostUseChecksum)
		taskLogger.Debugf("%s (try %d): %s, next state: %s", state, attempt, msg, next)
		if nextIsRetry {
			attempt++
			trace.Note(ctx, fmt.Sprintf("\"%s\" retries", state.String()), attempt)
		} else {
			endTrace() // end the trace for the current state and start it for the next one
			_, endTrace = trace.Start(ctx, next.TraceKind(), task.FlowID, fmt.Sprintf("%s_%s %s", task.Config.Ident, task.FlowID.Short(), next.String()))
		}
		state = next
	}
	// Clean up the loaded data in case we exited early without unloading (usually due to an error in an earlier state)
	if err != nil {
		if unloadErr := unload(ctx, task, taskLogger, &loadedData, alloc, &resultUnloaded); unloadErr != nil {
			taskLogger.Debugf("error unloading data after task failure, this wastes disk space on the alloc: %s", unloadErr)
		}
	}
	if err == nil && s.TaskDB != nil {
		err = s.TaskDB.SetTaskAttrs(ctx, task.ID(), task.RunInfo.Stdout.Digest, task.RunInfo.Stderr.Digest, task.RunInfo.InspectDigest.Digest)
	}
	task.Err = err
	switch {
	case err == nil:
		task.Set(TaskDone)
	case alloc.Context.Err() != nil:
		task.Config.Args = savedArgs
		task.Set(TaskLost)
	case errors.Is(errors.Canceled, err), errors.Is(errors.Net, err), errors.Is(errors.Timeout, err), errors.Is(errors.Unavailable, err):
		task.Config.Args = savedArgs
		task.Set(TaskLost)
	default:
		task.Set(TaskDone)
	}
	taskLogger.Debugf("returning task with state: %s", task.State())
	returnc <- task
}

func unload(ctx context.Context, task *Task, taskLogger *log.Logger, loadedData *sync.Map, alloc *alloc, resultUnloaded *bool) error {
	g, gctx := errgroup.WithContext(ctx)
	loadedData.Range(func(key, value interface{}) bool {
		i, loaded := key.(int), value.(bool)
		if !loaded {
			// Don't unload data that wasn't loaded.
			return true
		}
		fs := *task.Config.Args[i].Fileset
		g.Go(func() error {
			taskLogger.Debugf("unloading %v", fs.Short())
			uerr := alloc.Unload(gctx, fs)
			if uerr != nil {
				return uerr
			}
			taskLogger.Debugf("unloaded %v", fs.Short())
			loadedData.Delete(i)
			return nil
		})
		return true
	})
	// Extern loads the files to be externed and gets unloaded above. Extern's result
	// fileset includes files that were externed and not necessarily any new data
	// that was produced. Hence we don't need to unload the result.
	if task.Config.Type != "extern" && !*resultUnloaded {
		g.Go(func() error {
			fs := task.Result.Fileset
			taskLogger.Debugf("unloading %v", fs.Short())
			uerr := alloc.Unload(gctx, fs)
			if uerr != nil {
				return uerr
			}
			taskLogger.Debugf("unloaded %v", fs.Short())
			*resultUnloaded = true
			return nil
		})
	}
	return g.Wait()
}

func (s *Scheduler) directTransfer(ctx context.Context, task *Task) {
	const identifier = "scheduler.directTransfer"
	taskLogger := s.Log.Tee(nil, fmt.Sprintf("direct transfer %s: ", task.ID().IDShort()))
	if s.TaskDB != nil {
		taskdbErr := s.TaskDB.CreateTask(ctx, taskdb.Task{
			ID:       task.ID(),
			RunID:    task.RunID,
			FlowID:   task.FlowID,
			ImgCmdID: taskdb.ImgCmdID(digest.Digest{}),
			Ident:    identifier,
			URI:      "local",
		})
		if taskdbErr != nil {
			taskLogger.Errorf("taskdb createtask: %v", taskdbErr)
		} else {
			tctx, tcancel := context.WithCancel(ctx)
			defer func() {
				if err := s.TaskDB.SetTaskComplete(context.Background(), task.ID(), task.Err, time.Now()); err != nil {
					taskLogger.Errorf("taskdb settaskcomplete: %v", err)
				}
				tcancel()
			}()
			go func() { _ = taskdb.KeepTaskAlive(tctx, s.TaskDB, task.ID()) }()
		}
	}
	task.Set(TaskRunning)
	task.Err = s.doDirectTransfer(ctx, task, taskLogger)
	if task.Err != nil && errors.Is(errors.NotSupported, task.Err) {
		taskLogger.Debugf("switching to non-direct due to error: %v", task.Err)
		task.nonDirectTransfer = true
		task.Set(TaskLost)
		s.submitc <- []*Task{task}
		return
	}
	if task.Err != nil {
		taskLogger.Error(task.Err)
	}
	if s.TaskDB != nil && task.Result.Err == nil {
		if err := s.TaskDB.SetTaskResult(ctx, task.ID(), task.Result.Fileset.Digest()); err != nil {
			taskLogger.Errorf("taskdb settaskresult: %v", err)
		}
	}
	task.Set(TaskDone)
}

func requirements(tasks []*Task) reflow.Requirements {
	// TODO(marius): We should revisit this requirements model and how
	// it interacts with the underlying cluster providers. Doing this
	// optimally requires changing the interaction model between the
	// scheduler and cluster so that the cluster presents a "menu" of
	// different alloc types, and the scheduler is allowed to pick from this menu.

	// We create a minimum requirement matching the resource needs of the largest task
	// and then expand its width by packing tasks as tightly as possible.
	var (
		req  reflow.Requirements
		have reflow.Resources
		i    int
	)
	tasksCopy := append([]*Task{}, tasks...)
	// Sort the tasks by resource needs
	sort.Slice(tasksCopy, func(i, j int) bool {
		return tasksCopy[i].Config.Resources.ScaledDistance(nil) > tasksCopy[j].Config.Resources.ScaledDistance(nil)
	})
	for len(tasksCopy) > 0 {
		i = sort.Search(len(tasksCopy), func(i int) bool {
			return have.Available(tasksCopy[i].Config.Resources)
		})
		if i == len(tasksCopy) {
			// Found nothing, so add the current biggest task's resources
			req.AddParallel(tasksCopy[0].Config.Resources)
			i = 0
			// Reset current available resources
			have.Set(req.Min)
		}
		// Found the biggest one which'll fit in the current available resources.
		have.Sub(have, tasksCopy[i].Config.Resources)
		tasksCopy = append(tasksCopy[0:i], tasksCopy[i+1:]...)
	}
	return req
}

type directTransfer struct {
	filename       string
	file           reflow.File
	srcUrl, dstUrl string
}

// doDirectTransfer attempts to do a direct transfer for externs.
// Direct transfers are supported only if the scheduler's Repository
// and the destination repository are both blob stores.
func (s *Scheduler) doDirectTransfer(ctx context.Context, task *Task, taskLogger *log.Logger) error {
	if task.Config.Type != "extern" {
		panic("direct transfers only supported for extern")
	}
	if len(task.Config.Args) != 1 {
		return errors.E(errors.Precondition,
			errors.Errorf("unexpected args (must be 1, but was %d): %v", len(task.Config.Args), task.Config.Args))
	}
	// Check if the task's repository supports blobLocator.
	fileLocator, ok := task.Repository.(blobLocator)
	if !ok {
		return errors.E(errors.NotSupported, errors.New("scheduler repository does not support locating blobs"))
	}
	// Check if the destination is a blob store.
	if _, _, err := s.Mux.Bucket(ctx, task.Config.URL); err != nil {
		return err
	}

	extUrl := strings.TrimSuffix(task.Config.URL, "/")
	fs := task.Config.Args[0].Fileset.Pullup()

	var transfers []directTransfer
	for k, v := range fs.Map {
		filename, file := k, v
		var srcUrl string
		if !file.IsRef() {
			// resolved file
			if src, err := fileLocator.Location(ctx, file.ID); err != nil {
				return err
			} else {
				srcUrl = src
			}
		} else {
			// reference file
			srcUrl = file.Source
		}
		dstUrl := extUrl + "/" + filename
		if filename == "." {
			dstUrl = extUrl
		}
		if ok, err := s.Mux.CanTransfer(ctx, dstUrl, srcUrl); !ok {
			return errors.E(fmt.Sprintf("scheduler cannot direct transfer: %s -> %s", srcUrl, dstUrl), err)
		}
		transfers = append(transfers, directTransfer{filename, file, srcUrl, dstUrl})
	}

	task.mu.Lock()
	task.Result.Fileset.Map = map[string]reflow.File{}
	task.mu.Unlock()

	var (
		failed          []directTransfer
		stalledAttempts int
	)
	const maxStalledAttempts = 3

	for stalledAttempts <= maxStalledAttempts && len(transfers) > 0 {
		g, gctx := errgroup.WithContext(ctx)
		for _, t := range transfers {
			taskLogger.Debugf("%d transfers remaining, stalled attempts: %d/%d", len(transfers), stalledAttempts, maxStalledAttempts)
			t := t
			g.Go(func() error {
				start := time.Now()
				if err := s.Mux.Transfer(gctx, t.dstUrl, t.srcUrl); err != nil {
					if !errors.Restartable(err) {
						return errors.E(fmt.Sprintf("scheduler direct transfer: %s -> %s", t.srcUrl, t.dstUrl), err)
					}
					// retryable, so log and track it in failed, but don't return an error for now
					taskLogger.Debugf("retryable failure %s -> %s: %s", t.srcUrl, t.dstUrl, err)
					task.mu.Lock()
					failed = append(failed, t)
					task.mu.Unlock()
					return nil
				}
				dur := time.Since(start).Round(time.Second)
				if dur < 1 {
					dur += time.Second
				}
				sz := t.file.Size
				taskLogger.Debugf("completed %s -> %s (%s) in %s (%s/s) ", t.srcUrl, t.dstUrl, data.Size(sz), dur, data.Size(sz/int64(dur.Seconds())))
				task.mu.Lock()
				task.Result.Fileset.Map[t.filename] = t.file
				task.mu.Unlock()
				return nil
			})
		}
		err := g.Wait()
		if err != nil && !errors.Restartable(err) {
			task.Result.Err = errors.Recover(err)
			break // no point in trying further
		}
		switch {
		case len(failed) == 0:
		case len(failed) == len(transfers):
			stalledAttempts++
		case len(failed) < len(transfers):
			// completed some transfers, so no longer stalled
			stalledAttempts = 0
		}
		// swap the underlying arrays; reset failed slice
		transfers, failed = failed, transfers[:0]
	}
	if stalledAttempts > maxStalledAttempts && task.Result.Err == nil {
		task.Result.Err = errors.Recover(errors.E(fmt.Sprintf("scheduler direct transfer: progress stalled for retryable errors (see earlier logs); %d incomplete transfers", len(transfers))))
	}
	return nil
}
