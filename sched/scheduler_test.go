// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sched_test

import (
	"context"
	"sync"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/test/testutil"
)

func newTestScheduler() (scheduler *sched.Scheduler, cluster *testCluster, repository *testutil.InmemoryRepository, shutdown func()) {
	repository = testutil.NewInmemoryRepository()
	cluster = newTestCluster()
	scheduler = sched.New()
	scheduler.Transferer = testutil.Transferer
	scheduler.Repository = repository
	scheduler.Cluster = cluster
	scheduler.MinAlloc = reflow.Resources{}
	var ctx context.Context
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		scheduler.Do(ctx)
		wg.Done()
	}()
	shutdown = func() {
		cancel()
		wg.Wait()
	}
	return
}

func expectExists(t *testing.T, repo reflow.Repository, fs reflow.Fileset) {
	t.Helper()
	missing, err := repository.Missing(context.TODO(), repo, fs.Files()...)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) > 0 {
		t.Errorf("missing files: %v", missing)
	}
}

func TestSchedulerBasic(t *testing.T) {
	scheduler, cluster, repo, shutdown := newTestScheduler()
	defer shutdown()
	ctx := context.Background()
	in := randomFileset(repo)
	expectExists(t, repo, in)

	task := newTask(10, 10<<30, 0)
	task.Config.Args = []reflow.Arg{{Fileset: &in}}

	scheduler.Submit(task)
	req := <-cluster.Req()
	if got, want := req.Requirements, newRequirements(10, 10<<30, 1); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	alloc := newTestAlloc(reflow.Resources{"cpu": 25, "mem": 20 << 30})
	out := randomFileset(alloc.Repository())
	req.Reply <- testClusterAllocReply{Alloc: alloc, Err: nil}

	// By the time the task is running, it should have all of the dependent objects
	// in its repository.
	task.Wait(ctx, sched.TaskRunning)
	expectExists(t, alloc.Repository(), in)

	// Complete the task and check that all of its output is placed back into
	// the main repository.
	exec := alloc.exec(task.ID)
	exec.complete(reflow.Result{Fileset: out}, nil)
	task.Wait(ctx, sched.TaskDone)
	if task.Err != nil {
		t.Errorf("unexpected task error: %v", task.Err)
	}
	expectExists(t, repo, out)
}

func TestSchedulerAlloc(t *testing.T) {
	scheduler, cluster, _, shutdown := newTestScheduler()
	defer shutdown()
	ctx := context.Background()

	tasks := []*sched.Task{
		newTask(5, 10<<30, 1),
		newTask(10, 10<<30, 1),
		newTask(20, 10<<30, 0),
		newTask(20, 10<<30, 1),
	}
	scheduler.Submit(tasks...)
	req := <-cluster.Req()
	if got, want := req.Requirements, newRequirements(20, 10<<30, 4); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	// There shouldn't be another one:
	select {
	case <-cluster.Req():
		t.Error("too many requests")
	default:
	}
	for i, task := range tasks {
		if got, want := task.State(), sched.TaskInit; got != want {
			t.Errorf("task %d: got %v, want %v", i, got, want)
		}
	}
	// Partially satisfy the request: we can fit some tasks, but not all in this alloc.
	// task[2] since it has a higher priority than others and
	// task[0] since it is has the smallest resource requirements in the lower priority group.
	alloc := newTestAlloc(reflow.Resources{"cpu": 30, "mem": 30 << 30})
	req.Reply <- testClusterAllocReply{Alloc: alloc}

	tasks[0].Wait(ctx, sched.TaskRunning)
	tasks[2].Wait(ctx, sched.TaskRunning)
	if got, want := tasks[1].State(), sched.TaskInit; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := tasks[3].State(), sched.TaskInit; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// We should see another request now for the remaining.
	req = <-cluster.Req()
	if got, want := req.Requirements, newRequirements(20, 10<<30, 2); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// Don't satisfy this allocation but instead finish tasks[0] and tasks[2]. This
	// means the scheduler should be able to schedule tasks[1] and tasks[3].
	exec := alloc.exec(tasks[2].ID)
	exec.complete(reflow.Result{}, nil)
	tasks[1].Wait(ctx, sched.TaskRunning)

	// We should see another request now for the remaining.
	req = <-cluster.Req()
	if got, want := req.Requirements, newRequirements(20, 10<<30, 1); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	exec = alloc.exec(tasks[0].ID)
	exec.complete(reflow.Result{}, nil)
	tasks[3].Wait(ctx, sched.TaskRunning)

	// There shouldn't be another one:
	select {
	case <-cluster.Req():
		t.Error("too many requests")
	default:
	}
}

func TestTaskLost(t *testing.T) {
	scheduler, cluster, _, shutdown := newTestScheduler()
	defer shutdown()
	ctx := context.Background()

	tasks := []*sched.Task{
		newTask(1, 1, 0),
		newTask(1, 1, 0),
		newTask(1, 1, 0),
	}
	scheduler.Submit(tasks...)
	allocs := []*testAlloc{
		newTestAlloc(reflow.Resources{"cpu": 2, "mem": 2}),
		newTestAlloc(reflow.Resources{"cpu": 1, "mem": 1}),
	}
	req := <-cluster.Req()
	req.Reply <- testClusterAllocReply{Alloc: allocs[0]}

	// Wait for two of the tasks to be allocated.
	statusCtx, statusCancel := context.WithCancel(context.Background())
	var running, done sync.WaitGroup
	running.Add(2)
	done.Add(3)
	for i := range tasks {
		go func(i int) {
			if tasks[i].Wait(statusCtx, sched.TaskRunning) == nil {
				running.Done()
			}
			done.Done()
		}(i)
	}
	running.Wait()
	statusCancel()
	done.Wait()

	var singleTask *sched.Task
	for _, task := range tasks {
		if task.State() == sched.TaskInit {
			singleTask = task
			break
		}
	}
	if singleTask == nil {
		t.Fatal("inconsistent state")
	}

	req = <-cluster.Req()
	req.Reply <- testClusterAllocReply{Alloc: allocs[1]}
	singleTask.Wait(ctx, sched.TaskRunning)

	// Fail the alloc. By the tiem we get a new request, the task should
	// be back in init state.
	allocs[1].error(errors.E(errors.Fatal, "alloc failed"))

	req = <-cluster.Req()
	if got, want := singleTask.State(), sched.TaskInit; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// When we recover, the task is reassigned.
	req.Reply <- testClusterAllocReply{Alloc: newTestAlloc(reflow.Resources{"cpu": 1, "mem": 1})}
	singleTask.Wait(ctx, sched.TaskRunning)
}
