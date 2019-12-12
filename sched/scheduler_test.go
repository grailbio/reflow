// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sched_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/testblob"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/test/testutil"
)

func newTestScheduler(t *testing.T) (scheduler *sched.Scheduler, cluster *testCluster, repository *testutil.InmemoryRepository, shutdown func()) {
	t.Helper()
	repository = testutil.NewInmemoryRepository()
	scheduler, cluster, shutdown = newTestSchedulerWithRepo(t, repository)
	return
}

func newTestSchedulerWithRepo(t *testing.T, repo reflow.Repository) (scheduler *sched.Scheduler, cluster *testCluster, shutdown func()) {
	t.Helper()
	cluster = newTestCluster()
	scheduler = sched.New()
	scheduler.Transferer = testutil.Transferer
	scheduler.Repository = repo
	scheduler.Cluster = cluster
	scheduler.MinAlloc = reflow.Resources{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_ = scheduler.Do(ctx)
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
	scheduler, cluster, repo, shutdown := newTestScheduler(t)
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
	// TODO(pgopal): There is no way to wait for the tasks to be added to the scheduler queue.
	// Hence we cannot check task stats here.
	stats := scheduler.Stats.GetStats()
	if got, want := len(stats.Allocs), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// pending allocs will not have an entry in stats.Allocs.
	if got, want := stats.OverallStats.TotalTasks, int64(1); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	out := randomFileset(alloc.Repository())
	// Increment the refcount for the result files in the alloc repository, so that we can unload
	// them later.
	for _, f := range out.Files() {
		alloc.refCount[f.ID]++
	}
	req.Reply <- testClusterAllocReply{Alloc: alloc, Err: nil}

	// By the time the task is running, it should have all of the dependent objects
	// in its repository.
	if err := task.Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}
	stats = scheduler.Stats.GetStats()
	if got, want := len(stats.Tasks), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := stats.Tasks[task.ID.ID()].State, 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(stats.Allocs), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	want := sched.OverallStats{TotalTasks: 1, TotalAllocs: 1}
	if got := stats.OverallStats; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	expectExists(t, alloc.Repository(), in)

	// Complete the task and check that all of its output is placed back into
	// the main repository.
	exec := alloc.exec(digest.Digest(task.ID))
	exec.complete(reflow.Result{Fileset: out}, nil)
	if err := task.Wait(ctx, sched.TaskDone); err != nil {
		t.Fatal(err)
	}
	if task.Err != nil {
		t.Errorf("unexpected task error: %v", task.Err)
	}
	stats = scheduler.Stats.GetStats()
	if got, want := len(stats.Tasks), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := stats.Tasks[task.ID.ID()].State, 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(stats.Allocs), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	want = sched.OverallStats{TotalAllocs: 1, TotalTasks: 1}
	if got := stats.OverallStats; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	expectExists(t, repo, out)
}

func TestSchedulerAlloc(t *testing.T) {
	scheduler, cluster, _, shutdown := newTestScheduler(t)
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

	if err := tasks[0].Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}
	if err := tasks[2].Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}
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
	exec := alloc.exec(digest.Digest(tasks[2].ID))
	exec.complete(reflow.Result{}, nil)
	if err := tasks[1].Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}

	exec = alloc.exec(digest.Digest(tasks[0].ID))
	exec.complete(reflow.Result{}, nil)
	if err := tasks[3].Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}

	// There shouldn't be another one:
	select {
	case <-cluster.Req():
		t.Error("too many requests")
	default:
	}
}

func TestTaskLost(t *testing.T) {
	scheduler, cluster, _, shutdown := newTestScheduler(t)
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
	if err := singleTask.Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}

	// Fail the alloc. By the time we get a new request, the task should
	// be back in init state.
	allocs[1].error(errors.E(errors.Fatal, "alloc failed"))

	req = <-cluster.Req()
	if got, want := singleTask.State(), sched.TaskInit; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// When we recover, the task is reassigned.
	req.Reply <- testClusterAllocReply{Alloc: newTestAlloc(reflow.Resources{"cpu": 1, "mem": 1})}
	if err := singleTask.Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}
}

func TestTaskNetError(t *testing.T) {
	scheduler, cluster, _, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx := context.Background()

	tasks := []*sched.Task{
		newTask(1, 1, 0),
		newTask(1, 1, 0),
		newTask(3, 3, 0),
	}
	scheduler.Submit(tasks...)
	allocs := []*testAlloc{
		newTestAlloc(reflow.Resources{"cpu": 2, "mem": 2}),
		newTestAlloc(reflow.Resources{"cpu": 5, "mem": 5}),
	}
	req := <-cluster.Req()
	req.Reply <- testClusterAllocReply{Alloc: allocs[0]}

	var err error
	// Wait for two of the tasks (which will fit in the first alloc) to be allocated.
	if err = tasks[0].Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}
	if err = tasks[1].Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}

	if tasks[2].State() != sched.TaskInit {
		t.Fatal("inconsistent state")
	}

	// Return the second (bigger) alloc and wait for the third task to be allocated.
	req = <-cluster.Req()
	req.Reply <- testClusterAllocReply{Alloc: allocs[1]}
	_ = tasks[2].Wait(ctx, sched.TaskRunning)

	// Fail one of the tasks in the first alloc with a Network Error.
	exec := allocs[0].exec(digest.Digest(tasks[0].ID))
	exec.complete(reflow.Result{}, errors.E(errors.Net, "test network error"))
	// Wait for it to be rescheduled on the second alloc.
	_ = tasks[0].Wait(ctx, sched.TaskRunning)
	// Confirm its on the second alloc (will hang if not).
	allocs[1].exec(digest.Digest(tasks[0].ID))
	// Confirm the other task is still on the first alloc.
	_ = tasks[1].Wait(ctx, sched.TaskRunning)
	allocs[0].exec(digest.Digest(tasks[1].ID))
}

func TestSchedulerDirectTransfer(t *testing.T) {
	repo := testutil.NewInmemoryLocatorRepository()
	scheduler, _, shutdown := newTestSchedulerWithRepo(t, repo)
	blb := testblob.New("test")
	scheduler.Mux = blob.Mux{"test": blb}
	defer shutdown()
	ctx := context.Background()
	in := randomFileset(repo)
	expectExists(t, repo, in)
	for _, f := range in.Files() {
		loc := fmt.Sprintf("test://bucketin/objects/%s", f.ID)
		repo.SetLocation(f.ID, loc)
		rc, _ := repo.Get(ctx, f.ID)
		_ = scheduler.Mux.Put(ctx, loc, f.Size, rc, "")
	}
	task := newTask(1, 10<<20, 0)
	task.Config.Args = []reflow.Arg{{Fileset: &in}}
	task.Config.Type = "extern"
	task.Config.URL = "test://bucketout/"

	scheduler.Submit(task)
	_ = task.Wait(ctx, sched.TaskDone)

	infs, outfs := in.Pullup(), task.Result.Fileset.Pullup()
	if got, want := infs.Size(), outfs.Size(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for k, inf := range infs.Map {
		if got, want := outfs.Map[k].Assertions, inf.Assertions; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestSchedulerDirectTransferUnsupported(t *testing.T) {
	scheduler, cluster, repo, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx := context.Background()
	in := randomFileset(repo)
	expectExists(t, repo, in)
	task := newTask(1, 10<<20, 0)
	task.Config.Args = []reflow.Arg{{Fileset: &in}}
	task.Config.Type = "extern"
	task.Config.URL = "test://bucketout/"

	scheduler.Submit(task)
	// Scheduler's repository doesn't implement blobLocator,
	// so the direct transfer fails with unsupported error.
	_ = task.Wait(ctx, sched.TaskLost)
	if !errors.Is(errors.NotSupported, task.Err) {
		t.Fatal("task must fail with unsupported")
	}

	allocs := []*testAlloc{newTestAlloc(reflow.Resources{"cpu": 2, "mem": 10 << 30})}
	req := <-cluster.Req()
	req.Reply <- testClusterAllocReply{Alloc: allocs[0]}
	for {
		// Extern is weird in that the state machine can go from the end state to the initial state
		// when we fail on a direct transfer and try an indirect transfer. Hence it is not sufficient if
		// task.Wait(TaskRunning) returns successfully (which it would even after the direct
		// transfer failed). We need to ensure that it is actually running the indirect transfer here.
		_ = task.Wait(ctx, sched.TaskRunning)
		if task.State() == sched.TaskRunning {
			break
		}
	}
	select {
	case <-cluster.Req():
		t.Errorf("Cluster should have no requests")
	default:
	}
}
func TestSchedulerLoadUnloadExtern(t *testing.T) {
	scheduler, cluster, repo, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx := context.Background()
	in := randomFileset(repo)
	expectExists(t, repo, in)

	task := newTask(10, 10<<30, 0)
	task.Config.Args = []reflow.Arg{{Fileset: &in}}
	task.Config.Type = "extern"

	scheduler.Submit(task)
	// Wait for the direct transfer to fail
	_ = task.Wait(ctx, sched.TaskLost)
	if !errors.Is(errors.NotSupported, task.Err) {
		t.Fatal("task must fail with unsupported")
	}

	req := <-cluster.Req()
	if got, want := req.Requirements, newRequirements(10, 10<<30, 1); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	// TODO(pgopal): There is no way to wait for the tasks to be added to the scheduler queue.
	// Hence we cannot check task stats here.
	stats := scheduler.Stats.GetStats()
	if got, want := len(stats.Allocs), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// pending allocs will not have an entry in stats.Allocs.
	if got, want := stats.OverallStats.TotalTasks, int64(2); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	alloc := newTestAlloc(reflow.Resources{"cpu": 25, "mem": 20 << 30})
	req.Reply <- testClusterAllocReply{Alloc: alloc, Err: nil}

	// By the time the task is running, it should have all of the dependent objects
	// in its repository.
	for {
		// Extern is weird in that the state machine can go from the end state to the initial state
		// when we fail on a direct transfer and try an indirect transfer. Hence it is not sufficient if
		// task.Wait(TaskRunning) returns successfully (which it would even after the direct
		// transfer failed). We need to ensure that it is actually running the indirect transfer here.
		_ = task.Wait(ctx, sched.TaskRunning)
		if task.State() == sched.TaskRunning {
			break
		}
	}
	if err := task.Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}
	stats = scheduler.Stats.GetStats()
	if got, want := len(stats.Tasks), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := stats.Tasks[task.ID.ID()].State, 2; got != want {
		for k, v := range stats.Tasks {
			t.Errorf("task %v: %v", k, v)
		}
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(stats.Allocs), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	want := sched.OverallStats{TotalAllocs: 1, TotalTasks: 2}
	if got := stats.OverallStats; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	expectExists(t, alloc.Repository(), in)

	// Complete the task and check that all of its output is placed back into
	// the main repository.
	exec := alloc.exec(digest.Digest(task.ID))
	out := randomFileset(alloc.Repository())
	exec.complete(reflow.Result{Fileset: out}, nil)
	if err := task.Wait(ctx, sched.TaskDone); err != nil {
		t.Fatal(err)
	}
	if task.Err != nil {
		t.Errorf("unexpected task error: %v", task.Err)
	}
	stats = scheduler.Stats.GetStats()
	if got, want := len(stats.Tasks), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := stats.Tasks[task.ID.ID()].State, 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(stats.Allocs), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	want = sched.OverallStats{TotalAllocs: 1, TotalTasks: 2}
	if got := stats.OverallStats; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	expectExists(t, repo, out)
}

func TestSchedulerLoadUnloadFiles(t *testing.T) {
	scheduler, cluster, repo, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx := context.Background()
	in := randomFileset(repo)
	expectExists(t, repo, in)

	remote := testutil.NewInmemoryRepository()
	remotes := randomRepoFileset(remote)
	refs := reflow.Fileset{Map: make(map[string]reflow.File)}
	for k := range remotes.Map {
		v := reflow.File{Source: remotes.Map[k].Source}
		refs.Map[k] = v
	}

	task := newTask(10, 10<<30, 0)
	task.Config.Args = []reflow.Arg{{Fileset: &in}, {Fileset: &refs}}

	scheduler.Submit(task)
	req := <-cluster.Req()
	if got, want := req.Requirements, newRequirements(10, 10<<30, 1); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	alloc := newTestAlloc(reflow.Resources{"cpu": 25, "mem": 20 << 30})
	// TODO(pgopal): There is no way to wait for the tasks to be added to the scheduler queue.
	// Hence we cannot check task stats here.
	stats := scheduler.Stats.GetStats()
	if got, want := len(stats.Allocs), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// pending allocs will not have an entry in stats.Allocs.
	if got, want := stats.OverallStats.TotalTasks, int64(1); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	req.Reply <- testClusterAllocReply{Alloc: alloc, Err: nil}

	// By the time the task is running, it should have all of the dependent objects
	// in its repository.
	if err := task.Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}
	expectExists(t, alloc.Repository(), remotes)

	stats = scheduler.Stats.GetStats()
	if got, want := len(stats.Tasks), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := stats.Tasks[task.ID.ID()].State, 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(stats.Allocs), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	want := sched.OverallStats{TotalTasks: 1, TotalAllocs: 1}
	if got := stats.OverallStats; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	expectExists(t, alloc.Repository(), in)

	// Complete the task and check that all of its output is placed back into
	// the main repository.
	exec := alloc.exec(digest.Digest(task.ID))
	out := randomFileset(alloc.Repository())
	// Increment the refcount for the result files in the alloc repository, so that we can unload
	// them later.
	for _, f := range out.Files() {
		alloc.refCount[f.ID]++
	}
	exec.complete(reflow.Result{Fileset: out}, nil)
	if err := task.Wait(ctx, sched.TaskDone); err != nil {
		t.Fatal(err)
	}
	if task.Err != nil {
		t.Errorf("unexpected task error: %v", task.Err)
	}
	stats = scheduler.Stats.GetStats()
	if got, want := len(stats.Tasks), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := stats.Tasks[task.ID.ID()].State, 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(stats.Allocs), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	want = sched.OverallStats{TotalAllocs: 1, TotalTasks: 1}
	if got := stats.OverallStats; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	expectExists(t, repo, out)
}
