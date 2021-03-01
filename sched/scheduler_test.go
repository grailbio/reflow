// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sched_test

import (
	"context"
	"fmt"
	golog "log"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/testblob"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/sched/internal/utiltest"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/test/testutil"
)

func newTestScheduler(t *testing.T) (scheduler *sched.Scheduler, cluster *utiltest.TestCluster, repository *testutil.InmemoryRepository, shutdown func()) {
	t.Helper()
	repository = testutil.NewInmemoryRepository()
	scheduler, cluster, shutdown = newTestSchedulerWithRepo(t, repository)
	return
}

func newTestSchedulerWithRepo(t *testing.T, repo reflow.Repository) (scheduler *sched.Scheduler, cluster *utiltest.TestCluster, shutdown func()) {
	t.Helper()
	cluster = utiltest.NewTestCluster()
	scheduler = sched.New()
	scheduler.Transferer = testutil.Transferer
	scheduler.Repository = repo
	scheduler.Cluster = cluster
	scheduler.PostUseChecksum = true
	scheduler.MinAlloc = reflow.Resources{}
	out := golog.New(os.Stderr, "scheduler: ", golog.LstdFlags)
	scheduler.Log = log.New(out, log.DebugLevel)
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
	in := utiltest.RandomFileset(repo)
	expectExists(t, repo, in)

	task := utiltest.NewTask(10, 10<<30, 0)
	task.Config.Args = []reflow.Arg{{Fileset: &in}}

	scheduler.Submit(task)
	req := <-cluster.Req()
	if got, want := req.Requirements, utiltest.NewRequirements(10, 10<<30, 0); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	alloc := utiltest.NewTestAlloc(reflow.Resources{"cpu": 25, "mem": 20 << 30})
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

	out := utiltest.RandomFileset(alloc.Repository())
	// Increment the refcount for the result files in the alloc repository, so that we can unload
	// them later.
	for _, f := range out.Files() {
		alloc.RefCountInc(f.ID)
	}
	req.Reply <- utiltest.TestClusterAllocReply{Alloc: alloc, Err: nil}

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
	exec := alloc.Exec(digest.Digest(task.ID))
	exec.Complete(reflow.Result{Fileset: out}, nil)
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
		utiltest.NewTask(5, 10<<30, 1),
		utiltest.NewTask(10, 10<<30, 1),
		utiltest.NewTask(20, 10<<30, 0),
		utiltest.NewTask(20, 10<<30, 1),
	}
	scheduler.Submit(tasks...)
	req := <-cluster.Req()
	if got, want := req.Requirements, utiltest.NewRequirements(20, 10<<30, 3); !got.Equal(want) {
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
	alloc := utiltest.NewTestAlloc(reflow.Resources{"cpu": 30, "mem": 30 << 30})
	req.Reply <- utiltest.TestClusterAllocReply{Alloc: alloc}

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
	if got, want := req.Requirements, utiltest.NewRequirements(20, 10<<30, 1); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// Don't satisfy this allocation but instead finish tasks[0] and tasks[2]. This
	// means the scheduler should be able to schedule tasks[1] and tasks[3].
	exec := alloc.Exec(digest.Digest(tasks[2].ID))
	exec.Complete(reflow.Result{}, nil)
	if err := tasks[1].Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}

	exec = alloc.Exec(digest.Digest(tasks[0].ID))
	exec.Complete(reflow.Result{}, nil)
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

func TestSchedulerTaskTooBig(t *testing.T) {
	scheduler, _, _, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx := context.Background()
	task := utiltest.NewTask(10, 512<<30, 0)

	scheduler.Submit(task)
	// By the time the task is running, it should have all of the dependent objects
	// in its repository.
	if err := task.Wait(ctx, sched.TaskDone); err != nil {
		t.Fatal(err)
	}
	if task.Err == nil {
		t.Error("must get error for too big task")
	}
}

func TestTaskLost(t *testing.T) {
	scheduler, cluster, _, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx := context.Background()

	tasks := []*sched.Task{
		utiltest.NewTask(1, 1, 0),
		utiltest.NewTask(1, 1, 0),
		utiltest.NewTask(1, 1, 0),
	}
	scheduler.Submit(tasks...)
	allocs := []*utiltest.TestAlloc{
		utiltest.NewTestAlloc(reflow.Resources{"cpu": 2, "mem": 2}),
		utiltest.NewTestAlloc(reflow.Resources{"cpu": 1, "mem": 1}),
	}
	req := <-cluster.Req()
	req.Reply <- utiltest.TestClusterAllocReply{Alloc: allocs[0]}

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
	req.Reply <- utiltest.TestClusterAllocReply{Alloc: allocs[1]}
	if err := singleTask.Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}

	// Fail the alloc. By the time we get a new request, the task should
	// be back in init state.
	allocs[1].Error(errors.E(errors.Fatal, "alloc failed"))

	req = <-cluster.Req()
	if got, want := singleTask.State(), sched.TaskInit; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	for _, task := range tasks {
		want := 0
		if task.ID == singleTask.ID {
			want = 1
		}
		if got := task.Attempt(); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	// When we recover, the task is reassigned.
	req.Reply <- utiltest.TestClusterAllocReply{Alloc: utiltest.NewTestAlloc(reflow.Resources{"cpu": 1, "mem": 1})}
	if err := singleTask.Wait(ctx, sched.TaskRunning); err != nil {
		t.Fatal(err)
	}
}

func TestTaskNetError(t *testing.T) {
	scheduler, cluster, _, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx := context.Background()

	tasks := []*sched.Task{
		utiltest.NewTask(1, 1, 0),
		utiltest.NewTask(1, 1, 0),
		utiltest.NewTask(3, 3, 0),
	}
	scheduler.Submit(tasks...)
	allocs := []*utiltest.TestAlloc{
		utiltest.NewTestAlloc(reflow.Resources{"cpu": 2, "mem": 2}),
		utiltest.NewTestAlloc(reflow.Resources{"cpu": 5, "mem": 5}),
	}
	req := <-cluster.Req()
	req.Reply <- utiltest.TestClusterAllocReply{Alloc: allocs[0]}

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
	req.Reply <- utiltest.TestClusterAllocReply{Alloc: allocs[1]}
	_ = tasks[2].Wait(ctx, sched.TaskRunning)

	// Fail one of the tasks in the first alloc with a Network Error.
	exec := allocs[0].Exec(digest.Digest(tasks[0].ID))
	exec.Complete(reflow.Result{}, errors.E(errors.Net, "test network error"))
	// Wait for it to be rescheduled on the second alloc.
	_ = tasks[0].Wait(ctx, sched.TaskRunning)
	// Confirm its on the second alloc (will hang if not).
	allocs[1].Exec(digest.Digest(tasks[0].ID))
	// Confirm the other task is still on the first alloc.
	_ = tasks[1].Wait(ctx, sched.TaskRunning)
	allocs[0].Exec(digest.Digest(tasks[1].ID))
}

func TestTaskErrors(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantTaskState sched.TaskState
		wantTaskError errors.Kind
	}{
		{"non-retryable error", errors.E(errors.Fatal, "some fatal error"), sched.TaskDone, errors.Fatal},
		{"transient error", errors.E(errors.Unavailable, "some transient error"), sched.TaskLost, errors.Unavailable},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			scheduler, cluster, repo, shutdown := newTestScheduler(t)
			defer shutdown()
			ctx := context.Background()
			task := utiltest.NewTask(1, 1, 0)

			// create a random fileset which will be loaded onto the alloc
			// later we will check that all the filesets are properly unloaded, even when tasks fail
			in := utiltest.RandomFileset(repo)
			expectExists(t, repo, in)
			task.Config.Args = []reflow.Arg{{Fileset: &in}}

			scheduler.Submit(task)
			alloc := utiltest.NewTestAlloc(reflow.Resources{"cpu": 2, "mem": 2})
			req := <-cluster.Req()
			req.Reply <- utiltest.TestClusterAllocReply{Alloc: alloc}

			// Wait for the task (which will fit in the first alloc) to be allocated.
			if err := task.Wait(ctx, sched.TaskRunning); err != nil {
				t.Fatal(err)
			}

			// check that the input fileset got loaded
			wantRefCounts := int64(len(in.Map))
			var gotRefCounts int64
			for _, v := range alloc.RefCount() {
				gotRefCounts += v
			}
			if gotRefCounts != wantRefCounts {
				t.Errorf("got loaded ref count: %v, want: %v", gotRefCounts, wantRefCounts)
			}

			// Complete the exec with the test case provided error
			exec := alloc.Exec(digest.Digest(task.ID))
			exec.Complete(reflow.Result{}, tt.err)

			if e := task.Wait(ctx, tt.wantTaskState); e != nil {
				t.Fatal(e)
			}
			if got, want := task.State(), tt.wantTaskState; got != want {
				t.Errorf("got state: %v, want: %v", got, want)
			}
			if got, want := errors.Recover(task.Err).Kind, tt.wantTaskError; got != want {
				t.Errorf("got error: %v, want: %v", got, want)
			}

			// check that the input fileset got unloaded
			gotRefCounts = 0
			for _, v := range alloc.RefCount() {
				gotRefCounts += v
			}
			if gotRefCounts != 0 {
				t.Errorf("got unloaded ref count: %v, want: 0", gotRefCounts)
			}
		})
	}
}

// TestLostTasksSwitchAllocs tests scenarios where lost tasks are re-allocated.
// Only some type of task errors are considered 'lost' (and retries are attempted),
// whereas any error from alloc keepalives will result in tasks being considered as lost.
func TestLostTasksSwitchAllocs(t *testing.T) {
	old := pool.KeepaliveRetryInitialWaitInterval
	pool.KeepaliveRetryInitialWaitInterval = 50 * time.Millisecond
	defer func() {
		pool.KeepaliveRetryInitialWaitInterval = old
	}()
	allocFatal := errors.E("fatal alloc failure", errors.Fatal)
	allocCancel := errors.E("alloc cancelled", errors.Canceled)
	taskNet := errors.E("network error", errors.Net)
	taskCancel := errors.E("task cancelled", errors.Canceled)
	tests := []struct {
		allocErr, taskErr error
	}{
		{allocFatal, nil},
		{allocCancel, nil},
		{errors.New("some error"), nil},
		{nil, taskNet},
		{nil, taskCancel},
	}
	for _, tt := range tests {
		scheduler, cluster, _, shutdown := newTestScheduler(t)
		defer shutdown()
		tasks := []*sched.Task{
			utiltest.NewTask(1, 1, 0),
		}
		scheduler.Submit(tasks...)
		allocs := []*utiltest.TestAlloc{
			utiltest.NewTestAlloc(reflow.Resources{"cpu": 2, "mem": 2}),
			utiltest.NewTestAlloc(reflow.Resources{"cpu": 2, "mem": 2}),
		}
		req := <-cluster.Req()
		req.Reply <- utiltest.TestClusterAllocReply{Alloc: allocs[0]}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		// Wait for the task to stage, so we know it started processing.
		if err := tasks[0].Wait(ctx, sched.TaskStaging); err != nil {
			t.Fatal(err)
		}
		cancel()
		// Let the alloc's keepalive fail with an error in a bit.
		if tt.allocErr != nil {
			allocs[0].Error(tt.allocErr)
		}
		// Fail the task
		exec := allocs[0].Exec(digest.Digest(tasks[0].ID))
		if tt.taskErr != nil {
			exec.Complete(reflow.Result{}, tt.taskErr)
		}
		// The task should be considered lost and then re-initialized resulting
		// in another cluster allocation request.
		req = <-cluster.Req()
		if got, want := tasks[0].State(), sched.TaskInit; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		req.Reply <- utiltest.TestClusterAllocReply{Alloc: allocs[1]}
		allocs[1].Exec(digest.Digest(tasks[0].ID)).Complete(reflow.Result{}, nil)
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		if err := tasks[0].Wait(ctx, sched.TaskDone); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
}

func TestSchedulerDirectTransfer(t *testing.T) {
	repo := testutil.NewInmemoryLocatorRepository()
	scheduler, _, shutdown := newTestSchedulerWithRepo(t, repo)
	blb := testblob.New("test")
	scheduler.Mux = blob.Mux{"test": blb}
	defer shutdown()
	ctx := context.Background()
	in := utiltest.RandomFileset(repo)
	expectExists(t, repo, in)
	for _, f := range in.Files() {
		loc := fmt.Sprintf("test://bucketin/objects/%s", f.ID)
		repo.SetLocation(f.ID, loc)
		rc, _ := repo.Get(ctx, f.ID)
		_ = scheduler.Mux.Put(ctx, loc, f.Size, rc, "")
	}
	// Set one of the files to be unresolved (by setting its ID to zero)
	// add a source instead (which points to the same object and uses the same Mux implementation)
	var fn string
	for k := range in.Map {
		fn = k
		break
	}
	file := in.Map[fn]
	file.Source = fmt.Sprintf("test://bucketin/objects/%s", file.ID)
	file.ID = digest.Digest{}
	in.Map[fn] = file

	task := utiltest.NewTask(1, 10<<20, 0)
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

func TestSchedulerDirectTransfer_noLocator(t *testing.T) {
	scheduler, _, repo, shutdown := newTestScheduler(t)
	defer shutdown()
	in := utiltest.RandomFileset(repo)
	expectExists(t, repo, in)
	assertNonDirectTransfer(t, scheduler, &in)
}

func TestSchedulerDirectTransfer_unresolvedFile(t *testing.T) {
	repo := testutil.NewInmemoryLocatorRepository()
	scheduler, _, shutdown := newTestSchedulerWithRepo(t, repo)
	blb1, blb2 := testblob.New("test"), testblob.New("test2")
	scheduler.Mux = blob.Mux{"test": blb1, "test2": blb2}
	defer shutdown()
	in := utiltest.RandomFileset(repo)
	expectExists(t, repo, in)
	ctx := context.Background()
	for _, f := range in.Files() {
		loc := fmt.Sprintf("test://bucketin/objects/%s", f.ID)
		repo.SetLocation(f.ID, loc)
		rc, _ := repo.Get(ctx, f.ID)
		_ = scheduler.Mux.Put(ctx, loc, f.Size, rc, "")
	}
	// Set one of the files to be unresolved (by setting its ID to zero)
	// add a source instead (which points to the same object and uses the same Mux implementation)
	var fn string
	for k := range in.Map {
		fn = k
		break
	}
	file := in.Map[fn]
	file.Source = "test2://unsupportedscheme"
	file.ID = digest.Digest{}
	in.Map[fn] = file
	assertNonDirectTransfer(t, scheduler, &in)
}

func assertNonDirectTransfer(t *testing.T, scheduler *sched.Scheduler, in *reflow.Fileset) {
	ctx := context.Background()

	task := utiltest.NewTask(1, 10<<20, 0)
	task.Config.Args = []reflow.Arg{{Fileset: in}}
	task.Config.Type = "extern"
	task.Config.URL = "test://bucketout/"

	scheduler.Submit(task)
	// Scheduler's repository doesn't implement blobLocator,
	// so the direct transfer fails with unsupported error.
	_ = task.Wait(ctx, sched.TaskLost)
	if !task.NonDirectTransfer() {
		t.Fatal("task must be marked as non-direct")
	}
}

func TestSchedulerLoadUnloadExtern(t *testing.T) {
	scheduler, cluster, repo, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx := context.Background()
	in := utiltest.RandomFileset(repo)
	expectExists(t, repo, in)

	task := utiltest.NewTask(10, 10<<30, 0)
	task.Config.Args = []reflow.Arg{{Fileset: &in}}
	task.Config.Type = "extern"

	scheduler.Submit(task)
	// Wait for the direct transfer to fail
	_ = task.Wait(ctx, sched.TaskLost)
	if !errors.Is(errors.NotSupported, task.Err) {
		t.Fatal("task must fail with unsupported")
	}

	req := <-cluster.Req()
	if got, want := req.Requirements, utiltest.NewRequirements(10, 10<<30, 0); !got.Equal(want) {
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
	alloc := utiltest.NewTestAlloc(reflow.Resources{"cpu": 25, "mem": 20 << 30})
	req.Reply <- utiltest.TestClusterAllocReply{Alloc: alloc, Err: nil}

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
	exec := alloc.Exec(digest.Digest(task.ID))
	out := utiltest.RandomFileset(alloc.Repository())
	exec.Complete(reflow.Result{Fileset: out}, nil)
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

func TestSchedulerLoadFailRetryTask(t *testing.T) {
	scheduler, cluster, repo, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx := context.Background()
	in := utiltest.RandomFileset(repo)
	expectExists(t, repo, in)

	remote := testutil.NewInmemoryRepository()
	remotes := utiltest.RandomRepoFileset(remote)
	refs := reflow.Fileset{Map: make(map[string]reflow.File)}
	for k := range remotes.Map {
		v := reflow.File{Source: remotes.Map[k].Source}
		refs.Map[k] = v
	}

	task := utiltest.NewTask(10, 10<<30, 0)
	task.Config.Args = []reflow.Arg{{Fileset: &in}, {Fileset: &refs}}

	scheduler.Submit(task)
	req := <-cluster.Req()
	if got, want := req.Requirements, utiltest.NewRequirements(10, 10<<30, 0); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	// Start a task. Fail the alloc, so that the task gets assigned to a new alloc.
	{
		alloc := utiltest.NewTestAlloc(reflow.Resources{"cpu": 25, "mem": 20 << 30})
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
		req.Reply <- utiltest.TestClusterAllocReply{Alloc: alloc, Err: nil}

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
		alloc.Error(errors.E(errors.Fatal, "alloc failed"))
		if err := task.Wait(ctx, sched.TaskInit); err != nil {
			t.Fatal(err)
		}
		if task.Err != nil {
			t.Errorf("unexpected task error: %v", task.Err)
		}
	}
	// In the new alloc, the fileset which was loaded in the previous alloc (and hence resolved), should be still
	// unresolved in this round of loading, since this is a completely new alloc.
	{
		req := <-cluster.Req()
		alloc2 := utiltest.NewTestAlloc(reflow.Resources{"cpu": 25, "mem": 20 << 30})
		req.Reply <- utiltest.TestClusterAllocReply{Alloc: alloc2, Err: nil}

		if err := task.Wait(ctx, sched.TaskRunning); err != nil {
			t.Fatal(err)
		}
		expectExists(t, alloc2.Repository(), remotes)
		expectExists(t, alloc2.Repository(), in)

		exec := alloc2.Exec(digest.Digest(task.ID))
		out := utiltest.RandomFileset(alloc2.Repository())
		// Increment the refcount for the result files in the alloc repository, so that we can unload
		// them later.
		for _, f := range out.Files() {
			alloc2.RefCountInc(f.ID)
		}
		exec.Complete(reflow.Result{Fileset: out}, nil)
		if err := task.Wait(ctx, sched.TaskDone); err != nil {
			t.Fatal(err)
		}
		if task.Err != nil {
			t.Errorf("unexpected task error: %v", task.Err)
		}
		stats := scheduler.Stats.GetStats()
		if got, want := len(stats.Tasks), 1; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := stats.Tasks[task.ID.ID()].State, 4; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := len(stats.Allocs), 2; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		want := sched.OverallStats{TotalAllocs: 2, TotalTasks: 1}
		if got := stats.OverallStats; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		expectExists(t, repo, out)
	}
}

func TestSchedulerLoadUnloadFiles(t *testing.T) {
	scheduler, cluster, repo, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx := context.Background()
	in := utiltest.RandomFileset(repo)
	expectExists(t, repo, in)

	remote := testutil.NewInmemoryRepository()
	remotes := utiltest.RandomRepoFileset(remote)
	refs := reflow.Fileset{Map: make(map[string]reflow.File)}
	for k := range remotes.Map {
		v := reflow.File{Source: remotes.Map[k].Source}
		refs.Map[k] = v
	}

	task := utiltest.NewTask(10, 10<<30, 0)
	task.Config.Args = []reflow.Arg{{Fileset: &in}, {Fileset: &refs}}

	scheduler.Submit(task)
	req := <-cluster.Req()
	if got, want := req.Requirements, utiltest.NewRequirements(10, 10<<30, 0); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	alloc := utiltest.NewTestAlloc(reflow.Resources{"cpu": 25, "mem": 20 << 30})
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
	req.Reply <- utiltest.TestClusterAllocReply{Alloc: alloc, Err: nil}

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
	exec := alloc.Exec(digest.Digest(task.ID))
	out := utiltest.RandomFileset(alloc.Repository())
	// Increment the refcount for the result files in the alloc repository, so that we can unload
	// them later.
	for _, f := range out.Files() {
		alloc.RefCountInc(f.ID)
	}
	exec.Complete(reflow.Result{Fileset: out}, nil)
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

func newTasks(numTasks int) []*sched.Task {
	tasks := make([]*sched.Task, numTasks)
	for i := 0; i < numTasks; i++ {
		tasks[i] = &sched.Task{
			ID: taskdb.NewTaskID(),
			Config: reflow.ExecConfig{
				Type: "exec",
			},
		}
	}
	return tasks
}

func TestTaskSet(t *testing.T) {
	var (
		tasks = newTasks(20)
		set   = sched.NewTaskSet(tasks...)
	)
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID.ID() < tasks[j].ID.ID()
	})
	if got, want := set.Len(), len(tasks); got != want {
		t.Errorf("set len: got %d tasks, want %d", got, want)
	}

	taskSlice := set.Slice()
	sort.Slice(taskSlice, func(i, j int) bool {
		return taskSlice[i].ID.ID() < taskSlice[j].ID.ID()
	})
	if got, want := len(tasks), len(taskSlice); got != want {
		t.Errorf("set slice: got %d tasks, want %d", got, want)
	}
	for i := range taskSlice {
		if got, want := taskSlice[i].ID.ID(), tasks[i].ID.ID(); got != want {
			t.Errorf("set slice index %d: got %s, want %s", i, got, want)
		}
	}

	taskID := tasks[0].ID
	set.RemoveAll(tasks[0])
	if got, want := set.Len(), len(tasks)-1; got != want {
		t.Errorf("set delete: got %d tasks, want %d", got, want)
	}
	if got, want := tasks[0].ID.ID(), taskID.ID(); got != want {
		t.Errorf("set delete altered task: got %s, want %s", got, want)
	}
}

func TestRequirements(t *testing.T) {
	for _, tc := range []struct {
		tasks []*sched.Task
		req   reflow.Requirements
	}{
		{
			[]*sched.Task{
				utiltest.NewTask(1, 1, 0),
				utiltest.NewTask(1, 1, 0),
				utiltest.NewTask(3, 5, 0),
				utiltest.NewTask(5, 8, 0),
			},
			reflow.Requirements{Min: reflow.Resources{"cpu": 5, "mem": 8}, Width: 1},
		},
		{
			[]*sched.Task{
				utiltest.NewTask(1, 4, 0),
				utiltest.NewTask(1, 4, 0),
				utiltest.NewTask(1, 4, 0),
				utiltest.NewTask(8, 32, 0),
				utiltest.NewTask(1, 4, 0),
				utiltest.NewTask(1, 4, 0),
				utiltest.NewTask(1, 4, 0),
				utiltest.NewTask(1, 4, 0),
				utiltest.NewTask(1, 4, 0),
			},
			reflow.Requirements{Min: reflow.Resources{"cpu": 8, "mem": 32}, Width: 1},
		},
		{
			[]*sched.Task{
				utiltest.NewTask(1, 4, 0),
				utiltest.NewTask(2, 8, 0),
				utiltest.NewTask(3, 10, 0),
				utiltest.NewTask(8, 32, 0),
				utiltest.NewTask(4, 10, 0),
				utiltest.NewTask(2, 12, 0),
				utiltest.NewTask(1, 5, 0),
				utiltest.NewTask(1, 5, 0),
				utiltest.NewTask(2, 10, 0),
			},
			reflow.Requirements{Min: reflow.Resources{"cpu": 8, "mem": 32}, Width: 3},
		},
	} {
		if got, want := sched.Requirements(tc.tasks), tc.req; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
