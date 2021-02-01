// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package scale_test

import (
	"context"
	"fmt"
	golog "log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/sched/internal/utiltest"
	"github.com/grailbio/reflow/test/testutil"
)

const (
	allocationDelay = 50 * time.Millisecond
	completionDelay = 200 * time.Millisecond
)

func newTestScheduler(t *testing.T) (scheduler *sched.Scheduler, cluster *allocator, repository *testutil.InmemoryRepository, shutdown func()) {
	t.Helper()
	repository = testutil.NewInmemoryRepository()
	cluster = new(allocator)
	scheduler = sched.New()
	scheduler.Transferer = testutil.Transferer
	scheduler.Repository = repository
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

func TestSchedScaleSmall(t *testing.T) {
	tasks := append([]*taskNode{
		{
			sizedTask(medium, completionDelay),
			[]*taskNode{
				{sizedTask(large, 6*completionDelay), nodes(small, 6, 6*completionDelay)},
			},
		},
		{
			sizedTask(medium, 2*completionDelay),
			[]*taskNode{
				{sizedTask(small, 3*completionDelay), nodes(tiny, 10, 2*completionDelay)},
			},
		},
	}, nodes(tiny, 10, 2*completionDelay)...)
	testSchedScale(t, tasks, 1, reflow.Resources{"cpu": 32, "mem": 240 << 30})
}

func TestSchedScaleMedium(t *testing.T) {
	tasks1 := append([]*taskNode{
		{sizedTask(large, 4*completionDelay), nodes(enormous, 1, 8*completionDelay)},
		{sizedTask(small, 2*completionDelay), nodes(enormous, 1, 8*completionDelay)},
	}, nodes(small, 16, 4*completionDelay)...)
	tasks := append([]*taskNode{
		{
			task:     sizedTask(large, 10*completionDelay),
			children: append(nodes(small, 16, 4*completionDelay), nodes(large, 2, 2*completionDelay)...),
		},
		{task: sizedTask(large, 6*completionDelay), children: tasks1},
		{task: sizedTask(medium, 2*completionDelay), children: nodes(medium, 4, 6*completionDelay)},
	}, nodes(small, 10, 2*completionDelay)...)
	testSchedScale(t, tasks, 1, reflow.Resources{"cpu": 64, "mem": 500 << 30})
}

func TestSchedScaleLarge(t *testing.T) {
	tasks1 := append([]*taskNode{
		{sizedTask(enormous, 4*completionDelay), nodes(medium, 10, 2*completionDelay)},
		{sizedTask(large, completionDelay), nodes(large, 5, 4*completionDelay)},
	}, nodes(small, 10, 2*completionDelay)...)
	tasks := append([]*taskNode{
		{
			task:     sizedTask(tiny, 10*completionDelay),
			children: append(nodes(small, 5, completionDelay), nodes(large, 10, 2*completionDelay)...),
		},
		{
			task:     sizedTask(medium, 2*completionDelay),
			children: append(nodes(large, 10, 10*completionDelay), nodes(tiny, 100, 2*completionDelay)...),
		},
		{task: sizedTask(large, 6*completionDelay), children: tasks1},
		{task: sizedTask(enormous, 2*completionDelay), children: nodes(medium, 20, 3*completionDelay)},
	}, nodes(large, 5, 4*completionDelay)...)
	testSchedScale(t, tasks, 2, reflow.Resources{"cpu": 100, "mem": 400 << 30})
}

// testSchedScale runs a scale test of the scheduler.
// The purpose of this test is to exercise the scheduler <-> cluster interaction at scale.
// The test is setup using an `allocator` (acts as a Cluster) and a `taskSubmitter` (acts as an Evaluator).
// The submitter submits the given set of tasks and upon each of their completion, submits its children.
// The allocator automatically satisfies the scheduler's requirement with an appropriate instance type.
func testSchedScale(t *testing.T, tasks []*taskNode, maxAllocs int, maxResources reflow.Resources) {
	scheduler, allocator, _, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var submitter = newTaskSubmitter(scheduler, tasks)
	go submitter.start()
	go allocator.start(ctx)
	submitter.wg.Wait()
	gotN, gotNused, gotR, gotRused, types := allocator.describe()
	log.Printf("(%s) completed %d tasks (%s) using %d allocs (%s) of types: %s",
		t.Name(), submitter.count, submitter.res, gotNused, gotRused, types)
	if gotNused > maxAllocs {
		t.Errorf("got %d allocs, want %d", gotNused, maxAllocs)
	}
	if gotRused.ScaledDistance(nil) > maxResources.ScaledDistance(nil) {
		t.Errorf("got %s resources, want %s", gotRused, maxResources)
	}
	if gotN > gotNused+1 {
		t.Errorf("too many unused allocs (used %d/%d)", gotNused, gotN)
	}
	if gotR.ScaledDistance(nil) > maxResources.ScaledDistance(nil) {
		t.Errorf("too much resources allocated %s, want %s", gotR, maxResources)
	}
}

type taskNode struct {
	task     *sched.Task
	children []*taskNode
}

func nodes(size taskSize, n int, dur time.Duration) []*taskNode {
	nodes := make([]*taskNode, n)
	for i := 0; i < n; i++ {
		nodes[i] = &taskNode{task: sizedTask(size, dur), children: nil}
	}
	return nodes
}

func (t *taskNode) count() int {
	count := 1
	for _, t := range t.children {
		count += t.count()
	}
	return count
}

func (t *taskNode) resources() reflow.Resources {
	var r reflow.Resources
	r.Add(r, t.task.Config.Resources)
	for _, t := range t.children {
		r.Add(r, t.resources())
	}
	return r
}

type taskSize int

const (
	tiny taskSize = iota
	small
	medium
	large
	enormous
)

func sizedTask(size taskSize, dur time.Duration) *sched.Task {
	var task *sched.Task
	switch size {
	case small:
		task = utiltest.NewTask(2, 8<<30, 0)
	case medium:
		task = utiltest.NewTask(8, 32<<30, 0)
	case large:
		task = utiltest.NewTask(16, 64<<30, 0)
	case enormous:
		task = utiltest.NewTask(32, 128<<30, 0)
	default:
		task = utiltest.NewTask(1, 2<<30, 0)
	}
	task.Config.Ident = fmt.Sprintf("%s", dur)
	return task
}

// taskSubmitter submits a batch of tasks (ie, task nodes) to the given scheduler.
// Upon the completion of each task node, its children are then submitted, and so on.
type taskSubmitter struct {
	tasks []*taskNode
	sched *sched.Scheduler
	count int
	res   reflow.Resources
	wg    sync.WaitGroup
}

func newTaskSubmitter(sched *sched.Scheduler, tasks []*taskNode) *taskSubmitter {
	var (
		count int
		r     reflow.Resources
	)
	for _, t := range tasks {
		count += t.count()
		r.Add(r, t.resources())
	}
	ts := &taskSubmitter{tasks: tasks, sched: sched, count: count, res: r}
	ts.wg.Add(count)
	return ts
}

func (t *taskSubmitter) submit(nodes []*taskNode) {
	var r reflow.Resources
	for _, node := range nodes {
		node := node
		// add a goroutine to await completion of the task and then submit its childrem.
		go func() {
			defer t.wg.Done()
			if err := node.task.Wait(context.TODO(), sched.TaskDone); err != nil {
				log.Printf("wait %v: %v", node.task.ID, err)
			}
			t.submit(node.children)
		}()
		t.sched.Submit(node.task)
		r.Add(r, node.task.Config.Resources)
	}
}

func (t *taskSubmitter) start() {
	t.submit(t.tasks)
}

type namedTestAlloc struct {
	*utiltest.TestAlloc
	name string
}

// allocator fulfills a testCluster's allocation requests.
type allocator struct {
	mu     sync.Mutex
	allocs []*namedTestAlloc
	m      *ec2cluster.Manager
	nextId int32
}

func (a *allocator) describe() (int, int, reflow.Resources, reflow.Resources, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var (
		r, rused reflow.Resources
		n, nused int
	)
	var names []string
	for _, alloc := range a.allocs {
		ar := alloc.Resources()
		r.Add(r, ar)
		n += 1
		if nExecs := alloc.NExecs(); nExecs > 0 {
			nused += 1
			rused.Add(rused, ar)
			names = append(names, fmt.Sprintf("%s%s", alloc.name, ar))
		}
	}
	return n, nused, r, rused, strings.Join(names, ", ")
}

func (a *allocator) start(ctx context.Context) {
	a.m = ec2cluster.NewManager(a, 20, 5, log.Std)
	a.m.SetTimeouts(10*time.Millisecond, 10*time.Millisecond, 5*time.Second)
	a.m.Start()
	<-ctx.Done()
	a.m.Shutdown()
}

func (a *allocator) CanAllocate(r reflow.Resources) (bool, error) {
	return true, nil
}

func (a *allocator) Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (pool.Alloc, error) {
	// TODO(swami):  Make this use pool.Allocate() to simulate the same (internal) alloc picking logic.
	// log.Printf("allocating for req %s", req)
	var found *namedTestAlloc
	for {
		a.mu.Lock()
		for _, alloc := range a.allocs {
			// log.Printf("existing alloc %s available %s, checking for req %s", alloc.ID(), alloc.Available(), req)
			if !alloc.Available().Available(req.Min) {
				continue
			}
			found = alloc
			// log.Printf("found alloc %s with available %s for req %s", alloc.ID(), alloc.Available(), req)
			found.Reserve(req.Min)
			break
		}
		a.mu.Unlock()
		if found != nil {
			break
		}
		needch := a.m.Allocate(ctx, req)
		select {
		case <-needch:
		case <-ctx.Done():
			return nil, fmt.Errorf("failed to allocate")
		}
	}
	go found.CompleteAll(ctx)
	return found, nil
}

// Launch launches an instance with the given specification.
func (a *allocator) Launch(ctx context.Context, spec ec2cluster.InstanceSpec) ec2cluster.ManagedInstance {
	id := fmt.Sprintf("%s-%d", spec.Type, atomic.AddInt32(&a.nextId, 1))
	alloc := &namedTestAlloc{TestAlloc: utiltest.NewTestAlloc(spec.Resources)}
	alloc.name = id
	time.AfterFunc(allocationDelay, func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.allocs = append(a.allocs, alloc)
	})
	// log.Printf("launched %s with resources %s", id, spec.Resources)
	return spec.Instance(id)
}

// Refresh refreshes the managed cluster.
func (a *allocator) Refresh(ctx context.Context) (map[string]bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	m := make(map[string]bool, len(a.allocs))
	for _, alloc := range a.allocs {
		m[alloc.name] = true
	}
	return m, nil
}

// Available returns any available instance specification that can satisfy the need.
// The returned InstanceSpec should be subsequently be 'Launch'able.
func (a *allocator) Available(need reflow.Resources) (ec2cluster.InstanceSpec, bool) {
	typ, r := ec2cluster.InstanceType(need, true)
	return ec2cluster.InstanceSpec{typ, r}, true
}

func (a *allocator) Notify(waiting, pending reflow.Resources) {
	// do nothing
}
