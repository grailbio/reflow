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
	"testing"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/sched/internal/utiltest"
	"github.com/grailbio/reflow/test/testutil"
)

const (
	allocationDelay = 100 * time.Millisecond
	completionDelay = 100 * time.Millisecond
)

func newTestScheduler(t *testing.T) (scheduler *sched.Scheduler, cluster *utiltest.TestCluster, repository *testutil.InmemoryRepository, shutdown func()) {
	t.Helper()
	repository = testutil.NewInmemoryRepository()
	cluster = utiltest.NewTestCluster()
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
	testSchedScale(t, tasks, 2, reflow.Resources{"cpu": 50, "mem": 200 << 30})
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
	testSchedScale(t, tasks, 5, reflow.Resources{"cpu": 200, "mem": 800 << 30})
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
	testSchedScale(t, tasks, 10, reflow.Resources{"cpu": 780, "mem": 10240 << 30})
}

// testSchedScale runs a scale test of the scheduler.
// The purpose of this test is to exercise the scheduler <-> cluster interaction at scale.
// The test is setup using an `allocator` (acts as a Cluster) and a `taskSubmitter` (acts as an Evaluator).
// The submitter submits the given set of tasks and upon each of their completion, submits its children.
// The allocator automatically satisfies the scheduler's requirement with an appropriate instance type.
func testSchedScale(t *testing.T, tasks []*taskNode, maxAllocs int, maxResources reflow.Resources) {
	scheduler, cluster, _, shutdown := newTestScheduler(t)
	defer shutdown()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		submitter = newTaskSubmitter(scheduler, tasks)
		allocator allocator
	)
	go submitter.start()
	go allocator.start(ctx, cluster)
	submitter.wg.Wait()
	gotN, gotR, types := allocator.describe()
	log.Printf("(%s) completed %d tasks (%s) using %d allocs (%s) of types: %s",
		t.Name(), submitter.count, submitter.res, gotN, gotR, types)
	if gotN > maxAllocs {
		t.Errorf("got %d allocs, want %d", gotN, maxAllocs)
	}
	if gotR.ScaledDistance(nil) > maxResources.ScaledDistance(nil) {
		t.Errorf("got %s resources, want %s", gotR, maxResources)
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
	tasks := make([]*sched.Task, len(nodes))
	var r reflow.Resources
	for i, node := range nodes {
		node := node
		// add a goroutine to await completion of the task and then submit its childrem.
		go func() {
			defer t.wg.Done()
			if err := node.task.Wait(context.TODO(), sched.TaskDone); err != nil {
				log.Printf("wait %v: %v", node.task.ID, err)
			}
			t.submit(node.children)
		}()
		tasks[i] = node.task
		r.Add(r, node.task.Config.Resources)
	}
	t.sched.Submit(tasks...)
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
}

func (a *allocator) describe() (int, reflow.Resources, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var (
		r reflow.Resources
		n int
	)
	var names []string
	for _, alloc := range a.allocs {
		if nExecs := alloc.NExecs(); nExecs > 0 {
			ar := alloc.Resources()
			n += 1
			r.Add(r, ar)
			names = append(names, fmt.Sprintf("%s%s", alloc.name, ar))
		}
	}
	return n, r, strings.Join(names, ", ")
}

func (a *allocator) start(ctx context.Context, cluster *utiltest.TestCluster) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-cluster.Req():
			instType, r := getInstanceType(req.Requirements)
			if r.Equal(nil) {
				panic(fmt.Sprintf("min req %s too big", req.Min))
			}
			time.AfterFunc(allocationDelay, func() {
				reply := utiltest.TestClusterAllocReply{}
				a.mu.Lock()
				alloc := &namedTestAlloc{TestAlloc: utiltest.NewTestAlloc(r)}
				alloc.name = instType
				go alloc.CompleteAll(ctx)
				a.allocs = append(a.allocs, alloc)
				reply.Alloc = alloc
				a.mu.Unlock()
				req.Reply <- reply
			})
		}
	}
}

// getInstanceType gets the best matching instance type (and its resources)
// given the req from the available types in `resourcesByType`.
// We try to find the smallest instance type which will fit `req.Max`.
// If none fit, then we reduce the width by one and keep trying.
// If the max doesn't fit, then we try a smaller width, and so on.
func getInstanceType(req reflow.Requirements) (string, reflow.Resources) {
	var (
		best  string
		bestR reflow.Resources
		need  = req.Max()
	)
	for {
		for name, r := range resourcesByType {
			if !r.Available(need) {
				continue
			}
			if bestR.Equal(nil) || r.ScaledDistance(nil) < bestR.ScaledDistance(nil) {
				best = name
				bestR = r
			}
		}
		if bestR.Equal(nil) {
			// Can't find one, so reduce requirement and try again
			need.Sub(need, req.Min)
		} else {
			break
		}
		// Can't be looking for lower than min, so quit
		if !need.Available(req.Min) {
			break
		}
	}
	return best, bestR
}

// resourcesByType consists of some selected EC2 instance types and the resources they offer.
// This was generated by running the following command and then pruning the list (manually):
// `reflow ec2instances | awk '{print "\""$1"\": {\"cpu\": "$3", \"mem\": "$2" << 30},"}'`
var resourcesByType = map[string]reflow.Resources{
	"c5.18xlarge":   {"cpu": 72, "mem": 144.00 << 30},
	"c5.24xlarge":   {"cpu": 96, "mem": 192.00 << 30},
	"c5.2xlarge":    {"cpu": 8, "mem": 16.00 << 30},
	"c5.4xlarge":    {"cpu": 16, "mem": 32.00 << 30},
	"c5.9xlarge":    {"cpu": 36, "mem": 72.00 << 30},
	"c5.large":      {"cpu": 2, "mem": 4.00 << 30},
	"i3.16xlarge":   {"cpu": 64, "mem": 488.00 << 30},
	"i3en.24xlarge": {"cpu": 96, "mem": 768.00 << 30},
	"m5.12xlarge":   {"cpu": 48, "mem": 192.00 << 30},
	"m5.16xlarge":   {"cpu": 64, "mem": 256.00 << 30},
	"m5.24xlarge":   {"cpu": 96, "mem": 384.00 << 30},
	"m5.2xlarge":    {"cpu": 8, "mem": 32.00 << 30},
	"m5.4xlarge":    {"cpu": 16, "mem": 64.00 << 30},
	"m5.8xlarge":    {"cpu": 32, "mem": 128.00 << 30},
	"m5.large":      {"cpu": 2, "mem": 8.00 << 30},
	"m5.xlarge":     {"cpu": 4, "mem": 16.00 << 30},
	"r3.2xlarge":    {"cpu": 8, "mem": 61.00 << 30},
	"r3.4xlarge":    {"cpu": 16, "mem": 122.00 << 30},
	"r3.8xlarge":    {"cpu": 32, "mem": 244.00 << 30},
	"r4.16xlarge":   {"cpu": 64, "mem": 488.00 << 30},
	"r4.2xlarge":    {"cpu": 8, "mem": 61.00 << 30},
	"r4.4xlarge":    {"cpu": 16, "mem": 122.00 << 30},
	"r4.8xlarge":    {"cpu": 32, "mem": 244.00 << 30},
	"r5.12xlarge":   {"cpu": 48, "mem": 384.00 << 30},
	"r5.16xlarge":   {"cpu": 64, "mem": 512.00 << 30},
	"r5.24xlarge":   {"cpu": 96, "mem": 768.00 << 30},
	"r5.2xlarge":    {"cpu": 8, "mem": 64.00 << 30},
	"r5.4xlarge":    {"cpu": 16, "mem": 128.00 << 30},
	"r5.8xlarge":    {"cpu": 32, "mem": 256.00 << 30},
	"r5.large":      {"cpu": 2, "mem": 16.00 << 30},
	"r5.xlarge":     {"cpu": 4, "mem": 32.00 << 30},
	"r5d.12xlarge":  {"cpu": 48, "mem": 384.00 << 30},
	"r5d.16xlarge":  {"cpu": 64, "mem": 512.00 << 30},
	"r5d.24xlarge":  {"cpu": 96, "mem": 768.00 << 30},
	"x1.16xlarge":   {"cpu": 64, "mem": 976.00 << 30},
	"x1.32xlarge":   {"cpu": 128, "mem": 1952.00 << 30},
	"x1e.32xlarge":  {"cpu": 128, "mem": 3904.00 << 30},
}
