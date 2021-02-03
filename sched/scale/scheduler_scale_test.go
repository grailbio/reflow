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
	allocTimeout    = 10 * time.Second
	allocationDelay = 50 * time.Millisecond
	completionDelay = 200 * time.Millisecond
)

func newTestScheduler(t *testing.T) (scheduler *sched.Scheduler, cluster *allocator, shutdown func()) {
	t.Helper()
	cluster = new(allocator)
	scheduler = sched.New()
	scheduler.Transferer = testutil.Transferer
	scheduler.Repository = testutil.NewInmemoryRepository()
	scheduler.Cluster = cluster
	scheduler.PostUseChecksum = true
	scheduler.MinAlloc = reflow.Resources{}
	scheduler.MaxAllocIdleTime = 30 * time.Second
	logger := log.New(golog.New(os.Stderr, "", golog.LstdFlags), log.DebugLevel)
	scheduler.Log = logger.Tee(nil, "scheduler: ")
	cluster.logger = logger
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

func TestSchedScaleSimple(t *testing.T) {
	testSchedScale(t, 0, []*taskNode{{sizedTask(medium, completionDelay), nil}}, 1, 1, reflow.Resources{"cpu": 8, "mem": 32 << 30}, 0.0)
}

func smallTasks() []*taskNode {
	return append([]*taskNode{
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
}

func TestSchedScaleSmall(t *testing.T) {
	testSchedScale(t, 0, smallTasks(), 2, 5, reflow.Resources{"cpu": 48, "mem": 190 << 30}, 0.1)
}

func TestSchedScaleSmallWithDrain(t *testing.T) {
	testSchedScale(t, 50*time.Millisecond, smallTasks(), 2, 3, reflow.Resources{"cpu": 48, "mem": 190 << 30}, 0.1)
}

func mediumTasks() []*taskNode {
	tasks1 := append([]*taskNode{
		{sizedTask(large, 4*completionDelay), nodes(enormous, 1, 8*completionDelay)},
		{sizedTask(small, 2*completionDelay), nodes(enormous, 1, 8*completionDelay)},
	}, nodes(small, 16, 4*completionDelay)...)
	return append([]*taskNode{
		{
			task:     sizedTask(large, 10*completionDelay),
			children: append(nodes(small, 16, 4*completionDelay), nodes(large, 2, 2*completionDelay)...),
		},
		{task: sizedTask(large, 6*completionDelay), children: tasks1},
		{task: sizedTask(medium, 2*completionDelay), children: nodes(medium, 4, 6*completionDelay)},
	}, nodes(small, 10, 2*completionDelay)...)
}

func TestSchedScaleMedium(t *testing.T) {
	testSchedScale(t, 0, mediumTasks(), 8, 18, reflow.Resources{"cpu": 250, "mem": 1100 << 30}, 0.2)
}

func TestSchedScaleMediumWithDrain(t *testing.T) {
	testSchedScale(t, 50*time.Millisecond, mediumTasks(), 8, 9, reflow.Resources{"cpu": 250, "mem": 1100 << 30}, 0.2)
}

func largeTasks() []*taskNode {
	tasks1 := append([]*taskNode{
		{sizedTask(enormous, 4*completionDelay), nodes(medium, 10, 2*completionDelay)},
		{sizedTask(large, completionDelay), nodes(large, 5, 4*completionDelay)},
	}, nodes(small, 10, 2*completionDelay)...)
	return append([]*taskNode{
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
}

func TestSchedScaleLarge(t *testing.T) {
	// TODO(swami): Fix various issues causing over-allocation and wastage.
	testSchedScale(t, 0, largeTasks(), 12, 25, reflow.Resources{"cpu": 1000, "mem": 10000 << 30}, 0.3)
}

func TestSchedScaleLargeWithDrain(t *testing.T) {
	// TODO(swami): Fix various issues causing over-allocation and wastage.
	testSchedScale(t, 50*time.Millisecond, largeTasks(), 12, 13, reflow.Resources{"cpu": 1000, "mem": 10000 << 30}, 0.3)

}

// testSchedScale runs a scale test of the scheduler.
// The purpose of this test is to exercise the scheduler <-> cluster interaction at scale.
// The test is setup using an `allocator` (acts as a Cluster) and a `taskSubmitter` (acts as an Evaluator).
// The submitter submits the given set of tasks and upon each of their completion, submits its children.
// The allocator automatically satisfies the scheduler's requirement with an appropriate instance type.
// wastageThresholdPct is the amount of resource wastage tolerated by the test assertions.
func testSchedScale(t *testing.T, drainTimeout time.Duration, tasks []*taskNode, maxPools, maxAllocs int, maxResources reflow.Resources, wastageThresholdPct float32) {
	scheduler, allocator, shutdown := newTestScheduler(t)
	scheduler.DrainTimeout = drainTimeout
	defer shutdown()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var submitter = newTaskSubmitter(scheduler, tasks)
	go submitter.start()
	go allocator.start(ctx)
	submitter.wg.Wait()
	pds := allocator.inspect()
	var (
		nAllocs, nUsedAllocs, unusedPools int
		rPools, rAllocs, rUsedAllocs      reflow.Resources
	)
	var pdstrs []string
	for _, pd := range pds {
		rPools.Add(rPools, pd.r)
		n, r := pd.total()
		nAllocs += n
		rAllocs.Add(rAllocs, r)
		n, r = pd.used()
		if n == 0 {
			unusedPools += 1
		}
		nUsedAllocs += n
		rUsedAllocs.Add(rUsedAllocs, r)
		pdstrs = append(pdstrs, fmt.Sprintf("%s", pd))
	}
	log.Printf("(%s) completed %d tasks (%s) using pools N=%d Resources%s, allocs (%d, %s)\n%s",
		t.Name(), submitter.count, submitter.res, len(pds), rPools, nAllocs, rAllocs, strings.Join(pdstrs, "\n"))
	if got, max := len(pds), maxPools; got > max {
		t.Errorf("got %d pools, want max %d", got, max)
	}
	if got, max := unusedPools, int(float32(maxPools)*wastageThresholdPct); got > max {
		t.Errorf("got %d unused pools, want max %d", got, max)
	}
	if got, max := nAllocs, maxAllocs; got > max {
		t.Errorf("got %d allocs, want max %d", got, max)
	}
	if got, max := rUsedAllocs, maxResources; got.ScaledDistance(nil) > max.ScaledDistance(nil) {
		t.Errorf("got %s resources, want max %s", got, max)
	}
	if total, used, limit := nAllocs, nUsedAllocs, int((1+wastageThresholdPct)*float32(nUsedAllocs)); total > limit {
		t.Errorf("too many unused allocs %d (allocated: %d, used %d, limit %d)", (total - used), total, used, limit)
	}
	var rLimit reflow.Resources
	rLimit.Scale(rUsedAllocs, float64(1+wastageThresholdPct))
	if total, used := rAllocs, rUsedAllocs; total.ScaledDistance(nil) > rLimit.ScaledDistance(nil) {
		// TODO(swami): Turn this into an error once wastage is reduced
		t.Logf("too much unused resources (allocated: %s, used %s, limit %s)", total, used, rLimit)
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
		task = utiltest.NewTask(2, 6<<30, 0)
	case medium:
		task = utiltest.NewTask(8, 24<<30, 0)
	case large:
		task = utiltest.NewTask(16, 54<<30, 0)
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

// allocator fulfills a testCluster's allocation requests.
type allocator struct {
	pool.Mux
	m       *ec2cluster.Manager
	logger  *log.Logger
	started time.Time
	nextId  int32
}

func (a *allocator) inspect() []poolDetails {
	pds := make([]poolDetails, len(a.Pools()))
	for i, p := range a.Pools() {
		tp := p.(*utiltest.TestPool)
		pds[i] = poolDetails{id: tp.ID(), r: tp.Resources(), startRef: a.started}
		allocs, err := p.Allocs(context.Background())
		if err != nil {
			panic(err)
		}
		pds[i].allocs = make([]allocDetails, len(allocs))
		for j, alloc := range allocs {
			inspect, err := alloc.Inspect(context.Background())
			if err != nil {
				panic(err)
			}
			ad := allocDetails{
				id:      alloc.ID(),
				inspect: inspect,
				nExecs:  alloc.(*utiltest.TestAlloc).NExecs(),
			}
			pds[i].allocs[j] = ad
		}
	}
	return pds
}

func (a *allocator) start(ctx context.Context) {
	a.started = time.Now()
	a.m = ec2cluster.NewManager(a, 50, 5, a.logger.Tee(nil, "manager: "))
	a.m.SetTimeouts(10*time.Millisecond, 10*time.Millisecond, 5*time.Second)
	a.m.Start()
	<-ctx.Done()
	a.m.Shutdown()
}

func (a *allocator) CanAllocate(r reflow.Resources) (bool, error) {
	return true, nil
}

func (a *allocator) Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (alloc pool.Alloc, err error) {
	//log.Printf("allocating for req %s", req)
	pctx := ctx
	defer func() {
		if alloc != nil {
			go alloc.(*utiltest.TestAlloc).CompleteAll(pctx)
		}
	}()
	// TODO(swami):  Dedup the following code (copied from Cluster.Allocate() in ec2cluster.go)
	if a.Size() > 0 {
		//log.Printf("attempting to allocate from existing pool")
		actx, acancel := context.WithTimeout(ctx, allocTimeout)
		alloc, err = pool.Allocate(actx, a, req, labels)
		acancel()
		if err == nil {
			return
		}
		log.Printf("failed to allocate from existing pool: %v; provisioning", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ticker := time.NewTicker(allocTimeout)
	defer ticker.Stop()
	needch := a.m.Allocate(ctx, req)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-needch:
			actx, acancel := context.WithTimeout(ctx, allocTimeout)
			alloc, err = pool.Allocate(actx, a, req, labels)
			acancel()
			if err == nil {
				return alloc, nil
			}
			// We didn't get it--try again!
			needch = a.m.Allocate(ctx, req)
		case <-ticker.C:
			actx, acancel := context.WithTimeout(ctx, allocTimeout)
			log.Printf("calling (timer-based) pool.Allocate(%s)", req)
			alloc, err = pool.Allocate(actx, a, req, labels)
			acancel()
			if err == nil {
				return alloc, nil
			}
		}
	}
}

// Launch launches an instance with the given specification.
func (a *allocator) Launch(ctx context.Context, spec ec2cluster.InstanceSpec) ec2cluster.ManagedInstance {
	id := fmt.Sprintf("%s-%d", spec.Type, atomic.AddInt32(&a.nextId, 1))
	var r reflow.Resources
	r.Set(spec.Resources)
	r["disk"] = 2e12
	p := utiltest.NewTestPool(id, r)
	time.AfterFunc(allocationDelay, func() {
		pools := a.Pools()
		pools = append(pools, p)
		a.SetPools(pools)
		log.Printf("launched %s with resources %s", id, spec.Resources)
	})
	return spec.Instance(id)
}

// Refresh refreshes the managed cluster.
func (a *allocator) Refresh(ctx context.Context) (map[string]string, error) {
	pools := a.Pools()
	m := make(map[string]string, len(pools))
	for _, p := range pools {
		m[p.(*utiltest.TestPool).Name()] = "type-unknown"
	}
	return m, nil
}

// Available returns any available instance specification that can satisfy the need.
// The returned InstanceSpec should be subsequently be 'Launch'able.
func (a *allocator) Available(need reflow.Resources, maxPrice float64) (ec2cluster.InstanceSpec, bool) {
	typ, r := ec2cluster.InstanceType(need, true, maxPrice)
	if typ == "" || r.Equal(nil) || maxPrice < testInstancePrice {
		return ec2cluster.InstanceSpec{}, false
	}
	return ec2cluster.InstanceSpec{typ, r}, true
}

func (a *allocator) Notify(waiting, pending reflow.Resources) {
	// do nothing
}

const (
	testInstancePrice = 1.0
)

// for the purpose of the scale test, all instances cost $1 and the max hourly cost is $200.
func (a *allocator) InstancePriceUSD(typ string) float64 {
	return testInstancePrice
}

func (a *allocator) CheapestInstancePriceUSD() float64 {
	return testInstancePrice
}

type allocDetails struct {
	id      string
	inspect pool.AllocInspect
	nExecs  int
}

type poolDetails struct {
	id       string
	r        reflow.Resources
	startRef time.Time
	allocs   []allocDetails
}

func (pd poolDetails) total() (int, reflow.Resources) {
	return pd.sum(func(a allocDetails) bool { return true })
}

func (pd poolDetails) used() (int, reflow.Resources) {
	return pd.sum(func(a allocDetails) bool { return a.nExecs > 0 })
}

func (pd poolDetails) sum(accept func(a allocDetails) bool) (int, reflow.Resources) {
	var (
		n int
		r reflow.Resources
	)
	for _, alloc := range pd.allocs {
		if accept(alloc) {
			n++
			r.Add(r, alloc.inspect.Resources)
		}
	}
	return n, r
}

func (pd poolDetails) String() string {
	var b strings.Builder
	n, r := pd.total()
	nused, rused := pd.used()
	b.WriteString(fmt.Sprintf("pool[%s]: N=%d, Resources%s (used: %d, Resources%s)", pd.id, n, r, nused, rused))
	for _, alloc := range pd.allocs {
		id, r, n := alloc.id, alloc.inspect.Resources, alloc.nExecs
		var prefix string
		if n == 0 {
			prefix = "UNUSED "
		}
		t := alloc.inspect.Created.Sub(pd.startRef).Round(time.Millisecond)
		d := alloc.inspect.Expires.Sub(alloc.inspect.Created).Round(time.Millisecond)
		b.WriteString(fmt.Sprintf("\n\t%salloc[%s]: Resources%s, nExecs: %d (started: %s later, duration: %s)", prefix, id, r, n, t, d))
	}
	return b.String()
}
