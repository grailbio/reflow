// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/taskdb"
	op "github.com/grailbio/reflow/test/flow"
	"github.com/grailbio/reflow/test/testutil"
)

type allocateResult struct {
	alloc pool.Alloc
	err   error
}

type testCluster struct {
	mu       sync.Mutex
	requests map[string]chan allocateResult
}

func (t *testCluster) Init() {
	t.requests = make(map[string]chan allocateResult)
}

func (testCluster) ID() string                                               { panic("not implemented") }
func (testCluster) Alloc(ctx context.Context, id string) (pool.Alloc, error) { panic("not implemented") }
func (testCluster) Allocs(ctx context.Context) ([]pool.Alloc, error)         { panic("not implemented") }
func (testCluster) Offer(ctx context.Context, id string) (pool.Offer, error) { panic("not implemented") }
func (testCluster) Offers(ctx context.Context) ([]pool.Offer, error)         { panic("not implemented") }

func (t *testCluster) allocate(req reflow.Requirements) chan allocateResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := fmt.Sprint(req)
	if t.requests[key] == nil {
		t.requests[key] = make(chan allocateResult)
	}
	return t.requests[key]
}

func (t *testCluster) Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (pool.Alloc, error) {
	select {
	case res := <-t.allocate(req):
		return res.alloc, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *testCluster) Shutdown() error { return nil }

func (t *testCluster) Grant(req reflow.Requirements, result allocateResult) {
	t.allocate(req) <- result
}

type testAlloc struct {
	testutil.Executor
	Freed bool
}

func (testAlloc) Pool() pool.Pool { panic("not implemented") }
func (testAlloc) ID() string      { return "testalloc" }
func (testAlloc) Keepalive(ctx context.Context, interval time.Duration) (time.Duration, error) {
	return interval, nil
}
func (testAlloc) Inspect(ctx context.Context) (pool.AllocInspect, error) {
	return pool.AllocInspect{}, nil
}
func (t *testAlloc) Free(ctx context.Context) error {
	t.Freed = true
	return nil
}

func TestRunner(t *testing.T) {
	var (
		transferer testutil.WaitTransferer
		cluster    testCluster
		resources  = reflow.Resources{"mem": 5 << 30, "cpu": 10, "disk": 10 << 30}

		r = &Runner{
			Cluster:    &cluster,
			Transferer: &transferer,
			Flow:       op.Exec("image", "blah", resources),
			EvalConfig: flow.EvalConfig{},
		}
	)
	testutil.AssignExecId(nil, r.Flow)
	r.ID = taskdb.RunID(reflow.Digester.FromString("test"))
	transferer.Init()
	cluster.Init()

	var (
		c  = make(chan *Runner)
		rc = make(chan bool)
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer close(c)
	go func() {
		for r := range c {
			rc <- r.Do(ctx)
		}
	}()

	c <- r
	var alloc testAlloc
	alloc.Have = resources
	alloc.Init()
	cluster.Grant(r.Flow.Requirements(), allocateResult{alloc: &alloc})
	if !<-rc {
		t.Fatal("early termination")
	}
	if got, want := r.Alloc, &alloc; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	c <- r
	alloc.Ok(ctx, r.Flow, testutil.Files("ok"))
	if <-rc {
		t.Fatal("late termination")
	}
	if r.Err != nil {
		t.Errorf("got %v, want nil", r.Err)
	}
	if got, want := r.Result, (reflow.Result{Fileset: testutil.Files("ok")}); got != want.String() {
		t.Errorf("got %v, want %v", got, want)
	}
}
