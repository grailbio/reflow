// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/test"
	"github.com/grailbio/reflow/test/flow"
)

type allocateResult struct {
	alloc pool.Alloc
	err   error
}

type allocateRequest struct {
	min, max reflow.Resources
}

type testCluster struct {
	mu       sync.Mutex
	requests map[allocateRequest]chan allocateResult
}

func (t *testCluster) Init() {
	t.requests = map[allocateRequest]chan allocateResult{}
}

func (testCluster) ID() string                                               { panic("not implemented") }
func (testCluster) Alloc(ctx context.Context, id string) (pool.Alloc, error) { panic("not implemented") }
func (testCluster) Allocs(ctx context.Context) ([]pool.Alloc, error)         { panic("not implemented") }
func (testCluster) Offer(ctx context.Context, id string) (pool.Offer, error) { panic("not implemented") }
func (testCluster) Offers(ctx context.Context) ([]pool.Offer, error)         { panic("not implemented") }

func (t *testCluster) allocate(min, max reflow.Resources) chan allocateResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	req := allocateRequest{min, max}
	if t.requests[req] == nil {
		t.requests[req] = make(chan allocateResult)
	}
	return t.requests[req]
}

func (t *testCluster) Allocate(ctx context.Context, min, max reflow.Resources, labels pool.Labels) (pool.Alloc, error) {
	select {
	case res := <-t.allocate(min, max):
		return res.alloc, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *testCluster) Grant(min, max reflow.Resources, result allocateResult) {
	t.allocate(min, max) <- result
}

type testAlloc struct {
	test.Executor
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
		transferer test.Transferer
		cluster    testCluster
		resources  = reflow.Resources{Memory: 5 << 30, CPU: 10, Disk: 10 << 30}
		r          = &Runner{
			Cluster:    &cluster,
			Transferer: &transferer,
			Flow:       flow.Exec("image", "blah", resources),
		}
	)
	r.Name = Name{"@local", reflow.Digester.FromString("test")}
	transferer.Init()
	cluster.Init()

	var (
		c  = make(chan *Runner)
		rc = make(chan bool)
	)
	ctx, cancel := context.WithCancel(context.Background())
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
	cluster.Grant(resources, resources, allocateResult{alloc: &alloc})
	if !<-rc {
		t.Fatal("early termination")
	}
	if got, want := r.Alloc, &alloc; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	c <- r
	alloc.Ok(r.Flow, test.Files("ok"))
	if <-rc {
		t.Fatal("late termination")
	}
	if r.Err != nil {
		t.Errorf("got %v, want nil", r.Err)
	}
	if got, want := r.Result, (reflow.Result{Fileset: test.Files("ok")}); got != want.String() {
		t.Errorf("got %v, want %v", got, want)
	}
}
