// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sched_test

import (
	"bytes"
	"context"
	"crypto"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	golog "log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/sync/ctxsync"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/test/testutil"
)

var logTasks = flag.Bool("logtasks", false, "log task output to stderr")

type counter uint64

func (c *counter) Next() uint64 {
	return atomic.AddUint64((*uint64)(c), 1)
}

func (c *counter) NextID() digest.Digest {
	n := crypto.Hash(reflow.Digester).Size()
	b := make([]byte, n)
	binary.LittleEndian.PutUint64(b, c.Next())
	return reflow.Digester.New(b)
}

var ntask, nalloc counter

func newTask(cpu, mem float64) *sched.Task {
	task := sched.NewTask()
	task.ID = ntask.NextID()
	task.Config.Resources = reflow.Resources{"cpu": cpu, "mem": mem}
	if *logTasks {
		out := golog.New(os.Stderr, fmt.Sprintf("task %s (%s): ", task.ID.Short(), task.Config.Resources), golog.LstdFlags)
		task.Log = log.New(out, log.DebugLevel)
	}
	return task
}

func newRequirements(cpu, mem float64, width int) reflow.Requirements {
	return reflow.Requirements{
		Min:   reflow.Resources{"cpu": cpu, "mem": mem},
		Width: width,
	}
}

func randomFileset(repo reflow.Repository) reflow.Fileset {
	n := rand.Intn(100) + 1
	var fs reflow.Fileset
	fs.Map = make(map[string]reflow.File, n)
	for i := 0; i < n; i++ {
		p := make([]byte, rand.Intn(1024)+1)
		if _, err := rand.Read(p); err != nil {
			panic(err)
		}
		d, err := repo.Put(context.TODO(), bytes.NewReader(p))
		if err != nil {
			panic(err)
		}
		path := fmt.Sprintf("file%d", i)
		fs.Map[path] = reflow.File{ID: d, Size: int64(len(p))}
	}
	return fs
}

type testClusterAllocReply struct {
	Alloc pool.Alloc
	Err   error
}

type testClusterAllocReq struct {
	reflow.Requirements
	Labels pool.Labels
	Reply  chan<- testClusterAllocReply
}

type testCluster struct {
	reqs chan testClusterAllocReq
}

func newTestCluster() *testCluster {
	return &testCluster{reqs: make(chan testClusterAllocReq)}
}

func (c *testCluster) Req() <-chan testClusterAllocReq {
	return c.reqs
}

func (c *testCluster) Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (pool.Alloc, error) {
	replyc := make(chan testClusterAllocReply)
	c.reqs <- testClusterAllocReq{
		Requirements: req,
		Labels:       labels,
		Reply:        replyc,
	}
	select {
	case reply := <-replyc:
		return reply.Alloc, reply.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type testExecState int

type testExec struct {
	reflow.Exec
	Config reflow.ExecConfig

	id digest.Digest

	mu   sync.Mutex
	cond *ctxsync.Cond

	done   bool
	result reflow.Result
	err    error
}

func newTestExec(id digest.Digest, config reflow.ExecConfig) *testExec {
	exec := &testExec{id: id, Config: config}
	exec.cond = ctxsync.NewCond(&exec.mu)
	return exec
}

func (e *testExec) ID() digest.Digest {
	return e.id
}

func (e *testExec) Result(ctx context.Context) (reflow.Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.done {
		panic("Result called but not done")
	}
	return e.result, e.err
}

func (e *testExec) Inspect(ctx context.Context) (reflow.ExecInspect, error) {
	_, err := e.Result(ctx)
	return reflow.ExecInspect{}, err
}

func (e *testExec) Promote(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

func (e *testExec) Wait(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for !e.done {
		if err := e.cond.Wait(ctx); err != nil {
			return err
		}
	}
	return e.err
}

func (e *testExec) complete(res reflow.Result, err error) {
	e.mu.Lock()
	e.done = true
	e.result = res
	e.err = err
	e.cond.Broadcast()
	e.mu.Unlock()
}

type testAlloc struct {
	pool.Alloc
	id         uint64
	repository *testutil.InmemoryRepository
	resources  reflow.Resources

	mu    sync.Mutex
	cond  *sync.Cond
	execs map[digest.Digest]*testExec
	err   error
	hung  bool
}

func newTestAlloc(resources reflow.Resources) *testAlloc {
	alloc := &testAlloc{
		repository: testutil.NewInmemoryRepository(),
		execs:      make(map[digest.Digest]*testExec),
		resources:  resources,
		id:         nalloc.Next(),
	}
	alloc.cond = sync.NewCond(&alloc.mu)
	return alloc
}

func (a *testAlloc) ID() string {
	return fmt.Sprintf("test%d", a.id)
}

func (a *testAlloc) Resources() reflow.Resources {
	return a.resources
}

func (a *testAlloc) Repository() reflow.Repository {
	return a.repository
}

func (a *testAlloc) Load(ctx context.Context, fs reflow.Fileset) (reflow.Fileset, error) {
	for _, file := range fs.Files() {
		if file.IsRef() {
			return reflow.Fileset{}, errors.New("unexpected file reference")
		}
	}
	return fs, nil
}

func (a *testAlloc) Put(ctx context.Context, id digest.Digest, config reflow.ExecConfig) (reflow.Exec, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.execs[id]; !ok {
		a.execs[id] = newTestExec(id, config)
		a.cond.Broadcast()
	}
	return a.execs[id], nil
}

func (a *testAlloc) Keepalive(ctx context.Context, interval time.Duration) (time.Duration, error) {
	a.mu.Lock()
	hung, err := a.hung, a.err
	a.mu.Unlock()
	if hung {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	if err == nil {
		err = ctx.Err()
	}
	return 50 * time.Millisecond, err
}

func (a *testAlloc) exec(id digest.Digest) *testExec {
	a.mu.Lock()
	defer a.mu.Unlock()
	for a.execs[id] == nil {
		a.cond.Wait()
	}
	return a.execs[id]
}

func (a *testAlloc) error(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.err = err
}

func (a *testAlloc) hang() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hung = true
}
