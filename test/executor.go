// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package test

import (
	"context"
	"io"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
)

// ExecResult stores the result of a completed exec.
type ExecResult struct {
	Result  reflow.Result
	Inspect reflow.ExecInspect
}

// Exec is the Exec type used by testExecutor. They permit
// the caller to rendezvous on results.
type Exec struct {
	id      digest.Digest
	resultc chan ExecResult
	err     chan error
	config  reflow.ExecConfig
}

// newExec returns a new testExec given an ID and a config.
func newExec(id digest.Digest, config reflow.ExecConfig) *Exec {
	return &Exec{
		id:      id,
		resultc: make(chan ExecResult, 1),
		err:     make(chan error),
		config:  config,
	}
}

// ID returns the exec's ID
func (e *Exec) ID() digest.Digest { return e.id }

// URI is not implemented
func (e *Exec) URI() string { panic("not implemented") }

// Value rendezvous the result (value or error) of this exec.
func (e *Exec) Result(ctx context.Context) (reflow.Result, error) {
	r, err := e.result(ctx)
	return r.Result, err
}

// Promote is a no-op for the test exec.
func (e *Exec) Promote(ctx context.Context) error {
	_, err := e.result(ctx)
	return err
}

// Inspect rendezvous the result of this exec and returns the inspection output.
func (e *Exec) Inspect(ctx context.Context) (reflow.ExecInspect, error) {
	r, err := e.result(ctx)
	return r.Inspect, err
}

// Wait rendezvous this exec.
func (e *Exec) Wait(ctx context.Context) error {
	//	log.Printf("(*testExec).Wait %v %v", e.id, e.config)
	_, err := e.result(ctx)
	return err
}

// Logs is not implemented.
func (e *Exec) Logs(ctx context.Context, stdout bool, stderr bool, follow bool) (io.ReadCloser, error) {
	panic("not implemented")
}

// Shell is not implemented
func (e *Exec) Shell(ctx context.Context) (io.ReadWriteCloser, error) {
	panic("not implemented")
}

// Ok rendezvous the value v as a successful result by this testExec.
func (e *Exec) Ok(res reflow.Result) {
	select {
	case <-e.err:
		panic("error defined")
	default:
	}
	select {
	case e.resultc <- ExecResult{Result: res}:
	default:
		panic("result already set")
	}
}

// Error rendezvous the error err as the result of this testExec.
func (e *Exec) Error(err error) {
	select {
	case <-e.resultc:
		panic("result already set")
	default:
	}
	e.err <- err
}

func (e *Exec) result(ctx context.Context) (ExecResult, error) {
	select {
	case result := <-e.resultc:
		e.resultc <- result
		result.Inspect.Config = e.config
		return result, nil
	case err := <-e.err:
		// We don't put error back--it resets the result.
		return ExecResult{}, err
	case <-ctx.Done():
		return ExecResult{}, ctx.Err()
	}
}

// Executor implements Executor for testing purposes.
// It allows the caller to await creation of testExecs.
type Executor struct {
	Have reflow.Resources

	repo  reflow.Repository
	mu    sync.Mutex
	cond  *sync.Cond
	execs map[digest.Digest]*Exec
}

// Init initializes the test executor.
func (e *Executor) Init() {
	e.cond = sync.NewCond(&e.mu)
	e.execs = map[digest.Digest]*Exec{}
	e.repo = &panicRepository{}
}

// Put defines a new exec (idempotently).
func (e *Executor) Put(ctx context.Context, id digest.Digest, config reflow.ExecConfig) (reflow.Exec, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.execs[id] == nil {
		e.execs[id] = newExec(id, config)
		e.cond.Broadcast()
	}
	return e.execs[id], nil
}

// Get retrieves an exec.
func (e *Executor) Get(ctx context.Context, id digest.Digest) (reflow.Exec, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	x := e.execs[id]
	if x == nil {
		return nil, errors.E("get", id, errors.NotExist)
	}
	return x, nil
}

// Remove is not implemented.
func (e *Executor) Remove(ctx context.Context, id digest.Digest) error {
	panic("not implemented")
}

// Execs enumerates the execs managed by this executor.
func (e *Executor) Execs(ctx context.Context) ([]reflow.Exec, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var execs []reflow.Exec
	for _, e := range e.execs {
		execs = append(execs, e)
	}
	return execs, nil
}

// Resources returns this executor's total resources.
func (e *Executor) Resources() reflow.Resources {
	return e.Have
}

// Repository returns this executor's repository.
func (e *Executor) Repository() reflow.Repository {
	return e.repo
}

// Equiv tells whether this executor contains precisely a set of flows.
func (e *Executor) Equiv(flows ...*reflow.Flow) bool {
	ids := map[digest.Digest]bool{}
	for _, f := range flows {
		ids[f.Digest()] = true
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for id := range e.execs {
		if !ids[id] {
			return false
		}
		delete(ids, id)
	}
	return len(ids) == 0
}

func (e *Executor) exec(f *reflow.Flow) *Exec {
	e.mu.Lock()
	defer e.mu.Unlock()
	for {
		if x := e.execs[f.Digest()]; x != nil {
			return x
		}
		e.cond.Wait()
	}
}

// Wait blocks until a Flow is defined in the executor.
func (e *Executor) Wait(f *reflow.Flow) {
	e.exec(f)
}

// Ok defines a successful result for a Flow.
func (e *Executor) Ok(f *reflow.Flow, res interface{}) {
	switch arg := res.(type) {
	case reflow.Fileset:
		e.exec(f).Ok(reflow.Result{Fileset: arg})
	case error:
		e.exec(f).Ok(reflow.Result{Err: errors.Recover(arg)})
	default:
		panic("invalid result")
	}
}

// Error defines an erroneous result for the flow.
func (e *Executor) Error(f *reflow.Flow, err error) {
	e.exec(f).Error(err)
}
