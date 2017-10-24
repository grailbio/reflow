// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository/file"
)

// localfileExec implements an exec for interning and externing
// localfiles, under the localfile:// scheme. Localfile assumes that
// the files to be interned are on the same filesystem as the
// executor's run directory so that they may be hardlinked without
// copying.
//
// TODO(marius): automatically detect this and copy files if
// necessary.
type localfileExec struct {
	// The Executor that owns this exec.
	Executor *Executor
	// The (possibly nil) Logger that logs exec's actions, for external consumption.
	Log *log.Logger

	staging file.Repository

	id    digest.Digest
	cfg   reflow.ExecConfig
	fs    reflow.Fileset
	mu    sync.Mutex
	cond  *sync.Cond
	state execState
	err   error
}

func newLocalfileExec(id digest.Digest, x *Executor, cfg reflow.ExecConfig) *localfileExec {
	e := &localfileExec{
		Executor: x,
		id:       id,
		cfg:      cfg,
	}
	e.staging.Root = e.Executor.execPath(e.id, objectsDir)
	e.staging.Log = x.Log
	e.cond = sync.NewCond(&e.mu)
	return e
}

func (e *localfileExec) Go(ctx context.Context) {
	for state, err := e.getState(); err == nil && state != execComplete; e.setState(state, err) {
		switch state {
		case execUnstarted:
			state = execRunning
		case execRunning:
			err = e.do(ctx)
			state = execComplete
		default:
			panic("bug")
		}
	}
}

func (e *localfileExec) do(ctx context.Context) error {
	u, err := url.Parse(e.cfg.URL)
	if err != nil {
		return errors.E("exec", e.id, err)
	}
	if u.Scheme != "localfile" {
		return errors.E("exec", e.id, errors.NotSupported, errors.Errorf("unsupported scheme %v", u.Scheme))
	}
	switch e.cfg.Type {
	case "intern":
		e.fs, err = e.Executor.install(ctx, filepath.Join(e.Executor.Prefix, u.Host+u.Path), false, &e.staging)
		if err != nil {
			e.Log.Errorf("installing %s: %v", filepath.Join(e.Executor.Prefix, u.Path), err)
		} else {
			e.Log.Printf("installed %s: %v", filepath.Join(e.Executor.Prefix, u.Path), e.fs.Short())
		}
		return err
	case "extern":
		if n := len(e.cfg.Args); n != 1 {
			return errors.E("exec", e.id, errors.Errorf("localfile extern needed one arg, got %d", n))
		}
		arg := e.cfg.Args[0]
		binds := map[string]digest.Digest{}
		for path, file := range arg.Fileset.Map {
			binds[path] = file.ID
		}
		e.Log.Printf("materializing %s", filepath.Join(e.Executor.Prefix, u.Path))
		return e.Executor.FileRepository.Materialize(filepath.Join(e.Executor.Prefix, u.Path), binds)
	default:
		return errors.E("exec", e.id, errors.NotSupported, errors.Errorf("unsupported exec type %v", e.cfg.Type))
	}
	panic("bug")
}

// setState sets the current state and error. It broadcasts
// on the exec's condition variable to wake up all waiters.
func (e *localfileExec) setState(state execState, err error) {
	e.mu.Lock()
	e.state = state
	e.err = err
	e.cond.Broadcast()
	e.mu.Unlock()
}

// getState returns the current state of the exec.
func (e *localfileExec) getState() (execState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state, e.err
}

func (e *localfileExec) WaitUntil(min execState) error {
	e.mu.Lock()
	for e.state < min && e.err == nil {
		e.cond.Wait()
	}
	e.mu.Unlock()
	return e.err
}

func (e *localfileExec) Kill(context.Context) error {
	panic("not implemented")
}

func (e *localfileExec) ID() digest.Digest {
	return e.id
}

func (e *localfileExec) URI() string {
	return e.Executor.URI() + "/" + e.id.Hex()
}

func (e *localfileExec) Result(ctx context.Context) (reflow.Result, error) {
	state, err := e.getState()
	if err != nil {
		return reflow.Result{}, err
	}
	if state != execComplete {
		return reflow.Result{}, errors.Errorf("result %v: exec not complete", e.id)
	}
	return reflow.Result{Fileset: e.fs}, nil
}

func (e *localfileExec) Promote(ctx context.Context) error {
	return e.Executor.FileRepository.Vacuum(ctx, &e.staging)
}

func (e *localfileExec) Inspect(ctx context.Context) (reflow.ExecInspect, error) {
	inspect := reflow.ExecInspect{Config: e.cfg}
	state, err := e.getState()
	if err != nil {
		inspect.Error = errors.Recover(err)
	}
	if state < execComplete {
		inspect.State = "running"
		inspect.Status = "files are being linked"
	} else {
		inspect.State = "complete"
		inspect.Status = "file linking is complete"
	}
	return inspect, nil
}

func (e *localfileExec) Wait(ctx context.Context) error {
	return e.WaitUntil(execComplete)
}

func (e *localfileExec) Logs(ctx context.Context, stdout bool, stderr bool) (io.ReadCloser, error) {
	return ioutil.NopCloser(bytes.NewReader(nil)), nil
}
