// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/trace"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

//go:generate stringer -type=Phase

const (
	pollInterval     = 10 * time.Second
	keepaliveTimeout = 10 * time.Second
	maxTries         = 10
)

var minResources = reflow.Resources{"cpu": 1, "mem": 500 << 20, "disk": 1 << 30}
var minRequirements = reflow.Requirements{Min: minResources}

// Phase enumerates the possible phases of a run.
type Phase int

const (
	// Init indicates the run is needs initialization.
	Init Phase = iota
	// Eval indicates the run needs evaluation.
	Eval
	// Retry indicates the run needs to be considered for retrying.
	Retry
	// Done indicates the run is complete.
	Done

	// MaxPhase is the maximum value of Phase.
	MaxPhase
)

// State contains the full state of a run. A State can be serialized
// and later recovered in order to resume a run.
type State struct {
	// ID is this run's global ID.
	ID taskdb.RunID
	// Program stores the reflow program name.
	Program string
	// Params is the run parameters
	Params map[string]string
	// Args stores the run arguments
	Args []string
	// Phase holds the current phase of the run.
	Phase Phase
	// AllocID is the full URI for the run's alloc.
	AllocID string
	// AllocInspect is the alloc's inspect output.
	AllocInspect pool.AllocInspect
	// Value contains the result of the evaluation,
	// rendered as a string.
	// TODO(marius): serialize the value into JSON.
	Result string
	// Err contains runtime errors.
	Err *errors.Error
	// NumTries is the number of evaluation attempts
	// that have been made.
	NumTries int
	// LastTry is the timestamp of the last evaluation attempt.
	LastTry time.Time
	// Created is the time of the run's creation.
	Created time.Time
	// Completion is the time of the run's completion.
	Completion time.Time

	// TotalResources stores the total amount of resources used
	// by this run. Note that the resources are in resource-minutes.
	TotalResources reflow.Resources
}

// Reset resets the state so that it will reinitialize if run.
// Run metadata (including its name) are preserved.
func (s *State) Reset() {
	s.Phase = Init
	s.AllocID = ""
	s.AllocInspect = pool.AllocInspect{}
	s.Result = ""
	s.Err = nil
	s.NumTries = 0
	s.LastTry = time.Time{}
	s.Created = time.Time{}
	s.Completion = time.Time{}
}

// String returns a string representation of the state.
func (s State) String() string {
	switch s.Phase {
	case Init:
		return "init"
	case Eval:
		if s.AllocID != "" {
			return fmt.Sprintf("eval alloc %v", s.AllocID)
		}
		return fmt.Sprintf("eval")
	case Retry:
		return fmt.Sprintf("retry error %v try %d/%d last %v", s.Err, s.NumTries+1, maxTries, s.LastTry)
	case Done:
		if s.Err != nil {
			return fmt.Sprintf("done error %v", s.Err)
		}
		return fmt.Sprintf("done result %v", s.Result)
	}
	panic("unknown state")
}

// A Runner is responsible for evaluating a flow.Flow on a cluster.
// Runners also launch and maintain auxilliary work-stealing allocs,
// and manages data transfer and failure handling between the primary
// evaluation alloc and the auxilliary workers.
//
// TODO(marius): introduce a "stealer-only" mode where there is no
// primary alloc, but with a shared repository (e.g., S3) attached to
// the Eval.
type Runner struct {
	// State contains the state of the run. The user can serialize
	// this in order to resume runs.
	State

	flow.EvalConfig

	// Cluster is the main cluster from which Allocs are allocated.
	Cluster Cluster

	// ClusterAux defines the cluster from which capacity
	// for auxilliary workers is allocated. If nil, Cluster is used
	// instead.
	ClusterAux Cluster

	// Flow is the flow to be evaluated.
	Flow *flow.Flow

	// Type is the type of output. When Type is nil, it is taken to be
	// (legacy) reflow.Fileset.
	Type *types.T

	// Transferer is the transfer manager used for node-to-node data
	// transfers.
	Transferer reflow.Transferer

	// Retain is the amount of time the primary alloc should be retained
	// after failure.
	Retain time.Duration

	// Alloc is the primary alloc in which the flow is evaluated.
	Alloc pool.Alloc

	// Labels are the set of labels affiliated with this run.
	Labels pool.Labels

	// Cmdline is a debug string with program name, params and args.
	Cmdline string
}

// Do steps the runner state machine. Do returns true whenever
// it can make more progress, thus a caller should call Do in a loop:
//
//	for r.Do(ctx) {
//		// report progress, save state, etc.
//	}
func (r *Runner) Do(ctx context.Context) bool {
	if r.Created.IsZero() {
		r.Created = time.Now()
	}
	if r.Scheduler != nil && r.Phase == Init {
		r.Phase = Eval
		return true
	}
	switch r.Phase {
	case Init:
		if err := r.Allocate(ctx); err != nil {
			r.Err = errors.Recover(err)
			r.Phase = Done
			break
		}
		r.AllocID = r.Alloc.ID()
		var err error
		r.AllocInspect, err = r.Alloc.Inspect(ctx)
		if err != nil {
			r.Err = errors.Recover(err)
			r.Phase = Done
			break
		}
		r.Phase = Eval
	case Eval:
		r.LastTry = time.Now()
		if r.Scheduler == nil && r.Alloc == nil {
			var err error
			r.Alloc, err = r.Cluster.Alloc(ctx, r.AllocID)
			if err != nil {
				// TODO(marius): perhaps single out NotExist errors here
				// in an attempt to reuse allocs where we can.
				r.Err = errors.Recover(err)
				r.Phase = Retry
				break
			}
		}
		var err error
		r.Result, err = r.Eval(ctx)
		if err == nil {
			r.Phase = Done
			r.Completion = time.Now()
			break
		}
		r.Err = errors.Recover(err)
		// We retry potentially transient errors here: there is no harm
		// beyond extra resource usage.
		if errors.Restartable(r.Err) {
			r.Log.Debugf("marking run for retry after restartable error %v", r.Err)
			r.Phase = Retry
		} else {
			r.Log.Debugf("marking run done after nonrecoverable error %v", r.Err)
			r.Completion = time.Now()
			r.Phase = Done
		}
	case Retry:
		// TODO(marius): ideally we'd simply continue here as long as each
		// evaluation is making progress (instead of relying on a fixed
		// retry budget). We could measure this by the number of evaluation
		// steps that take place. We'll retry as long as they are
		// monotonically increasing.
		r.NumTries++
		if r.NumTries > maxTries {
			r.Err = errors.Recover(errors.E(errors.TooManyTries, r.Err))
			r.Completion = time.Now()
			r.Phase = Done
			break
		}
		var w time.Duration
		if d := time.Since(r.LastTry); d < time.Minute {
			w = time.Minute - d
		}
		time.Sleep(w)
		r.Phase = Init
		r.Err = nil
	}
	return r.Phase != Done
}

// Allocate reserves a new alloc from r.Cluster when r.Alloc is nil.
func (r *Runner) Allocate(ctx context.Context) error {
	req := r.Flow.Requirements()
	req.Add(minRequirements)
	var err error
	r.Alloc, err = r.Cluster.Allocate(ctx, req, r.labels())
	if err != nil {
		return err
	}
	r.Log.Debugf("accepted alloc %v", r.Alloc.ID())
	return nil
}

// Eval evaluates the flow, returning the resulting Value. In the
// case of failure, r.Alloc is kept-alive for an additional r.Retain
// duration.
func (r *Runner) Eval(ctx context.Context) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	if r.Alloc != nil {
		wg.Add(1)
		go func() {
			err := pool.Keepalive(ctx, r.Log, r.Alloc)
			if err != ctx.Err() {
				r.Log.Errorf("keepalive: %v", err)
				r.Alloc = nil
			}
			cancel()
			wg.Done()
		}()
	}

	config := r.EvalConfig
	config.Executor = r.Alloc
	eval := flow.NewEval(r.Flow, config)

	ctx, done := trace.Start(ctx, trace.Run, r.Flow.Digest(), r.Cmdline)
	traceid := trace.URL(ctx)
	if traceid != "" {
		r.Log.Printf("Trace ID: %v", traceid)
	}

	// Run stealers if we're running with an alloc. Otherwise,
	// tasks are submitted directly to the scheduler.
	if r.Alloc != nil {
		stealer := &Stealer{
			Cluster: r.ClusterAux,
			Log:     r.Log,
			Labels:  r.labels().Add("type", "aux"),
		}
		if stealer.Cluster == nil {
			stealer.Cluster = r.Cluster
		}
		go stealer.Go(ctx, eval)
	}

	err := eval.Do(ctx)
	done()
	if err == nil {
		// TODO(marius): use logger for this.
		eval.LogSummary(r.Log)
	}
	cancel()
	wg.Wait() // TODO(marius): wait for stealers too?

	var retain time.Duration
	if err != nil || eval.Err() != nil {
		retain = r.Retain
	}
	if alloc := r.Alloc; alloc != nil {
		ctx, cancel = context.WithTimeout(context.Background(), keepaliveTimeout)
		if _, err := alloc.Keepalive(ctx, retain); err != nil {
			r.Log.Errorf("retain %v: %v", r.Retain, err)
		}
		cancel()
	}
	if err != nil {
		return "", err
	}
	if err := eval.Err(); err != nil {
		return "", errors.E(errors.Eval, err)
	}
	if r.Type == nil {
		return eval.Value().(reflow.Fileset).String(), nil
	}
	return values.Sprint(eval.Value(), r.Type), nil
}

func (r Runner) labels() pool.Labels {
	labels := r.Labels.Copy()
	labels["ID"] = r.ID.IDShort()
	labels["program"] = r.Program
	for k, v := range r.Params {
		labels[fmt.Sprintf("param[%s]", k)] = v
	}
	for i, v := range r.Args {
		labels[fmt.Sprintf("arg[%d]", i)] = v
	}
	return labels
}
