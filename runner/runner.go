// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

//go:generate stringer -type=Phase

const (
	pollInterval     = 10 * time.Second
	allocTimeout     = 5 * time.Minute
	keepaliveTimeout = 10 * time.Second
	maxTries         = 10
)

var minResources = reflow.Resources{CPU: 1, Memory: 500 << 20, Disk: 1 << 30}

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

// A Name identifies a single run.
type Name struct {
	// User is a username of the form "user@domain.com".
	User string
	// ID is the unique ID of the run.
	ID digest.Digest
}

// IsZero tells whether n is the zero value.
func (n Name) IsZero() bool {
	return n == Name{}
}

// String formats an identifier, e.g.:
//	user@domain.com/7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730
//
// An empty string is returned for a zero name.
func (n Name) String() string {
	if n.IsZero() {
		return ""
	}
	return n.User + "/" + n.ID.Hex()
}

// Short formats a short identifier, e.g.,:
//	user@domain.com/7d865e95
func (n Name) Short() string {
	return n.User + "/" + n.ID.Short()
}

// ParseName parses a name from a string.
func ParseName(s string) (Name, error) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) == 1 {
		return Name{}, fmt.Errorf("invalid name %s: missing ID", s)
	}
	if strings.Count(parts[0], "@") != 1 {
		return Name{}, fmt.Errorf("invalid name %s: invalid user ID", s)
	}
	name := Name{User: parts[0]}
	var err error
	name.ID, err = reflow.Digester.Parse(parts[1])
	if err != nil {
		return Name{}, err
	}
	return name, nil
}

// State contains the full state of a run. A State can be serialized
// and later recovered in order to resume a run.
type State struct {
	// Name is the name of the run.
	Name Name
	// Program stores the reflow program name.
	Program string
	// Params is the run parameters
	Params map[string]string
	// Args stores the run arguments
	Args []string
	// Phase holds the current phase of the run.
	Phase Phase
	// AllocID is the ID of the run's alloc, if any.
	AllocID string
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
}

// Reset resets the state so that it will reinitialize if run.
// Run metadata (including its name) are preserved.
func (s *State) Reset() {
	s.Phase = Init
	s.AllocID = ""
	s.Result = ""
	s.Err = nil
	s.NumTries = 0
	s.LastTry = time.Time{}
	s.Created = time.Time{}
}

// String returns a string representation of the state.
func (s State) String() string {
	switch s.Phase {
	case Init:
		return "init"
	case Eval:
		return fmt.Sprintf("eval alloc %v", s.AllocID)
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

// A Runner is responsible for evaluating a reflow.Flow on a cluster.
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

	reflow.EvalConfig

	// Cluster is the main cluster from which Allocs are allocated.
	Cluster Cluster

	// ClusterAux defines the cluster from which capacity
	// for auxilliary workers is allocated. If nil, Cluster is used
	// instead.
	ClusterAux Cluster

	// Flow is the flow to be evaluated.
	Flow *reflow.Flow

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

	switch r.Phase {
	case Init:
		if err := r.Allocate(ctx); err != nil {
			r.Err = errors.Recover(err)
			r.Phase = Done
			break
		}
		r.AllocID = r.Alloc.ID()
		r.Phase = Eval
	case Eval:
		r.LastTry = time.Now()
		if r.Alloc == nil {
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
			break
		}
		r.Err = errors.Recover(err)
		// We retry potentially transient errors here: there is no harm
		// beyond extra resource usage.
		if errors.Transient(r.Err) {
			r.Log.Debugf("marking run for retry after transient error %v", r.Err)
			r.Phase = Retry
		} else {
			r.Log.Debugf("marking run done after nonrecoverable error %v", r.Err)
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
	case Done:
	}
	return r.Phase != Done
}

// Allocate reserves a new alloc from r.Cluster when r.Alloc is nil.
func (r *Runner) Allocate(ctx context.Context) error {
	min, max := r.Flow.Requirements()
	min, max = min.Max(minResources), max.Max(minResources)
	var err error
	r.Alloc, err = r.Cluster.Allocate(ctx, min, max, r.labels())
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
	wg.Add(1)
	go func() {
		err := pool.Keepalive(ctx, r.Alloc)
		if err != ctx.Err() {
			r.Log.Errorf("keepalive: %v", err)
			r.Alloc = nil
		}
		cancel()
		wg.Done()
	}()

	config := r.EvalConfig
	config.Executor = r.Alloc
	eval := reflow.NewEval(r.Flow, config)
	stealer := &Stealer{
		Cache:   r.Cache,
		Cluster: r.ClusterAux,
		Log:     r.Log,
		Labels:  r.labels().Add("type", "aux"),
	}
	if stealer.Cluster == nil {
		stealer.Cluster = r.Cluster
	}
	go stealer.Go(ctx, eval)
	err := eval.Do(ctx)
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
	labels["Name"] = r.Name.String()
	labels["program"] = r.Program
	for k, v := range r.Params {
		labels[fmt.Sprintf("param[%s]", k)] = v
	}
	for i, v := range r.Args {
		labels[fmt.Sprintf("arg[%d]", i)] = v
	}
	return labels
}
