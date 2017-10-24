// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package batch implements support for running batches of reflow
// (stateful) evaluations. The user specifies a reflow program and
// enumerates each run by way of parameters specified in a CSV file.
// The batch runner then takes care of the rest.
package batch

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	golog "log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/state"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/lang"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
	"golang.org/x/time/rate"
)

//go:generate stringer -type=State

const (
	configPath  = "config.json"
	statePrefix = "state"
)

// State tells the state of an individual batch run.
type State int

const (
	// StateInit indicates that the run is initializing.
	StateInit State = iota
	// StateRunning indicates that the run is currently evaluating.
	StateRunning
	// StateDone indicates that the run has completed without a runtime error.
	StateDone
	// StateError indicates that the attempted run errored out with a runtime error.
	StateError
	stateMax
)

// Name returns an abbreviated name for a state.
func (s State) Name() string {
	switch s {
	case StateInit:
		return "init"
	case StateRunning:
		return "running"
	case StateDone:
		return "done"
	case StateError:
		return "error"
	default:
		panic("bad state")
	}
}

// A Run comprises the state for an individual run. Runs are
// serialized and can be restored across process invocations. Run is
// mostly deprecated in favor of runner.State, but we still maintain
// the old fields so that we can upgrade old serialized states.
//
// TODO(marius): clean this up when it's safe to do so.
type Run struct {
	// ID is the run's identifier. IDs must be unique inside of a batch.
	ID string
	// Program is the path of the Reflow program to be evaluated.
	Program string
	// Args contains the run's parameters.
	Args map[string]string
	// Argv contains the run's argument vector.
	Argv []string

	// Name stores the name of the run.
	Name runner.Name

	// State stores the runner state of this run.
	State runner.State `json:"-"`

	batch *Batch
	log   *log.Logger
}

// Go runs the run according to its state. If the run was previously
// started, Go resumes it. Go returns on completion, error, or when
// the context is done. initWG is used to coordinate multiple runs.
// Runs that require new allocs wait for initWG while those that
// already have allocs reclaim them and then call initWG.Done. This
// allows the batch to ensure that we don't collect allocs that are
// potentially useful.
//
// Go commits the run state at every phase transition so that progress
// should never be lost.
func (r *Run) Go(ctx context.Context, initWG *sync.WaitGroup) error {
	flow, typ, err := r.flow()
	if err != nil {
		return err
	}
	path := r.batch.path(fmt.Sprintf("log.%s", r.ID))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	r.log = log.New(golog.New(f, "", golog.LstdFlags), log.InfoLevel)
	run := &runner.Runner{
		State:         r.State,
		Cluster:       r.batch.Cluster,
		ClusterAux:    r.batch.ClusterAux,
		Flow:          flow,
		Cache:         r.batch.Cache,
		NoCacheExtern: r.batch.NoCacheExtern,
		GC:            r.batch.GC,
		Transferer:    r.batch.Transferer,
		Log:           r.log,
		Type:          typ,
		Labels:        pool.Labels{"program": r.Program},
	}
	run.Program = r.Program
	run.Params = r.Args
	run.Args = r.Argv
	switch run.Phase {
	default:
		initWG.Wait()
		if lim := r.batch.Admitter; run.Phase != runner.Done && lim != nil {
			if err := lim.Wait(ctx); err != nil {
				return err
			}
		}
	case runner.Eval:
		// We manually restore the runner's alloc here so
		// that we can properly coordinate between batch runs
		// that have allocs vs. those that don't.
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		alloc, err := run.Cluster.Alloc(ctx, run.AllocID)
		if err == nil {
			run.Alloc = alloc
			run.Alloc.Keepalive(ctx, time.Minute) // best-effort
		}
		cancel()
		initWG.Done()
	}
	for ok := true; ok; {
		ok = run.Do(ctx)
		r.State = run.State
		r.log.Debugf("run %s: state: %v", r.ID, run.State)
		r.batch.commit(r)
	}
	if run.Err != nil {
		return run.Err
	}
	return nil
}

func (r *Run) flow() (*reflow.Flow, *types.T, error) {
	switch ext := filepath.Ext(r.Program); ext {
	default:
		return nil, nil, fmt.Errorf("unrecognized file extension %s", ext)
	case ".reflow":
		f, err := os.Open(r.batch.path(r.Program))
		if err != nil {
			return nil, nil, err
		}
		file := f.Name()
		prog := &lang.Program{File: file, Errors: os.Stderr, Args: r.Argv}
		if err := prog.ParseAndTypecheck(f); err != nil {
			return nil, nil, err
		}
		flags := prog.Flags()
		flags.VisitAll(func(f *flag.Flag) {
			f.Value.Set(r.Args[f.Name])
			if f.Value.String() == "" {
				err = fmt.Errorf("argument %q is undefined\n", f.Name)
			}
		})
		return prog.Eval(), nil, err
	case ".rf":
		sess := syntax.NewSession()
		m, err := sess.Open(r.Program)
		if err != nil {
			return nil, nil, err
		}
		var maintyp *types.T
		for _, f := range m.Type.Fields {
			if f.Name == "Main" {
				maintyp = f.T
				break
			}
		}
		if maintyp == nil {
			return nil, nil, fmt.Errorf("module %v does not define symbol Main", r.Program)
		}
		flags, err := m.Flags(sess, sess.Values)
		if err != nil {
			return nil, nil, err
		}
		flags.VisitAll(func(f *flag.Flag) {
			if v, ok := r.Args[f.Name]; ok {
				f.Value.Set(v)
			}
		})
		env := sess.Values.Push()
		if err := m.FlagEnv(flags, env); err != nil {
			return nil, nil, err
		}
		v, err := m.Make(sess, env)
		if err != nil {
			return nil, nil, err
		}
		v = v.(values.Module)["Main"]
		v = syntax.Force(v, maintyp)
		switch v := v.(type) {
		case *reflow.Flow:
			if min, _ := v.Requirements(); min.IsZeroAll() {
				return nil, nil, fmt.Errorf("flow does not have resource requirements; add a @requires annotation to val Main")
			}
			return v, maintyp, nil
		default:
			return &reflow.Flow{Op: reflow.OpVal, Value: v}, maintyp, nil
		}
	}
}

type config struct {
	Program  string `json:"program"`
	RunsFile string `json:"runs_file"`
}

// Batch represents a batch of reflow evaluations. It manages setting
// up runs and instantiating them; subsequently it manages the run
// state of the batch (through state.File) so that batches are
// resumable across process invocations.
//
// Batches assume they have a (user provided) directory in which its
// configuration, state, and log files are stored.
type Batch struct {
	// Dir is the batch directory in which configuration, state, and log files
	// are stored.
	Dir string
	// Rundir is the directory where run state files are stored.
	Rundir string
	// User is the user running the batch; batch runs are named using
	// this value.
	User string
	// Cluster is the cluster from which allocs are reserved.
	Cluster runner.Cluster
	// ClusterAux is an optional auxilliary cluster. If it is defined,
	// work-stealing works are allocated from this cluster, while
	// primary workers are allocated from Cluster.
	ClusterAux runner.Cluster
	// Log receives log messages that pertain to batch management.
	// Individual run logs are stored in their own files in the batch
	// directory.
	Log *log.Logger
	// Cache is used to store the results of every reflow subexpression.
	// If nil, no results are stored.
	Cache reflow.Cache
	// NoCacheExtern should be set to true if externs should not be cached.
	NoCacheExtern bool
	// GC specifies whether the underlying Eval peforms garbage collection
	// after each exec has completed.
	GC bool
	// Transferer is the reflow transfer manager used for the whole batch.
	// (Across all runs.)
	Transferer reflow.Transferer
	// Runs is the set of runs managed by this batch.
	Runs map[string]*Run
	// Admitter is a rate limiter to control the rate of new evaluations.
	// This can be used to prevent "thundering herds" against systems
	// like S3.
	Admitter *rate.Limiter

	file   *state.File
	states map[string]*state.File
	config config
	flow   *reflow.Flow
}

// Init initializes a batch. If reset is set to true, then previously saved
// state is discarded.
//
// Init also upgrades old state files.
func (b *Batch) Init(reset bool) error {
	b.Admitter = rate.NewLimiter(rate.Every(5*time.Second), 1)
	f, err := os.Open(filepath.Join(b.Dir, configPath))
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&b.config); err != nil {
		return err
	}
	b.file, err = state.Open(filepath.Join(b.Dir, statePrefix))
	if err != nil {
		return err
	}
	b.states = map[string]*state.File{}
	b.Runs = map[string]*Run{}
	if reset {
		if err := b.file.Marshal(b.Runs); err != nil {
			b.Log.Errorf("marshal: %v", err)
		}
	} else {
		if err := b.file.Unmarshal(&b.Runs); err != nil && err != state.ErrNoState {
			return err
		}
	}
	b.commit(nil)
	b.Log.Printf("batch program %v runsfile %v", b.config.Program, b.config.RunsFile)
	return b.read()
}

// Close releases resources held by the batch.
func (b *Batch) Close() {
	for _, file := range b.states {
		file.Close()
	}
	if b.file != nil {
		b.file.Close()
	}
}

// Run runs the batch until completion, too many errors, or context
// completion. Run reports batch progress to the batch's logger every
// 10 seconds.
func (b *Batch) Run(ctx context.Context) error {
	done := make(chan *Run)
	var wg sync.WaitGroup
	for _, run := range b.Runs {
		if run.State.Phase == runner.Eval {
			wg.Add(1)
		}
	}
	for _, run := range b.Runs {
		go func(run *Run) {
			err := run.Go(ctx, &wg)
			switch {
			case ctx.Err() != nil:
			case err != nil:
				b.Log.Errorf("run %v: error: %v", run.ID, err)
			default:
				b.Log.Printf("run %v: done: %v", run.ID, run.State.Result)
			}
			done <- run
		}(run)
	}
	n := 0
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for n < len(b.Runs) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			n++
		case <-tick.C:
			b.logReport()
		}
	}
	b.logReport()
	return nil
}

func (b *Batch) read() error {
	f, err := os.Open(b.path(b.config.RunsFile))
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return err
	}
	if len(records) < 1 {
		return errors.New("empty batch")
	}
	header := records[0]
	records = records[1:]
	runs := map[string]*Run{}
	for _, fields := range records {
		id := fields[0]
		attrs := map[string]string{}
		for j := 1; j < len(header); j++ {
			attrs[header[j]] = fields[j]
		}
		run := b.Runs[id]
		if run == nil {
			run = new(Run)
			run.Name.User = b.User
			run.Name.ID = reflow.Digester.Rand()
		}
		run.ID = id
		run.Args = attrs
		run.Argv = fields[len(header):]
		run.Program = b.config.Program
		run.batch = b
		b.states[id], err = state.Open(filepath.Join(b.Rundir, run.Name.String()))
		if err != nil {
			return err
		}
		if err := b.states[id].Unmarshal(&run.State); err != nil && err != state.ErrNoState {
			return err
		}
		if run.State.Name.IsZero() {
			run.State.Name = run.Name
		}
		runs[id] = run
	}
	b.Runs = runs
	b.commit(nil)
	return nil
}

func (b *Batch) path(path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(b.Dir, path)
}

func (b *Batch) commit(run *Run) {
	if run == nil {
		b.file.LockLocal()
		if err := b.file.Marshal(b.Runs); err != nil {
			b.Log.Errorf("marshal: %v", err)
		}
		b.file.UnlockLocal()
	} else {
		if err := b.states[run.ID].Marshal(&run.State); err != nil {
			b.Log.Errorf("marshal %s: %v", run.ID, err)
		}
	}
}

func (b *Batch) logReport() {
	var phases [runner.MaxPhase]int
	for _, run := range b.Runs {
		phases[run.State.Phase]++
	}
	var counts [runner.MaxPhase]string
	for i, n := range phases {
		counts[i] = fmt.Sprintf("%s:%d", strings.ToLower(runner.Phase(i).String()), n)
	}
	b.Log.Debugf("%s", strings.Join(counts[:], " "))
}
