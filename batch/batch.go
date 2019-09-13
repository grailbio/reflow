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
	"io"
	golog "log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/state"
	"github.com/grailbio/base/status"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
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

	// RunID is the global run ID for this run.
	RunID digest.Digest

	// State stores the runner state of this run.
	State runner.State `json:"-"`

	// Status receives status updates from batch execution.
	Status *status.Task

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
	defer r.Status.Done()
	flow, typ, err := r.flow()
	if err != nil {
		return err
	}
	path := r.batch.path(fmt.Sprintf("log.%s", r.ID))
	batchLogFile, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer batchLogFile.Close()
	var w io.Writer = batchLogFile
	// Also tee the output to the standard runlog location.
	// TODO(marius): this should be handled in a standard way.
	// (And run state management generally.)
	runLogPath := filepath.Join(r.batch.Rundir, r.RunID.Hex()+".execlog")
	os.MkdirAll(filepath.Dir(runLogPath), 0777)
	runLogFile, err := os.Create(runLogPath)
	if err != nil {
		r.log.Errorf("create %s: %v", runLogPath, err)
	} else {
		defer runLogFile.Close()
		w = io.MultiWriter(w, runLogFile)
	}
	r.log = log.New(
		golog.New(w, "", golog.LstdFlags),
		// We always use debug-level logging for execution transcripts,
		// since these are not meant for interactive output.
		log.DebugLevel,
	)
	evalConfig := r.batch.EvalConfig
	evalConfig.Log = r.log
	run := &runner.Runner{
		State:      r.State,
		Cluster:    r.batch.Cluster,
		ClusterAux: r.batch.ClusterAux,
		Flow:       flow,
		EvalConfig: evalConfig,
		Type:       typ,
		Labels:     pool.Labels{"program": r.Program},
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
		r.Status.Print(run.State)
		r.log.Debugf("run %s: state: %v", r.ID, run.State)
		r.batch.commit(r)
	}
	if run.Err != nil {
		r.Status.Printf("error %v", run.Err)
		return run.Err
	}
	return nil
}

func (r *Run) flow() (*flow.Flow, *types.T, error) {
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
		err = parseFlags(flags, r.Args, r.batch.Args)
		if err != nil {
			return nil, nil, err
		}
		flags.VisitAll(func(f *flag.Flag) {
			if f.Value.String() == "" {
				err = fmt.Errorf("argument %q is undefined", f.Name)
			}
		})
		return prog.Eval(), nil, err
	case ".rf", ".rfx":
		if r.batch.GC {
			r.log.Errorf("garbage collection disabled for v1 reflows")
			r.batch.GC = false
		}
		sess := syntax.NewSession(nil)
		m, err := sess.Open(r.Program)
		if err != nil {
			return nil, nil, err
		}
		var maintyp *types.T
		for _, f := range m.Type(nil).Fields {
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
		err = parseFlags(flags, r.Args, r.batch.Args)
		if err != nil {
			return nil, nil, err
		}
		env := sess.Values.Push()
		if err := m.FlagEnv(flags, env, types.NewEnv()); err != nil {
			return nil, nil, err
		}
		v, err := m.Make(sess, env)
		if err != nil {
			return nil, nil, err
		}
		v = v.(values.Module)["Main"]
		v = syntax.Force(v, maintyp)
		switch v := v.(type) {
		case *flow.Flow:
			if v.Requirements().Equal(reflow.Requirements{}) {
				return nil, nil, fmt.Errorf("flow does not have resource requirements; add a @requires annotation to val Main")
			}
			return v, maintyp, nil
		default:
			return &flow.Flow{Op: flow.Val, Value: v}, maintyp, nil
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
	// ConfigFilename is the name of the configuration file (relative to Dir)
	ConfigFilename string
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
	// Args are additional command line arguments (parsed as flags).
	// They override any supplied in the batch configuration.
	Args []string

	// Status is the status group used to report batch status;
	// individual run statuses are reported as tasks in the group.
	Status *status.Group

	flow.EvalConfig

	// Runs is the set of runs managed by this batch.
	Runs map[string]*Run
	// Admitter is a rate limiter to control the rate of new evaluations.
	// This can be used to prevent "thundering herds" against systems
	// like S3. Admitter should be set prior to running the batch.
	Admitter *rate.Limiter

	file   *state.File
	states map[string]*state.File
	config config
	flow   *flow.Flow
}

// Init initializes a batch. If reset is set to true, then previously saved
// state is discarded.
//
// Init also upgrades old state files.
func (b *Batch) Init(reset bool) error {
	f, err := os.Open(filepath.Join(b.Dir, b.ConfigFilename))
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
	b.Status.Printf("remaining: %d", len(b.Runs))
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
	for n < len(b.Runs) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			b.Status.Printf("remaining: %d", len(b.Runs)-n)
			n++
		}
	}
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
		if len(fields) != len(header) {
			return errors.Errorf("batch file row [%v] has %v fields, need %v", fields, len(fields), len(header))
		}
		id := fields[0]
		attrs := map[string]string{}
		for j := 1; j < len(header); j++ {
			attrs[header[j]] = fields[j]
		}
		run := b.Runs[id]
		if run == nil {
			run = new(Run)
			// Create fresh run ID the first time we encounted a run.
			run.RunID = reflow.Digester.Rand(nil)
		}
		run.ID = id
		run.Args = attrs
		run.Argv = fields[len(header):]
		run.Program = b.path(b.config.Program)
		run.Status = b.Status.Start(run.RunID.Short())
		run.Status.Print("waiting")
		run.batch = b
		b.states[id], err = state.Open(filepath.Join(b.Rundir, run.RunID.Hex()))
		if err != nil {
			return err
		}
		if err := b.states[id].Unmarshal(&run.State); err != nil && err != state.ErrNoState {
			return err
		}
		if run.State.ID.IsZero() {
			run.State.ID = run.RunID
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
