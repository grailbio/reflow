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
	"io/ioutil"
	golog "log"
	"os"
	"path/filepath"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/limiter"
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
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
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
	RunID taskdb.RunID

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
	defer func() {
		r.Status.Done()
		r.batch.Limiter.Release(1)
	}()
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
	runLogPath := filepath.Join(r.batch.Rundir, digest.Digest(r.RunID).Hex()+".execlog")
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
	evalConfig.RunID = r.RunID
	run := &runner.Runner{
		State:      r.State,
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
		if err := r.batch.Limiter.Acquire(ctx, 1); err != nil {
			return err
		}
	case runner.Eval:
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
	// Args are additional command line arguments (parsed as flags).
	// They override any supplied in the batch configuration.
	Args []string

	// Status is the status group used to report batch status;
	// individual run statuses are reported as tasks in the group.
	Status *status.Group

	// BatchState is the state of the current batch.
	BatchState

	flow.EvalConfig

	// Limiter is a rate limiter to control the number of parallel evaluations.
	// This can be used to prevent "thundering herds" against systems like S3.
	// Limiterr should be set prior to running the batch.
	Limiter *limiter.Limiter

	file   *state.File
	states map[string]*state.File
	config config
	flow   *flow.Flow
}

// BatchState identifies a batch. It has a unique identifier based on the program and the batch being run.
// It also includes the state of the runs in the batch.
type BatchState struct {
	// ID is the batch identifier. It uniquely identifies a batch (program and the contents of the batch).
	ID digest.Digest
	// Runs is the set of runs managed by this batch.
	Runs map[string]*Run
}

// Init initializes a batch. If reset is set to true, then previously saved state is discarded.
// If retry is set, then only failed runs in the batch are retried. Init also upgrades old state files.
func (b *Batch) Init(reset bool, retry bool) error {
	var (
		err error
		f   *os.File
	)
	if f, err = os.Open(filepath.Join(b.Dir, b.ConfigFilename)); err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&b.config); err != nil {
		return err
	}
	// Digest the contents of the config file and the runs file to see if we are rerunning an
	// existing configuration or running a completely new configuration.
	dw := reflow.Digester.NewWriter()
	if _, err = dw.Write([]byte(b.config.Program)); err != nil {
		return err
	}
	if _, err = dw.Write([]byte(b.config.RunsFile)); err != nil {
		return err
	}
	var rf *os.File
	if rf, err = os.Open(b.path(b.config.RunsFile)); err != nil {
		return err
	}
	var rfb []byte
	if rfb, err = ioutil.ReadAll(rf); err != nil {
		rf.Close()
		return err
	}
	rf.Close()
	if _, err = dw.Write(rfb); err != nil {
		return err
	}
	batchID := dw.Digest()

	if b.file, err = state.Open(filepath.Join(b.Dir, statePrefix)); err != nil {
		return err
	}
	b.states = map[string]*state.File{}
	b.Runs = map[string]*Run{}
	if reset {
		b.ID = batchID
		if err := b.file.Marshal(b.BatchState); err != nil {
			b.Log.Errorf("marshal: %v", err)
		}
	} else {
		if err := b.file.Unmarshal(&b.BatchState); err != nil && err != state.ErrNoState {
			return err
		}
		if b.ID != batchID {
			b.ID = batchID
			if err := b.file.Marshal(b.BatchState); err != nil {
				b.Log.Errorf("marshal: %v", err)
			}
		}
	}
	b.commit(nil)
	b.Log.Printf("batch program %v runsfile %v", b.config.Program, b.config.RunsFile)
	return b.read(retry)
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
			n++
			b.Status.Printf("remaining: %d", len(b.Runs)-n)
		}
	}
	return nil
}

// ReadState populates the current state of the batch run.
func (b *Batch) ReadState() error {
	var err error
	b.file, err = state.Open(filepath.Join(b.Dir, statePrefix))
	if err != nil {
		return err
	}
	b.states = map[string]*state.File{}
	err = b.file.Unmarshal(&b.BatchState)
	if err != nil && err != state.ErrNoState {
		return err
	}
	for _, run := range b.Runs {
		if run == nil || !run.RunID.IsValid() {
			continue
		}
		prefix := filepath.Join(b.Rundir, run.RunID.Hex())
		run.Status = b.Status.Start(run.RunID.IDShort())
		run.Status.Print("waiting")
		run.batch = b
		id := run.ID
		run.State.ID = taskdb.RunID(reflow.Digester.FromString(id))
		b.states[id], err = state.Open(filepath.Join(prefix))
		if err != nil && err != state.ErrNoState {
			return err
		}
		if err = b.states[id].Unmarshal(&run.State); err != nil && err != state.ErrNoState {
			return err
		}
	}
	return nil
}

func (b *Batch) read(retry bool) error {
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
			// Create fresh run ID the first time we encountered a run.
			run.RunID = taskdb.NewRunID()
		}
		run.ID = id
		run.Args = attrs
		run.Argv = fields[len(header):]
		run.Program = b.path(b.config.Program)
		var prevRunID taskdb.RunID

		if run.RunID.IsValid() {
			prevRunID = run.RunID
		}
		// Assign a new run id, and restore state, if any, from the prev run id.
		run.RunID = taskdb.NewRunID()
		prefix := filepath.Join(b.Rundir, run.RunID.Hex())
		if prevRunID.IsValid() {
			var prevState runner.State
			if err = state.Unmarshal(filepath.Join(b.Rundir, prevRunID.Hex()), &prevState); err != nil && err != state.ErrNoState {
				return err
			}
			if err = state.Marshal(prefix, prevState); err != nil {
				return err
			}
		}
		if retry {
			switch run.State.Phase {
			case runner.Done, runner.Retry:
				if run.State.Err != nil {
					run.State.Reset()
					b.Log.Printf("retrying run %v\n", id)
				}
			}
		}
		run.Status = b.Status.Start(run.RunID.IDShort())
		run.Status.Print("waiting")
		run.batch = b

		b.states[id], err = state.Open(filepath.Join(prefix))
		if err != nil {
			return err
		}
		if err := b.states[id].Unmarshal(&run.State); err != nil && err != state.ErrNoState {
			return err
		}
		if !run.State.ID.IsValid() {
			run.State.ID = run.RunID
		}
		runs[id] = run
		b.commit(run)
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
		if err := b.file.Marshal(b.BatchState); err != nil {
			b.Log.Errorf("marshal: %v", err)
		}
		b.file.UnlockLocal()
	} else {
		if err := b.states[run.ID].Marshal(&run.State); err != nil {
			b.Log.Errorf("marshal %s: %v", run.ID, err)
		}
	}
}
