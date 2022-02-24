// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runtime

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/state"
	"github.com/grailbio/base/status"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/predictor"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/wg"
)

// RunnerParams defines the set of parameters necessary to initialize a ReflowRunner.
type RunnerParams struct {
	// TODO(swami): Replace InfraRunConfig with a run-specific config which only takes
	// keys that are run-specific (and the rest of the config is obtained from the runtime).
	InfraRunConfig infra.Config
	RunConfig RunConfig
	Logger *log.Logger

	// Optional parameters

	// Status is the status object specific to a run.
	Status *status.Status
}

// ReflowRunner supports the ability to run a reflow program.
type ReflowRunner interface {
	Go(context.Context) (runner.State, error)
	GetRunID() taskdb.RunID

	// TODO(swami):  Remove this.
	SetDotWriter(io.Writer)
}

// NewRunner returns a new runner that can run the given run config. If scheduler is non nil,
// it will be used for scheduling tasks.
func (rt *runtime) NewRunner(params RunnerParams) (ReflowRunner, error) {
	scheduler := rt.Scheduler()
	if scheduler == nil {
		panic(fmt.Sprintf("NewRunner must have scheduler"))
	}
	// Configure the Predictor.
	var pred *predictor.Predictor
	if params.RunConfig.RunFlags.Pred {
		if cfg, cerr := PredictorConfig(params.InfraRunConfig); cerr != nil {
			params.Logger.Errorf("error while configuring predictor: %s", cerr)
		} else {
			pred = predictor.New(scheduler.TaskDB, params.Logger.Tee(nil, "predictor: "), cfg.MinData, cfg.MaxInspect, cfg.MemPercentile)
		}
	}
	var (
		runID *taskdb.RunID
		err error
	)
	if err = params.InfraRunConfig.Instance(&runID); err != nil {
		return nil, err
	}
	var user *infra2.User
	if err = params.InfraRunConfig.Instance(&user); err != nil {
		return nil, err
	}
	r := &runnerImpl{
		RunConfig: params.RunConfig,
		RunID:     *runID,
		scheduler: scheduler,
		predictor: pred,
		Log:       params.Logger,
		user:      user.User(),
		Status:    new(status.Status),
	}
	if params.Status != nil {
		r.Status = params.Status
	}
	if err = params.InfraRunConfig.Instance(&r.sess); err != nil {
		return nil, err
	}
	if err = params.InfraRunConfig.Instance(&r.assoc); err != nil {
		if params.RunConfig.RunFlags.needAssocAndRepo() {
			return nil, err
		}
		params.Logger.Debug("assoc missing but was not needed")
	}
	if err = params.InfraRunConfig.Instance(&r.repo); err != nil {
		if params.RunConfig.RunFlags.needAssocAndRepo() {
			return nil, err
		}
		params.Logger.Debug("repo missing but was not needed")
	}
	if err = params.InfraRunConfig.Instance(&r.cache); err != nil {
		return nil, err
	}
	if err = params.InfraRunConfig.Instance(&r.labels); err != nil {
		return nil, err
	}
	return r, nil
}

// RunConfig defines all the material (configuration, program and args) for a specific run.
type RunConfig struct {
	// Program is the local path to the reflow bundle/program.
	Program string
	// Args is the arguments to be passed to the reflow program in the form of "--<argname>=<value>"
	Args []string
	// RunFlags is the run flags for this run.
	RunFlags RunFlags
}

// runnerImpl implements ReflowRunner.
// It defines a reflow program/bundle, args and configuration that can be
// evaluated to obtain a result.
type runnerImpl struct {
	// RunID is the unique id for this run.
	RunID taskdb.RunID
	// Log is the params.Logger to log to.
	Log *log.Logger
	// DotWriter is the writer to write the flow evaluation graph to write to, in dot format.
	DotWriter io.Writer

	RunConfig RunConfig
	scheduler *sched.Scheduler
	predictor *predictor.Predictor
	cmdline   string
	wg        *wg.WaitGroup

	// infra
	sess   *session.Session
	repo   reflow.Repository
	assoc  assoc.Assoc
	cache  *infra2.CacheProvider
	labels pool.Labels

	// TODO(swami): unexport this.
	Status *status.Status
	user   string
}

func (r *runnerImpl) SetDotWriter(w io.Writer) {
	r.DotWriter = w
}

// Go runs the reflow program. It returns a non nil error if did not succeed.
func (r *runnerImpl) Go(ctx context.Context) (runner.State, error) {
	var err error
	if err = r.RunConfig.RunFlags.Err(); err != nil {
		return runner.State{}, err
	}
	r.Log.Printf("run ID: %s", r.RunID.IDShort())
	e := Eval{
		Program: r.RunConfig.Program,
		Args:    r.RunConfig.Args,
	}
	bundle, err := e.Run(true)
	if err != nil {
		return runner.State{}, err
	}
	if err = e.ResolveImages(r.sess); err != nil {
		return runner.State{}, err
	}
	path, err := filepath.Abs(e.Program)
	if err != nil {
		r.Log.Errorf("abs %s: %v", e.Program, err)
		path = e.Program
	}
	r.cmdline = path
	var b bytes.Buffer
	fmt.Fprintf(&b, "evaluating program %s", path)
	if len(e.Params) > 0 {
		var keys []string
		for key := range e.Params {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fmt.Fprintf(&b, "\n\tparams:")
		for _, key := range keys {
			fmt.Fprintf(&b, "\n\t\t%s=%s", key, e.Params[key])
			r.cmdline += fmt.Sprintf(" -%s=%s", key, e.Params[key])
		}
	} else {
		fmt.Fprintf(&b, "\n\t(no params)")
	}
	if len(e.Args) > 0 {
		fmt.Fprintf(&b, "\n\targuments:")
		for _, arg := range e.Args {
			fmt.Fprintf(&b, "\n\t%s", arg)
			r.cmdline += fmt.Sprintf(" %s", arg)
		}
	} else {
		fmt.Fprintf(&b, "\n\t(no arguments)")
	}
	r.Log.Print(b.String())

	tctx, tcancel := context.WithCancel(ctx)
	defer tcancel()
	if r.scheduler == nil {
		panic("unexpectedly scheduler is nil")
	}
	tdb := r.scheduler.TaskDB
	if tdb != nil {
		if rerr := tdb.CreateRun(tctx, r.RunID, r.user); rerr != nil {
			r.Log.Debugf("error writing run to taskdb: %v", rerr)
		} else {
			go func() { _ = taskdb.KeepRunAlive(tctx, tdb, r.RunID) }()
			go func() { _ = r.uploadBundle(tctx, tdb, r.RunID, bundle, r.RunConfig.Program, r.RunConfig.Args) }()
		}
	}
	run := runner.Runner{
		Flow: e.Main(),
		EvalConfig: flow.EvalConfig{
			Log:                r.Log,
			Repository:         r.repo,
			Snapshotter:        r.scheduler.Mux,
			Assoc:              r.assoc,
			AssertionGenerator: reflow.AssertionGeneratorMux{reflow.BlobAssertionsNamespace: r.scheduler.Mux},
			CacheMode:          r.cache.CacheMode,
			Status:             r.Status.Group(r.RunID.IDShort()),
			Scheduler:          r.scheduler,
			Predictor:          r.predictor,
			ImageMap:           e.ImageMap,
			RunID:              r.RunID,
			DotWriter:          r.DotWriter,
		},
		Type:    e.MainType(),
		Labels:  r.labels,
		Cmdline: r.cmdline,
	}

	if err = r.RunConfig.RunFlags.Configure(&run.EvalConfig); err != nil {
		return runner.State{}, err
	}
	run.ID = r.RunID
	run.Program = e.Program
	run.Params = e.Params
	run.Args = e.Args
	if r.RunConfig.RunFlags.Trace {
		run.Trace = r.Log
	}
	var base string
	if base, err = r.Runbase(); err != nil {
		return runner.State{}, err
	}
	stateFile, err := state.Open(base)
	if err != nil {
		return runner.State{}, errors.E("failed to open state file: %v", err)
	}
	if err = stateFile.Marshal(run.State); err != nil {
		return runner.State{}, errors.E("failed to marshal state: %v", err)
	}
	r.wg = new(wg.WaitGroup)
	ctx, bgcancel := flow.WithBackground(ctx, r.wg)

	for ok := true; ok; {
		ok = run.Do(ctx)
		if run.State.Phase == runner.Retry {
			r.Log.Printf("runner state 'Retry': %s", run.State)
		}
		if err = stateFile.Marshal(run.State); err != nil {
			r.Log.Errorf("failed to marshal state: %v", err)
		}
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if tdb != nil {
			if errTDB := r.setRunComplete(tctx, tdb, run.State.Completion); errTDB != nil {
				r.Log.Debugf("error writing run result to taskdb: %v", errTDB)
			}
		}
	}()

	if run.Err != nil {
		r.Log.Error(run.Err)
	} else {
		r.Log.Printf("result: %s", run.Result)
	}
	r.waitForBackgroundTasks(r.RunConfig.RunFlags.BackgroundTimeout)
	bgcancel()
	return run.State, nil
}

// GetRunID is a getter for the runID associated with the runner.
func (r *runnerImpl) GetRunID() taskdb.RunID {
	return r.RunID
}

func (r *runnerImpl) setRunComplete(ctx context.Context, tdb taskdb.TaskDB, endTime time.Time) error {
	var (
		runLog, dotFile, trace digest.Digest
		rc                     io.ReadCloser
	)
	if runbase, err := r.Runbase(); err == nil {
		if rc, err = os.Open(runbase + ".runlog"); err == nil {
			pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if runLog, err = r.repo.Put(pctx, rc); err != nil {
				r.Log.Debugf("put runlog in repo %s: %v", r.repo.URL(), err)
			}
			cancel()
			_ = rc.Close()
		}
		if rc, err = os.Open(runbase + ".gv"); err == nil {
			pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if dotFile, err = r.repo.Put(pctx, rc); err != nil {
				r.Log.Debugf("put dotfile in repo %s: %v", r.repo.URL(), err)
			}
			cancel()
			_ = rc.Close()
		}
		if rc, err = os.Open(runbase + ".trace"); err == nil {
			// this file will only exist if the localtracer is used
			pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if trace, err = r.repo.Put(pctx, rc); err != nil {
				r.Log.Debugf("put trace file in repo %s: %v", r.repo.URL(), err)
			}
			cancel()
			_ = rc.Close()
		}
	} else {
		r.Log.Debugf("unable to determine runbase: %v", err)
	}
	err := tdb.SetRunComplete(ctx, r.RunID, runLog, dotFile, trace, endTime)
	if err == nil {
		var ds []string
		if !runLog.IsZero() {
			ds = append(ds, fmt.Sprintf("runLog: %s", runLog.Short()))
		}
		if !dotFile.IsZero() {
			ds = append(ds, fmt.Sprintf("evalGraph: %s", dotFile.Short()))
		}
		if !trace.IsZero() {
			ds = append(ds, fmt.Sprintf("trace: %s", trace.Short()))
		}
		r.Log.Debugf("Saved all logs in task db %s", strings.Join(ds, ", "))
	}
	return err
}

// UploadBundle generates a bundle and updates taskdb with its digest. If the bundle does not already exist in taskdb,
// uploadBundle caches it.
func (r *runnerImpl) uploadBundle(ctx context.Context, tdb taskdb.TaskDB, runID taskdb.RunID, bundle *syntax.Bundle, file string, args []string) error {
	var (
		repo     = tdb.Repository()
		bundleId digest.Digest
		rc       io.ReadCloser
		err      error
		tmpName  string
	)

	if ext := filepath.Ext(file); ext == ".rfx" {
		rc, bundleId, err = getBundle(file)
	} else {
		rc, bundleId, tmpName, err = makeBundle(bundle)
		if err == nil {
			defer os.Remove(tmpName)
		}
	}
	if err != nil {
		return err
	}
	defer rc.Close()

	if _, err = repo.Stat(ctx, bundleId); errors.Is(errors.NotExist, err) {
		bundleId, err = repo.Put(ctx, rc)
		if err != nil {
			return err
		}
	}
	r.Log.Debugf("created bundle %s with args: %v\n", bundleId.String(), args)
	return tdb.SetRunAttrs(ctx, runID, bundleId, args)
}

// waitForBackgroundTasks waits until all background tasks complete, or if the provided
// timeout expires.
func (r runnerImpl) waitForBackgroundTasks(timeout time.Duration) {
	waitc := r.wg.C()
	select {
	case <-waitc:
	default:
		n := r.wg.N()
		if n == 0 {
			return
		}
		r.Log.Debugf("waiting for %d background tasks to complete", n)
		select {
		case <-waitc:
		case <-time.After(timeout):
			r.Log.Errorf("some cache writes still pending after timeout %v", timeout)
		}
	}
}

func (r runnerImpl) Runbase() (string, error) {
	return reflow.Runbase(digest.Digest(r.RunID))
}
