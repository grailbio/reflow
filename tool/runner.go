// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	aws2 "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/state"
	"github.com/grailbio/base/status"
	"github.com/grailbio/infra"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/predictor"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/repository/blobrepo"
	repositoryhttp "github.com/grailbio/reflow/repository/http"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/wg"
	"golang.org/x/net/http2"
)

// TransferLimit returns the configured transfer limit.
func transferLimit(config infra.Config) (int, error) {
	lim := config.Value("transferlimit")
	if lim == nil {
		return defaultTransferLimit, nil
	}
	v, ok := lim.(int)
	if !ok {
		return 0, errors.New(fmt.Sprintf("non-integer limit %v", lim))
	}
	return v, nil
}

func awsSession(config infra.Config) (sess *session.Session, err error) {
	err = config.Instance(&sess)
	return
}

// blobMux returns the configured blob muxer.
func blobMux(config infra.Config) (blob.Mux, error) {
	if sess, err := awsSession(config); err != nil {
		return nil, errors.E(errors.Fatal, err)
	} else {
		return blob.Mux{"s3": s3blob.New(sess)}, nil
	}
}

func httpClient(config infra.Config) (*http.Client, error) {
	var ca tls.Certs
	err := config.Instance(&ca)
	if err != nil {
		return nil, err
	}
	clientConfig, _, err := ca.HTTPS()
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{TLSClientConfig: clientConfig}
	if err := http2.ConfigureTransport(transport); err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}

func clusterInstance(config infra.Config, status *status.Status) (runner.Cluster, error) {
	var cluster runner.Cluster
	err := config.Instance(&cluster)
	if err != nil {
		return nil, err
	}
	if ec, ok := cluster.(*ec2cluster.Cluster); ok {
		if status != nil {
			ec.Status = status.Group("ec2cluster")
		}
		ec.Configuration = config
	}
	var sess *session.Session
	err = config.Instance(&sess)
	if err != nil {
		return nil, err
	}
	blobrepo.Register("s3", s3blob.New(sess))
	repositoryhttp.HTTPClient, err = httpClient(config)
	if err != nil {
		return nil, err
	}
	if n, ok := cluster.(needer); ok {
		http.HandleFunc("/clusterneed", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				http.Error(w, "bad method", http.StatusMethodNotAllowed)
				return
			}
			need := n.Need()
			enc := json.NewEncoder(w)
			if err := enc.Encode(need); err != nil {
				http.Error(w, fmt.Sprintf("internal error: %v", err), http.StatusInternalServerError)
				return
			}
		})
	}
	return cluster, nil
}

// NewScheduler returns a new scheduler with the specified configuration.
// Cancelling the returned context.CancelFunc stops the scheduler.
func NewScheduler(config infra.Config, cluster runner.Cluster, logger *log.Logger) (*sched.Scheduler, error) {
	if cluster == nil {
		panic(fmt.Sprintf("NewScheduler must have cluster"))
	}
	var (
		err   error
		tdb   taskdb.TaskDB
		repo  reflow.Repository
		limit int
	)
	if logger == nil {
		if err = config.Instance(&logger); err != nil {
			return nil, err
		}
	}
	if err = config.Instance(&tdb); err != nil {
		if !strings.HasPrefix(err.Error(), "no providers for type taskdb.TaskDB") {
			return nil, err
		}
		logger.Debug(err)
	}
	if err = config.Instance(&repo); err != nil {
		return nil, err
	}
	if limit, err = transferLimit(config); err != nil {
		return nil, err
	}
	transferer := &repository.Manager{
		Status:           new(status.Status).Group("transfers"),
		PendingTransfers: repository.NewLimits(limit),
		Stat:             repository.NewLimits(statLimit),
		Log:              logger,
	}
	if repo != nil {
		transferer.PendingTransfers.Set(repo.URL().String(), int(^uint(0)>>1))
	}
	scheduler := sched.New()
	scheduler.Cluster = cluster
	scheduler.Transferer = transferer
	scheduler.Log = logger.Tee(nil, "scheduler: ")
	scheduler.TaskDB = tdb
	mux, err := blobMux(config)
	if err != nil {
		return nil, err
	}
	scheduler.Mux = mux
	return scheduler, nil
}

// NewRunner returns a new runner that can run the given run config. If scheduler is non nil,
// it will be used for scheduling tasks.
func NewRunner(infraRunConfig infra.Config, runConfig RunConfig, logger *log.Logger, scheduler *sched.Scheduler) (r *Runner, err error) {
	if scheduler == nil {
		panic(fmt.Sprintf("NewRunner must have scheduler"))
	}
	// Configure the Predictor.
	var pred *predictor.Predictor
	if runConfig.RunFlags.Pred {
		if cfg, cerr := getPredictorConfig(infraRunConfig, false); cerr != nil {
			logger.Errorf("error while configuring predictor: %s", cerr)
		} else {
			pred = predictor.New(scheduler.TaskDB, logger.Tee(nil, "predictor: "), cfg.MinData, cfg.MaxInspect, cfg.MemPercentile)
		}
	}
	var runID *taskdb.RunID
	if err = infraRunConfig.Instance(&runID); err != nil {
		return
	}
	var user *infra2.User
	if err = infraRunConfig.Instance(&user); err != nil {
		return
	}
	var repo reflow.Repository
	if err = infraRunConfig.Instance(&repo); err != nil {
		return
	}
	r = &Runner{
		runConfig: runConfig,
		RunID:     *runID,
		scheduler: scheduler,
		predictor: pred,
		Log:       logger,
		repo:      repo,
		user:      user.User(),
		status:    new(status.Status),
	}
	if r.sess, err = awsSession(infraRunConfig); err != nil {
		return
	}
	if err = infraRunConfig.Instance(&r.assoc); err != nil {
		return
	}
	if err = infraRunConfig.Instance(&r.cache); err != nil {
		return
	}
	if r.mux, err = blobMux(infraRunConfig); err != nil {
		return
	}
	if err = infraRunConfig.Instance(&r.labels); err != nil {
		return
	}
	return
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

// Runner defines a reflow program/bundle, args and configuration that can be
// evaluated to obtain a result.
type Runner struct {
	// RunID is the unique id for this run.
	RunID taskdb.RunID
	// Log is the logger to log to.
	Log *log.Logger
	// DotWriter is the writer to write the flow evaluation graph to write to, in dot format.
	DotWriter io.Writer

	runConfig RunConfig
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

	mux    blob.Mux
	status *status.Status
	user   string
}

// Go runs the reflow program. It returns a non nil error if did not succeed.
func (r *Runner) Go(ctx context.Context) (runner.State, error) {
	var err error
	if err = r.runConfig.RunFlags.Err(); err != nil {
		return runner.State{}, err
	}
	r.Log.Printf("run ID: %s", r.RunID.IDShort())
	e := Eval{
		Program: r.runConfig.Program,
		Args:    r.runConfig.Args,
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
			go func() { _ = r.uploadBundle(tctx, tdb, r.RunID, bundle, r.runConfig.Program, r.runConfig.Args) }()
		}
	}
	run := runner.Runner{
		Flow: e.Main(),
		EvalConfig: flow.EvalConfig{
			Log:                r.Log,
			Repository:         r.repo,
			Snapshotter:        r.mux,
			Assoc:              r.assoc,
			AssertionGenerator: assertionGenerator(r.mux),
			CacheMode:          r.cache.CacheMode,
			Status:             r.status.Group(r.RunID.IDShort()),
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

	if err = r.runConfig.RunFlags.Configure(&run.EvalConfig); err != nil {
		return runner.State{}, err
	}
	run.ID = r.RunID
	run.Program = e.Program
	run.Params = e.Params
	run.Args = e.Args
	if r.runConfig.RunFlags.Trace {
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
	r.waitForBackgroundTasks(r.runConfig.RunFlags.BackgroundTimeout)
	bgcancel()
	return run.State, nil
}

// GetRunID is a getter for the runID associated with the runner.
func (r *Runner) GetRunID() taskdb.RunID {
	return r.RunID
}

func (r *Runner) setRunComplete(ctx context.Context, tdb taskdb.TaskDB, endTime time.Time) error {
	var (
		runLog, dotFile, trace digest.Digest
		rc                     io.ReadCloser
	)
	runbase, err := r.Runbase()
	if err == nil {
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
	err = tdb.SetRunComplete(ctx, r.RunID, runLog, dotFile, trace, endTime)
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
func (r *Runner) uploadBundle(ctx context.Context, tdb taskdb.TaskDB, runID taskdb.RunID, bundle *syntax.Bundle, file string, args []string) error {
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
func (r Runner) waitForBackgroundTasks(timeout time.Duration) {
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

// Runbase returns the base path for the run
func (r Runner) Runbase() (string, error) {
	rundir, err := Rundir()
	if err != nil {
		return "", err
	}
	return Runbase(rundir, r.RunID), nil
}

func Rundir() (string, error) {
	var rundir string
	if home, ok := os.LookupEnv("HOME"); ok {
		rundir = filepath.Join(home, ".reflow", "runs")
		if err := os.MkdirAll(rundir, 0777); err != nil {
			return "", err
		}
	} else {
		var err error
		rundir, err = ioutil.TempDir("", "prefix")
		if err != nil {
			return "", errors.E("failed to create temporary directory: %v", err)
		}
	}
	return rundir, nil
}

func Runbase(rundir string, runID taskdb.RunID) string {
	return filepath.Join(rundir, runID.Hex())
}

// getPredictorConfig gets a PredictorConfig and/or an error (if any).
func getPredictorConfig(cfg infra.Config, nonEC2ok bool) (*infra2.PredictorConfig, error) {
	var (
		predConfig *infra2.PredictorConfig
		repo       reflow.Repository
		sess       *session.Session
		tdb        taskdb.TaskDB
	)
	if err := cfg.Instance(&tdb); err != nil {
		return nil, errors.E("predictor config: no taskdb", err)
	}
	if err := cfg.Instance(&repo); err != nil {
		return nil, errors.E("predictor config: no repo", err)
	}
	if err := cfg.Instance(&predConfig); err != nil {
		return nil, errors.E("predictor config: no predconfig", err)
	}
	if err := cfg.Instance(&sess); err != nil || sess == nil {
		return nil, errors.E("predictor config: no session", err)
	}
	return predConfig, validatePredictorConfig(sess, repo, tdb, predConfig, nonEC2ok)
}

// validatePredictorConfig validates if the Predictor can be used by reflow.
// The Predictor can only be used if the following conditions are true:
// 1. A repo is provided for retrieving cached ExecInspects.
// 2. A taskdb is present in the provided config, for querying tasks.
// 3. Reflow is being run from an ec2 instance OR either the Predictor config (using NonEC2Ok)
//    or the flag nonEC2ok gives explicit permission to run the Predictor non-ec2-instance machines.
//    This is because the Predictor is network-intensive and its performance will be hampered by poor network.
func validatePredictorConfig(sess *session.Session, repo reflow.Repository, tdb taskdb.TaskDB, predConfig *infra2.PredictorConfig, nonEC2ok bool) error {
	if tdb == nil {
		return fmt.Errorf("validatePredictorConfig: no taskdb")
	}
	if repo == nil {
		return fmt.Errorf("validatePredictorConfig: no repo")
	}
	if predConfig == nil {
		return fmt.Errorf("validatePredictorConfig: no predconfig")
	}
	if !predConfig.NonEC2Ok && !nonEC2ok {
		if md := ec2metadata.New(sess, &aws2.Config{MaxRetries: aws2.Int(3)}); !md.Available() {
			return fmt.Errorf("not running on ec2 instance (and nonEc2Ok is not true)")
		}
	}
	return nil
}

func setTransfererStatus(t reflow.Transferer, s *status.Status) {
	if m, ok := t.(*repository.Manager); ok {
		m.Status = s.Group("transfers")
	}
}
