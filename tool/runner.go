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
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/state"
	"github.com/grailbio/base/status"
	"github.com/grailbio/infra"
	"github.com/grailbio/infra/aws"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/ec2authenticator"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	reflowinfra "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/local"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/repository/blobrepo"
	repositoryhttp "github.com/grailbio/reflow/repository/http"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/trace"
	"github.com/grailbio/reflow/types"
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

// blobMux returns the configured blob muxer.
func blobMux(config infra.Config) (blob.Mux, error) {
	var sess *session.Session
	err := config.Instance(&sess)
	if err != nil {
		return nil, errors.E(errors.Fatal, err)
	}
	return blob.Mux{
		"s3": s3blob.New(sess),
	}, nil
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
	var ec *ec2cluster.Cluster
	if cerr := config.Instance(&ec); cerr == nil {
		if status != nil {
			ec.Status = status.Group("ec2cluster")
		}
		ec.Configuration = config
	} else {
		log.Printf("not a ec2cluster! : %v", cerr)
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
func NewScheduler(config infra.Config, wg *wg.WaitGroup, logger *log.Logger, status *status.Status) (*sched.Scheduler, context.CancelFunc, error) {
	var (
		err     error
		tdb     taskdb.TaskDB
		cluster runner.Cluster
		repo    reflow.Repository
		limit   int
	)
	if err = config.Instance(&logger); err != nil {
		return nil, nil, err
	}
	if err = config.Instance(&tdb); err != nil {
		if !strings.HasPrefix(err.Error(), "no providers for type taskdb.TaskDB") {
			return nil, nil, err
		}
		logger.Debug(err)
	}
	if cluster, err = clusterInstance(config, status); err != nil {
		return nil, nil, err
	}
	if err = config.Instance(&repo); err != nil {
		return nil, nil, err
	}
	if limit, err = transferLimit(config); err != nil {
		return nil, nil, err
	}
	transferer := &repository.Manager{
		Status:           status.Group("transfers"),
		PendingTransfers: repository.NewLimits(limit),
		Stat:             repository.NewLimits(statLimit),
		Log:              logger,
	}
	if repo != nil {
		transferer.PendingTransfers.Set(repo.URL().String(), int(^uint(0)>>1))
	}
	ctx := context.Background()
	scheduler := sched.New()
	scheduler.Cluster = cluster
	scheduler.Repository = repo
	scheduler.Transferer = transferer
	scheduler.Log = logger.Tee(nil, "scheduler: ")
	scheduler.TaskDB = tdb
	scheduler.ExportStats()
	mux, err := blobMux(config)
	if err != nil {
		return nil, nil, err
	}
	scheduler.Mux = mux
	schedCtx, schedCancel := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		err := scheduler.Do(schedCtx)
		if err != nil && err != schedCtx.Err() {
			logger.Printf("scheduler: %v,", err)
		}
		wg.Done()
	}()
	return scheduler, schedCancel, nil
}

// NewRunner returns a new runner that can run the given run config. If scheduler is non nil,
// it will be used for scheduling tasks.
func NewRunner(runConfig RunConfig, scheduler *sched.Scheduler, logger *log.Logger) (*Runner, error) {
	var (
		cluster     runner.Cluster
		mux         blob.Mux
		repo        reflow.Repository
		schedCancel context.CancelFunc
		err         error
		wg          wg.WaitGroup
		limit       int
	)
	if !runConfig.RunFlags.Local {
		if cluster, err = clusterInstance(runConfig.Config, runConfig.Status); err != nil {
			return nil, err
		}
	}
	if err = runConfig.Config.Instance(&repo); err != nil {
		return nil, err
	}
	if limit, err = transferLimit(runConfig.Config); err != nil {
		return nil, err
	}
	manager := &repository.Manager{
		Status:           runConfig.Status.Group("transfers"),
		PendingTransfers: repository.NewLimits(limit),
		Stat:             repository.NewLimits(statLimit),
		Log:              logger,
	}
	var transferer reflow.Transferer = manager
	if repo != nil {
		manager.PendingTransfers.Set(repo.URL().String(), int(^uint(0)>>1))
	}
	mux, err = blobMux(runConfig.Config)
	if err != nil {
		return nil, err
	}

	var predictor *sched.Predictor
	if runConfig.RunFlags.Sched {
		if scheduler != nil {
			mux = scheduler.Mux
			repo = scheduler.Repository
			transferer = scheduler.Transferer
		} else {
			scheduler, schedCancel, err = NewScheduler(runConfig.Config, &wg, logger, runConfig.Status)
			if err != nil {
				return nil, err
			}
			scheduler.Repository = repo
			scheduler.Transferer = transferer
			scheduler.Mux = mux
		}

		// The Predictor can only be used if the following conditions are true:
		// 1. The user specifies that they want to use the Predictor with the -pred=true flag.
		// 2. A repo is present for retrieving cached ExecInspects.
		// 3. A taskdb is present for querying tasks.
		// 4. The reflow config specifies minData > 0 because a prediction cannot be made
		//    with no data.
		// 5. Reflow is being run from an ec2 instance. This is because the Predictor
		//    is network-intensive and its performance will be severely hampered by a poor
		//    network connection.

		var (
			usePredictor       bool
			predFailureMessage string
		)
		if runConfig.RunFlags.Pred {
			usePredictor = true
		}

		if usePredictor && repo == nil {
			usePredictor = false
			predFailureMessage = "no repo available"
		}

		var tdb taskdb.TaskDB
		if usePredictor {
			if err = runConfig.Config.Instance(&tdb); err != nil || tdb == nil {
				usePredictor = false
				predFailureMessage = fmt.Sprintf("no taskdb available: %s", err)
			}
		}

		var (
			minData, maxInspect int
			memPercentile       float64
		)
		if usePredictor {
			var predConfig *reflowinfra.PredictorConfig
			if err = runConfig.Config.Instance(&predConfig); err != nil || predConfig == nil {
				usePredictor = false
				predFailureMessage = fmt.Sprintf("no predconfig available: %s", err)
			} else {
				minData = predConfig.MinData
				maxInspect = predConfig.MaxInspect
				memPercentile = predConfig.MemPercentile
			}
		}

		var sess *session.Session
		if usePredictor {
			if err = runConfig.Config.Instance(&sess); err != nil || sess == nil {
				usePredictor = false
				predFailureMessage = fmt.Sprintf("no session available: %s", err)
			} else if md := ec2metadata.New(sess, &aws2.Config{MaxRetries: aws2.Int(3)}); !md.Available() {
				usePredictor = false
				predFailureMessage = "reflow controller not running on ec2 instance"
			}
		}

		if usePredictor {
			predictor = sched.NewPred(repo, tdb, logger.Tee(nil, "predictor: "), minData, maxInspect, memPercentile)
		} else if !usePredictor && runConfig.RunFlags.Pred {
			logger.Errorf("error while configuring predictor: %s", predFailureMessage)
		}
	}
	runner := &Runner{
		runConfig:   runConfig,
		RunID:       taskdb.NewRunID(),
		scheduler:   scheduler,
		predictor:   predictor,
		schedCancel: schedCancel,
		Log:         logger,
		wg:          &wg,
		repo:        repo,
		mux:         mux,
		transferer:  transferer,
		cluster:     cluster,
	}
	if err := runner.initInfra(); err != nil {
		return nil, err
	}
	return runner, nil
}

// RunConfig defines all the material (configuration, program and args) for a specific run.
type RunConfig struct {
	// Program is the local path to the reflow bundle/program.
	Program string
	// Args is the arguments to be passed to the reflow program in the form of "--<argname>=<value>"
	Args []string
	// Config is the infrastructure configuration to use for this run.
	Config infra.Config
	// Status is the status printer for this run.
	Status *status.Status
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

	runConfig   RunConfig
	scheduler   *sched.Scheduler
	predictor   *sched.Predictor
	schedCancel context.CancelFunc
	cmdline     string
	wg          *wg.WaitGroup

	// infra
	repo    reflow.Repository
	assoc   assoc.Assoc
	cache   *reflowinfra.CacheProvider
	tdb     taskdb.TaskDB
	cluster runner.Cluster

	mux                blob.Mux
	transferer         reflow.Transferer
	assertionGenerator reflow.AssertionGenerator
}

func (r *Runner) initInfra() error {
	var (
		err    error
		config = r.runConfig.Config
	)
	if err = config.Instance(&r.assoc); err != nil {
		return err
	}
	if err = config.Instance(&r.cache); err != nil {
		return err
	}
	r.assertionGenerator, err = assertionGenerator(config)
	if err != nil {
		return err
	}
	err = r.runConfig.Config.Instance(&r.tdb)
	if err != nil {
		if strings.HasPrefix(err.Error(), "no providers for type taskdb.TaskDB") {
			r.Log.Debug(err)
		} else {
			return err
		}
	}
	return nil
}

// Go runs the reflow program. It returns a non nil error if did not succeed.
func (r *Runner) Go(ctx context.Context) (runner.State, error) {
	var err error
	if err = r.runConfig.RunFlags.Err(); err != nil {
		return runner.State{}, err
	}
	var labels pool.Labels
	if err = r.runConfig.Config.Instance(&labels); err != nil {
		r.Log.Error(err)
	}
	r.Log.Printf("run ID: %s", r.RunID.IDShort())
	e := Eval{
		Program: r.runConfig.Program,
		Args:    r.runConfig.Args,
	}
	if err = e.Run(); err != nil {
		return runner.State{}, err
	}
	if err = e.ResolveImages(r.runConfig.Config); err != nil {
		return runner.State{}, err
	}
	path, err := filepath.Abs(e.Program)
	if err != nil {
		r.Log.Errorf("abs %s: %v", e.Program, err)
		path = e.Program
	}
	cmdline := path
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
			cmdline += fmt.Sprintf(" -%s=%s", key, e.Params[key])
		}
	} else {
		fmt.Fprintf(&b, "\n\t(no params)")
	}
	if len(e.Args) > 0 {
		fmt.Fprintf(&b, "\n\targuments:")
		for _, arg := range e.Args {
			fmt.Fprintf(&b, "\n\t%s", arg)
			cmdline += fmt.Sprintf(" %s", arg)
		}
	} else {
		fmt.Fprintf(&b, "\n\t(no arguments)")
	}
	r.Log.Debug(b.String())

	tctx, tcancel := context.WithCancel(ctx)
	defer tcancel()
	if r.tdb != nil {
		var user *reflowinfra.User
		errTDB := r.runConfig.Config.Instance(&user)
		if errTDB != nil {
			r.Log.Debug(errTDB)
		}
		errTDB = r.tdb.CreateRun(tctx, r.RunID, string(*user))
		if errTDB != nil {
			r.Log.Debugf("error writing run to taskdb: %v", errTDB)
		} else {
			go func() { _ = taskdb.KeepRunAlive(tctx, r.tdb, r.RunID) }()
			go func() { _ = r.uploadBundle(tctx, r.repo, r.tdb, r.RunID, e, r.runConfig.Program, r.runConfig.Args) }()
		}
	}

	if r.runConfig.RunFlags.Local {
		return r.runLocal(ctx, e.Main(), e.MainType(), e.ImageMap, cmdline)
	}
	run := runner.Runner{
		Flow: e.Main(),
		EvalConfig: flow.EvalConfig{
			Log:                r.Log,
			Repository:         r.repo,
			Snapshotter:        r.mux,
			Assoc:              r.assoc,
			AssertionGenerator: r.assertionGenerator,
			CacheMode:          r.cache.CacheMode,
			Transferer:         r.transferer,
			Status:             r.runConfig.Status.Group(r.RunID.IDShort()),
			Scheduler:          r.scheduler,
			Predictor:          r.predictor,
			ImageMap:           e.ImageMap,
			TaskDB:             r.tdb,
			RunID:              r.RunID,
		},
		Type:    e.MainType(),
		Labels:  labels,
		Cluster: r.cluster,
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
	if r.runConfig.RunFlags.Alloc != "" {
		run.AllocID = r.runConfig.RunFlags.Alloc
		run.Phase = runner.Eval
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
	ctx, bgcancel := flow.WithBackground(ctx, r.wg)
	for ok := true; ok; {
		ok = run.Do(ctx)
		if run.State.Phase == runner.Retry {
			r.Log.Printf("retrying error %v", run.State.Err)
		}
		if err = stateFile.Marshal(run.State); err != nil {
			r.Log.Errorf("failed to marshal state: %v", err)
		}
	}

	if run.Err != nil {
		r.Log.Error(run.Err)
	} else {
		r.Log.Print(run.Result)
	}
	if r.schedCancel != nil {
		r.schedCancel()
	}
	r.waitForBackgroundTasks(10 * time.Minute)
	bgcancel()
	return run.State, nil
}

// GetRunID is a getter for the runID associated with the runner.
func (r *Runner) GetRunID() taskdb.RunID {
	return r.RunID
}

// UploadBundle generates a bundle and updates taskdb with its digest. If the bundle does not already exist in taskdb,
// uploadBundle caches it.
func (r *Runner) uploadBundle(ctx context.Context, repo reflow.Repository, tdb taskdb.TaskDB, runID taskdb.RunID, e Eval, file string, args []string) error {
	var (
		bundleId digest.Digest
		rc       io.ReadCloser
		err      error
		tmpName  string
	)

	if ext := filepath.Ext(file); ext == ".rfx" {
		rc, bundleId, err = getBundle(file)
	} else {
		rc, bundleId, tmpName, err = makeBundle(e.Bundle)
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

func (r *Runner) runLocal(ctx context.Context, f *flow.Flow, typ *types.T, imageMap map[string]string, cmdline string) (runner.State, error) {
	flags := r.runConfig.RunFlags
	client, resources, err := dockerClient()
	if err != nil {
		return runner.State{}, err
	}

	var sess *session.Session
	err = r.runConfig.Config.Instance(&sess)
	if err != nil {
		return runner.State{}, err
	}
	var creds *credentials.Credentials
	err = r.runConfig.Config.Instance(&creds)
	if err != nil {
		return runner.State{}, err
	}
	var awstool *aws.AWSTool
	err = r.runConfig.Config.Instance(&awstool)
	if err != nil {
		return runner.State{}, err
	}

	dir := flags.LocalDir
	if flags.Dir != "" {
		dir = flags.Dir
	}
	x := &local.Executor{
		Client:        client,
		Dir:           dir,
		Authenticator: ec2authenticator.New(sess),
		AWSImage:      string(*awstool),
		AWSCreds:      creds,
		Blob:          r.mux,
		Log:           r.Log.Tee(nil, "executor: "),
	}
	if !flags.Resources.Equal(nil) {
		resources = flags.Resources
	}
	x.SetResources(resources)
	if err = x.Start(); err != nil {
		return runner.State{}, err
	}
	var labels pool.Labels
	if err = r.runConfig.Config.Instance(&labels); err != nil {
		return runner.State{}, err
	}
	evalConfig := flow.EvalConfig{
		Executor:           x,
		Snapshotter:        r.mux,
		Transferer:         r.transferer,
		Log:                r.Log,
		Repository:         r.repo,
		Assoc:              r.assoc,
		AssertionGenerator: r.assertionGenerator,
		CacheMode:          r.cache.CacheMode,
		Status:             r.runConfig.Status.Group(r.RunID.IDShort()),
		ImageMap:           imageMap,
		TaskDB:             r.tdb,
		RunID:              r.RunID,
	}
	if err = flags.CommonRunFlags.Configure(&evalConfig); err != nil {
		return runner.State{}, err
	}
	if flags.Trace {
		evalConfig.Trace = r.Log
	}
	eval := flow.NewEval(f, evalConfig)
	ctx, bgcancel := flow.WithBackground(ctx, r.wg)
	ctx, done := trace.Start(ctx, trace.Run, f.Digest(), cmdline)
	defer done()
	traceid := trace.URL(ctx)
	if len(traceid) > 0 {
		r.Log.Printf("Trace ID: %v", traceid)
	}
	if err := eval.Do(ctx); err != nil {
		return runner.State{}, err
	}
	r.waitForBackgroundTasks(10 * time.Minute)
	bgcancel()
	var result runner.State
	if err := eval.Err(); err != nil {
		result.Err = errors.Recover(err)
	}
	result.Result = sprintval(eval.Value(), typ)
	eval.LogSummary(r.Log)
	if result.Err != nil {
		r.Log.Error(result.Err)
	} else {
		r.Log.Print(result.Result)
	}
	return result, nil
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

// rundir returns the directory that stores run state, creating it if necessary.
func (r *Runner) rundir() (string, error) {
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

// Runbase returns the base path for the run
func (r Runner) Runbase() (string, error) {
	rundir, err := r.rundir()
	if err != nil {
		return "", err
	}
	return filepath.Join(rundir, digest.Digest(r.RunID).Hex()), nil
}
