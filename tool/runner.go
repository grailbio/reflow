package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/state"

	"github.com/aws/aws-sdk-go/aws/session"
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
	reflowinfra "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/repository/blobrepo"
	repositoryhttp "github.com/grailbio/reflow/repository/http"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/sched"
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
	if cluster, err = clusterInstance(runConfig.Config, runConfig.Status); err != nil {
		return nil, err
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
	}

	var assoc assoc.Assoc
	if err = runConfig.Config.Instance(&assoc); err != nil {
		return nil, err
	}
	var cache *reflowinfra.CacheProvider
	if err = runConfig.Config.Instance(&cache); err != nil {
		return nil, err
	}
	assertionGenerator, err := assertionGenerator(runConfig.Config)
	if err != nil {
		return nil, err
	}
	return &Runner{
		runConfig:          runConfig,
		RunID:              taskdb.NewRunID(),
		scheduler:          scheduler,
		schedCancel:        schedCancel,
		assoc:              assoc,
		cache:              cache,
		assertionGenerator: assertionGenerator,
		log:                logger,
		wg:                 &wg,
		repo:               repo,
		mux:                mux,
		transferer:         transferer,
		cluster:            cluster,
	}, nil
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
	// RunId is the unique id for this run.
	RunID              taskdb.RunID
	log                *log.Logger
	runConfig          RunConfig
	scheduler          *sched.Scheduler
	schedCancel        context.CancelFunc
	assertionGenerator reflow.AssertionGenerator
	assoc              assoc.Assoc
	cache              *reflowinfra.CacheProvider
	tdb                taskdb.TaskDB
	cluster            runner.Cluster
	cmdline            string
	wg                 *wg.WaitGroup
	repo               reflow.Repository
	mux                blob.Mux
	transferer         reflow.Transferer
}

// Go runs the reflow program. It returns a non nil error if did not succeed.
func (r *Runner) Go(ctx context.Context) (runner.State, error) {
	if err := r.runConfig.RunFlags.Err(); err != nil {
		return runner.State{}, err
	}
	var labels pool.Labels
	err := r.runConfig.Config.Instance(&labels)
	if err != nil {
		r.log.Error(err)
	}
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
	run := runner.Runner{
		Flow: e.Main(),
		EvalConfig: flow.EvalConfig{
			Log:                r.log,
			Repository:         r.repo,
			Snapshotter:        r.mux,
			Assoc:              r.assoc,
			AssertionGenerator: r.assertionGenerator,
			CacheMode:          r.cache.CacheMode,
			Transferer:         r.transferer,
			Status:             r.runConfig.Status.Group(r.RunID.IDShort()),
			Scheduler:          r.scheduler,
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
		run.Trace = r.log
	}
	if r.runConfig.RunFlags.Alloc != "" {
		run.AllocID = r.runConfig.RunFlags.Alloc
		run.Phase = runner.Eval
	}
	// Set up run transcript and log files.
	base, err := r.runbase()
	if err != nil {
		return runner.State{}, err
	}
	if err = os.MkdirAll(filepath.Dir(base), 0777); err != nil {
		return runner.State{}, err
	}

	statefile, err := state.Open(base)
	if err != nil {
		return runner.State{}, errors.E("failed to open state file: %v", err)
	}
	if err = statefile.Marshal(run.State); err != nil {
		return runner.State{}, errors.E("failed to marshal state: %v", err)
	}

	ctx, bgcancel := flow.WithBackground(ctx, r.wg)

	for ok := true; ok; {
		ok = run.Do(ctx)
		if run.State.Phase == runner.Retry {
			r.log.Printf("retrying error %v", run.State.Err)
		}
		r.log.Debugf("run state: %s\n", run.State)
		if err = statefile.Marshal(run.State); err != nil {
			r.log.Errorf("failed to marshal state: %v", err)
		}
	}

	if run.Err != nil {
		r.log.Error(run.Err)
	} else {
		r.log.Print(run.Result)
	}
	if r.schedCancel != nil {
		r.schedCancel()
	}
	r.waitForBackgroundTasks(10 * time.Minute)
	bgcancel()
	return run.State, nil
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
		r.log.Debugf("waiting for %d background tasks to complete", n)
		select {
		case <-waitc:
		case <-time.After(timeout):
			r.log.Errorf("some cache writes still pending after timeout %v", timeout)
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
func (r Runner) runbase() (string, error) {
	rundir, err := r.rundir()
	if err != nil {
		return "", err
	}
	return filepath.Join(rundir, digest.Digest(r.RunID).Hex()), nil
}
