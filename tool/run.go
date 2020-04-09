// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	golog "log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"docker.io/go-docker"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/state"
	"github.com/grailbio/infra/aws"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/ec2authenticator"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/local"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/trace"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/wg"
)

const maxConcurrentStreams = 2000
const defaultFlowDir = "/tmp/flow"

type commonRunConfig struct {
	gc             bool
	nocacheextern  bool
	recomputeempty bool
	eval           string
	invalidate     string
	assert         string
}

func (r *commonRunConfig) Flags(flags *flag.FlagSet) {
	flags.BoolVar(&r.gc, "gc", false, "enable garbage collection during evaluation")
	flags.BoolVar(&r.nocacheextern, "nocacheextern", false, "don't cache extern ops")
	flags.BoolVar(&r.recomputeempty, "recomputeempty", false, "recompute empty cache values")
	flags.StringVar(&r.eval, "eval", "topdown", "evaluation strategy")
	flags.StringVar(&r.invalidate, "invalidate", "", "regular expression for node identifiers that should be invalidated")
	flags.StringVar(&r.assert, "assert", "never", "policy used to assert cached flow result compatibility (eg: never, exact)")
}

func (r *commonRunConfig) Err() error {
	switch r.eval {
	case "topdown", "bottomup":
	default:
		return fmt.Errorf("invalid evaluation strategy %s", r.eval)
	}
	if r.invalidate != "" {
		_, err := regexp.Compile(r.invalidate)
		if err != nil {
			return err
		}
	}
	return nil
}

// Configure stores the runConfig's configuration into the provided
// EvalConfig.
func (r *commonRunConfig) Configure(c *flow.EvalConfig, cmd *Cmd) {
	c.Assert = cmd.asserter(r.assert)
	c.NoCacheExtern = r.nocacheextern
	c.GC = r.gc
	c.RecomputeEmpty = r.recomputeempty
	c.BottomUp = r.eval == "bottomup"
	if r.invalidate != "" {
		re := regexp.MustCompile(r.invalidate)
		c.Invalidate = func(f *flow.Flow) bool {
			return re.MatchString(f.Ident)
		}
	}
}

type runConfig struct {
	localDir      string
	dir           string
	local         bool
	alloc         string
	trace         bool
	resources     reflow.Resources
	resourcesFlag string
	cache         bool
	sched         bool
	needAss       bool
	needRepo      bool

	common commonRunConfig
}

func (r *runConfig) Flags(flags *flag.FlagSet) {
	r.common.Flags(flags)
	flags.BoolVar(&r.local, "local", false, "execute flow on the local Docker instance")
	flags.StringVar(&r.localDir, "localdir", defaultFlowDir, "directory where execution state is stored in local mode")
	flags.StringVar(&r.dir, "dir", "", "directory where execution state is stored in local mode (alias for local dir for backwards compatibilty)")
	flags.StringVar(&r.alloc, "alloc", "", "use this alloc to execute program (don't allocate a fresh one)")
	flags.BoolVar(&r.trace, "trace", false, "trace flow evaluation")
	flags.StringVar(&r.resourcesFlag, "resources", "", "override offered resources in local mode (JSON formatted reflow.Resources)")
	flags.BoolVar(&r.sched, "sched", true, "use scalable scheduler instead of work stealing")
}

func (r *runConfig) Err() error {
	if r.local {
		r.sched = false
		if r.alloc != "" {
			return errors.New("-alloc cannot be used in local mode")
		}
		if r.resourcesFlag != "" {
			if err := json.Unmarshal([]byte(r.resourcesFlag), &r.resources); err != nil {
				return fmt.Errorf("-resources: %s", err)
			}
		}
		if r.cache {
			r.needAss = true
			r.needRepo = true
		}
	} else {
		if r.resourcesFlag != "" {
			return errors.New("-resources can only be used in local mode")
		}
		r.needAss = true
		r.needRepo = true
	}
	if r.sched && r.alloc != "" {
		return errors.New("-alloc cannot be used with -sched")
	}
	return nil
}

func (c *Cmd) run(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	help := `Run type checks, then evaluates a Reflow program on the
cluster specified by the runtime profile. In local mode, run uses the
locally-available Docker daemon to evaluate the Reflow. 

If the Reflow program has the suffix ".reflow", it is taken to use
the legacy syntax; programs with suffixes ".rf" use the modern
syntax.

Arguments that are supplied after reflow program are parsed and
passed to that program. For programs using legacy syntax, these are
used to define "param" expressions; in modern programs, these are
used to define the module's parameters.

Run transcripts are printed to standard error and are logged in
	$HOME/.reflow/runs/yyyy-mm-dd/hhmmss-progname.exec
	$HOME/.reflow/runs/yyyy-mm-dd/hhmmss-progname.log

Reflow logs abbreviated task summaries for execs, interns, and
externs. On error, or if the logging level is set to debug, the full
task state is printed together with context.

Run exits with an error code according to evaluation status. Exit
code 10 indicates a transient runtime error. Exit codes greater than
10 indicate errors during program evaluation, which are likely not
retriable.`
	var config runConfig
	config.Flags(flags)

	c.Parse(flags, args, help, "run [-local] [flags] path [args]")
	if err := config.Err(); err != nil {
		c.Errorln(err)
		flags.Usage()
	}

	if flags.NArg() == 0 {
		flags.Usage()
	}
	file, args := flags.Arg(0), flags.Args()[1:]
	e := Eval{
		InputArgs: flags.Args(),
	}
	err := c.Eval(&e)
	if e.V1 && config.common.gc {
		log.Errorf("garbage collection disabled for v1 reflows")
		config.common.gc = false
	} else if config.sched && config.common.gc {
		log.Errorf("garbage collection disabled for with scalable scheduling")
		config.common.gc = false
	}
	if err != nil {
		c.Fatal(err)
	}
	if e.Main() == nil {
		c.Fatal("module has no Main")
	}
	if !config.sched && e.Main().Requirements().Equal(reflow.Requirements{}) && e.Main().Op != flow.Val {
		c.Fatal("Main requirements unspecified; add a @requires annotation")
	}
	c.runCommon(ctx, config, e, file, args)
}

// runCommon is the helper function used by run commands.
func (c *Cmd) runCommon(ctx context.Context, config runConfig, e Eval, file string, args []string) {
	// In the case where a flow is immediate, we print the result and quit.
	if e.Main().Op == flow.Val {
		c.Println(sprintval(e.Main().Value, e.MainType()))
		c.Exit(0)
	}
	// Construct a unique name for this run, used to identify this invocation
	// throughout the system.
	runID := taskdb.NewRunID()
	c.Log.Printf("run ID: %s", runID.IDShort())
	var tdb taskdb.TaskDB
	// TODO(dnicoloau): Add setup-tasktb command to setup a
	// taskdb for reflow open source.
	err := c.Config.Instance(&tdb)
	if err != nil {
		if strings.HasPrefix(err.Error(), "no provider for type taskdb.TaskDB") {
			c.Log.Debug(err)
		} else {
			c.Fatal(err)
		}
	}
	// Set up run transcript and log files.
	base := c.Runbase(runID)
	os.MkdirAll(filepath.Dir(base), 0777)
	execfile, err := os.Create(base + ".execlog")
	c.must(err)
	defer execfile.Close()
	logfile, err := os.Create(base + ".syslog")
	c.must(err)
	defer logfile.Close()

	// execLogger is the target for exec status; we also output
	// this to the main logger's outputter. The file-based log always
	// gets debug logs.
	execLogger := c.Log.Tee(golog.New(execfile, "", golog.LstdFlags), "")
	execLogger.Level = log.DebugLevel
	// Additionally, save logs to the run's log file.
	saveOut := c.Log.Outputter
	c.Log.Outputter = log.MultiOutputter(saveOut, golog.New(logfile, "", golog.LstdFlags))
	defer func() {
		c.Log.Outputter = saveOut
	}()
	path, err := filepath.Abs(e.Program)
	if err != nil {
		log.Errorf("abs %s: %v", e.Program, err)
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
	c.Log.Debug(b.String())
	ctx, cancel := context.WithCancel(ctx)

	var tracer trace.Tracer
	c.must(c.Config.Instance(&tracer))
	ctx = trace.WithTracer(ctx, tracer)

	var cache *infra.CacheProvider
	c.must(c.Config.Instance(&cache))

	var ass assoc.Assoc
	if err = c.Config.Instance(&ass); config.needAss {
		c.must(err)
	}
	var repo reflow.Repository
	if err = c.Config.Instance(&repo); config.needRepo {
		c.must(err)
	}

	tctx, tcancel := context.WithCancel(ctx)
	defer tcancel()
	if tdb != nil {
		var user *infra.User
		errTDB := c.Config.Instance(&user)
		if errTDB != nil {
			c.Log.Debug(errTDB)
		}
		errTDB = tdb.CreateRun(tctx, runID, string(*user))
		if errTDB != nil {
			c.Log.Debugf("error writing run to taskdb: %v", errTDB)
		} else {
			go func() { _ = taskdb.KeepRunAlive(tctx, tdb, runID) }()
			go func() { _ = c.uploadBundle(tctx, repo, tdb, runID, e, file, args) }()
		}
	}

	defer cancel()
	if config.local {
		c.runLocal(ctx, config, execLogger, runID, e.Main(), e.MainType(), e.ImageMap, cmdline, ass, repo, tdb, cache)
		return
	}

	// Default case: execute on cluster with shared cache.
	// TODO: get rid of profile here
	cluster := c.Cluster(c.Status.Group("ec2cluster"))
	transferer := &repository.Manager{
		Status:           c.Status.Group("transfers"),
		PendingTransfers: repository.NewLimits(c.TransferLimit()),
		Stat:             repository.NewLimits(statLimit),
		Log:              c.Log,
	}
	if repo != nil {
		transferer.PendingTransfers.Set(repo.URL().String(), int(^uint(0)>>1))
	}
	var labels pool.Labels
	c.must(c.Config.Instance(&labels))

	var scheduler *sched.Scheduler
	var wg wg.WaitGroup
	// TODO(marius): teardown is too complicated
	var donecancel func()
	if config.sched {
		scheduler = sched.New()
		scheduler.Transferer = transferer
		scheduler.Mux = c.blob()
		scheduler.Repository = repo
		scheduler.Cluster = cluster
		scheduler.Log = c.Log.Prefix("scheduler: ")
		scheduler.MinAlloc.Max(scheduler.MinAlloc, e.Main().Requirements().Min)
		scheduler.TaskDB = tdb
		var schedctx context.Context
		schedctx, donecancel = context.WithCancel(ctx)
		wg.Add(1)
		scheduler.ExportStats()
		go func() {
			err := scheduler.Do(schedctx)
			if err != nil && err != schedctx.Err() {
				c.Log.Printf("scheduler: %v", err)
			}
			wg.Done()
		}()
	}
	run := runner.Runner{
		Flow: e.Main(),
		EvalConfig: flow.EvalConfig{
			Log:                execLogger,
			Repository:         repo,
			Snapshotter:        c.blob(),
			Assoc:              ass,
			AssertionGenerator: c.assertionGenerator(),
			CacheMode:          cache.CacheMode,
			Transferer:         transferer,
			Status:             c.Status.Group(runID.IDShort()),
			Scheduler:          scheduler,
			ImageMap:           e.ImageMap,
			TaskDB:             tdb,
			RunID:              runID,
		},
		Type:    e.MainType(),
		Labels:  make(pool.Labels),
		Cluster: cluster,
		Cmdline: cmdline,
	}
	config.common.Configure(&run.EvalConfig, c)
	run.ID = runID
	run.Program = e.Program
	run.Params = e.Params
	run.Args = e.Args
	if config.trace {
		run.Trace = c.Log
	}
	if config.alloc != "" {
		run.AllocID = config.alloc
		run.Phase = runner.Eval
	}
	statefile, err := state.Open(base)
	if err != nil {
		c.Fatalf("failed to open state file: %v", err)
	}
	if err := statefile.Marshal(run.State); err != nil {
		c.Log.Errorf("failed to marshal state: %v", err)
	}
	ctx, bgcancel := flow.WithBackground(ctx, &wg)
	for ok := true; ok; {
		ok = run.Do(ctx)
		if run.State.Phase == runner.Retry {
			c.Log.Printf("retrying error %v", run.State.Err)
		}
		c.Log.Debugf("run state: %s\n", run.State)
		if err := statefile.Marshal(run.State); err != nil {
			c.Log.Errorf("failed to marshal state: %v", err)
		}
	}
	if run.Err != nil {
		c.Errorln(run.Err)
	} else {
		c.Println(run.Result)
	}
	if donecancel != nil {
		donecancel()
	}
	c.WaitForBackgroundTasks(&wg, 10*time.Minute)
	bgcancel()
	if run.Err != nil {
		if errors.Is(errors.Eval, run.Err) {
			// Error that occured during evaluation. Probably not recoverable.
			// TODO(marius): if this was caused by an underyling exit (from a tool)
			// then propagate this here.
			c.Exit(11)
		}
		if errors.Restartable(run.Err) {
			c.Exit(10)
		}
		c.Exit(1)
	}
}

func (c *Cmd) runLocal(ctx context.Context, config runConfig, execLogger *log.Logger, runID taskdb.RunID, f *flow.Flow, typ *types.T, imageMap map[string]string, cmdline string, ass assoc.Assoc, repo reflow.Repository, tdb taskdb.TaskDB, cache *infra.CacheProvider) {
	client, resources := c.dockerClient()

	var sess *session.Session
	c.must(c.Config.Instance(&sess))

	var creds *credentials.Credentials
	c.must(c.Config.Instance(&creds))

	var awstool *aws.AWSTool
	c.must(c.Config.Instance(&awstool))

	transferer := &repository.Manager{
		Status:           c.Status.Group("transfers"),
		PendingTransfers: repository.NewLimits(c.TransferLimit()),
		Stat:             repository.NewLimits(statLimit),
		Log:              c.Log,
	}
	if repo != nil {
		transferer.PendingTransfers.Set(repo.URL().String(), int(^uint(0)>>1))
	}
	dir := config.localDir
	if config.dir != "" {
		dir = config.dir
	}
	x := &local.Executor{
		Client:        client,
		Dir:           dir,
		Authenticator: ec2authenticator.New(sess),
		AWSImage:      string(*awstool),
		AWSCreds:      creds,
		Blob:          c.blob(),
		Log:           c.Log.Tee(nil, "executor: "),
	}
	if !config.resources.Equal(nil) {
		resources = config.resources
	}
	x.SetResources(resources)

	c.must(x.Start())

	var labels pool.Labels
	if err := c.Config.Instance(&labels); err != nil {
		c.Log.Debug(err)
	}

	evalConfig := flow.EvalConfig{
		Executor:           x,
		Snapshotter:        c.blob(),
		Transferer:         transferer,
		Log:                execLogger,
		Repository:         repo,
		Assoc:              ass,
		AssertionGenerator: c.assertionGenerator(),
		CacheMode:          cache.CacheMode,
		Status:             c.Status.Group(runID.IDShort()),
		ImageMap:           imageMap,
		TaskDB:             tdb,
		RunID:              runID,
	}
	config.common.Configure(&evalConfig, c)
	if config.trace {
		evalConfig.Trace = c.Log
	}
	eval := flow.NewEval(f, evalConfig)
	var wg wg.WaitGroup
	ctx, bgcancel := flow.WithBackground(ctx, &wg)
	ctx, done := trace.Start(ctx, trace.Run, f.Digest(), cmdline)
	c.onexit(done)
	traceid := trace.URL(ctx)
	if len(traceid) > 0 {
		c.Log.Printf("Trace ID: %v", traceid)
	}
	if err := eval.Do(ctx); err != nil {
		c.Errorln(err)
		if errors.Restartable(err) {
			c.Exit(10)
		}
		c.Exit(1)
	}
	c.WaitForBackgroundTasks(&wg, 10*time.Minute)
	bgcancel()
	if err := eval.Err(); err != nil {
		c.Errorln(err)
		c.Exit(11)
	}
	eval.LogSummary(c.Log)
	c.Println(sprintval(eval.Value(), typ))
	c.Exit(0)
}

// rundir returns the directory that stores run state, creating it if necessary.
func (c *Cmd) rundir() string {
	var rundir string
	if home, ok := os.LookupEnv("HOME"); ok {
		rundir = filepath.Join(home, ".reflow", "runs")
		os.MkdirAll(rundir, 0777)
	} else {
		var err error
		rundir, err = ioutil.TempDir("", "prefix")
		if err != nil {
			c.Fatalf("failed to create temporary directory: %v", err)
		}
	}
	return rundir
}

// uploadBundle generates a bundle and updates taskdb with its digest. If the bundle does not already exist in taskdb,
// uploadBundle caches it.
func (c *Cmd) uploadBundle(ctx context.Context, repo reflow.Repository, tdb taskdb.TaskDB, runID taskdb.RunID, e Eval, file string, args []string) error {
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
	c.Log.Debugf("created bundle %s with args: %v\n", bundleId.String(), args)
	return tdb.SetRunAttrs(ctx, runID, bundleId, args)
}

// Runbase returns the base path for the run with the provided name
func (c Cmd) Runbase(runID taskdb.RunID) string {
	return filepath.Join(c.rundir(), digest.Digest(runID).Hex())
}

// WaitForBackgroundTasks waits until all background tasks complete, or if the provided
// timeout expires.
func (c Cmd) WaitForBackgroundTasks(wg *wg.WaitGroup, timeout time.Duration) {
	waitc := wg.C()
	select {
	case <-waitc:
	default:
		n := wg.N()
		if n == 0 {
			return
		}
		c.Log.Debugf("waiting for %d background tasks to complete", n)
		select {
		case <-waitc:
		case <-time.After(timeout):
			c.Log.Errorf("some cache writes still pending after timeout %v", timeout)
		}
	}
}

// AssertionGenerator returns the configured AssertionGenerator mux.
func (c Cmd) assertionGenerator() reflow.AssertionGeneratorMux {
	mux := make(reflow.AssertionGeneratorMux)
	mux[blob.AssertionsNamespace] = c.blob()
	return mux
}

// asserter returns a reflow.Assert based on the given name.
func (c Cmd) asserter(name string) reflow.Assert {
	switch name {
	case "never":
		return reflow.AssertNever
	case "exact":
		return reflow.AssertExact
	default:
		c.Fatal(fmt.Errorf("unknown assert policy %s", name))
	}
	return nil
}

// Blob returns the configured blob muxer.
func (c Cmd) blob() blob.Mux {
	var sess *session.Session
	err := c.Config.Instance(&sess)
	if err != nil {
		c.Fatal(err)
	}
	return blob.Mux{
		"s3": s3blob.New(sess),
	}
}

func (c Cmd) dockerClient() (*docker.Client, reflow.Resources) {
	addr := os.Getenv("DOCKER_HOST")
	if addr == "" {
		addr = "unix:///var/run/docker.sock"
	}
	client, err := docker.NewClient(
		addr, "1.22", /*client.DefaultVersion*/
		nil, map[string]string{"user-agent": "reflow"})
	if err != nil {
		c.Fatal(err)
	}
	info, err := client.Info(context.Background())
	if err != nil {
		c.Fatal(err)
	}
	resources := reflow.Resources{
		"mem":  math.Floor(float64(info.MemTotal) * 0.95),
		"cpu":  float64(info.NCPU),
		"disk": 1e13, // Assume 10TB. TODO(marius): real disk management
	}
	return client, resources
}

func getBundle(file string) (io.ReadCloser, digest.Digest, error) {
	dw := reflow.Digester.NewWriter()
	f, err := os.Open(file)
	if err != nil {
		return nil, digest.Digest{}, err
	}
	if _, err = io.Copy(dw, f); err != nil {
		return nil, digest.Digest{}, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, digest.Digest{}, err
	}
	return f, dw.Digest(), nil
}

func makeBundle(b *syntax.Bundle) (io.ReadCloser, digest.Digest, string, error) {
	dw := reflow.Digester.NewWriter()
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, digest.Digest{}, "", err
	}
	if err = b.WriteTo(io.MultiWriter(dw, f)); err != nil {
		return nil, digest.Digest{}, "", err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, digest.Digest{}, "", err
	}
	return f, dw.Digest(), f.Name(), nil
}
