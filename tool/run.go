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
	"io/ioutil"
	golog "log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"time"

	dockerclient "github.com/docker/engine-api/client"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/state"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/ec2authenticator"
	"github.com/grailbio/reflow/internal/iputil"
	"github.com/grailbio/reflow/internal/wg"
	"github.com/grailbio/reflow/local"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/repository/file"
	repositoryserver "github.com/grailbio/reflow/repository/server"
	"github.com/grailbio/reflow/rest"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/types"
	"golang.org/x/net/http2"
)

const maxConcurrentStreams = 2000

type runConfig struct {
	localDir       string
	dir            string
	local          bool
	hybrid         string
	alloc          string
	gc             bool
	trace          bool
	resources      reflow.Resources
	resourcesFlag  string
	cache          bool
	nocacheextern  bool
	recomputeempty bool
	eval           string
}

func (r *runConfig) Flags(flags *flag.FlagSet) {
	flags.BoolVar(&r.local, "local", false, "execute flow on the local Docker instance")
	flags.StringVar(&r.hybrid, "hybrid", "", "execute flow in hybrid local mode; serve repository over the provided port")
	flags.StringVar(&r.localDir, "localdir", "/tmp/flow", "directory where execution state is stored in local mode")
	flags.StringVar(&r.dir, "dir", "", "directory where execution state is stored in local mode (alias for local dir for backwards compatibilty)")
	flags.StringVar(&r.alloc, "alloc", "", "use this alloc to execute program (don't allocate a fresh one)")
	flags.BoolVar(&r.gc, "gc", false, "enable garbage collection during evaluation")
	flags.BoolVar(&r.trace, "trace", false, "trace flow evaluation")
	flags.StringVar(&r.resourcesFlag, "resources", "", "override offered resources in local mode (JSON formatted reflow.Resources)")
	flags.BoolVar(&r.nocacheextern, "nocacheextern", false, "don't cache extern ops")
	flags.BoolVar(&r.recomputeempty, "recomputeempty", false, "recompute empty cache values")
	flags.StringVar(&r.eval, "eval", "topdown", "evaluation strategy")
}

func (r *runConfig) Err() error {
	switch r.eval {
	case "topdown", "bottomup":
	default:
		return fmt.Errorf("invalid evaluation strategy %s", r.eval)
	}
	if r.local || r.hybrid != "" {
		if r.alloc != "" {
			return errors.New("-alloc cannot be used in local mode")
		}
		if r.resourcesFlag != "" {
			if err := json.Unmarshal([]byte(r.resourcesFlag), &r.resources); err != nil {
				return fmt.Errorf("-resources: %s", err)
			}
		}
		if r.local && r.hybrid != "" {
			return errors.New("only one of -local and -hybrid can be specified at any time")
		}
	} else {
		if r.resourcesFlag != "" {
			return errors.New("-resources can only be used in local mode")
		}
	}
	return nil
}

// Configure stores the runConfig's configuration into the provided
// EvalConfig.
func (r *runConfig) Configure(c *reflow.EvalConfig) {
	c.NoCacheExtern = r.nocacheextern
	c.GC = r.gc
	c.RecomputeEmpty = r.recomputeempty
	c.BottomUp = r.eval == "bottomup"
}

func (c *Cmd) run(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	help := `Run type checks, then evaluates a Reflow program on the
cluster specified by the runtime profile. In local mode, run uses the
locally-available Docker daemon to evaluate the Reflow. In hybrid
mode, the program is evaluated using the local Docker daemon, but 
it may allocate work-stealing nodes on the configured cluster in order
to offload excess work.

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

	c.Parse(flags, args, help, "run [-local] [-hybrid addr] [flags] path [args]")
	if err := config.Err(); err != nil {
		c.Errorln(err)
		flags.Usage()
	}

	if flags.NArg() == 0 {
		flags.Usage()
	}
	er, err := c.eval(flags.Args())
	if er.V1 && config.gc {
		log.Errorf("garbage collection disabled for v1 reflows")
		config.gc = false
	}
	if err != nil {
		c.Fatal(err)
	}
	// In the case where a flow is immediate, we print the result and quit.
	if er.Flow.Op == reflow.OpVal {
		c.Println(sprintval(er.Flow.Value, er.Type))
		os.Exit(0)
	}

	// Construct a unique name for this run, used to identify this invocation
	// throughout the system.
	runID := reflow.Digester.Rand()
	c.Log.Printf("run ID: %s", runID.Short())

	// Set up run transcript and log files.
	base := c.runbase(runID)
	os.MkdirAll(filepath.Dir(base), 0777)
	execfile, err := os.Create(base + ".execlog")
	if err != nil {
		c.Fatal(err)
	}
	defer execfile.Close()
	logfile, err := os.Create(base + ".syslog")
	if err != nil {
		c.Fatal(err)
	}
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
	if c.Log.At(log.DebugLevel) {
		path, err := filepath.Abs(er.Program)
		if err != nil {
			log.Errorf("abs %s: %v", er.Program, err)
			path = er.Program
		}
		var b bytes.Buffer
		fmt.Fprintf(&b, "evaluating program %s", path)
		if len(er.Params) > 0 {
			var keys []string
			for key := range er.Params {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			fmt.Fprintf(&b, "\n\tparams:")
			for _, key := range keys {
				fmt.Fprintf(&b, "\n\t\t%s=%s", key, er.Params[key])
			}
		} else {
			fmt.Fprintf(&b, "\n\t(no params)")
		}
		if len(er.Args) > 0 {
			fmt.Fprintf(&b, "\n\targuments:")
			for _, arg := range er.Args {
				fmt.Fprintf(&b, "\n\t%s", arg)
			}
		} else {
			fmt.Fprintf(&b, "\n\t(no arguments)")
		}
		c.Log.Debug(b.String())
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if config.local || config.hybrid != "" {
		c.runLocal(ctx, config, execLogger, runID, er.Flow, er.Type)
		return
	}

	// Default case: execute on cluster with shared cache.
	// TODO: get rid of profile here
	cluster := c.cluster()
	assoc, err := c.Config.Assoc()
	if err != nil {
		c.Fatal(err)
	}
	repo, err := c.Config.Repository()
	if err != nil {
		c.Fatal(err)
	}
	transferer := &repository.Manager{
		Log:              c.Log.Tee(nil, "transferer: "),
		PendingTransfers: repository.NewLimits(c.transferLimit()),
		Stat:             repository.NewLimits(statLimit),
	}
	if repo != nil {
		transferer.PendingTransfers.Set(repo.URL().String(), int(^uint(0)>>1))
	}
	if c.Log.At(log.DebugLevel) {
		go transferer.Report(ctx, time.Minute)
	}
	run := runner.Runner{
		Flow: er.Flow,
		EvalConfig: reflow.EvalConfig{
			Log:        execLogger,
			Repository: repo,
			Assoc:      assoc,
			CacheMode:  c.Config.CacheMode(),
			Transferer: transferer,
		},
		Type:    er.Type,
		Labels:  make(pool.Labels),
		Cluster: cluster,
	}
	config.Configure(&run.EvalConfig)
	run.ID = runID
	run.Program = er.Program
	run.Params = er.Params
	run.Args = er.Args
	if config.trace {
		run.Trace = c.Log
	}
	if config.alloc != "" {
		run.AllocID = config.alloc
		run.Phase = runner.Eval
	}
	var wg wg.WaitGroup
	ctx, bgcancel := reflow.WithBackground(ctx, &wg)
	statefile, err := state.Open(base)
	if err != nil {
		c.Fatalf("failed to open state file: %v", err)
	}
	if err := statefile.Marshal(run.State); err != nil {
		c.Log.Errorf("failed to marshal state: %v", err)
	}
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
	c.waitForCacheWrites(&wg, 10*time.Minute)
	bgcancel()
	cancel()
	if run.Err != nil {
		if errors.Is(errors.Eval, run.Err) {
			// Error that occured during evaluation. Probably not recoverable.
			// TODO(marius): if this was caused by an underyling exit (from a tool)
			// then propagate this here.
			os.Exit(11)
		}
		if errors.Restartable(run.Err) {
			os.Exit(10)
		}
		os.Exit(1)
	}
}

func (c *Cmd) runLocal(ctx context.Context, config runConfig, execLogger *log.Logger, runID digest.Digest, flow *reflow.Flow, typ *types.T) {
	client, err := dockerclient.NewClient(
		"unix:///var/run/docker.sock", "1.22", /*client.DefaultVersion*/
		nil, map[string]string{"user-agent": "reflow"})
	if err != nil {
		c.Fatal(err)
	}
	repo, err := c.Config.Repository()
	if err != nil {
		c.Fatal(err)
	}
	assoc, err := c.Config.Assoc()
	if err != nil {
		c.Fatal(err)
	}
	sess, err := c.Config.AWS()
	if err != nil {
		c.Fatal(err)
	}
	creds, err := c.Config.AWSCreds()
	if err != nil {
		c.Fatal(err)
	}
	awstool, err := c.Config.AWSTool()
	if err != nil {
		c.Fatal(err)
	}
	transferer := &repository.Manager{
		Log:              c.Log.Tee(nil, "transferer: "),
		PendingTransfers: repository.NewLimits(c.transferLimit()),
		Stat:             repository.NewLimits(statLimit),
	}
	if repo != nil {
		transferer.PendingTransfers.Set(repo.URL().String(), int(^uint(0)>>1))
	}
	go transferer.Report(ctx, time.Minute)
	dir := config.localDir
	if config.dir != "" {
		dir = config.dir
	}
	x := &local.Executor{
		Client:        client,
		Dir:           dir,
		Authenticator: ec2authenticator.New(sess),
		AWSImage:      awstool,
		AWSCreds:      creds,
		Log:           c.Log.Tee(nil, "executor: "),
	}
	resources := config.resources
	if resources.IsZeroAll() {
		info, err := client.Info(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		resources = reflow.Resources{
			"mem":  math.Floor(float64(info.MemTotal) * 0.95),
			"cpu":  float64(info.NCPU),
			"disk": 1e13, // Assume 10TB. TODO(marius): real disk management
		}
	}
	x.SetResources(resources)

	if config.hybrid != "" {
		// In hybrid mode, serve the repository through an HTTP server so that it is
		// accessible to work stealers.
		x.FileRepository = &file.Repository{Root: filepath.Join(x.Prefix, x.Dir, "objects")}
		ip4, err := iputil.ExternalIP4()
		if err != nil {
			c.Fatalf("failed to retrieve the machine's IPv4 address: %s", err)
		}
		x.FileRepository.RepoURL = &url.URL{
			Scheme: "https",
			Host:   ip4.String() + config.hybrid,
		}
		http.Handle("/", rest.Handler(repositoryserver.Node{x.FileRepository}, nil))
		log.Printf("serving repository at %s", x.FileRepository.RepoURL.String())
		_, serverConfig, err := c.Config.HTTPS()
		if err != nil {
			c.Fatal(err)
		}
		httpServer := &http.Server{
			TLSConfig: serverConfig,
			Addr:      config.hybrid,
		}
		http2.ConfigureServer(httpServer, &http2.Server{
			MaxConcurrentStreams: maxConcurrentStreams,
		})
		go func() {
			log.Fatal(httpServer.ListenAndServeTLS("", ""))
		}()
	}

	if err := x.Start(); err != nil {
		log.Fatal(err)
	}
	evalConfig := reflow.EvalConfig{
		Executor:   x,
		Transferer: transferer,
		Log:        execLogger,
		Repository: repo,
		Assoc:      assoc,
		CacheMode:  c.Config.CacheMode(),
	}
	config.Configure(&evalConfig)
	if config.trace {
		evalConfig.Trace = c.Log
	}
	eval := reflow.NewEval(flow, evalConfig)
	var wg wg.WaitGroup
	ctx, bgcancel := reflow.WithBackground(ctx, &wg)
	if config.hybrid != "" {
		cluster := c.cluster()
		if err != nil {
			c.Fatal(err)
		}
		stealer := &runner.Stealer{
			Cluster: cluster,
			Log:     c.Log.Tee(nil, "hybrid: "),
			Labels: pool.Labels{
				"ID": runID.Hex(),
			},
		}
		go stealer.Go(ctx, eval)
	}
	if err = eval.Do(ctx); err != nil {
		c.Errorln(err)
		if errors.Restartable(err) {
			os.Exit(10)
		}
		os.Exit(1)
	}
	c.waitForCacheWrites(&wg, 10*time.Minute)
	bgcancel()
	if err := eval.Err(); err != nil {
		c.Errorln(err)
		os.Exit(11)
	}
	eval.LogSummary(c.Log)
	c.Println(sprintval(eval.Value(), typ))
	os.Exit(0)
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

// runbase returns the base path for the run with the provided name
func (c Cmd) runbase(id digest.Digest) string {
	return filepath.Join(c.rundir(), id.Hex())
}

func (c Cmd) waitForCacheWrites(wg *wg.WaitGroup, timeout time.Duration) {
	waitc := wg.C()
	select {
	case <-waitc:
	default:
		c.Log.Debugf("waiting for cache writes to complete")
		select {
		case <-waitc:
		case <-time.After(timeout):
			c.Log.Errorf("some cache writes still pending after timeout %v", timeout)
		}
	}
}
