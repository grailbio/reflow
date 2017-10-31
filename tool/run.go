// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	golog "log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	dockerclient "github.com/docker/engine-api/client"
	"github.com/grailbio/base/state"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/cache"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/ctxwg"
	"github.com/grailbio/reflow/internal/ec2authenticator"
	"github.com/grailbio/reflow/internal/iputil"
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
	localDir      string
	dir           string
	local         bool
	hybrid        string
	alloc         string
	gc            bool
	trace         bool
	resources     reflow.Resources
	resourcesFlag string
	cache         bool
	nocacheextern bool
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
}

func (r *runConfig) Err() error {
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
	//
	// Note that we could use the hash of the computed flow as well, and
	// (re-map) runs back to old states, but this might be a little confusing
	// to users.
	user, err := c.Config.User()
	if err != nil {
		log.Fatal(err)
	}
	runName := runner.Name{
		User: user,
		ID:   reflow.Digester.Rand(),
	}
	c.Log.Printf("run name: %s", runName.Short())

	// Set up run transcript and log files.
	base := c.runbase(runName)
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

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if config.local || config.hybrid != "" {
		c.runLocal(ctx, config, execLogger, runName, er.Flow, er.Type)
		return
	}

	// Default case: execute on cluster with shared cache.
	// TODO: get rid of profile here
	cluster := c.cluster()
	rcache, err := c.Config.Cache()
	if err != nil {
		c.Fatal(err)
	}
	transferer := &repository.Manager{
		Log:          c.Log.Tee(nil, "transferer: "),
		PendingBytes: repository.NewLimits(transferLimit),
		Stat:         repository.NewLimits(statLimit),
	}
	if cache, ok := rcache.(*cache.Cache); ok {
		transferer.PendingBytes.Set(cache.Repository.URL().String(), int(^uint(0)>>1))
		cache.Transferer = transferer
	}
	if c.Log.At(log.DebugLevel) {
		go transferer.Report(ctx, time.Minute)
	}
	run := runner.Runner{
		Flow:          er.Flow,
		Log:           execLogger,
		Cache:         rcache,
		NoCacheExtern: config.nocacheextern,
		GC:            config.gc,
		Transferer:    transferer,
		Type:          er.Type,
		Labels:        make(pool.Labels),
		Cluster:       cluster,
	}
	run.Name = runName
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
	var wg ctxwg.WaitGroup
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
	ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
	c.Log.Debugf("waiting for cache writes to complete")
	if err := wg.Wait(ctx); err != nil {
		c.Log.Errorf("some cache writes still pending: %v", err)
	}
	bgcancel()
	cancel()
	if run.Err != nil {
		if errors.Match(errors.Eval, run.Err) {
			// Error that occured during evaluation. Probably not recoverable.
			// TODO(marius): if this was caused by an underyling exit (from a tool)
			// then propagate this here.
			os.Exit(11)
		}
		if errors.Transient(run.Err) {
			os.Exit(10)
		}
		os.Exit(1)
	}
}

func (c *Cmd) runLocal(ctx context.Context, config runConfig, execLogger *log.Logger, runName runner.Name, flow *reflow.Flow, typ *types.T) {
	client, err := dockerclient.NewClient(
		"unix:///var/run/docker.sock", "1.22", /*client.DefaultVersion*/
		nil, map[string]string{"user-agent": "reflow"})
	if err != nil {
		c.Fatal(err)
	}
	rcache, err := c.Config.Cache()
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
		Log:          c.Log.Tee(nil, "transferer: "),
		PendingBytes: repository.NewLimits(transferLimit),
		Stat:         repository.NewLimits(statLimit),
	}
	if cache, ok := rcache.(*cache.Cache); ok {
		transferer.PendingBytes.Set(cache.Repository.URL().String(), int(^uint(0)>>1))
		cache.Transferer = transferer
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
		resources.Memory = uint64(float64(info.MemTotal) * 0.95)
		resources.CPU = uint16(info.NCPU)
		resources.Disk = 1e13 // Assume 10TB. TODO(marius): real disk management
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
	eval := reflow.Eval{
		Executor:      x,
		Transferer:    transferer,
		Log:           execLogger,
		GC:            config.gc,
		Cache:         rcache,
		NoCacheExtern: config.nocacheextern,
	}
	if config.trace {
		eval.Trace = c.Log
	}
	eval.Init(flow)
	var wg ctxwg.WaitGroup
	ctx, bgcancel := reflow.WithBackground(ctx, &wg)
	if config.hybrid != "" {
		cluster := c.cluster()
		if err != nil {
			c.Fatal(err)
		}
		stealer := &runner.Stealer{
			Cache:   rcache,
			Cluster: cluster,
			Log:     c.Log.Tee(nil, "hybrid: "),
			Labels: pool.Labels{
				"Name": runName.String(),
			},
		}
		go stealer.Go(ctx, &eval)
	}
	if err = eval.Do(ctx); err != nil {
		c.Errorln(err)
		if errors.Transient(err) {
			os.Exit(10)
		}
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	c.Log.Debugf("waiting for cache writes to complete")
	if err := wg.Wait(ctx); err != nil {
		c.Log.Errorf("some cache writes still pending: %v", err)
	}
	bgcancel()
	cancel()
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
func (c Cmd) runbase(name runner.Name) string {
	return filepath.Join(c.rundir(), name.String())
}
