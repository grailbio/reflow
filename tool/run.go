// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	golog "log"
	"os"
	"path/filepath"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	reflowinfra "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/metrics"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/runtime"
	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/trace"
	"github.com/grailbio/reflow/wg"
)

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
	var config RunFlags
	config.flags(flags)

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
	_, err := e.Run(false)
	c.must(err)
	if e.Main() == nil {
		c.Fatal("module has no Main")
	}
	// In the case where a flow is immediate, we print the result and quit.
	if e.Main().Op == flow.Val {
		c.Println(sprintval(e.Main().Value, e.MainType()))
		c.Exit(0)
	}
	c.runCommon(ctx, config, file, args)
}

// runCommon is the helper function used by run commands.
func (c *Cmd) runCommon(ctx context.Context, runFlags RunFlags, file string, args []string) {
	if runFlags.Local {
		dir := runFlags.LocalDir
		if runFlags.Dir != "" {
			dir = runFlags.Dir
		}
		var err error
		c.SchemaKeys[reflowinfra.Cluster] = fmt.Sprintf("localcluster,dir=%v", dir)
		c.Config, err = c.Schema.Make(c.SchemaKeys)
		c.must(err)
	}

	rr, err := runtime.NewRuntime(runtime.RuntimeParams{
		Config: c.Config,
		Logger: c.Log,
		Status: c.Status,
	})
	c.must(err)

	var (
		tracer trace.Tracer
		mc metrics.Client
	)
	c.must(c.Config.Instance(&tracer))
	c.must(c.Config.Instance(&mc))

	ctx, cancel := context.WithCancel(ctx)
	ctx = metrics.WithClient(trace.WithTracer(ctx, tracer), mc)
	rr.Start(ctx)

	defer cancel()

	runConfig := RunConfig{
		Program:  file,
		Args:     args,
		RunFlags: runFlags,
	}

	r, err := NewRunner(c.Config, runConfig, c.Log, rr)
	c.must(err)
	r.status = c.Status

	// Set up run transcript and log files.
	base := c.Runbase(r.RunID)
	c.must(os.MkdirAll(filepath.Dir(base), 0777))
	var (
		logfile, dotfile *os.File
	)
	if logfile, err = os.Create(base + ".runlog"); err != nil {
		c.Fatal(err)
	}
	defer logfile.Close()

	if runFlags.DotGraph {
		if dotfile, err = os.Create(base + ".gv"); err != nil {
			c.Fatal(err)
		}
		defer dotfile.Close()
	}

	runlog := golog.New(logfile, "", golog.LstdFlags)
	// Use a special logger which includes the log level for each log in the run file
	runLogger := log.NewWithLevelPrefix(runlog)
	runLogger.Parent = c.Log

	if !r.runConfig.RunFlags.Local {
		// make sure cluster logs go to the syslog.
		var ec *ec2cluster.Cluster
		if err = c.Config.Instance(&ec); err == nil {
			ec.Log.Parent = runLogger
			defer func() {
				ec.Log.Parent = nil
			}()
		}
	}
	r.Log = runLogger
	if dotfile != nil {
		r.DotWriter = dotfile
	}

	r.Log.Printf("reflow version: %s", c.version())

	var result runner.State
	result, err = r.Go(ctx)
	if err != nil {
		c.Errorln(err)
		c.Exit(1)
	}
	if result.Err != nil {
		if errors.Is(errors.Eval, result.Err) {
			// Error that occurred during evaluation. Probably not recoverable.
			// TODO(marius): if this was caused by an underyling exit (from a tool)
			// then propagate this here.
			c.Exit(11)
		}
		if errors.Restartable(result.Err) {
			c.Exit(10)
		}
		c.Exit(1)
	}
}

// rundir returns the directory that stores run state, creating it if necessary.
func (c *Cmd) rundir() string {
	rundir, err := Rundir()
	if err != nil {
		c.Fatalf("failed to create temporary directory: %v", err)
	}
	return rundir
}

// Runbase returns the base path for the run with the provided name
func (c Cmd) Runbase(runID taskdb.RunID) string {
	return Runbase(c.rundir(), runID)
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

// asserter returns a reflow.Assert based on the given name.
func asserter(name string) (reflow.Assert, error) {
	switch name {
	case "never":
		return reflow.AssertNever, nil
	case "exact":
		return reflow.AssertExact, nil
	default:
		return nil, fmt.Errorf("unknown Assert policy %s", name)
	}
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
