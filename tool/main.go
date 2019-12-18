// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package tool implements the reflow command.
package tool

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	golog "log"
	"net/http" // Global pprof handlers for all instantiations of the tool.
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sort"
	"syscall"

	"github.com/grailbio/base/status"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/flow"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"gopkg.in/yaml.v2"
)

// Func is the type of a command function.
type Func func(*Cmd, context.Context, ...string)

// Cmd holds the configuration, flag definitions, and runtime objects
// required for tool invocations.
type Cmd struct {
	// Schema is the infrastructure schema.
	Schema infra.Schema
	// SchemaKeys is the schema keys to providers,flags.
	SchemaKeys infra.Keys
	// Config must be specified.
	Config            infra.Config
	DefaultConfigFile string
	Version           string
	Variant           string

	// Commands contains the additional set of invocable commands.
	Commands map[string]Func

	// ConfigFile stores the path of the active configuration file.
	// May be overridden by the -config flag.
	ConfigFile string

	// Intro is an additional introduction printed after the standard one.
	Intro string

	// The standard output and error as defined by this command;
	// these are wrapped through a status writer so that output is
	// properly interleaved.
	Stdout, Stderr io.Writer

	// Status object for the current cmd invocation. This is used to continuously update the
	// progress of the cmd execution.
	Status *status.Status

	// BootstrapBinary stores the path of the bootstrap binary.
	BootstrapBinary string

	configFlags    map[string]*string
	httpFlag       string
	cpuProfileFlag string
	memProfileFlag string
	logFlag        string

	onexits []func()

	flags *flag.FlagSet

	Log *log.Logger
}

var commands = map[string]Func{
	"list":         (*Cmd).list,
	"ps":           (*Cmd).ps,
	"version":      (*Cmd).versionCmd,
	"run":          (*Cmd).run,
	"bundle":       (*Cmd).bundle,
	"check":        (*Cmd).check,
	"doc":          (*Cmd).doc,
	"info":         (*Cmd).info,
	"cat":          (*Cmd).cat,
	"sync":         (*Cmd).sync,
	"kill":         (*Cmd).kill,
	"logs":         (*Cmd).logs,
	"batchrun":     (*Cmd).batchrun,
	"runbatch":     (*Cmd).runbatch,
	"genbatch":     (*Cmd).genbatch,
	"batchinfo":    (*Cmd).batchinfo,
	"listbatch":    (*Cmd).listbatch,
	"ec2instances": (*Cmd).ec2instances,
	"config":       (*Cmd).config,
	"images":       (*Cmd).images,
	"rmcache":      (*Cmd).rmcache,
	"serve":        (*Cmd).serveCmd,
	"shell":        (*Cmd).shell,
	"test":         (*Cmd).test,
	"repair":       (*Cmd).repair,
	"collect":      (*Cmd).collect,
	"http":         (*Cmd).http,
	"upgrade":      (*Cmd).upgrade,
	"ec2verify":    (*Cmd).ec2verify,
}

var intro = `The reflow command helps users run Reflow programs, ExecInspect their
outputs, and query their statuses.

The command comprises a set of subcommands; the list of supported
commands can be obtained by running

	reflow -help

Each subcommand can in turn be invoked with -help, displaying its
usage and help text. For example, the following displays help for the
"run" command.

	reflow run -help

Each subcommand defines a set of (optional) flags and arguments.
Additionally, reflow defines a number of global flags. Flags must be
supplied in order: global flags after the "reflow" command; command
flags after that command's name. For example, the following turns
caching off (global) while running a reflow program in local mode:

	reflow -cache=off run -local align.rf

Reflow is configured from a single configuration file. A default
configuration is built in and may be examined by

	reflow config

Reflow may be invoked with a custom configuration by supplying the
-config flag:

	reflow -config myconfig ...

Reflow's configuration is documented by the config command:

	reflow config -help

Reflow's toplevel configuration keys may be overridden by flags. These
are: -logger, -aws, -awscreds, -awstool, -user, -https, -cache, and
-cluster. They take the same values as the configuration file: see
reflow config -help for details.`

var help = `Reflow is a tool for managing execution of Reflow programs.

Usage of reflow:
	reflow [flags] <command> [args]`

func (c *Cmd) usage(flags *flag.FlagSet) {
	fmt.Fprintln(os.Stderr, help)
	fmt.Fprintln(os.Stderr, "Reflow commands:")
	var cmds []string
	for name := range c.commands() {
		// This is an informational alias.
		if name == "batchrun" {
			continue
		}
		cmds = append(cmds, name)
	}
	sort.Strings(cmds)
	for _, name := range cmds {
		fmt.Fprintln(os.Stderr, "\t"+name)
	}
	fmt.Fprintln(os.Stderr, "Global flags:")
	flags.PrintDefaults()
	c.Exit(2)
}

// Main parses command line flags and then invokes the requested
// command. Main uses Cmd's config (and other initialization), which
// may be overridden by flag configs. It should be invoked only once,
// at the beginning of command line execution.
// The caller is expected to have parsed the flagset for us before
// calling Main.
//
// Main should only be called once.
func (c *Cmd) Main() {
	if c.Stdout == nil {
		c.Stdout = os.Stdout
	}
	if c.Stderr == nil {
		c.Stderr = os.Stderr
	}
	flags := c.Flags()
	if flags.NArg() == 0 {
		fmt.Fprintln(os.Stderr, intro)
		if c.Intro != "" {
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr, c.Intro)
		}
		c.Exit(2)
	}
	cmd := flags.Arg(0)
	fn := c.commands()[cmd]
	if fn == nil {
		flags.Usage()
	}
	var (
		level     log.Level
		logflags  int
		logprefix = "reflow: "
	)
	switch c.logFlag {
	case "off":
		level = log.OffLevel
	case "error":
		level = log.ErrorLevel
	case "info":
		level = log.InfoLevel
	case "debug":
		level = log.DebugLevel
	default:
		c.Fatalf("unrecognized log level %v", c.logFlag)
	}
	if level > log.InfoLevel {
		logflags = golog.LstdFlags
		logprefix = ""
	}

	c.Status = new(status.Status)
	http.Handle("/debug/status", status.Handler(c.Status))
	if level < log.DebugLevel {
		reporter := make(status.Reporter)
		c.Stdout = reporter.Wrap(os.Stdout)
		c.Stderr = reporter.Wrap(os.Stderr)
		go reporter.Go(os.Stderr, c.Status)
		c.onexit(reporter.Stop)
	}

	// Set the system wide logger with the same level and output
	// as the one that's threaded through Cmd.
	log.Std = log.New(golog.New(c.Stderr, logprefix, logflags), level)
	c.Log = log.Std

	// Define logs as configured by flags.
	if c.ConfigFile != "" {
		b, err := ioutil.ReadFile(c.ConfigFile)
		if err != nil && c.ConfigFile != c.DefaultConfigFile {
			c.Fatal(err)
		}
		keys := make(infra.Keys)
		if err := yaml.Unmarshal(b, keys); err != nil {
			c.Fatalf("config %v: %v", c.ConfigFile, err)
		}
		for k, v := range keys {
			c.SchemaKeys[k] = v
		}
	}
	for k, v := range c.configFlags {
		if *v == "" {
			continue
		}
		c.SchemaKeys[k] = *v
	}
	c.SchemaKeys["logger"] = fmt.Sprintf("logger,level=%v", c.logFlag)
	// Set the reflow version to always match the version of the binary, regardless of the provided configuration.
	c.SchemaKeys[infra2.Reflow] = fmt.Sprintf("reflowversion,version=%s", c.Version)
	var err error
	c.Config, err = c.Schema.Make(c.SchemaKeys)
	c.must(err)

	var (
		bootstrapimage *infra2.BootstrapImage
		dockerconfig   *infra2.DockerConfig
	)
	c.must(c.Config.Instance(&bootstrapimage))
	c.must(c.Config.Instance(&dockerconfig))

	// Set the bootstrap image to the official image for this distribution
	if ok := bootstrapimage.Set(c.BootstrapBinary); !ok {
		c.Log.Printf("using bootstrap image from config %s (instead of built-in one: %s)\n", bootstrapimage.Value(), c.BootstrapBinary)
	}

	if c.httpFlag != "" {
		go func() {
			c.Fatal(http.ListenAndServe(c.httpFlag, nil))
		}()
	}
	if c.cpuProfileFlag != "" {
		file, err := os.Create(c.cpuProfileFlag)
		c.must(err)
		pprof.StartCPUProfile(file)
		c.onexit(func() {
			pprof.StopCPUProfile()
			_ = file.Close()
		})
	}
	if c.memProfileFlag != "" {
		file, err := os.Create(c.memProfileFlag)
		c.must(err)
		c.onexit(func() {
			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(file); err != nil {
				c.Errorf("WriteHeapProfile: %v", err)
			}
			_ = file.Close()
		})
	}

	if err := increaseFDRlimit(); err != nil {
		c.Log.Errorf("Unable to increase file descriptor soft limit: %v", err)
	}

	c.Log.Debug("reflow version ", c.version())
	c.Log.Debug("bootstrap binary: ", bootstrapimage.Value())

	// Create a context and cancel it if we receive an interrupt.
	// The second interrupt we receive results in a hard exit.
	ctx, cancel := context.WithCancel(context.Background())
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	go func() {
		<-sigc
		cancel()
		c.Errorln("cleaning up...")
		<-sigc
		c.Exit(1)
	}()

	// If the command panics, we want to recover, log and exit normally.
	var perr error
	func() {
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if err, ok := v.(error); ok {
				perr = err
			} else {
				perr = fmt.Errorf("panic: %v", v)
			}
		}()
		// Note that the flag package stops parsing flags after the first
		// non-flag argument (ExecInspect.e., the first argument that does not begin
		// with "-"); thus flag.Args()[1:] contains all the flags and
		// arguments for the command in flags.Arg[0].
		fn(c, ctx, flags.Args()[1:]...)
	}()
	c.must(perr)
	c.Exit(0)
}

// Fatal formats a message in the manner of fmt.Print, prints it to
// stderr, and then exits the tool.
func (c *Cmd) Fatal(v ...interface{}) {
	fmt.Fprintln(c.Stderr, v...)
	c.Exit(1)
}

// Fatalf formats a message in the manner of fmt.Printf, prints it to
// stderr, and then exits the tool.
func (c *Cmd) Fatalf(format string, v ...interface{}) {
	fmt.Fprintf(c.Stderr, format, v...)
	fmt.Fprintln(c.Stderr)
	c.Exit(1)
}

// Errorln formats a message in the manner of fmt.Println and prints it
// to stderr.
func (c Cmd) Errorln(v ...interface{}) {
	fmt.Fprintln(c.Stderr, v...)
}

// Errorf formats a message in the manner of fmt.Printf and prints it
// to stderr.
func (c *Cmd) Errorf(format string, v ...interface{}) {
	fmt.Fprintf(c.Stderr, format, v...)
}

// Println formats a message in the manner of fmt.Println and prints
// it to stdout.
func (c *Cmd) Println(v ...interface{}) {
	fmt.Fprintln(c.Stdout, v...)
}

// Printf formats a message in the manner of fmt.Printf and prints it
// to stdout.
func (c *Cmd) Printf(format string, v ...interface{}) {
	fmt.Fprintf(c.Stdout, format, v...)
}

// Exit causes the command to exit with the provided status code.
// Exit ensures that command teardown is properly handled.
func (c *Cmd) Exit(code int) {
	for _, fn := range c.onexits {
		fn()
	}
	os.Exit(code)
}

// Flags initializes and returns the FlagSet used by this Cmd instance.
// The user should parse this flagset before invoking (*Cmd).Main, e.g.:
//
//	cmd.Flags().Parse(os.Args[1:])
func (c *Cmd) Flags() *flag.FlagSet {
	if c.flags == nil {
		c.flags = flag.NewFlagSet("reflow", flag.ExitOnError)
		c.flags.Usage = func() { c.usage(c.flags) }
		c.flags.StringVar(&flow.Universe, "universe", "", "digest namespace")
		c.flags.StringVar(&c.ConfigFile, "config", c.DefaultConfigFile, "path to configuration file; otherwise use default (builtin) config")
		c.flags.StringVar(&c.httpFlag, "http", "", "run a diagnostic HTTP server on this port")
		c.flags.StringVar(&c.cpuProfileFlag, "cpuprofile", "", "capture a CPU profile and deposit it to the provided path")
		c.flags.StringVar(&c.memProfileFlag, "memprofile", "", "capture a Memory profile and deposit it to the provided path")
		c.flags.StringVar(&c.logFlag, "log", "info", "set the log level: off, error, info, debug")
		// Add flags to override configuration.
		c.configFlags = make(map[string]*string)
		for key := range c.SchemaKeys {
			c.configFlags[key] = c.flags.String(key, "", fmt.Sprintf("override %s from config; see reflow config -help", key))
		}
	}
	return c.flags
}

func (c *Cmd) commands() map[string]Func {
	m := make(map[string]Func)
	for name, f := range commands {
		m[name] = f
	}
	for name, f := range c.Commands {
		m[name] = f
	}
	return m
}

func (c *Cmd) onexit(fn func()) {
	c.onexits = append(c.onexits, fn)
}

// increaseFDRlimit maxes out the FD soft limit.
func increaseFDRlimit() error {
	var l syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &l); err != nil {
		return err
	}
	if l.Cur == l.Max {
		// Already at soft max, nothing to do
		return nil
	}
	l.Cur = l.Max

	// The following is a workaround for this issue:
	// https://github.com/golang/go/issues/30401
	if runtime.GOOS == "darwin" && l.Cur > 24576 {
		// The max file limit is 24576, even though the max returned by
		// Getrlimit is 1<<63-1.
		l.Cur = 24576
	}

	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &l)
}
