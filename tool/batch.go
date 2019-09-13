// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/batch"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/wg"
)

type batchConfig struct {
	configFilepath string
}

func (bc *batchConfig) Flags(fs *flag.FlagSet) {
	fs.StringVar(&bc.configFilepath, "batchconfig", "config.json", "path to the config file (default 'config.json'")
}

func (bc *batchConfig) Configure(b *batch.Batch) {
	b.Dir = filepath.Dir(bc.configFilepath)
	b.ConfigFilename = filepath.Base(bc.configFilepath)
}

func (c *Cmd) batchrun(ctx context.Context, args ...string) {
	c.Fatal("command batchrun has been renamed runbatch")
}
func (c *Cmd) runbatch(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("runbatch", flag.ExitOnError)
	help := `Runbatch runs the batch defined in this directory.

A batch is defined by a directory with a batch configuration file named
config.json, which stores a single JSON dictionary with two entries,
defining the paths of the reflow program to be used and the run file
that contains each run's parameter. For example, the config.json file

	{
		"program": "pipeline.rf",
		"runs_file": "samples.csv"
	}

specifies that batch should run "pipeline.reflow" with the parameters
specified in each row of "samples.csv".

The runs file must contain a header naming its columns. Its first
column must be named "id" and contain the unique identifier of each
run. The other columns name the parameters to be used for each reflow
run. Unnamed columns are used as arguments to the run. For example,
if the above "pipeline.reflow" specified parameters "bam" and
"sample", then the following CSV defines that three runs are part of
the batch: bam=1.bam,sample=a; bam=2.bam,sample=b; and
bam=3.bam,sample=c.

	id,bam,sample
	1,1.bam,a
	2,2.bam,b
	3,3.bam,c

Reflow deposits individual log files into the working directory for
each run in the batch. These are in addition to the standard log
files that are peristed for runs, and are always logged at the debug
level.

Remaining arguments are passed on as parameters to all runs; these
flags override any parameters in the batch sample file.
`
	retryFlag := flags.Bool("retry", false, "retry failed runs")
	resetFlag := flags.Bool("reset", false, "reset failed runs")
	idsFlag := flags.String("ids", "", "comma-separated list of ids to run; an empty list runs all")
	var bc batchConfig
	bc.Flags(flags)
	var config commonRunConfig
	config.Flags(flags)
	c.Parse(flags, args, help, "runbatch [-retry] [-reset] [flags]")

	if err := config.Err(); err != nil {
		c.Errorln(err)
		flags.Usage()
	}
	var user *infra.User
	err := c.Config.Instance(&user)
	if err != nil {
		c.Fatal(err)
	}
	cluster := c.Cluster(c.Status.Group("ec2cluster"))
	var repo reflow.Repository
	err = c.Config.Instance(&repo)
	if err != nil {
		c.Fatal(err)
	}
	var assoc assoc.Assoc
	err = c.Config.Instance(&assoc)
	if err != nil {
		c.Fatal(err)
	}
	transferer := &repository.Manager{
		Status:           c.Status.Group("transfers"),
		PendingTransfers: repository.NewLimits(c.TransferLimit()),
		Stat:             repository.NewLimits(statLimit),
		Log:              c.Log,
	}
	if repo != nil {
		transferer.PendingTransfers.Set(repo.URL().String(), int(^uint(0)>>1))
	}
	wd, err := os.Getwd()
	if err != nil {
		c.Log.Error(err)
	}
	var cache *infra.CacheProvider
	err = c.Config.Instance(&cache)
	if err != nil {
		c.Log.Error(err)
	}
	b := &batch.Batch{
		EvalConfig: flow.EvalConfig{
			Log:                c.Log,
			Snapshotter:        c.blob(),
			Repository:         repo,
			Assoc:              assoc,
			AssertionGenerator: c.assertionGenerator(),
			CacheMode:          cache.CacheMode,
			Transferer:         transferer,
		},
		Args:    flags.Args(),
		Rundir:  c.rundir(),
		User:    string(*user),
		Cluster: cluster,
		Status:  c.Status.Groupf("batch %s", wd),
	}
	config.Configure(&b.EvalConfig, c)
	bc.Configure(b)
	if err := b.Init(*resetFlag); err != nil {
		c.Fatal(err)
	}
	defer b.Close()
	if *idsFlag != "" {
		list := strings.Split(*idsFlag, ",")
		ids := make(map[string]bool)
		for _, id := range list {
			ids[id] = true
		}
		for id := range b.Runs {
			if !ids[id] {
				delete(b.Runs, id)
			}
		}
	}
	if *retryFlag {
		for id, run := range b.Runs {
			var retry bool
			switch run.State.Phase {
			case runner.Init, runner.Eval:
				continue
			case runner.Done, runner.Retry:
				retry = run.State.Err != nil
			}
			if !retry {
				continue
			}
			c.Errorf("retrying run %v\n", id)
			run.State.Reset()
		}
	}
	var wg wg.WaitGroup
	ctx, bgcancel := flow.WithBackground(ctx, &wg)
	err = b.Run(ctx)
	if err != nil {
		c.Log.Errorf("batch failed with error %v", err)
	}
	c.WaitForBackgroundTasks(&wg, 20*time.Minute)
	bgcancel()
	if err != nil {
		c.Exit(1)
	}
}

func (c *Cmd) batchinfo(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("batchinfo", flag.ExitOnError)
	var bc batchConfig
	bc.Flags(flags)
	help := `Batchinfo displays runtime information for the batch in the current directory.
See runbatch -help for information about Reflow's batching mechanism.`
	c.Parse(flags, args, help, "batchinfo")
	if flags.NArg() != 0 {
		flags.Usage()
	}

	var b batch.Batch
	b.Rundir = c.rundir()
	bc.Configure(&b)
	if err := b.Init(false); err != nil {
		c.Fatal(err)
	}
	defer b.Close()
	ids := make([]string, len(b.Runs))
	i := 0
	for id := range b.Runs {
		ids[i] = id
		i++
	}
	sort.Strings(ids)
	var tw tabwriter.Writer
	tw.Init(c.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()

	for _, id := range ids {
		run := b.Runs[id]
		fmt.Fprintf(&tw, "run %s: %s\n", id, run.State.ID.Short())
		c.printRunInfo(ctx, &tw, run.State.ID)
		fmt.Fprintf(&tw, "\tlog:\t%s\n", filepath.Join(b.Dir, "log."+id))
	}
}

func (c *Cmd) listbatch(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("listbatch", flag.ExitOnError)
	var bc batchConfig
	bc.Flags(flags)
	help := `Listbatch lists runtime status for the batch in the current directory.
See runbatch -help for information about Reflow's batching mechanism.

The columns displayed by listbatch are:

	id    the batch run ID
	run   the run's name
	state the run's state`
	c.Parse(flags, args, help, "listbatch")
	if flags.NArg() != 0 {
		flags.Usage()
	}

	var b batch.Batch
	b.Rundir = c.rundir()
	bc.Configure(&b)
	if err := b.Init(false); err != nil {
		c.Fatal(err)
	}
	defer b.Close()
	ids := make([]string, len(b.Runs))
	i := 0
	for id := range b.Runs {
		ids[i] = id
		i++
	}
	sort.Strings(ids)
	var tw tabwriter.Writer
	tw.Init(c.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()

	for _, id := range ids {
		run := b.Runs[id]
		var state string
		switch run.State.Phase {
		case runner.Init:
			state = "waiting"
		case runner.Eval:
			state = "running"
		case runner.Retry:
			state = "retrying"
		case runner.Done:
			if err := run.State.Err; err != nil {
				state = errors.Recover(err).ErrorSeparator(": ")
			} else {
				state = "done"
			}
		}
		fmt.Fprintf(&tw, "%s\t%s\t%s\n", id, run.State.ID.Short(), state)
	}
}

type inlineOrFileSystemSourcer map[string][]byte

func (s inlineOrFileSystemSourcer) Source(path string) ([]byte, error) {
	p, ok := s[path]
	if !ok {
		var err error
		p, err = syntax.Filesystem.Source(path)
		if err != nil {
			return nil, fmt.Errorf("module %s: %v", path, err)
		}
	}
	return p, nil
}

var moduleTemplate = template.Must(template.New("module").Parse(`// This file was automatically generated by {{.user}} at {{.time}}
//	{{.dir}} $ {{.command}}

// Main computes the batch runs as specificed in the file:
//	{{.runs}}
{{$prog := .prog}}val Main = [{{range $_, $entry := .entries }}
	make({{$prog | printf "%q"}},{{range $key, $value := $entry }}
		{{$key}} := {{$value}},{{end}}
	).Main,{{end}}
]
`))

func (c *Cmd) genbatch(ctx context.Context, args ...string) {
	var (
		flags   = flag.NewFlagSet("genbatch", flag.ExitOnError)
		bc      batchConfig
		outFlag = flags.String("o", "", "output module path")
		help    = `Genbatch generates a single Reflow program that's equivalent to the
batch configuration present in the working directory from which the
command is run. (See reflow runbatch -help for details.) Programs
generates by reflow genbatch should be run with the scalable
scheduler:

	$ reflow run -sched batch.rf
`
	)
	bc.Flags(flags)
	c.Parse(flags, args, help, "genbatch [-o module.rf]")
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	p, err := ioutil.ReadFile(bc.configFilepath)
	if err != nil {
		log.Fatal(err)
	}
	var config struct {
		Program string `json:"program"`
		Runs    string `json:"runs_file"`
	}
	if err := json.Unmarshal(p, &config); err != nil {
		log.Fatal(err)
	}
	configDir := filepath.Dir(bc.configFilepath)
	config.Program = filepath.Join(configDir, config.Program)
	config.Runs = filepath.Join(configDir, config.Runs)

	sess := syntax.NewSession(nil)
	m, err := sess.Open(config.Program)
	if err != nil {
		log.Fatalf("open %s: %v", config.Program, err)
	}

	params := make(map[string]syntax.Param)
	for _, p := range m.Params() {
		params[p.Ident] = p
	}

	runsPath, err := filepath.Abs(config.Runs)
	if err != nil {
		log.Fatal(err)
	}
	runs, err := os.Open(runsPath)
	if err != nil {
		log.Fatal(err)
	}
	defer runs.Close()
	r := csv.NewReader(runs)
	header, err := r.Read()
	if err != nil {
		log.Fatal(err)
	}
	if len(header) < 2 {
		log.Fatal("expected at least two header fields")
	}
	header = header[1:]
	for _, h := range header {
		p, ok := params[h]
		if !ok {
			log.Fatalf("parameter %s is not defined in module %s", h, config.Program)
		}
		switch p.Type.Kind {
		case types.StringKind, types.IntKind, types.FloatKind, types.BoolKind, types.FileKind, types.DirKind:
		default:
			log.Fatalf("parameter %s: unsupported type %s", h, p.Type)
		}
	}

	var entries []map[string]string
	for nline := 0; ; nline++ {
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		if len(record) != len(header)+1 {
			log.Fatalf("%s:%d: length of record (%d) does not match length of header (%d)",
				config.Runs, nline, len(record), len(header))
		}
		record = record[1:]
		entry := make(map[string]string)
		for i, h := range header {
			p := params[h]
			switch p.Type.Kind {
			case types.StringKind:
				entry[h] = fmt.Sprintf("%q", record[i])
			case types.IntKind, types.FloatKind, types.BoolKind:
				entry[h] = record[i]
			case types.FileKind:
				entry[h] = fmt.Sprintf("file(%q)", record[i])
			case types.DirKind:
				entry[h] = fmt.Sprintf("dir(%q)", record[i])
			default:
				panic(h)
			}
		}
		entries = append(entries, entry)
	}

	var b bytes.Buffer
	err = moduleTemplate.Execute(&b, map[string]interface{}{
		"entries": entries,
		"prog":    config.Program,
		"user":    os.Getenv("USER"),
		"time":    time.Now().Local().Format(time.RFC822),
		"dir":     cwd,
		"command": strings.Join(os.Args, " "),
		"runs":    runsPath,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Perform a typecheck to ensure that the program is well-formed.
	prog, err := ioutil.ReadFile(config.Program)
	if err != nil {
		log.Fatal(err)
	}
	sess = syntax.NewSession(inlineOrFileSystemSourcer{
		"<stdout>.rf":  b.Bytes(),
		config.Program: prog,
	})
	if _, err = sess.Open("<stdout>.rf"); err != nil {
		if _, err := io.Copy(os.Stdout, &b); err != nil {
			log.Fatal(err)
		}
		log.Fatalf("invalid batch: %v", err)
	}

	if *outFlag != "" {
		f, err := os.Create(*outFlag)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := io.Copy(f, &b); err != nil {
			log.Fatal(err)
		}
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	} else {
		if _, err := io.Copy(os.Stdout, &b); err != nil {
			log.Fatal(err)
		}
	}

}
