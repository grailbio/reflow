// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/taskdb"
	"golang.org/x/sync/errgroup"
)

const (
	runHeader                   = "runid\tuser\tstart\tend"
	taskHeader                  = "taskid\tident\tstart\tend\tduration\tstate\tmem\tcpu\tdisk\tprocs"
	taskHeaderLongWithTaskDB    = "uri/resultid\tinspect"
	taskHeaderLongWithoutTaskDB = "uri"
)

type taskInfo struct {
	taskdb.Task
	reflow.ExecInspect
}

type runInfo struct {
	taskdb.Run
	taskInfo []taskInfo
}

type execInfo struct {
	URI string
	ID  digest.Digest
	reflow.ExecInspect
	Alloc pool.AllocInspect
}

func (c *Cmd) ps(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("ps", flag.ExitOnError)
	allFlag := flags.Bool("i", false, "list inactive/dead execs")
	longFlag := flags.Bool("l", false, "show long listing")
	userFlag := flags.String("u", "", "user")
	sinceFlag := flags.String("since", "", "runs that were active since")
	allUsersFlag := flags.Bool("a", false, "show runs of all users")
	help := `Ps lists runs and tasks.

The rows displayed by ps are runs or tasks. Tasks associated with a run
are listed below the run.
The columns associated with a run:
	runid     the run id
	user      user who initiated the run

task:
	taskid        the id associated with the task
	ident         the exec identifier
	time          the exec's start time
	duration      the exec's run duration
	state         the exec's state
	mem           the amount of memory used by the exec
	cpu           the number of CPU cores used by the exec
	disk          the total amount of disk space used by the exec
	procs         the set of processes running in the exec
	uri/resultid  the uri of a running task or the result id of a completed task (long listing only)
	inspect       the inspect of a completed task (long listing only)

Ps lists only running execs for the current user by default.
It supports the following filters:
    - User: run by a specific user (-u <user>) or any user (-a)
    - Since: run that was active since some duration before now (-since <duration>). Since uses Go's
duration format. Valid time units are "h", "m", "s". e.g: "24h"

Global flags that work in both query modes:
Flag -i lists all known execs in any state. Completed execs display profile
information for memory, cpu, and disk utilization in place of live utilization.
Flag -l shows the long listing; the live exec URI for a running task and the result id
and inspect for a completed task.

Ps must contact each node in the cluster to gather exec data. If a node 
does not respond within a predefined timeout, it is skipped, and an error is
printed on the console.`
	c.Parse(flags, args, help, "ps [-i] [-l] [-a | -u <user>] [-since <time>]")
	if flags.NArg() != 0 {
		flags.Usage()
	}

	var tdb taskdb.TaskDB
	err := c.Config.Instance(&tdb)
	if tdb == nil {
		cluster := c.Cluster(nil)
		allocsCtx, allocsCancel := context.WithTimeout(ctx, 5*time.Second)
		allocs := pool.Allocs(allocsCtx, cluster, c.Log)
		allocsCancel()
		g, ctx := errgroup.WithContext(ctx)
		allocInfos := make([]pool.AllocInspect, len(allocs))
		execInfos := make([][]execInfo, len(allocs))
		for i := range allocs {
			i, alloc := i, allocs[i]
			g.Go(func() error {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				execs, err := alloc.Execs(ctx)
				if err != nil {
					c.Log.Errorf("execs %s: %v", alloc.ID(), err)
					return nil
				}
				execInfos[i] = c.execInfos(ctx, execs)
				return nil
			})
			g.Go(func() error {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				var err error
				allocInfos[i], err = alloc.Inspect(ctx)
				if err != nil {
					c.Log.Errorf("inspect %s: %v", alloc.ID(), err)
				}
				return nil
			})
		}
		g.Wait() // ignore errors

		var infos []execInfo
		for i, alloc := range allocInfos {
			if alloc.ID == "" || execInfos[i] == nil {
				continue // e.g., because it timed out
			}
			for _, info := range execInfos[i] {
				if *allFlag || info.State == "running" || info.State == "initializing" {
					info.Alloc = alloc
					infos = append(infos, info)
				}
			}
		}
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Created.Before(infos[j].Created)
		})
		var tw tabwriter.Writer
		tw.Init(c.Stdout, 4, 4, 1, ' ', 0)
		defer tw.Flush()
		fmt.Fprint(&tw, taskHeader)
		if *longFlag {
			fmt.Fprint(&tw, "\t", taskHeaderLongWithoutTaskDB)
		}
		fmt.Fprint(&tw, "\n")
		for _, info := range infos {
			var layout = time.Kitchen
			switch dur := time.Since(info.Created); {
			case dur > 7*24*time.Hour:
				layout = "2Jan06"
			case dur > 24*time.Hour:
				layout = "Mon3:04PM"
			}
			var procs string
			switch info.Config.Type {
			case "exec":
				if len(info.Commands) == 0 {
					procs = "[exec]"
				} else {
					ncmd := make(map[string]int)
					for _, proc := range info.Commands {
						// Pick the first token as representative.
						c := strings.SplitN(proc, " ", 2)[0]
						c = path.Base(c)
						// Skip bash, it runs everywhere.
						if c == "bash" {
							continue
						}
						ncmd[c]++
					}
					cmds := make([]string, 0, len(ncmd))
					for cmd, n := range ncmd {
						if n > 1 {
							cmd += fmt.Sprintf("(%d)", n)
						}
						cmds = append(cmds, cmd)
					}
					procs = strings.Join(cmds, ",")
				}
			default:
				procs = "[" + info.Config.Type + "]"
			}
			var mem, cpu, disk float64
			switch info.State {
			case "running":
				mem = info.Gauges["mem"]
				cpu = info.Gauges["cpu"]
				disk = info.Gauges["disk"] + info.Gauges["tmp"]
			case "complete":
				mem = info.Profile["mem"].Max
				cpu = info.Profile["cpu"].Mean
				// This is a conservative estimate--we don't keep track of total max.
				disk = info.Profile["disk"].Max + info.Profile["tmp"].Max
			}
			runtime := info.Runtime()
			fmt.Fprintf(&tw, "%s\t%s\t%s\t%d:%02d\t%s\t%s\t%.1f\t%s\t%s",
				info.ID.Short(), info.Config.Ident,
				info.Created.Local().Format(layout),
				int(runtime.Hours()),
				int(runtime.Minutes()-60*runtime.Hours()),
				info.State,
				data.Size(mem), cpu, data.Size(disk),
				procs,
			)
			if *longFlag {
				fmt.Fprint(&tw, "\t", info.URI)
			}
			fmt.Fprint(&tw, "\n")
		}
		return
	}

	var q taskdb.RunQuery
	var user *infra.User
	err = c.Config.Instance(&user)
	if err != nil {
		c.Log.Debug(err)
	}
	if *userFlag != "" && *allUsersFlag {
		flags.Usage()
	}
	q.User = string(*user)
	switch {
	case *userFlag != "":
		q.User = *userFlag
	case *allUsersFlag:
		q.User = ""
	}
	q.Since = time.Now().Add(-time.Minute * 10)
	if *sinceFlag != "" {
		dur, err := time.ParseDuration(*sinceFlag)
		if err != nil {
			log.Fatalf("invalid duration %s: %s", *sinceFlag, err)
		}
		q.Since = time.Now().Add(-dur)
	}
	ri, err := c.runInfo(ctx, q, !*allFlag)
	if err != nil {
		c.Log.Debug(err)
	}
	var tw tabwriter.Writer
	tw.Init(c.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()
	c.writeRuns(ri, &tw, *longFlag)
}

func (c *Cmd) execInfos(ctx context.Context, execs []reflow.Exec) []execInfo {
	g, ctx := errgroup.WithContext(ctx)
	infos := make([]execInfo, len(execs))
	for i := range execs {
		i, exec := i, execs[i]
		g.Go(func() error {
			inspect, err := exec.Inspect(ctx)
			if err != nil {
				c.Log.Errorf("inspect %s: %v", exec.URI(), err)
			}
			infos[i] = execInfo{URI: exec.URI(), ID: exec.ID(), ExecInspect: inspect}
			return nil
		})
	}
	g.Wait()
	return infos
}

func (c *Cmd) taskInfo(ctx context.Context, q taskdb.TaskQuery, liveOnly bool) ([]taskInfo, error) {
	var tdb taskdb.TaskDB
	err := c.Config.Instance(&tdb)
	if err != nil {
		log.Fatal("taskdb: ", err)
	}
	if tdb == nil {
		log.Fatal("nil taskdb")
	}
	t, err := tdb.Tasks(ctx, q)
	if err != nil {
		log.Error(err)
	}
	ti := make([]taskInfo, len(t))
	g, gctx := errgroup.WithContext(ctx)
	for i, v := range t {
		i, v := i, v
		g.Go(func() error {
			var inspect reflow.ExecInspect
			if !v.Inspect.IsZero() {
				if liveOnly {
					return nil
				}
				inspect, err = c.reposExecInspect(gctx, v.Inspect)
				if err != nil {
					c.Log.Debug(err)
				}
			} else {
				n, err := parseName(v.URI)
				if err != nil {
					return err
				}
				// TODO(pgopal) Fix this when we can query local execs
				// Local reflow execs have URI of the form "/<exec id>
				// We don't support querying exec inspects from live local execs.
				// When we get the local running using a local reflowlet, we should make this work.
				if n.Kind != execName {
					c.Log.Debugf("error: %v is not an exec", n)
					return nil
				}
				inspect, err = c.liveExecInspect(gctx, n)
				if err != nil {
					c.Log.Debug(err)
				}
			}
			ti[i] = taskInfo{Task: v, ExecInspect: inspect}
			return nil
		})
	}
	err = g.Wait()
	b := ti[:0]
	for _, v := range ti {
		if v.Task != (taskdb.Task{}) {
			b = append(b, v)
		}
	}
	ti = b
	sort.Slice(ti, func(i, j int) bool {
		return ti[i].Start.Before(ti[j].Start)
	})
	return ti, err
}

func (c *Cmd) runInfo(ctx context.Context, q taskdb.RunQuery, liveOnly bool) ([]runInfo, error) {
	var tdb taskdb.TaskDB
	err := c.Config.Instance(&tdb)
	if err != nil {
		log.Fatal("taskdb: ", err)
	}
	if tdb == nil {
		log.Fatal("nil taskdb")
	}
	r, err := tdb.Runs(ctx, q)
	if err != nil {
		log.Error(err)
	}
	ri := make([]runInfo, len(r))
	g, gctx := errgroup.WithContext(ctx)
	for i, run := range r {
		i, run := i, run
		g.Go(func() error {
			qu := taskdb.TaskQuery{
				RunID: run.ID,
				Since: q.Since,
			}
			ti, err := c.taskInfo(gctx, qu, liveOnly)
			if err != nil {
				log.Debug(err)
			}
			ri[i] = runInfo{Run: run, taskInfo: ti}
			return nil
		})
	}
	err = g.Wait()
	return ri, err
}

func (c *Cmd) writeRuns(ri []runInfo, w io.Writer, longListing bool) {
	for _, run := range ri {
		if len(run.taskInfo) == 0 {
			continue
		}
		var st, et string
		layout := time.RFC822
		if t := run.Run.Start; !t.IsZero() {
			layout = format(t)
			st = t.Local().Format(layout)
		}
		if t := run.Run.End; !t.IsZero() {
			et = t.Local().Format(layout)
		}
		fmt.Fprint(w, runHeader, "\n")
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", run.Run.ID.IDShort(), run.Run.User, st, et)
		fmt.Fprint(w, "\t", taskHeader)
		if longListing {
			fmt.Fprint(w, "\t", taskHeaderLongWithTaskDB)
		}
		fmt.Fprint(w, "\n")
		for _, task := range run.taskInfo {
			if task.Task == (taskdb.Task{}) {
				continue
			}
			c.writeTask(task, w, longListing)
		}
		fmt.Fprint(w, "\n")
	}
}

// format returns an appropriate time layout for the given start time.
func format(start time.Time) string {
	var format = time.Kitchen
	switch dur := time.Since(start); {
	case dur > 7*24*time.Hour:
		format = "2Jan06"
	case dur > 24*time.Hour:
		format = "Mon3:04PM"
	}
	return format
}

func (c *Cmd) writeTask(task taskInfo, w io.Writer, longListing bool) {
	var (
		procs, ident, state string
		info                = task.ExecInspect
		layout              = format(task.Start)
		runtime             = info.Runtime()
		startTime, endTime  = task.Start, task.End
	)
	switch info.Config.Type {
	case "exec":
		ident = task.Config.Ident
		state = info.State
		if endTime.IsZero() {
			endTime = task.Start.Add(runtime)
		}
		if len(info.Commands) == 0 {
			procs = "[exec]"
		} else {
			ncmd := make(map[string]int)
			for _, proc := range info.Commands {
				// Pick the first token as representative.
				c := strings.SplitN(proc, " ", 2)[0]
				c = path.Base(c)
				// Skip bash, it runs everywhere.
				if c == "bash" {
					continue
				}
				ncmd[c]++
			}
			cmds := make([]string, 0, len(ncmd))
			for cmd, n := range ncmd {
				if n > 1 {
					cmd += fmt.Sprintf("(%d)", n)
				}
				cmds = append(cmds, cmd)
			}
			procs = strings.Join(cmds, ",")
		}
	default:
		ident = task.Ident
		if !endTime.IsZero() {
			state = "complete"
		} else {
			state = "unknown"
		}
		procs = "[" + info.Config.Type + "]"
	}
	var mem, cpu, disk float64
	switch info.State {
	case "running":
		mem = info.Gauges["mem"]
		cpu = info.Gauges["cpu"]
		disk = info.Gauges["disk"] + info.Gauges["tmp"]
	case "complete":
		mem = info.Profile["mem"].Max
		cpu = info.Profile["cpu"].Mean
		// This is a conservative estimate--we don't keep track of total max.
		disk = info.Profile["disk"].Max + info.Profile["tmp"].Max
	}
	fmt.Fprintf(w, "\t%s\t%s\t%s\t%s\t%d:%02d\t%s\t%s\t%.1f\t%s\t%s",
		task.Task.ID.IDShort(), ident,
		startTime.Local().Format(layout),
		endTime.Local().Format(layout),
		int(runtime.Hours()),
		int(runtime.Minutes()-60*runtime.Hours()),
		state,
		data.Size(mem), cpu, data.Size(disk),
		procs,
	)
	if longListing {
		if task.Task.ResultID.IsZero() {
			fmt.Fprint(w, "\t", task.Task.URI, "\tNA")
		} else {
			fmt.Fprintf(w, "\t%s\t%s", task.Task.ResultID.Short(), task.Task.Inspect.Short())
		}
	}
	fmt.Fprint(w, "\n")
}
