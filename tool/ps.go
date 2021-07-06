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
	"sync"
	"text/tabwriter"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/taskdb"
	"golang.org/x/sync/errgroup"
)

const (
	allFlagValue = "_all_"
	costHelp     = `
Cost: The cost displayed is the total (cumulative) cost (as of now, if the run/task/pool is still running).
It is an exact cost if it does not have an "<" sign. and an upper-bound otherwise.
The cost can be an upper-bound if the exact cost incurred for the underlying instance is not available,
and the computation was based on the on-demand price (of the relevant underlying instance type),
which is the maximum bid reflow uses in the spot market.
If only the upper-bound cost is displayed, the actual cost incurred can be smaller.
`
)

type headerDesc struct {
	name, description string
}

var (
	runCols = []headerDesc{
		{"runid", "the run id"},
		{"user", "user who initiated the run"},
		{"start", "start time of the run"},
		{"end", "(if completed) end time of the run"},
		{"cost", "cost of the run"},
		{"ExecLog", "(if completed) ID of the run's exec logs"},
		{"SysLog", "(if completed) ID of the run's sys logs"},
		{"EvalGraph", "(if completed) ID of the run's evaluation graph (in dot format)"},
		{"Trace", "(if completed) ID of the run's trace'"},
	}
	taskCols = []headerDesc{
		{"taskid", "ID of the task"},
		{"flowid", "ID of the flow (corresponds to the node in the evaluation graph)"},
		{"attempt", "the attempt number (of the flow) that this task represents"},
		{"ident", "the exec identifier"},
		{"cost", "cost of the task"},
		{"start", "the task's start time"},
		{"end", "(if completed) the task's end time"},
		{"taskDur", "the task's run duration"},
		{"execDur", "the exec's run duration"},
		{"state", "the task's (current) state"},
		{"mem", "the amount of memory used by the exec"},
		{"cpu", "the number of CPU cores used by the exec"},
		{"disk", "the total amount of disk space used by the exec"},
		{"procs", "the set of processes running in the exec"},
	}
	taskColsType    = headerDesc{"hosttype", "(long listing only) Type of the host on which the task was completed (if available)"}
	taskColsUri     = headerDesc{"uri/resultid", "(long listing only) URI of a running task or (if taskdb exists) ID of the result of a completed task"}
	taskColsInspect = headerDesc{"inspect", "(long listing and if taskdb exists) ID of the inspect of a completed task"}

	poolCols = []headerDesc{
		{"poolid", "ID of the pool"},
		{"instanceid", "ID of the pool's underlying EC2 instance"},
		{"type", "type of the EC2 instance"},
		{"cost", "cost of the EC2 instance based on the duration of use"},
		{"status", "current status of the pool's underlying instance"},
		{"start", "start time of the pool"},
		{"end", "if ended, end time of the pool, or the last keepalive"},
		{"dur", "the duration for which the pool was live"},
	}
	poolColsLong = []headerDesc{
		{"resources", "(long listing only) the pool's resources"},
		{"dns", "(long listing only) the pool's EC2 instance's public DNS"},
	}
	runTaskHelp = `
The columns associated with a run are as follows:
(content of IDs can be retrieved using "reflow cat")
` + description(runCols) + `

The columns associated with a task are as follows:
` + description(append(taskCols, taskColsType, taskColsUri, taskColsInspect)) + `
`
)

func header(hd []headerDesc) string {
	cols := make([]string, len(hd))
	for i, s := range hd {
		cols[i] = s.name
	}
	return strings.Join(cols, "\t")
}

func description(hd []headerDesc) string {
	cols := make([]string, len(hd))
	for i, s := range hd {
		cols[i] = fmt.Sprintf("\t%-15s\t%s", s.name, s.description)
	}
	return strings.Join(cols, "\n")
}

func (c *Cmd) ps(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("ps", flag.ExitOnError)
	allFlag := flags.Bool("i", false, "list inactive/dead execs")
	longFlag := flags.Bool("l", false, "show long listing")
	userFlag := flags.String("u", "", "user (full username, eg: <username>@grailbio.com)")
	sinceFlag := flags.String("since", "", "runs (or pools) that were active since")
	allUsersFlag := flags.Bool("a", false, "show runs (or pools) of all users")
	poolsFlag := flags.Bool("p", false, "show pools instead of runs and tasks")
	verFlag := flags.String("p_version", "", "show pools with this reflow version instead")
	clustNameFlag := flags.String("p_name", "", "show pools with this cluster name instead")
	exactCostFlag := flags.Bool("exact_cost", false, "show exact cost for pools (if available)")
	help := `--- ps lists runs and tasks

Tasks associated with a run are listed below the run.
` + runTaskHelp + `

(Note: ps does not report run/task level costs.  Use "reflow info" instead) 

Ps lists only running execs for the current user by default.
It supports the following filters:
    - User: run by a specific user (-u <user>) or any user (-a)
    - Since: run that was active since some duration before now (-since <duration>). Since uses Go's
duration format. Valid time units are "h", "m", "s". e.g: "24h"

Global flags that work in all both query modes:
Flag -i lists all known execs in any state. Completed execs display profile
information for memory, cpu, and disk utilization in place of live utilization.
Flag -l shows the long listing; the live exec URI for a running task and the result id
and inspect for a completed task.

Ps must contact each node in the cluster to gather exec data. If a node 
does not respond within a predefined timeout, it is skipped, and an error is
printed on the console.

--- "ps -p" lists pools

The columns associated with a pool are as follows:
` + description(append(poolCols, poolColsLong...)) + `
` + costHelp + `
"ps -p" only lists pools that are currently active and match the "current" cluster identifier.
A cluster identifier is a combination of <user, cluster name, reflow version>.
By default, the cluster identifier is based on:
- the current user
- the cluster name set in the current reflow config.
- the reflow version that is the same as the current binary.

Pools are listed grouped by each cluster identifier

Flag -l shows the long listing
It supports the same filters as mentioned above (ie, User and Since).
In addition, pools for a different reflow version and/or cluster name can be retrieved
using the flags -p_version and -p_name, respectively.
In order to match all available reflow versions and/or cluster names, these flags can
be set to the special value "` + allFlagValue + `".

For example, the following query will return all pools that were active in the last 12 hours:
	> reflow ps -p -since 12h -p_version ` + allFlagValue + ` -p_name ` + allFlagValue + `

To get the exact cost for pools, add -exact_cost.
(Note that one may still get non-exact costs in this case, depending on availability of spot feed data)

`
	c.Parse(flags, args, help, "ps [-i] [-l] [-a | -u <user>] [-since <time>] [-p] [-p_version <reflow_version>] [-p_name <cluster_name>] [-exact_cost]")
	if flags.NArg() != 0 {
		flags.Usage()
	}

	if *userFlag != "" && *allUsersFlag {
		flags.Usage()
	}

	if !*poolsFlag && *exactCostFlag {
		c.Fatalf("-exact_cost only works with -p")
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
		_ = g.Wait() // ignore errors

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
		fmt.Fprint(&tw, header(taskCols))
		if *longFlag {
			fmt.Fprint(&tw, "\t", header([]headerDesc{taskColsUri}))
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
				getShort(info.ID), info.Config.Ident,
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

	var (
		infrauser *infra.User
		user      string
		since     time.Time
	)
	if err = c.Config.Instance(&infrauser); err != nil {
		c.Log.Debug(err)
	}
	switch {
	case *userFlag != "":
		user = *userFlag
	case *allUsersFlag:
		user = ""
	default:
		user = infrauser.User()
	}
	since = time.Now().Add(-time.Minute * 10)
	if *sinceFlag != "" {
		dur, err := time.ParseDuration(*sinceFlag)
		if err != nil {
			c.Fatalf("invalid duration %s: %s", *sinceFlag, err)
		}
		since = time.Now().Add(-dur)
	}
	if *poolsFlag {
		cluster := c.Cluster(nil)
		ec2c, ok := cluster.(*ec2cluster.Cluster)
		if !ok {
			c.Fatalf("poolInfo: not applicable for non-ec2 cluster %T", cluster)
		}
		q := taskdb.PoolQuery{Since: since, Cluster: taskdb.ClusterID{User: user}}
		switch *verFlag {
		case "":
			q.Cluster.ReflowVersion = ec2c.ReflowVersion
		case allFlagValue:
		default:
			q.Cluster.ReflowVersion = *verFlag
		}
		switch *clustNameFlag {
		case "":
			q.Cluster.ClusterName = ec2c.Name
		case allFlagValue:
		default:
			q.Cluster.ClusterName = *clustNameFlag
		}
		prs, err := c.poolInfos(ctx, q, *exactCostFlag)
		if err != nil {
			c.Fatalf("poolInfo: %v", err)
		}
		var tw tabwriter.Writer
		tw.Init(c.Stdout, 4, 4, 1, ' ', 0)
		defer tw.Flush()
		c.writePools(&tw, prs, *longFlag)
		return
	}

	ri, err := c.runInfo(ctx, taskdb.RunQuery{User: user, Since: since}, !*allFlag, false /* cost */)
	if err != nil {
		c.Log.Debug(err)
	}
	var tw tabwriter.Writer
	tw.Init(c.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()
	c.writeRuns(ri, &tw, *longFlag)
}

type execInfo struct {
	URI string
	ID  digest.Digest
	reflow.ExecInspect
	Alloc pool.AllocInspect
}

func (c *Cmd) execInfos(ctx context.Context, execs []reflow.Exec) []execInfo {
	g, ctx := errgroup.WithContext(ctx)
	infos := make([]execInfo, len(execs))
	for i := range execs {
		i, exec := i, execs[i]
		g.Go(func() error {
			inspect, _, err := exec.Inspect(ctx, nil)
			if err != nil {
				c.Log.Errorf("inspect %s: %v", exec.ID(), err)
			} else {
				infos[i] = execInfo{URI: exec.URI(), ID: exec.ID(), ExecInspect: inspect}
			}
			return nil
		})
	}
	_ = g.Wait() // ignore errors g.Wait()
	var validInfos []execInfo
	for _, info := range infos {
		if info.ID.IsZero() {
			continue
		}
		validInfos = append(validInfos, info)
	}
	return validInfos
}

type taskInfo struct {
	taskdb.Task
	cost Cost
	reflow.ExecInspect
}

func (c *Cmd) taskInfo(ctx context.Context, q taskdb.TaskQuery, liveOnly, cost bool, cc *costComputer) ([]taskInfo, error) {
	var tdb taskdb.TaskDB
	err := c.Config.Instance(&tdb)
	if err != nil {
		log.Fatal("taskdb: ", err)
	}
	if tdb == nil {
		log.Fatal("nil taskdb")
	}
	q.WithAlloc = cost
	tasks, err := tdb.Tasks(ctx, q)
	if err != nil {
		log.Error(err)
	}
	ti := make([]taskInfo, len(tasks))
	g, gctx := errgroup.WithContext(ctx)
	for i, v := range tasks {
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
					c.Errorf("task %s URI %s: %v", v.ID.IDShort(), v.URI, err)
					return nil
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
	for _, t := range ti {
		if !t.Task.ID.IsValid() {
			continue
		}
		if cost && t.Alloc != nil && t.Alloc.Pool != nil {
			a, p := t.Alloc, t.Alloc.Pool
			pi := poolInfo{*p, cc}
			t.cost = pi.cost(t.TimeFields) // cost of the pool for the task's duration
			// Scale the cost by the ratio of the task's resources to the alloc's resources.
			t.cost.Mul(t.Resources.MaxRatio(a.Resources))
			// Then scale the cost by the ratio of the alloc's resources to the pool's resources.
			t.cost.Mul(a.Resources.MaxRatio(p.Resources))
		}
		b = append(b, t)
	}
	ti = b
	sort.Slice(ti, func(i, j int) bool {
		return ti[i].Start.Before(ti[j].Start)
	})
	return ti, err
}

type runInfo struct {
	taskdb.Run
	taskInfo []taskInfo
}

func (c *Cmd) runInfo(ctx context.Context, q taskdb.RunQuery, liveOnly, exactCost bool) ([]runInfo, error) {
	var tdb taskdb.TaskDB
	err := c.Config.Instance(&tdb)
	if err != nil {
		log.Fatal("taskdb: ", err)
	}
	if tdb == nil {
		log.Fatal("nil taskdb")
	}
	runs, err := tdb.Runs(ctx, q)
	if err != nil {
		log.Error(err)
	}
	var runsSt, runsEt time.Time
	for _, run := range runs {
		st, et := run.StartEnd()
		if runsSt.IsZero() || st.Before(runsSt) {
			runsSt = st
		}
		if runsEt.IsZero() || et.After(runsEt) {
			runsEt = et
		}
	}
	cc := c.costComputer(ctx, exactCost, runsSt, runsEt)
	ri := make([]runInfo, len(runs))
	g, gctx := errgroup.WithContext(ctx)
	for i, run := range runs {
		i, run := i, run
		g.Go(func() error {
			qu := taskdb.TaskQuery{RunID: run.ID}
			ti, terr := c.taskInfo(gctx, qu, liveOnly, true, cc)
			if terr != nil {
				log.Debug(terr)
			}
			ri[i] = runInfo{Run: run, taskInfo: ti}
			return nil
		})
	}
	err = g.Wait()
	return ri, err
}

type poolInfo struct {
	taskdb.PoolRow
	cc *costComputer
}

func diffTimes(a, b time.Time) time.Duration {
	if a.Before(b) {
		return b.Sub(a)
	}
	return a.Sub(b)
}

func (p poolInfo) cost(c taskdb.TimeFields) (cost Cost) {
	start, end := c.StartEnd()
	return p.cc.compute(p.PoolID.String(), p.PoolType, p.End, start, end)
}

func (c *Cmd) poolInfos(ctx context.Context, q taskdb.PoolQuery, exactCost bool) ([]poolInfo, error) {
	var tdb taskdb.TaskDB
	if err := c.Config.Instance(&tdb); err != nil {
		c.Fatalf("taskdb: %v", err)
	}
	if tdb == nil {
		return nil, fmt.Errorf("poolInfos: no taskdb")
	}
	var (
		cc *costComputer
		wg sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cc = c.costComputer(ctx, exactCost, q.Since, time.Now())
	}()
	prs, err := tdb.Pools(ctx, q)
	wg.Wait()
	return poolInfos(prs, cc), err
}

func poolInfos(prs []taskdb.PoolRow, cc *costComputer) []poolInfo {
	pis := make([]poolInfo, len(prs))
	for i, pr := range prs {
		pis[i] = poolInfo{PoolRow: pr, cc: cc}
	}
	return pis
}

func printTaskHeader(w io.Writer, longListing bool) {
	fmt.Fprint(w, "\t", header(taskCols))
	if longListing {
		fmt.Fprint(w, "\t", header([]headerDesc{taskColsType, taskColsUri, taskColsInspect}))
	}
	fmt.Fprint(w, "\n")
}

func (c *Cmd) writeRuns(ri []runInfo, w io.Writer, longListing bool) {
	for _, run := range ri {
		if len(run.taskInfo) == 0 {
			continue
		}
		st, et := formatStartEnd(run.TimeFields)
		var cost Cost
		for _, t := range run.taskInfo {
			cost.Add(t.cost)
		}
		exec := getShort(run.Run.ExecLog)
		sys := getShort(run.Run.SysLog)
		graph := getShort(run.Run.EvalGraph)
		trace := getShort(run.Run.Trace)
		fmt.Fprint(w, header(runCols), "\n")
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s", run.Run.ID.IDShort(), run.Run.User, st, et, cost)
		fmt.Fprintf(w, "\t%s\t%s\t%s\t%s", exec, sys, graph, trace)
		fmt.Fprintf(w, "\n\n")
		printTaskHeader(w, longListing)
		for _, task := range run.taskInfo {
			if !task.Task.ID.IsValid() {
				continue
			}
			c.writeTask(task, w, longListing)
		}
		fmt.Fprint(w, "\n")
	}
}

func getShort(d digest.Digest) (s string) {
	if !d.IsZero() {
		s = d.Short()
	}
	return
}

// formatStartEnd formats the start and end times from the given TimeFields
// and returns the them formatted in an appropriate time layout based on the start time.
func formatStartEnd(c taskdb.TimeFields) (st, et string) {
	start, end := c.StartEnd()
	if start.IsZero() {
		return
	}
	var layout = time.Kitchen
	switch dur := time.Since(start); {
	case dur > 6*24*time.Hour: // Older than 6 days
		layout = time.RFC3339
	case dur > 24*time.Hour: // Older than 24 hours
		layout = "Mon3:04PM"
	}
	st = start.Local().Format(layout)
	et = end.Local().Format(layout)
	return
}

func (c *Cmd) writeTask(task taskInfo, w io.Writer, longListing bool) {
	var (
		procs, ident, state string
		info                = task.ExecInspect
		runtime             = info.Runtime()
		st, et              = formatStartEnd(task.TimeFields)
	)
	switch info.Config.Type {
	case "exec":
		ident = task.Config.Ident
		state = info.State
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
		if !task.End.IsZero() {
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
	s, e := task.StartEnd()
	dur := e.Sub(s).Truncate(time.Second)
	fmt.Fprintf(w, "\t%s\t%s\t%d\t%s", task.ID.IDShort(), getShort(task.FlowID), 1+task.Attempt, ident)
	fmt.Fprintf(w, "\t%s\t%s\t%s\t%s", task.cost, st, et, dur)
	fmt.Fprintf(w, "\t%s\t%s\t%s\t%.1f\t%s\t%s", runtime.Truncate(time.Second), state, data.Size(mem), cpu, data.Size(disk), procs)
	if longListing {
		hostType := "unknown"
		if task.Alloc != nil && task.Alloc.Pool != nil {
			hostType = task.Alloc.Pool.PoolType
		}
		result := getShort(task.Task.ResultID)
		if result == "" {
			result = task.Task.URI
		}
		inspect := getShort(task.Task.Inspect)
		fmt.Fprintf(w, "\t%s\t%s\t%s", hostType, result, inspect)
	}
	fmt.Fprint(w, "\n")
}

func (c *Cmd) writePools(w io.Writer, pis []poolInfo, longListing bool) {
	byCluster := make(map[taskdb.ClusterID][]poolInfo)
	clusterCost := make(map[taskdb.ClusterID]Cost)
	sort.Slice(pis, func(i, j int) bool {
		return pis[i].Start.Before(pis[j].Start)
	})
	for _, pi := range pis {
		byCluster[pi.ClusterID] = append(byCluster[pi.ClusterID], pi)
		cc := clusterCost[pi.ClusterID]
		cc.Add(pi.cost(pi.TimeFields))
		clusterCost[pi.ClusterID] = cc
	}
	cols := poolCols
	if longListing {
		cols = append(cols, poolColsLong...)
	}

	for c, poolInfos := range byCluster {
		fmt.Fprintf(w, "Cluster id: %s (user), %s (name), %s (reflowversion)\n", c.User, c.ClusterName, c.ReflowVersion)
		fmt.Fprintf(w, "Cost: %s (see 'ps -help' for details)\n", clusterCost[c])
		fmt.Fprint(w, "\t", header(cols), "\n")
		for _, pi := range poolInfos {
			var (
				cost    = pi.cost(pi.TimeFields)
				st, et  = formatStartEnd(pi.TimeFields)
				id, iid string
				status  = "running"
			)
			if !pi.End.IsZero() {
				status = "ended"
			}
			if pid := pi.PoolID; pid.IsValid() {
				id = pid.Digest().Short()
				iid = pid.String()
			}
			s, e := pi.StartEnd()
			fmt.Fprintf(w, "\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", id, iid, pi.PoolType, cost, status, st, et, e.Sub(s).Truncate(time.Minute))
			if longListing {
				fmt.Fprintf(w, "\t%s\t%s", pi.Resources, pi.URI)
			}
			fmt.Fprintln(w, "")
		}
		fmt.Fprintln(w, "")
	}
}
