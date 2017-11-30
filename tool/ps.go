// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/runner"
	"golang.org/x/sync/errgroup"
)

type execInfo struct {
	URI string
	reflow.ExecInspect
	Alloc pool.AllocInspect
}

func (c *Cmd) ps(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("ps", flag.ExitOnError)
	allFlag := flags.Bool("a", false, "list dead execs")
	longFlag := flags.Bool("l", false, "show long listing")
	help := `Ps lists execs.

The columns displayed by ps are:

	run       the run associated with the exec
	ident     the exec identifier
	time      the exec's start time
	duration  the exec's run duration
	state     the exec's state
	mem       the amount of memory used by the exec
	cpu       the number of CPU cores used by the exec
	disk      the total amount of disk space used by the exec
	procs     the set of processes running in the exec

Ps lists only running execs; flag -a lists all known execs in any
state. Completed execs display profile information for memory, cpu,
and disk utilization in place of live utilization.

Ps must contact each node in the cluster to gather exec data. If a node 
does not respond within a predefined timeout, it is skipped, and an error is
printed on the console.`
	c.Parse(flags, args, help, "ps [-a] [-l]")
	if flags.NArg() != 0 {
		flags.Usage()
	}
	cluster := c.cluster()
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
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()
	for _, info := range infos {
		name, err := runner.ParseName(info.Alloc.Meta.Labels["Name"])
		if err != nil {
			c.Log.Errorf("parse %s: %v", info.Alloc.Meta.Labels["Name"], err)
		}
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
			name.Short(), info.Config.Ident,
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
			infos[i] = execInfo{URI: exec.URI(), ExecInspect: inspect}
			return nil
		})
	}
	g.Wait()
	return infos
}
