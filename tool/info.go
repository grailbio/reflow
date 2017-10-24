// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/runner"
	"grail.com/lib/state"
)

func (c *Cmd) info(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("info", flag.ExitOnError)
	help := "Info displays general information about allocs, execs, and runs."
	c.Parse(flags, args, help, "info uris...")
	if flags.NArg() == 0 {
		flags.Usage()
	}
	cluster := c.cluster()
	var tw tabwriter.Writer
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)

	for _, arg := range flags.Args() {
		fmt.Fprintln(&tw, arg)
		u, err := parseURI(arg)
		if err != nil {
			c.Fatalf("parse URI %s: %v", arg, err)
		}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		switch u.Kind {
		case runURI:
			c.printRunInfo(ctx, &tw, u.Name)
		case execURI:
			alloc, err := cluster.Alloc(ctx, u.AllocID)
			if err != nil {
				c.Fatal(err)
			}
			exec, err := alloc.Get(ctx, u.ExecID)
			if err != nil {
				c.Fatalf("failed to fetch exec for %q: %s", arg, err)
			}
			inspect, err := exec.Inspect(ctx)
			if err != nil {
				c.Fatalf("error inspecting exec %q: %s", arg, err)
			}
			var result reflow.Result
			if inspect.State == "complete" {
				result, err = exec.Result(ctx)
				if err != nil {
					c.Errorf("failed to fetch result for exec %s: %s\n", arg, err)
				}
			}
			c.printExec(ctx, &tw, inspect, result)
		case allocURI:
			alloc, err := cluster.Alloc(ctx, u.AllocID)
			if err != nil {
				c.Fatal(err)
			}
			inspect, err := alloc.Inspect(ctx)
			if err != nil {
				c.Fatal(err)
			}
			execs, err := alloc.Execs(ctx)
			if err != nil {
				c.Fatal(err)
			}
			c.printAlloc(ctx, &tw, inspect, execs)

		}
	}
	tw.Flush()
}

func (c *Cmd) printRunInfo(ctx context.Context, w io.Writer, name runner.Name) {
	rundir := filepath.Join(c.rundir(), name.User)
	f, err := os.Open(rundir)
	if os.IsNotExist(err) {
		c.Errorf("%s: no such run\n", name.Short())
		return
	} else if err != nil {
		c.Errorln(err)
		return
	}
	infos, err := f.Readdir(-1)
	if err != nil {
		c.Errorln(err)
		return
	}
	if name.ID.IsShort() {
		for _, info := range infos {
			d := info.Name()
			if filepath.Ext(d) != ".json" {
				continue
			}
			d = d[:len(d)-5]
			id, err := reflow.Digester.Parse(d)
			if err != nil {
				c.Errorf("%s: %v\n", info.Name(), err)
				continue
			}
			if id.Expands(name.ID) {
				name.ID = id
				break
			}
		}

	}
	base := filepath.Join(c.rundir(), name.String())
	_, err = os.Stat(base + ".json")
	if os.IsNotExist(err) {
		c.Errorf("%s: no such run\n", name.Short())
		return
	} else if err != nil {
		c.Errorf("%s: %v\n", name.Short(), err)
		return
	}
	statefile, err := state.Open(base)
	if err != nil {
		c.Errorf("%s: %v\n", name.Short(), err)
		return
	}

	var state runner.State
	statefile.Unmarshal(&state)

	fmt.Fprintf(w, "\ttime:\t%s\n", state.Created.Local().Format(time.ANSIC))
	fmt.Fprintf(w, "\tprogram:\t%s\n", state.Program)
	if len(state.Params) > 0 {
		fmt.Fprintf(w, "\tparams:\n")
		for k, v := range state.Params {
			fmt.Fprintf(w, "\t\t%s:\t%s\n", k, v)
		}
	}
	if len(state.Args) > 0 {
		fmt.Fprintf(w, "\targs:\n")
		for _, v := range state.Args {
			fmt.Fprintf(w, "\t\t%s\n", v)
		}
	}
	fmt.Fprintf(w, "\tphase:\t%s\n", state.Phase)
	if state.AllocID != "" {
		fmt.Fprintf(w, "\talloc:\t%s\n", state.AllocID)
	}
	if state.Err != nil {
		fmt.Fprintf(w, "\terror:\t%s\n", state.Err)
	}
	if state.Result != "" {
		fmt.Fprintf(w, "\tresult:\t%s\n", state.Result)
	}
	if _, err := os.Stat(base + ".execlog"); err == nil {
		fmt.Fprintf(w, "\tlog:\t%s.execlog\n", base)
	}
}

func (c *Cmd) printAlloc(ctx context.Context, w io.Writer, inspect pool.AllocInspect, execs []reflow.Exec) {
	fmt.Fprintf(w, "\tmem:\t%s\n", data.Size(inspect.Resources.Memory))
	fmt.Fprintf(w, "\tcpu:\t%d\n", inspect.Resources.CPU)
	fmt.Fprintf(w, "\tdisk:\t%s\n", data.Size(inspect.Resources.Disk))
	fmt.Fprintf(w, "\towner:\t%s\n", inspect.Meta.Owner)
	fmt.Fprintf(w, "\tkeepalive:\t%s (%s ago)\n", inspect.LastKeepalive, round(time.Since(inspect.LastKeepalive)))
	if expires := time.Until(inspect.Expires); expires < time.Duration(0) {
		fmt.Fprintf(w, "\texpires:\t%s (%s ago)\n", inspect.Expires, round(-expires))
	} else {
		fmt.Fprintf(w, "\texpires:\t%s (in %s)\n", inspect.Expires, round(expires))
	}
	if len(inspect.Meta.Labels) > 0 {
		var keys []string
		for key := range inspect.Meta.Labels {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fmt.Fprintf(w, "\tlabels:\n")
		for _, key := range keys {
			fmt.Fprintf(w, "\t  %s\t%s\n", key, inspect.Meta.Labels[key])
		}
	}
	if len(execs) > 0 {
		fmt.Fprintf(w, "\texecs:\n")
		for _, exec := range execs {
			fmt.Fprintf(w, "\t  %s\n", exec.URI())
		}
	}
}

func (c *Cmd) printExec(ctx context.Context, w io.Writer, inspect reflow.ExecInspect, result reflow.Result) {
	fmt.Fprintf(w, "\tstate:\t%s\n", inspect.State)
	fmt.Fprintf(w, "\ttype:\t%s\n", inspect.Config.Type)
	if inspect.Config.Ident != "" {
		fmt.Fprintf(w, "\tident:\t%s\n", inspect.Config.Ident)
	}
	if inspect.Config.URL != "" {
		fmt.Fprintf(w, "\turl:\t%s\n", inspect.Config.URL)
	}
	if inspect.Config.Image != "" {
		fmt.Fprintf(w, "\timage:\t%s\n", inspect.Config.Image)
	}
	if inspect.Config.Cmd != "" {
		// Find synonymous filesets so we can abbreviate our output.
		syns := make([]int, len(inspect.Config.Args))
		for i := range inspect.Config.Args {
			if inspect.Config.Args[i].Fileset == nil {
				continue
			}
			for j := i + 1; j < len(inspect.Config.Args); j++ {
				if inspect.Config.Args[j].Fileset == nil {
					continue
				}
				if inspect.Config.Args[i].Fileset.Equal(*inspect.Config.Args[j].Fileset) {
					syns[i] = j
					break
				}
			}
		}
		args := make([]interface{}, len(inspect.Config.Args))
		for i := range args {
			args[i] = fmt.Sprintf("{{arg[%d]}}", i)
		}
		fmt.Fprintf(w, "\tcmd:\t%q\n", fmt.Sprintf(inspect.Config.Cmd, args...))
		for i, arg := range inspect.Config.Args {
			if arg.Out {
				fmt.Fprintf(w, "\t  arg[%d]: output %d\n", i, arg.Index)
				continue
			}
			if syns[i] < 0 || arg.Fileset == nil {
				continue
			}
			indices := []int{i}
			for j := i; syns[j] > 0; j = syns[j] {
				indices = append(indices, syns[j])
			}
			for i := range indices {
				syns[i] = -1
			}
			strs := make([]string, len(indices))
			for i := range indices {
				strs[i] = fmt.Sprintf("arg[%d]", indices[i])
			}
			fmt.Fprintf(w, "\t  %s:\n", strings.Join(strs, ", "))
			c.printFileset(w, "\t    ", *arg.Fileset)
		}
	}
	if len(inspect.Commands) > 0 {
		fmt.Fprintln(w, "\ttop:")
		for _, cmd := range inspect.Commands {
			fmt.Fprintln(w, "\t\t", cmd)
		}
	}

	if result.Err != nil {
		fmt.Fprintf(w, "\terror:\t%s\n", result.Err)
	}
	if !result.Fileset.Empty() {
		fmt.Fprintf(w, "\tresult:\n")
		c.printFileset(w, "\t  ", result.Fileset)
	}
}

func (c *Cmd) printFileset(w io.Writer, prefix string, fs reflow.Fileset) {
	switch {
	case len(fs.List) > 0:
		for i := range fs.List {
			fmt.Fprintf(w, "%slist[%d]:\n", prefix, i)
			c.printFileset(w, prefix+"\t", fs.List[i])
		}
	case len(fs.Map) > 0:
		var keys []string
		for key := range fs.Map {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			file := fs.Map[key]
			fmt.Fprintf(w, "%s%s:\t%s (%s)\n", prefix, key, file.ID, data.Size(file.Size))
		}
	}
}

func round(d time.Duration) time.Duration {
	return d - d%time.Second
}

type uriKind int

const (
	allocURI uriKind = iota
	execURI
	runURI
)

type uri struct {
	Kind    uriKind
	AllocID string
	ExecID  digest.Digest
	Name    runner.Name
}

// parseURI parses an alloc, exec URI, or run URI, or else returns an error.
// If the URI is an allocUDI, then the execID is empty.
//
// Examples:
//
//	marius@grailbio.com/f707217e
//	ec2-35-165-199-174.us-west-2.compute.amazonaws.com:9000/bb97e35db4101030
//	ec2-35-165-199-174.us-west-2.compute.amazonaws.com:9000/bb97e35db4101030/9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49
func parseURI(rawURI string) (uri, error) {
	head, tail := peel(rawURI, "/")
	if strings.Contains(head, "@") {
		id, err := reflow.Digester.Parse(tail)
		if err != nil {
			return uri{}, err
		}
		return uri{
			Kind: runURI,
			Name: runner.Name{
				User: head,
				ID:   id,
			},
		}, nil
	}

	var u uri
	u.AllocID = head
	head, tail = peel(tail, "/")
	u.AllocID += "/" + head
	if tail == "" {
		u.Kind = allocURI
		return u, nil
	}
	var err error
	u.ExecID, err = reflow.Digester.Parse(tail)
	if err != nil {
		return uri{}, err
	}
	u.Kind = execURI
	return u, nil
}

func peel(s, sep string) (head, tail string) {
	switch parts := strings.SplitN(s, sep, 2); len(parts) {
	case 1:
		return parts[0], ""
	case 2:
		return parts[0], parts[1]
	default:
		panic("bug")
	}
}
