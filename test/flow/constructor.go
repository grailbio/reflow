// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package flow contains a number of constructors for Flow nodes
// that are convenient for testing.
package flow

import (
	"net/url"
	"regexp"

	"github.com/grailbio/reflow"
)

// Exec constructs a new reflow.OpExec node.
func Exec(image, cmd string, resources reflow.Resources, deps ...*reflow.Flow) *reflow.Flow {
	return &reflow.Flow{Op: reflow.OpExec, Deps: deps, Cmd: cmd, Image: image, Resources: resources}
}

// Intern constructs a new reflow.OpIntern node.
func Intern(rawurl string) *reflow.Flow {
	u, err := url.Parse(rawurl)
	if err != nil {
		panic(err)
	}
	return &reflow.Flow{Op: reflow.OpIntern, URL: u}
}

// Extern constructs a new reflow.OpExtern node.
func Extern(rawurl string, dep *reflow.Flow) *reflow.Flow {
	u, err := url.Parse(rawurl)
	if err != nil {
		panic(err)
	}
	return &reflow.Flow{Op: reflow.OpExtern, Deps: []*reflow.Flow{dep}, URL: u}
}

// Groupby constructs a new reflow.OpGroupby node.
func Groupby(re string, dep *reflow.Flow) *reflow.Flow {
	return &reflow.Flow{Op: reflow.OpGroupby, Deps: []*reflow.Flow{dep}, Re: regexp.MustCompile(re)}
}

// Map constructs a new reflow.OpMap node.
func Map(fn func(*reflow.Flow) *reflow.Flow, dep *reflow.Flow) *reflow.Flow {
	f := &reflow.Flow{Op: reflow.OpMap, Deps: []*reflow.Flow{dep}, MapFunc: fn}
	f.MapInit()
	return f
}

// Collect constructs a new reflow.OpCollect node.
func Collect(re, repl string, dep *reflow.Flow) *reflow.Flow {
	return &reflow.Flow{Op: reflow.OpCollect, Re: regexp.MustCompile(re), Repl: repl, Deps: []*reflow.Flow{dep}}
}

// Merge constructs a new reflow.OpMerge node.
func Merge(deps ...*reflow.Flow) *reflow.Flow {
	return &reflow.Flow{Op: reflow.OpMerge, Deps: deps}
}

// Pullup constructs a new reflow.OpPullup node.
func Pullup(deps ...*reflow.Flow) *reflow.Flow {
	return &reflow.Flow{Op: reflow.OpPullup, Deps: deps}
}

// Val constructs a new reflow.OpVal node.
func Val(v reflow.Fileset) *reflow.Flow {
	return v.Flow()
}

// Data constructs a new reflow.Data node.
func Data(b []byte) *reflow.Flow {
	return &reflow.Flow{Op: reflow.OpData, Data: b}
}
