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
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/values"
)

// Exec constructs a new flow.OpExec node.
func Exec(image, cmd string, resources reflow.Resources, deps ...*flow.Flow) *flow.Flow {
	return &flow.Flow{Op: flow.Exec, Deps: deps, Cmd: cmd, Image: image, Resources: resources}
}

// Intern constructs a new flow.OpIntern node.
func Intern(rawurl string) *flow.Flow {
	u, err := url.Parse(rawurl)
	if err != nil {
		panic(err)
	}
	return &flow.Flow{Op: flow.Intern, URL: u}
}

// Extern constructs a new flow.Extern node.
func Extern(rawurl string, dep *flow.Flow) *flow.Flow {
	u, err := url.Parse(rawurl)
	if err != nil {
		panic(err)
	}
	return &flow.Flow{Op: flow.Extern, Deps: []*flow.Flow{dep}, URL: u}
}

// Groupby constructs a new flow.Groupby node.
func Groupby(re string, dep *flow.Flow) *flow.Flow {
	return &flow.Flow{Op: flow.Groupby, Deps: []*flow.Flow{dep}, Re: regexp.MustCompile(re)}
}

// Map constructs a new flow.Map node.
func Map(fn func(*flow.Flow) *flow.Flow, dep *flow.Flow) *flow.Flow {
	f := &flow.Flow{Op: flow.Map, Deps: []*flow.Flow{dep}, MapFunc: fn}
	f.MapInit()
	return f
}

// Collect constructs a new flow.Collect node.
func Collect(re, repl string, dep *flow.Flow) *flow.Flow {
	return &flow.Flow{Op: flow.Collect, Re: regexp.MustCompile(re), Repl: repl, Deps: []*flow.Flow{dep}}
}

// Merge constructs a new flow.Merge node.
func Merge(deps ...*flow.Flow) *flow.Flow {
	return &flow.Flow{Op: flow.Merge, Deps: deps}
}

// Pullup constructs a new flow.Pullup node.
func Pullup(deps ...*flow.Flow) *flow.Flow {
	return &flow.Flow{Op: flow.Pullup, Deps: deps}
}

// Val constructs a new flow.Val node.
func Val(v reflow.Fileset) *flow.Flow {
	return &flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done}
}

// Data constructs a new reflow.Data node.
func Data(b []byte) *flow.Flow {
	return &flow.Flow{Op: flow.Data, Data: b}
}
