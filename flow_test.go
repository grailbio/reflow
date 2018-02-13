// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow_test

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/test/flow"
)

func mustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func TestDigestStability(t *testing.T) {
	// This is the benchmark flow we digest to ensure stability.
	// We try to use every feature here.
	intern := flow.Intern("internurl")
	collect := flow.Collect(".*", "$0", intern)
	groupby := flow.Groupby("foo-(.*)", collect)
	mapflow := flow.Map(func(f *reflow.Flow) *reflow.Flow { return flow.Exec("image", "command", reflow.Resources{}, f) }, groupby)
	stableFlow := flow.Extern("externurl", mapflow)

	const stableSHA256V1 = "sha256:5a3a916fe9a11b67f9a0dbd67f6fac0f986dd67803267e79f25f866ca9781e2f"
	const stableSHA256V2 = "sha256:02751e46c573a31747a30b05c2b73b2eb556fb45fb4c0aaf88d170f4b5e6d4e7"

	// Make sure that the digest is stable.
	if got, want := stableFlow.Canonicalize(reflow.Config{HashV1: true}).Digest().String(), stableSHA256V1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	// Make sure that the digest is stable.
	if got, want := stableFlow.Digest().String(), stableSHA256V2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCanonicalize(t *testing.T) {
	intern1 := flow.Intern("url")
	intern2 := flow.Intern("url")
	merged := flow.Merge(intern1, intern2)
	d := merged.Digest()
	canon := merged.Canonicalize(reflow.Config{})
	if got, want := canon.Digest(), d; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if canon.Deps[0] != canon.Deps[1] {
		t.Fatal("flow is not canonical")
	}
}

func TestVisitor(t *testing.T) {
	intern1 := flow.Intern("url")
	intern2 := flow.Intern("url")
	merged := flow.Merge(intern1, intern2)
	extern := flow.Extern("externurl", merged)

	var visited []*reflow.Flow
	for v := extern.Visitor(); v.Walk(); v.Visit() {
		visited = append(visited, v.Flow)
	}
	if got, want := visited, []*reflow.Flow{extern, merged, intern1, intern2}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestFlowRequirements(t *testing.T) {
	e1 := flow.Exec("cmd1", "image", reflow.Resources{"mem": 10, "cpu": 1, "disk": 110})
	e2 := flow.Exec("cmd2", "image", reflow.Resources{"mem": 20, "cpu": 1, "disk": 100})
	merge := flow.Merge(e1, e2)
	req := merge.Requirements()
	if req.Width != 0 {
		t.Errorf("expected width=0, got %v", req.Width)
	}
	if got, want := req.Min, (reflow.Resources{"mem": 20, "cpu": 1, "disk": 110}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	mapflow := flow.Map(func(f *reflow.Flow) *reflow.Flow {
		return flow.Exec("image", "command", reflow.Resources{}, f)
	}, merge)
	req = mapflow.Requirements()
	if !req.Wide() {
		t.Errorf("expected wide, got %v", req)
	}
	if got, want := req.Min, (reflow.Resources{"mem": 20, "cpu": 1, "disk": 110}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
