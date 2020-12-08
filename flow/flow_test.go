// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow_test

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/flow"
	op "github.com/grailbio/reflow/test/flow"
	"github.com/grailbio/reflow/test/testutil"
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
	intern := op.Intern("internurl")
	collect := op.Collect(".*", "$0", intern)
	groupby := op.Groupby("foo-(.*)", collect)
	mapflow := op.Map(func(f *flow.Flow) *flow.Flow { return op.Exec("image", "command", reflow.Resources{}, f) }, groupby)
	stableFlow := op.Extern("externurl", mapflow)

	const stableSHA256V1 = "sha256:5a3a916fe9a11b67f9a0dbd67f6fac0f986dd67803267e79f25f866ca9781e2f"
	const stableSHA256V2 = "sha256:02751e46c573a31747a30b05c2b73b2eb556fb45fb4c0aaf88d170f4b5e6d4e7"

	// Make sure that the digest is stable.
	if got, want := stableFlow.Canonicalize(flow.Config{HashV1: true}).Digest().String(), stableSHA256V1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	// Make sure that the digest is stable.
	if got, want := stableFlow.Digest().String(), stableSHA256V2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCanonicalize(t *testing.T) {
	intern1 := op.Intern("url")
	intern2 := op.Intern("url")
	merged := op.Merge(intern1, intern2)
	d := merged.Digest()
	canon := merged.Canonicalize(flow.Config{})
	if got, want := canon.Digest(), d; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if canon.Deps[0] != canon.Deps[1] {
		t.Fatal("flow is not canonical")
	}
	// make sure forced intern doesn't canonicalize to unforced ones.
	intern3 := op.Intern("url")
	intern3.MustIntern = true
	merged = op.Merge(intern1, intern3)
	d1 := merged.Digest()
	canon = merged.Canonicalize(flow.Config{})
	if got, want := canon.Digest(), d1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if canon.Deps[0] == canon.Deps[1] {
		t.Fatalf("intern and forced intern should not canonical, %v, %v", canon.Deps[0].DebugString(), canon.Deps[1].DebugString())
	}
	if d != d1 {
		t.Fatal("flow digests should be identical")
	}
}

func TestPhysicalDigests(t *testing.T) {
	e1 := op.Exec("image", "cmd1", reflow.Resources{"mem": 10, "cpu": 1, "disk": 110})
	n := len(flow.PhysicalDigests(e1))
	if n != 1 {
		t.Errorf("expected 1 physical digest; got %d", n)
	}

	e1.OriginalImage = "image"
	n = len(flow.PhysicalDigests(e1))
	if n != 1 {
		t.Errorf("expected 1 physical digest; got %d", n)
	}

	e1.OriginalImage = "origImage"
	n = len(flow.PhysicalDigests(e1))
	if n != 2 {
		t.Errorf("expected 2 physical digests; got %d", n)
	}
}

func TestVisitor(t *testing.T) {
	intern1 := op.Intern("url")
	intern2 := op.Intern("url")
	merged := op.Merge(intern1, intern2)
	extern := op.Extern("externurl", merged)

	var visited []*flow.Flow
	for v := extern.Visitor(); v.Walk(); v.Visit() {
		visited = append(visited, v.Flow)
	}
	if got, want := visited, []*flow.Flow{extern, merged, intern1, intern2}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestFlowRequirements(t *testing.T) {
	e1 := op.Exec("image", "cmd1", reflow.Resources{"mem": 10, "cpu": 1, "disk": 110})
	e2 := op.Exec("image", "cmd2", reflow.Resources{"mem": 20, "cpu": 1, "disk": 100})
	merge := op.Merge(e1, e2)
	req := merge.Requirements()
	if req.Width != 0 {
		t.Errorf("expected width=0, got %v", req.Width)
	}
	if got, want := req.Min, (reflow.Resources{"mem": 20, "cpu": 1, "disk": 110}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	mapflow := op.Map(func(f *flow.Flow) *flow.Flow {
		return op.Exec("image", "command", reflow.Resources{}, f)
	}, merge)
	req = mapflow.Requirements()
	if req.Width != 1 {
		t.Errorf("expected wide, got %v", req)
	}
	if got, want := req.Min, (reflow.Resources{"mem": 20, "cpu": 1, "disk": 110}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestDepAssertions(t *testing.T) {
	fuzz := testutil.NewFuzz(nil)
	i1, i1Fs := op.Intern("url"), fuzz.Fileset(true, true)
	i1.Value = i1Fs
	i1A := i1Fs.Assertions()
	i2, i2Fs := op.Intern("url"), fuzz.Fileset(true, true)
	i2.Value = i2Fs
	i2A := i2Fs.Assertions()
	mInterns := op.Merge(i1, i2)

	e1, e1Fs := op.Exec("image", "cmd1", reflow.Resources{"mem": 10, "cpu": 1, "disk": 110}, i1), fuzz.Fileset(true, true)
	e1.Value = e1Fs
	e1A := e1Fs.Assertions()
	e2, e2Fs := op.Exec("image", "cmd2", reflow.Resources{"mem": 20, "cpu": 1, "disk": 100}, i2), fuzz.Fileset(true, true)
	e2.Value = e2Fs
	e2A := e2Fs.Assertions()
	ex1 := op.Extern("externurl", e1)
	ex2 := op.Extern("externurl", e2)
	mExecs := op.Merge(e1, e2)
	exM := op.Extern("externurl", mExecs)

	tests := []struct {
		f    *flow.Flow
		want *reflow.Assertions
		we   bool
	}{
		{i1, nil, false}, {i2, nil, false}, {mInterns, nil, false},
		{e1, i1A, false}, {e2, i2A, false}, {mExecs, nil, false},
		{ex1, e1A, false}, {ex2, e2A, false}, {exM, nil, false},
	}
	for _, tt := range tests {
		got, gotE := reflow.MergeAssertions(flow.DepAssertions(tt.f)...)
		if tt.we != (gotE != nil) {
			t.Errorf("(%v).depAssertions() got %v, want error: %v ", tt.f, gotE, tt.we)
		}
		if tt.we {
			continue
		}
		if !got.Equal(tt.want) {
			t.Errorf("got %v, want %v", got, tt.want)
		}
	}
}

func TestImageQualifiers(t *testing.T) {
	tests := []struct {
		img, wImg     string
		wAws, wDocker bool
	}{
		{"someimg", "someimg", false, false},
		{"someimg$aws", "someimg", true, false},
		{"someimg$docker", "someimg", false, true},
		{"someimg$aws$docker", "someimg", true, true},
		{"someimg$docker$aws", "someimg", true, true},
	}
	for _, tt := range tests {
		got, gotaws, gotdocker := flow.ImageQualifiers(tt.img)
		if got != tt.wImg {
			t.Errorf("got %v, want %v", got, tt.wImg)
		}
		if gotaws != tt.wAws {
			t.Errorf("got %v, want %v", gotaws, tt.wAws)
		}
		if gotdocker != tt.wDocker {
			t.Errorf("got %v, want %v", gotdocker, tt.wDocker)
		}
	}
}
