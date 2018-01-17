// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow_test

import (
	"reflect"
	"testing"

	"github.com/grailbio/reflow"
)

func TestResources(t *testing.T) {
	r1 := reflow.Resources{"mem": 10, "cpu": 5, "disk": 1}
	r2 := reflow.Resources{"mem": 5, "cpu": 2, "disk": 3}
	var got, want reflow.Resources
	got.Sub(r1, r2)
	if want := (reflow.Resources{"mem": 5, "cpu": 3, "disk": -2}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	got.Add(r1, r2)
	if want := (reflow.Resources{"mem": 15, "cpu": 7, "disk": 4}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	got.Add(r1, r2)
	want.Add(r2, r1)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if r1.Available(r2) {
		t.Errorf("expected %v to be unavailable in %v", r2, r1)
	}
	r3 := reflow.Resources{"mem": 3, "cpu": 1, "disk": 1}
	if !r1.Available(r3) {
		t.Errorf("expected %v to be available in %v", r3, r1)
	}
	got.Min(r1, r2)
	if want := (reflow.Resources{"mem": 5, "cpu": 2, "disk": 1}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	got.Max(r1, r2)
	if want := (reflow.Resources{"mem": 10, "cpu": 5, "disk": 3}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRequirements(t *testing.T) {
	var (
		req  reflow.Requirements
		res1 = reflow.Resources{"mem": 10, "cpu": 5, "disk": 1}
		res2 = reflow.Resources{"mem": 20, "cpu": 3, "disk": 1}
	)
	req.Add(reflow.Requirements{res1, res1, false})
	req.Add(reflow.Requirements{res2, res2, false})
	if got, want := req.Min, (reflow.Resources{"mem": 20, "cpu": 5, "disk": 1}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

var (
	file1 = reflow.File{reflow.Digester.FromString("foo"), 3}
	file2 = reflow.File{reflow.Digester.FromString("bar"), 3}
	file3 = reflow.File{reflow.Digester.FromString("a/b/c"), 5}

	v1 = reflow.Fileset{Map: map[string]reflow.File{
		"foo": file1,
		"bar": file2,
	}}
	v2 = reflow.Fileset{Map: map[string]reflow.File{
		"a/b/c": file3,
		"bar":   file2,
	}}
	vlist = reflow.Fileset{List: []reflow.Fileset{v1, v2}}
)

const vlistSHA256 = "sha256:d60e67ce9e89548b502a5ad7968e99caed0d388f0a991b906f41a7ba65adb31f"

func TestValueDigest(t *testing.T) {
	if v1.Digest() == v2.Digest() {
		t.Errorf("did not expect v1, v2 to have same digest")
	}
	if got, want := vlist.Digest().String(), vlistSHA256; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestValueFile(t *testing.T) {
	files := vlist.Files()
	expected := map[reflow.File]bool{file1: true, file2: true, file3: true}
	for _, f := range files {
		if !expected[f] {
			t.Errorf("unexpected file %v", f)
		}
		delete(expected, f)
	}
	if len(expected) != 0 {
		t.Errorf("expected additional files %v", expected)
	}
}

func TestValuePullup(t *testing.T) {
	got := vlist.Pullup()
	want := reflow.Fileset{Map: map[string]reflow.File{"foo": file1, "bar": file2, "a/b/c": file3}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestValueN(t *testing.T) {
	if got, want := vlist.N(), 4; got != want {
		t.Errorf("got %v, want %v")
	}
}

func TestEmpty(t *testing.T) {
	empty := []reflow.Fileset{
		{},
		{List: make([]reflow.Fileset, 1)},
		{List: []reflow.Fileset{{}, {Map: map[string]reflow.File{}}, {List: make([]reflow.Fileset, 100)}}},
	}
	for i, fs := range empty {
		if !fs.Empty() {
			t.Errorf("expected empty %d %v", i, fs)
		}
	}
}

func TestAnyEmpty(t *testing.T) {
	empty := []reflow.Fileset{
		{},
		{List: make([]reflow.Fileset, 1)},
		{List: []reflow.Fileset{{}, {Map: map[string]reflow.File{}}, {List: make([]reflow.Fileset, 100)}}},
		{List: []reflow.Fileset{{Map: map[string]reflow.File{".": reflow.File{}}}, {}}},
	}
	for i, fs := range empty {
		if !fs.AnyEmpty() {
			t.Errorf("expected anyempty %d %v", i, fs)
		}
	}
}
