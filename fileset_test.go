// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow_test

import (
	"reflect"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/test/testutil"
)

var (
	file1 = reflow.File{ID: reflow.Digester.FromString("foo"), Size: 3}
	file2 = reflow.File{ID: reflow.Digester.FromString("bar"), Size: 3}
	file3 = reflow.File{ID: reflow.Digester.FromString("a/b/c"), Size: 5}

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
		t.Errorf("got %v, want %v", got, want)
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

func TestEqual(t *testing.T) {
	const N = 1000
	var last reflow.Fileset
	fuzz := testutil.NewFuzz(nil)
	for i := 0; i < N; i++ {
		fs := fuzz.Fileset(true)
		if !fs.Equal(fs) {
			t.Errorf("fileset %v not equal to self", fs)
		}
		if fs.Equal(last) {
			t.Errorf("fileset %v equal to %v", fs, last)
		}
		last = fs
	}
}

func TestSubst(t *testing.T) {
	fuzz := testutil.NewFuzz(nil)

	fs := fuzz.Fileset(true)
	_, ok := fs.Subst(nil)
	if ok {
		t.Fatal("unexpected resolved fileset")
	}

	// Create a substitution map:
	sub := make(map[reflow.File]reflow.File)
	for _, file := range fs.Files() {
		if !file.IsRef() {
			continue
		}
		sub[file] = fuzz.File(false)
	}
	fs, ok = fs.Subst(sub)
	if !ok {
		t.Error("expected resolved fileset")
	}
	for _, file := range fs.Files() {
		if file.IsRef() {
			t.Errorf("unexpected reference file %v", file)
		}
	}
}
