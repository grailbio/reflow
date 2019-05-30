// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow_test

import (
	"reflect"
	"testing"

	"encoding/json"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/test/testutil"
)

var (
	file1 = reflow.File{ID: reflow.Digester.FromString("foo"), Size: 3}
	file2 = reflow.File{ID: reflow.Digester.FromString("bar"), Size: 3}
	file3 = reflow.File{
		ID: reflow.Digester.FromString("a/b/c"), Size: 5,
		Assertions: reflow.AssertionsFromMap(map[reflow.AssertionKey]string{{"n", "a/b/c", "tag"}: "v"})}

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

const vlistSHA256 = "sha256:44f5143efffbecb099444784d548ac35c80de65d422038a2017cbbd205fc0cc5"

func TestValueDigest(t *testing.T) {
	if v1.Digest() == v2.Digest() {
		t.Errorf("did not w v1, v2 to have same digest")
	}
	if got, want := vlist.Digest().String(), vlistSHA256; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestValueFile(t *testing.T) {
	files := vlist.Files()
	expected := map[digest.Digest]reflow.File{file1.Digest(): file1, file2.Digest(): file2, file3.Digest(): file3}
	for _, f := range files {
		if _, ok := expected[f.Digest()]; !ok {
			t.Errorf("unexpected file %v", f)
		}
		delete(expected, f.Digest())
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
		fs := fuzz.Fileset(true, i%2 == 0)
		if !fs.Equal(fs) {
			t.Errorf("fileset %v not equal to self", fs)
		}
		if fs.Equal(last) {
			t.Errorf("fileset %v equal to %v", fs, last)
		}
		last = fs
	}
}

func TestEqual2(t *testing.T) {
	fid := reflow.Digester.FromString("foo")
	f1 := reflow.File{ID: fid, Assertions: reflow.AssertionsFromMap(map[reflow.AssertionKey]string{{"t", "s1", "tag"}: "v"})}
	f2 := reflow.File{ID: fid, Assertions: reflow.AssertionsFromMap(map[reflow.AssertionKey]string{{"t", "s2", "tag"}: "v"})}
	fempty := reflow.File{ID: fid, Assertions: reflow.AssertionsFromMap(map[reflow.AssertionKey]string{})}
	fnil := reflow.File{ID: fid}
	tests := []struct {
		a, b reflow.File
		w    bool
	}{
		{reflow.File{Source: "a"}, reflow.File{Source: "b"}, false},
		{reflow.File{Source: "a"}, reflow.File{Source: "a"}, false},
		{reflow.File{Source: "a", ETag: "e"}, reflow.File{Source: "a"}, false},
		{reflow.File{Source: "a", ETag: "e"}, reflow.File{Source: "a", ETag: "e"}, true},
		{f1, f2, true},
		{fempty, fnil, true}, {fnil, fempty, true},
		{f1, fempty, true}, {fempty, f1, true},
		{f1, fnil, true}, {fnil, f1, true},
	}
	for _, tt := range tests {
		if got, want := tt.a.Equal(tt.b), tt.w; got != want {
			t.Errorf("File<%s> equal File<%s>: got %v, want %v", tt.a, tt.b, got, want)
		}
	}
}

func TestJson(t *testing.T) {
	const N = 1000
	fuzz := testutil.NewFuzz(nil)
	for _, aok := range []bool{true, false} {
		for i := 0; i < N; i++ {
			fs := fuzz.Fileset(true, aok)
			b, err := json.Marshal(fs)
			if err != nil {
				t.Errorf("marshal %v", err)
			}
			var newFs reflow.Fileset
			if err := json.Unmarshal(b, &newFs); err != nil {
				t.Errorf("unmarshal %v", err)
			}
			if got, want := newFs, fs; !got.Equal(want) {
				t.Errorf("got %v, want %v", got, want)
			}
		}
	}
}

func TestSubst(t *testing.T) {
	fuzz := testutil.NewFuzz(nil)

	for _, aok := range []bool{true, false} {
		fs := fuzz.Fileset(true, aok)
		_, ok := fs.Subst(nil)
		if ok {
			t.Fatal("unexpected resolved fileset")
		}

		// Create a substitution map:
		sub := make(map[digest.Digest]reflow.File)
		for _, file := range fs.Files() {
			if !file.IsRef() {
				continue
			}
			sub[file.Digest()] = fuzz.File(false, aok)
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
}

func TestAssertions(t *testing.T) {
	fuzz := testutil.NewFuzz(nil)
	fs := fuzz.Fileset(true, true)
	a := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{{"t", "s1", "tag"}: "v", {"t", "s2", "tag"}: "v"})
	existingByPath := make(map[string]*reflow.Assertions)
	for _, f := range fs.Files() {
		existingByPath[f.Source] = f.Assertions
	}
	fs.AddAssertions(a)

	wantAll := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{})
	for _, f := range fs.Files() {
		ea := existingByPath[f.Source]
		got, want := f.Assertions, ea
		want.AddFrom(a)
		if !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
		wantAll.AddFrom(ea)
	}
	wantAll.AddFrom(a)
	gotAll, err := fs.Assertions()
	if err != nil {
		t.Errorf("unexpected %v", err)
	}
	if got, want := gotAll, wantAll; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	src := "some/random/file"
	file := reflow.File{Source: "some/random/file", ETag: "etag", Size: int64(len(src))}
	a = reflow.AssertionsFromMap(map[reflow.AssertionKey]string{{"test", src, "etag"}: "etag"})
	fs = reflow.Fileset{Map: map[string]reflow.File{src: file}}
	if err := fs.AddAssertions(a); err != nil {
		t.Errorf("unexpected %v", err)
	}
	fsa, err := fs.Assertions()
	if err != nil {
		t.Errorf("unexpected %v", err)
	}
	if got, want := fsa, a; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := fs.Map[src].Assertions, a; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
