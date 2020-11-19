// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strings"
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
		Assertions: reflow.AssertionsFromEntry(reflow.AssertionKey{"a/b/c", "n"}, map[string]string{"tag": "v"}),
	}

	fs1 = reflow.Fileset{Map: map[string]reflow.File{
		"foo": file1,
		"bar": file2,
	}}
	fs2 = reflow.Fileset{Map: map[string]reflow.File{
		"a/b/c": file3,
		"bar":   file2,
	}}
	vlist = reflow.Fileset{List: []reflow.Fileset{fs1, fs2}}
)

const vlistSHA256 = "sha256:d60e67ce9e89548b502a5ad7968e99caed0d388f0a991b906f41a7ba65adb31f"

func TestValueDigest(t *testing.T) {
	if fs1.Digest() == fs2.Digest() {
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
	f1 := reflow.File{
		ID:         fid,
		Assertions: reflow.AssertionsFromEntry(reflow.AssertionKey{"s1", "t"}, map[string]string{"tag": "v"}),
	}
	f2 := reflow.File{
		ID:         fid,
		Assertions: reflow.AssertionsFromEntry(reflow.AssertionKey{"s2", "t"}, map[string]string{"tag": "v"}),
	}
	fempty := reflow.File{ID: fid, Assertions: aempty}
	fnil := reflow.File{ID: fid}
	f3, f3dup := reflow.File{Source: "same_source", ETag: "etag"}, reflow.File{Source: "same_source", ETag: "etag"}
	f3mid := reflow.File{Source: "same_source", ETag: "etag", ContentHash: reflow.Digester.FromString("mid")}
	f3middup := reflow.File{Source: "same_source", ETag: "etag", ContentHash: reflow.Digester.FromString("mid")}
	f4mid := reflow.File{Source: "same_source", ETag: "etag", ContentHash: reflow.Digester.FromString("mid2")}
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
		{f3, f3dup, true}, {f3, f3mid, true}, {f3mid, f3middup, true}, {f3dup, f3middup, true},
		{f3mid, f4mid, false}, {f3middup, f4mid, false},
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

func TestDiff(t *testing.T) {
	for _, tt := range []struct {
		a, b  reflow.Fileset
		wantD bool
		want  string
	}{
		{fs1, fs1, false, ""},
		{vlist, vlist, false, ""},
		{
			reflow.Fileset{List: []reflow.Fileset{vlist, fs2}, Map: map[string]reflow.File{"foo": file1}},
			reflow.Fileset{List: []reflow.Fileset{vlist, fs2}, Map: map[string]reflow.File{"foo": file1}},
			false, "",
		},
		{
			vlist, fs1, true,
			`
"bar" = void -> fcde2b2e
"foo" = void -> 2c26b46b
[0]:val<bar=fcde2b2e, ...6B> -> empty
[1]:val<a/b/c=d76a7b72, ...8B> -> empty`,
		},
		{
			fs1, fs2, true,
			`
"a/b/c" = void -> d76a7b72
"foo" = 2c26b46b -> void`,
		},
		{
			reflow.Fileset{List: []reflow.Fileset{fs1, fs2}, Map: map[string]reflow.File{"foo": file1}},
			reflow.Fileset{List: []reflow.Fileset{fs2, fs1}},
			true,
			`
"foo" = 2c26b46b -> void
  [0]:"a/b/c" = void -> d76a7b72
  [0]:"foo" = 2c26b46b -> void
  [1]:"a/b/c" = d76a7b72 -> void
  [1]:"foo" = void -> 2c26b46b`,
		},
	} {
		got, gotD := tt.a.Diff(tt.b)
		if tt.wantD != gotD {
			t.Errorf("got %v, want %v", gotD, tt.wantD)
		}
		if want := strings.TrimPrefix(tt.want, "\n"); got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	}
	const N = 100
	fuzz := testutil.NewFuzz(nil)
	for i := 0; i < N; i++ {
		fs := fuzz.FilesetDeep(fuzz.Intn(10)+1, 5, false, false)
		if diffStr, diff := fs.Diff(fs); diff || diffStr != "" {
			t.Errorf("got different, want no different:\n%v", diffStr)
		}
	}
}

func TestAssertions(t *testing.T) {
	fuzz := testutil.NewFuzz(nil)
	fs := fuzz.Fileset(true, true)
	a := reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{
		{"s1", "t"}: {"tag": "v"},
		{"s2", "t"}: {"tag": "v"},
	})
	existingByPath := make(map[string]*reflow.Assertions)
	for _, f := range fs.Files() {
		existingByPath[f.Source] = f.Assertions
	}
	_ = fs.AddAssertions(a)

	wantAll := reflow.NewAssertions()
	for _, f := range fs.Files() {
		ea := existingByPath[f.Source]
		got, want := f.Assertions, ea
		if err := want.AddFrom(a); err != nil {
			t.Error(err)
		}
		if !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if err := wantAll.AddFrom(ea); err != nil {
			t.Error(err)
		}
	}
	_ = wantAll.AddFrom(a)
	gotAll, err := reflow.MergeAssertions(fs.Assertions()...)
	if err != nil {
		t.Errorf("unexpected %v", err)
	}
	if got, want := gotAll, wantAll; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	src := "some/random/file"
	file := reflow.File{Source: "some/random/file", ETag: "etag", Size: int64(len(src))}
	a = reflow.AssertionsFromEntry(reflow.AssertionKey{src, "test"}, map[string]string{"etag": "etag"})
	fs = reflow.Fileset{Map: map[string]reflow.File{src: file}}
	if err := fs.AddAssertions(a); err != nil {
		t.Errorf("unexpected %v", err)
	}
	fsa, err := reflow.MergeAssertions(fs.Assertions()...)
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

func createFilesets(nfs, nfiles, depth int) []reflow.Fileset {
	fuzz := testutil.NewFuzz(nil)
	fss := make([]reflow.Fileset, nfs)
	for i := 0; i < nfs; i++ {
		fss[i] = fuzz.FilesetDeep(nfiles, depth, false, true)
	}
	return fss
}

func createAssertions(n int) *reflow.Assertions {
	fuzz := testutil.NewFuzz(nil)
	m := make(map[reflow.AssertionKey]map[string]string, n)
	for i := 0; i < n; i++ {
		gk := reflow.AssertionKey{Subject: "subject-" + fuzz.StringMinLen(80, ""), Namespace: "namespace"}
		m[gk] = map[string]string{
			"object1": "value-" + fuzz.String(""),
			"object2": "value-" + fuzz.String(""),
			"object3": "value-" + fuzz.String(""),
		}
	}
	return reflow.AssertionsFromMap(m)
}

var benchCombinations = []struct{ nfiles, nass int }{
	{10, 1000},
	{10, 10000},
	{10, 100000},
	{100, 1000},
	{1000, 1000},
	{10000, 100},
	{10000, 1000},
	//{10000, 10000},
}

func BenchmarkFileset(b *testing.B) {
	fuzz := testutil.NewFuzz(nil)
	for _, nums := range []struct{ nfiles, nass int }{
		{1, 100000},
		{1000, 10000},
		{100000, 1000},
	} {
		fs := fuzz.FilesetDeep(nums.nfiles, 0, false, false)
		a := createAssertions(nums.nass)
		for _, s := range []struct {
			name string
			fn   func() error
		}{
			{"writeassertions", func() error { _, err := reflow.MergeAssertions(fs.Assertions()...); return err }},
			{"addassertions", func() error { return fs.AddAssertions(a) }},
		} {
			s, nums := s, nums
			b.Run(fmt.Sprintf("%s-nfiles-%d-nass-%d", s.name, nums.nfiles, nums.nass), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := s.fn(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkMarshal(b *testing.B) {
	b.ReportAllocs()
	fuzz := testutil.NewFuzz(nil)
	for _, nums := range benchCombinations {
		fs := fuzz.FilesetDeep(nums.nfiles, 0, false, false)
		a := createAssertions(nums.nass)
		if err := fs.AddAssertions(a); err != nil {
			b.Fatal(err)
		}
		want, err := json.Marshal(fs)
		if err != nil {
			b.Fatal(err)
		}
		for _, s := range []struct {
			name       string
			marshaller func(v interface{}) ([]byte, error)
		}{
			{"json-std-lib", json.Marshal},
		} {
			s, nums := s, nums
			b.Run(fmt.Sprintf("%s-nfiles-%d-nass-%d", s.name, nums.nfiles, nums.nass), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					got, err := s.marshaller(fs)
					if err != nil {
						b.Fatal(err)
					}
					if !bytes.Equal(got, want) {
						b.Error("mismatch")
					}
				}
			})
		}
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	b.ReportAllocs()
	fuzz := testutil.NewFuzz(nil)
	for _, nums := range benchCombinations {
		fs := fuzz.FilesetDeep(nums.nfiles, 0, false, false)
		a := createAssertions(nums.nass)
		if err := fs.AddAssertions(a); err != nil {
			b.Fatal(err)
		}
		fssb, err := json.Marshal(fs)
		if err != nil {
			b.Fatal(err)
		}
		r := bytes.NewReader(fssb)
		for _, s := range []struct {
			name string
			// unmarshaller func(data []byte, v interface{}) error
			decode func(r io.Reader, v interface{}) error
		}{
			{"json-std-lib", func(r io.Reader, v interface{}) error {
				return json.NewDecoder(r).Decode(v)
			}},
		} {
			s, nums := s, nums
			b.Run(fmt.Sprintf("%s-nfiles-%d-nass-%d", s.name, nums.nfiles, nums.nass), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var gotfs reflow.Fileset
					if err := s.decode(r, &gotfs); err != nil && err != io.EOF {
						b.Fatal(err)
					}
					if got, want := gotfs, fs; !got.Equal(want) {
						b.Error("mismatch")
					}
					r.Reset(fssb)
				}
			})
		}
	}
}

func BenchmarkMarshalToFile(b *testing.B) {
	b.ReportAllocs()
	fuzz := testutil.NewFuzz(nil)
	for _, nums := range benchCombinations {
		fs := fuzz.FilesetDeep(nums.nfiles, 0, false, false)
		a := createAssertions(nums.nass)
		if err := fs.AddAssertions(a); err != nil {
			b.Fatal(err)
		}
		for _, s := range []struct {
			name           string
			marshallToFile func(w io.Writer, v interface{}) error
		}{
			{"json-std-lib-full", func(w io.Writer, v interface{}) error {
				b, err := json.Marshal(v)
				if err != nil {
					return err
				}
				_, err = io.Copy(w, bytes.NewReader(b))
				return err
			}},
			{"json-std-lib-stream", func(w io.Writer, v interface{}) error {
				e := json.NewEncoder(w)
				return e.Encode(v)
			}},
		} {
			s, nums := s, nums
			b.Run(fmt.Sprintf("%s-nfiles-%d-nass-%d", s.name, nums.nfiles, nums.nass), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					// Note: Discard the marshalled results since we are only benchmarking here.
					if err := s.marshallToFile(ioutil.Discard, fs); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
