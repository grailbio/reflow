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

func TestFile(t *testing.T) {
	fuzz := testutil.NewFuzz(nil)
	fsf := fuzz.File(true, true)
	for _, tt := range []struct {
		fs      reflow.Fileset
		want    reflow.File
		wantErr bool
	}{
		{fuzz.Fileset(true, true), reflow.File{}, true},
		{reflow.Fileset{Map: map[string]reflow.File{".": fsf}}, fsf, false},
	} {
		file, err := tt.fs.File()
		if got, want := err != nil, tt.wantErr; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if tt.wantErr {
			continue
		}
		if got, want := file, tt.want; got != want {
			t.Errorf("got %v, want %v", got, want)
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

func TestMapAssertionsByFile(t *testing.T) {
	fuzz := testutil.NewFuzz(nil)
	for i := 0; i < 100; i++ {
		fs := fuzz.FilesetDeep(fuzz.Intn(5)+1, fuzz.Intn(2)+1, false, true)
		files := fs.Files()
		for i := range files {
			f := files[i]
			f.Assertions = reflow.AssertionsFromEntry(reflow.AssertionKey{f.Source + "_replaced", "blob"}, map[string]string{"etag": "replaced"})
			files[i] = f
		}
		fs.MapAssertionsByFile(files)
		for _, f := range fs.Files() {
			if got, want := f.Assertions, reflow.AssertionsFromEntry(reflow.AssertionKey{f.Source + "_replaced", "blob"}, map[string]string{"etag": "replaced"}); !got.Equal(want) {
				t.Errorf("got %v, want %v", got, want)
			}
		}
	}
}

func TestSubst(t *testing.T) {
	fuzz := testutil.NewFuzz(nil)
	for i := 0; i < 100; i++ {
		for _, aok := range []bool{true, false} {
			fs := fuzz.FilesetDeep(fuzz.Intn(5)+1, fuzz.Intn(2)+1, true, fuzz.Intn(10)%2 == 0)
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
	fs := fuzz.FilesetDeep(fuzz.Intn(5)+1, fuzz.Intn(2)+1, fuzz.Intn(10)%2 == 0, fuzz.Intn(10)%2 == 0)
	a := reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{
		{"s1", "t"}: {"tag": "v"},
		{"s2", "t"}: {"tag": "v"},
	})
	existingByPath := make(map[string]*reflow.Assertions)
	for _, f := range fs.Files() {
		existingByPath[f.Source] = f.Assertions
	}
	if err := fs.AddAssertions(a); err != nil {
		t.Error(err)
	}
	wantAll := reflow.NewRWAssertions(reflow.NewAssertions())
	for _, f := range fs.Files() {
		ea := existingByPath[f.Source]
		if ea.IsEmpty() {
			continue
		}
		got := f.Assertions
		want, err := reflow.MergeAssertions(ea, a)
		if err != nil {
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
	if got, want := fs.Assertions(), wantAll.Assertions(); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	src := "some/random/file"
	file := reflow.File{Source: "some/random/file", ETag: "etag", Size: int64(len(src))}
	a = reflow.AssertionsFromEntry(reflow.AssertionKey{src, "test"}, map[string]string{"etag": "etag"})
	fs = reflow.Fileset{Map: map[string]reflow.File{src: file}}
	if err := fs.AddAssertions(a); err != nil {
		t.Errorf("unexpected %v", err)
	}
	if got, want := fs.Assertions(), a; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := fs.Map[src].Assertions, a; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
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

// oldFile is a JSON representation of reflow.File before reflow.File.Assertions were omitted.
// This is used to test conversion between old and new formats.
type oldFile struct {
	reflow.File
	Assertions *reflow.Assertions `json:",omitempty"`
}

// oldFileset is a JSON representation of reflow.Fileset before reflow.File.Assertions were omitted.
// This is used to test conversion between old and new formats.
type oldFileset struct {
	List []oldFileset       `json:",omitempty"`
	Map  map[string]oldFile `json:"Fileset,omitempty"`
}

func oldFs(fs reflow.Fileset) (ofs oldFileset) {
	if fs.List != nil {
		ofs.List = make([]oldFileset, len(fs.List))
		for i := range fs.List {
			ofs.List[i] = oldFs(fs.List[i])
		}
	}
	if fs.Map != nil {
		ofs.Map = make(map[string]oldFile, len(fs.Map))
		for k, v := range fs.Map {
			ofs.Map[k] = oldFile{v, v.Assertions}
		}
	}
	return
}

func newFs(ofs oldFileset) (fs reflow.Fileset) {
	if ofs.List != nil {
		fs.List = make([]reflow.Fileset, len(ofs.List))
		for i := range ofs.List {
			fs.List[i] = newFs(ofs.List[i])
		}
	}
	if ofs.Map != nil {
		fs.Map = make(map[string]reflow.File, len(ofs.Map))
		for k, v := range ofs.Map {
			f := v.File
			f.Assertions = v.Assertions
			fs.Map[k] = f
		}
	}
	return
}

func TestCustomMarshal(t *testing.T) {
	fuzz := testutil.NewFuzz(nil)
	for i := 0; i < 100; i++ {
		fs := fuzz.FilesetDeep(fuzz.Intn(5)+1, fuzz.Intn(2)+1, fuzz.Intn(10)%2 == 0, fuzz.Intn(10)%2 == 0)
		ofs := oldFs(fs)
		var b bytes.Buffer
		if err := fs.Write(&b, reflow.FilesetMarshalFmtJSON); err != nil {
			t.Fatal(err)
		}
		want, err := json.Marshal(ofs)
		if err != nil {
			t.Fatal(err)
		}
		// The custom marshaller uses json.Encoder which internally adds a newline after each Assertion.
		// Using json.Marshal yield no difference in bytes, but the encoder is more efficient.
		// Newlines in JSON outside of tokens are are inconsequential anyway.
		if got := strings.ReplaceAll(b.String(), "\n", ""); got != string(want) {
			t.Fatalf("\ngot : %s\nwant: %s\n", got, want)
		}
	}
}

func TestCustomUnmarshal(t *testing.T) {
	fuzz := testutil.NewFuzz(nil)
	for i := 0; i < 100; i++ {
		want := fuzz.FilesetDeep(fuzz.Intn(5)+1, 1, false, fuzz.Intn(10)%2 == 0)
		// Custom marshalProto, and unmarshalProto to oldFileset using JSON std library.
		var bb bytes.Buffer
		if err := want.Write(&bb, reflow.FilesetMarshalFmtProtoParts); err != nil {
			t.Fatal(err)
		}
		var fs2 reflow.Fileset
		if err := fs2.Read(&bb, reflow.FilesetMarshalFmtProtoParts); err != nil {
			t.Fatal(err)
		}
		if diff, yes := want.Diff(fs2); yes {
			t.Fatalf("got:\n%s\nwant:\n%s\ndiff:\n%v", fs2, want, diff)
		}
	}
}

var benchCombinations = []struct{ nfiles, nass int }{
	// Small/medium
	{10, 100},
	{10, 1000},
	{10, 10000},
	{100, 10},
	{100, 100},
	{100, 1000},
	{1000, 10},
	{1000, 100},
	// Large
	//{10, 100000},
	//{100, 10000},
	//{1000, 1000},
	//{10000, 10},
	//{10000, 100},
	//{10000, 1000},
	// Enormous
	//{10000, 10000},  // Needs lots of memory
}

func BenchmarkFileset(b *testing.B) {
	fuzz := testutil.NewFuzz(nil)
	for _, tt := range []struct {
		nfiles, nass int
		aok          bool
	}{
		{1, 100000, false},
		{1000, 10000, false},
		{100000, 1000, false},
	} {
		fs := fuzz.FilesetDeep(tt.nfiles, 0, false, tt.aok)
		a := createAssertions(tt.nass)
		for _, s := range []struct {
			name string
			fn   func() error
		}{
			{"writeassertions", func() error {
				files := fs.Files()
				fas := make([]*reflow.Assertions, len(files))
				for i, f := range files {
					fas[i] = f.Assertions
				}
				_, err := reflow.MergeAssertions(fas...)
				return err
			}},
			{"fileset.addassertions", func() error { return fs.AddAssertions(a) }},
		} {
			ss, nums := s, tt
			b.Run(fmt.Sprintf("%s-nfiles-%d-nass-%d", s.name, nums.nfiles, nums.nass), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := ss.fn(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

type WriterWithCounter struct {
	sink    io.Writer
	counter int
}

func (w *WriterWithCounter) Write(p []byte) (n int, err error) {
	w.counter += len(p)
	return w.sink.Write(p)
}

func (w *WriterWithCounter) BytesWritten() int {
	return w.counter
}

func BenchmarkMarshal(b *testing.B) {
	fuzz := testutil.NewFuzz(nil)
	for _, nums := range benchCombinations {
		fs := fuzz.FilesetDeep(nums.nfiles, 0, false, false)
		a := createAssertions(nums.nass)
		if err := fs.AddAssertions(a); err != nil {
			b.Fatal(err)
		}
		for _, s := range []struct {
			name   string
			encode func(io.Writer, reflow.Fileset) error
		}{
			{"json-std-lib-full", func(w io.Writer, v reflow.Fileset) error {
				b, err := json.Marshal(oldFs(v))
				if err != nil {
					return err
				}
				_, err = io.Copy(w, bytes.NewReader(b))
				return err
			}},
			{"json-std-lib-stream", func(w io.Writer, fs reflow.Fileset) error {
				return json.NewEncoder(w).Encode(oldFs(fs))
			}},
			{"json-custom-stream", func(w io.Writer, fs reflow.Fileset) error {
				return fs.Write(w, reflow.FilesetMarshalFmtJSON)
			}},
			{"json-custom-stream-parts", func(w io.Writer, fs reflow.Fileset) error {
				return fs.Write(w, reflow.FilesetMarshalFmtProtoParts)
			}},
		} {
			s, nums := s, nums
			b.Run(fmt.Sprintf("%s-nfiles-%d-nass-%d", s.name, nums.nfiles, nums.nass), func(b *testing.B) {
				b.ReportAllocs()
				wc := &WriterWithCounter{sink: ioutil.Discard}
				for i := 0; i < b.N; i++ {
					// Note: Discard the marshalled results since we are only benchmarking here.
					// Otherwise, even if we were to hold the results in a buffer, the results
					// are affected by the need to have to continuously grow the buffer as we marshalProto.
					// (especially in the custom marshaling case).
					if err := s.encode(wc, fs); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(wc.BytesWritten()/b.N), "diskB/op")
			})
		}
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	fuzz := testutil.NewFuzz(nil)
	for _, nums := range benchCombinations {
		fs := fuzz.FilesetDeep(nums.nfiles, 0, false, false)
		a := createAssertions(nums.nass)
		if err := fs.AddAssertions(a); err != nil {
			b.Fatal(err)
		}
		for _, s := range []struct {
			name   string
			encode func(io.Writer, *reflow.Fileset) error
			decode func(io.Reader, *reflow.Fileset) error
		}{
			{
				"json-std-lib",
				func(w io.Writer, fs *reflow.Fileset) error {
					return fs.Write(w, reflow.FilesetMarshalFmtJSON)
				},
				func(r io.Reader, fs *reflow.Fileset) error {
					var ofs oldFileset
					err := json.NewDecoder(r).Decode(&ofs)
					*fs = newFs(ofs)
					return err
				},
			},
			{
				"json-custom",
				func(w io.Writer, fs *reflow.Fileset) error {
					return fs.Write(w, reflow.FilesetMarshalFmtJSON)
				},
				func(r io.Reader, fs *reflow.Fileset) error {
					return fs.Read(r, reflow.FilesetMarshalFmtJSON)
				},
			},
			{
				"proto-custom-parts",
				func(w io.Writer, fs *reflow.Fileset) error {
					return fs.Write(w, reflow.FilesetMarshalFmtProtoParts)
				},
				func(r io.Reader, fs *reflow.Fileset) error {
					return fs.Read(r, reflow.FilesetMarshalFmtProtoParts)
				},
			},
		} {
			s, nums := s, nums
			buf := new(bytes.Buffer)
			if err := s.encode(buf, &fs); err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("%s-nfiles-%d-nass-%d", s.name, nums.nfiles, nums.nass), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var gotfs reflow.Fileset
					if err := s.decode(buf, &gotfs); err != nil && err != io.EOF {
						b.Fatal(err)
					}
					buf.Reset()
				}
			})
		}
	}
}
