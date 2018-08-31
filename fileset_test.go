// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow_test

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/grailbio/reflow"
)

var (
	file1 = reflow.File{reflow.Digester.FromString("foo"), 3, "", ""}
	file2 = reflow.File{reflow.Digester.FromString("bar"), 3, "", ""}
	file3 = reflow.File{reflow.Digester.FromString("a/b/c"), 5, "", ""}

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
	r := rand.New(rand.NewSource(N))
	for i := 0; i < N; i++ {
		fs := randomFileset(r, 0)
		if !fs.Equal(fs) {
			t.Errorf("fileset %v not equal to self", fs)
		}
		if fs.Equal(last) {
			t.Errorf("fileset %v equal to %v", fs, last)
		}
		last = fs
	}
}

var genes = []string{
	"ATM", "BARD1", "BRCA1", "BRCA2", "CDH1", "CHEK2", "NBN", "NF1", "PALB2", "PTEN", "STK11", "TP53",
	"BRIP1", "RAD51C", "RAD51D", "EPCAM",
	"MLH1", "MSH2", "MSH6", "PMS2", "STK11",
	"EPCAM", "MLH1", "MSH2", "MSH6", "PMS2", "STK11",
}

func randomString(r *rand.Rand, sep string) string {
	var (
		b strings.Builder
		n = r.Intn(5)
	)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(sep)
		}
		b.WriteString(genes[r.Intn(len(genes))])
	}
	return b.String()
}

func randomFile(r *rand.Rand) reflow.File {
	if r.Float64() < 0.5 {
		return reflow.File{
			Size:   int64(r.Uint64()),
			Source: fmt.Sprintf("s3://%s/%s", randomString(r, ""), randomString(r, "/")),
			ETag:   randomString(r, ""),
		}
	} else {
		return reflow.File{ID: reflow.Digester.Rand(r)}
	}
}

func randomFileset(r *rand.Rand, depth int) (fs reflow.Fileset) {
	if r.Float64() < math.Pow(0.5, float64(depth+1)) {
		n := r.Intn(10) + 1
		fs.List = make([]reflow.Fileset, n)
		for i := range fs.List {
			fs.List[i] = randomFileset(r, depth+1)
		}
	} else {
		n := r.Intn(10) + 1
		fs.Map = make(map[string]reflow.File)
		for i := 0; i < n; i++ {
			fs.Map[randomString(r, "/")] = randomFile(r)
		}
	}
	return
}
