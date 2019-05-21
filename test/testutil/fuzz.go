// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package testutil

import (
	"fmt"
	"math"
	"math/rand"
	"strings"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
)

// Fuzz provides a simple deterministic fuzzer for Reflow
// data types.
type Fuzz struct{ *rand.Rand }

// NewFuzz returns a new fuzzer based on the provided
// random number generator. If r is nil, NewFuzz creates
// one with a fixed seed.
func NewFuzz(r *rand.Rand) *Fuzz {
	if r == nil {
		r = rand.New(rand.NewSource(23))
	}
	return &Fuzz{r}
}

var genes = []string{
	"ATM", "BARD1", "BRCA1", "BRCA2", "CDH1", "CHEK2", "NBN", "NF1", "PALB2", "PTEN", "STK11", "TP53",
	"BRIP1", "RAD51C", "RAD51D", "EPCAM",
	"MLH1", "MSH2", "MSH6", "PMS2", "STK11",
	"EPCAM", "MLH1", "MSH2", "MSH6", "PMS2", "STK11",
}

// String returns a random string comprising gene names separated
// by the provided separator.
func (f *Fuzz) String(sep string) string {
	var (
		b strings.Builder
		n = f.Intn(5) + 1
	)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(sep)
		}
		b.WriteString(genes[f.Intn(len(genes))])
	}
	return b.String()
}

// Digest returns a random Reflow digest.
func (f *Fuzz) Digest() digest.Digest {
	return reflow.Digester.Rand(f.Rand)
}

// File returns a random file. If refok is true, then
// the returned file may be a reference file.  If aok
// is true, then the returned file will contain assertions.
func (f *Fuzz) File(refok, wantAssertions bool) reflow.File {
	var file reflow.File
	if refok && f.Float64() < 0.5 {
		file = reflow.File{
			Size:   int64(f.Uint64()),
			Source: fmt.Sprintf("s3://%s/%s", f.String(""), f.String("/")),
			ETag:   f.String(""),
		}
	} else {
		file = reflow.File{ID: f.Digest()}
	}
	if wantAssertions {
		file.Source = fmt.Sprintf("s3://%s/%s", f.String(""), f.String("/"))
		file.Assertions = reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
			reflow.AssertionKey{"blob", file.Source, "etag"}: fmt.Sprintf("etag%d", f.Intn(10))})
	}
	return file
}

// Fileset returns a random fileset. If refok is true, then
// the returned fileset may contain reference files.  If aok
//// is true, then the returned fileset will contain assertions.
func (f *Fuzz) Fileset(refok, aok bool) reflow.Fileset {
	return f.fileset(0, refok, aok)
}

func (f *Fuzz) fileset(depth int, refok, aok bool) (fs reflow.Fileset) {
	if f.Float64() < math.Pow(0.5, float64(depth+1)) {
		n := f.Intn(10) + 1
		fs.List = make([]reflow.Fileset, n)
		for i := range fs.List {
			fs.List[i] = f.fileset(depth+1, refok, aok)
		}
	} else {
		n := f.Intn(10) + 1
		fs.Map = make(map[string]reflow.File)
		for i := 0; i < n; i++ {
			fs.Map[f.String("/")] = f.File(refok, aok)
		}
	}
	return
}
