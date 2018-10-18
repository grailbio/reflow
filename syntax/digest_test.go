// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"testing"

	"github.com/grailbio/reflow/flow"
)

func TestDigestExec(t *testing.T) {
	for _, expr := range []string{
		`exec(image := "ubuntu") (out file) {" cp {{file("s3://blah")}} {{out}} "}`,
		`exec(image := "ubuntu") (out file) {" cp {{    file("s3://blah")  }} {{  out}} "}`,
		`exec(image := "ubuntu") (x file) {" cp {{file("s3://blah")}} {{x}} "}`,
		`exec(image := "ubuntu", mem := 32*GiB) (x file) {" cp {{file("s3://blah")}} {{x}} "}`,
	} {
		v, _, _, err := eval(expr)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		f := v.(*flow.Flow)
		if got, want := f.Digest().String(), "sha256:ceff79828962397af02d8e2ea30cf6388f2858e0deefbecaa73fad1c6fc88816"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestDigestDelay(t *testing.T) {
	for _, expr := range []string{
		`{x := 1; delay(x)}`,
		`{y := 1; delay(y)}`,
	} {
		v, _, _, err := eval(expr)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		f := v.(*flow.Flow)
		if got, want := f.Digest().String(), "sha256:df14f3294bd1c14c9fd6423b6078f4699ae85d44bdf5c44bf838cbc6e9c99db1"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestDigestCompr(t *testing.T) {
	for _, expr := range []string{
		`[x*x | x <- delay([1,2,3])]`,
		`[y*y | y <- delay([1,2,3])]`,
	} {
		v, _, _, err := eval(expr)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		f := v.(*flow.Flow)
		if got, want := f.Digest().String(), "sha256:8310ad9a33309b3b9b37c1bed2ec7898757cad3b3b5929aee538797bfee8fba0"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestDigestMap(t *testing.T) {
	for _, expr := range []string{
		`delay(["x": 1, "y": 4, "z": 100])`,
		`delay(["y": 4, "z": 100, "x": 1])`,
	} {
		v, _, _, err := eval(expr)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		f := v.(*flow.Flow)
		if got, want := f.Digest().String(), "sha256:f8a6305d5c748c774a08c8db12d02a322e64562f9c11b83a0ca59b5642fcf631"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
