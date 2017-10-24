// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"testing"

	"github.com/grailbio/reflow"
)

func TestStableDigest(t *testing.T) {
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
		f := v.(*reflow.Flow)
		if got, want := f.Digest().String(), "sha256:ceff79828962397af02d8e2ea30cf6388f2858e0deefbecaa73fad1c6fc88816"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
