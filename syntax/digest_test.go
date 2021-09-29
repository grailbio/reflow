// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/values"
)

func TestDigestExec(t *testing.T) {
	for _, expr := range []struct {
		template string
		kSha     string
	}{
		{
			`exec(image := "ubuntu") (out file) {" cp {{file("s3://blah")}} {{out}} "}`,
			"sha256:8452af59a381c85ee9f0e46d54f90ace40068b034a69d732967a1eb9cd7ed3b5"},
		{
			`exec(image := "ubuntu") (out file) {" cp {{    file("s3://blah")  }} {{  out}} "}`,
			"sha256:217db429e085cd6cda825b6ef848491e26d28cc149fb1d522a6efc0f9039b701"},
		{
			`exec(image := "ubuntu") (x file) {" cp {{file("s3://blah")}} {{x}} "}`,
			"sha256:983af1e08bc0f1f84afa8660440c15361009d6cc1b3f1dc2e2872f5a7a47474a"},
		{
			`exec(image := "ubuntu", mem := 32*GiB) (x file) {" cp {{file("s3://blah")}} {{x}} "}`,
			"sha256:531ff51daa9804678e04b76e03bfc86d45a3028302574324e17e485144b5718a"},
	} {
		v, _, _, err := eval(expr.template)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		f := v.(*flow.Flow)
		if got, want := f.Digest().String(), expr.kSha; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := f.Op, flow.K; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		fd := reflow.File{ID: reflow.Digester.FromString("test")}
		f = f.K([]values.T{fd})
		if got, want := f.Digest().String(), "sha256:d8e460ec44666ed75cc4d41a46b961646e9c669266a2698cdb54f0323ebf7be4"; got != want {
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
	for _, tc := range []struct {
		expr string
		want string
	}{
		// we expect the first two cases to have the same digest because they are equivalent when evaluated
		{`[x*x | x <- delay([1,2,3])]`, "sha256:337a09abbd076b3c5744b6fc7eec05f3035a79431503b452153aced03a741d57"},
		{`[y*y | y <- delay([1,2,3])]`, "sha256:337a09abbd076b3c5744b6fc7eec05f3035a79431503b452153aced03a741d57"},
		// despite having the same identifiers as the previous case, this digest should be different
		{`[y*y | y <- delay([4,5,6])]`, "sha256:1a62e182832898d8d2e9d096be2af02302aa186732c5dfdb8f797f4ce9469462"},
	} {
		v, _, _, err := eval(tc.expr)
		if err != nil {
			t.Fatalf("%s: %v", tc.expr, err)
		}
		f := v.(*flow.Flow)
		if got, want := f.Digest().String(), tc.want; got != want {
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

func TestDigestVariant(t *testing.T) {
	for _, c := range []struct {
		expr   string
		digest string
	}{
		{
			`{x := 1; #Foo(x)}`,
			"sha256:0661ccd5ff53d713ec0e11ce3abdcddf4e82c0545ccae3584fd96acd21d4b47b",
		},
		{
			`{y := 1; #Foo(y)}`,
			"sha256:0661ccd5ff53d713ec0e11ce3abdcddf4e82c0545ccae3584fd96acd21d4b47b",
		},
		{
			`#Foo`,
			"sha256:d53df340bde56133204297f48a5d54c8b2d76f971846784061d6f76ed9d6ae76",
		},
	} {
		p := Parser{Body: bytes.NewReader([]byte(c.expr)), Mode: ParseExpr}
		if err := p.Parse(); err != nil {
			t.Fatalf("%s: %v", c.expr, err)
		}
		e := p.Expr
		_, venv := Stdlib()
		d := e.Digest(venv)
		if got, want := d.String(), c.digest; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
