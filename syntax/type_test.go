// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"testing"

	"github.com/grailbio/reflow/types"
)

func TestSynth(t *testing.T) {
	for _, ex := range []struct {
		expr string
		typ  string
	}{
		{
			`{f1: 123, f2: "okay", f3: [1,2,3]}`,
			`{f1 int, f2 string, f3 [int]}`,
		},
		{
			`[{f1: 123, f2: "blah"}, {f2: "ok", f3: 321}]`,
			`[{f2 string}]`,
		},
		{
			`{foo := 123; bar := foo; bar}`,
			`int`,
		},
		{
			`{val foo string = 123; bar := foo; bar}`,
			`error: identifier "foo" not defined`,
		},
		{
			`{x := 1; y := "a"; {x, y}}`,
			`{x int, y string}`,
		},
		{
			`{x := 2; y := a; 123}`,
			`error: identifier "a" not defined`,
		},
		{`1 == 2`, `bool`},
		{`if 1 == 2 { "ok" } else { "not ok" }`, `string`},
		{`if 1 == 2 { "ok" } else { 123 }`, `error: kind mismatch: string != int`},
		{`"ok" == 2`, `error: cannot apply binary operator "==" to type string and int`},
		{`"a"+"b"`, `string`},
		{`func(a, b string) => a+b`, `func(a, b string) string`},
		{`func(a int, f func(int) string) => f(a)`, `func(a int, f func(int) string) string`},
		{`{func f(x, y int) = x; f(1)}`, `error: too few arguments in call to f
	have (int)
	want (x, y int)`},
		{`{func f(x string, y int) = x; f(1, "ok")}`, `error: cannot use type int as type string in argument to f (type func(x string, y int) string)`},
		{`exec(image := "blah") (hello file) {" echo \{hello} \{ "ok"+"blah" }  "}`, `(hello file)`},
		{`exec(cpu := 10) string {" ok "}`, `error: exec image parameter is required`},
		{`exec(image := "blah") (ok dir) {" {{notok}} "}`, `error: interpolation expression error: identifier "notok" not defined`},
		{`{x := 1; exec(image := "foo") (ok dir) {" echo \{x} > \{ok}/foobar "}}`, `(ok dir)`},
		{`{func concat(x, y string) = x+y; exec(image := concat("a", "b")) (out file) {" "}}`, `(out file)`},
		{`exec(image := "a"+ "b") file {" "}`, `error: output 0 (type file) must be labelled`},
		{`exec(image := "a"+ "b") (xyz file) {" "}`, `(xyz file)`},
		{`exec(image := "") (xxx string) {" "}`, `error: execs can only return files and dirs, not (xxx string)`},
		{
			`exec(image := "", mem := len(exec(image := "") (out file) {" echo 123 "})) (out file) {" echo foo >{{out}} "}`,
			`error: exec parameter mem is not immediate`,
		},
		{`[{a: 1, b: 2}, {a: 1}]`, `[{a int}]`},
	} {
		p := Parser{Mode: ParseExpr, Body: bytes.NewReader([]byte(ex.expr))}
		if err := p.Parse(); err != nil {
			t.Errorf("parsing expression %q: %v", ex.expr, err)
			continue
		}
		p.Expr.init(nil, types.NewEnv())
		if got, want := p.Expr.Type.String(), ex.typ; got != want {
			t.Errorf("got {`%s`, `%s`}, want {`%s`, `%s`}", ex.expr, got, ex.expr, want)
		}
	}
}
