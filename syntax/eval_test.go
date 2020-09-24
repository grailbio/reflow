// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"errors"
	"math/big"
	"reflect"
	"regexp"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// eval parses, type checks, and then evaluates expression e
func eval(e string) (values.T, *types.T, *Session, error) {
	p := Parser{Body: bytes.NewReader([]byte(e)), Mode: ParseExpr}
	if err := p.Parse(); err != nil {
		return nil, nil, nil, err
	}
	tenv, venv := Stdlib()
	sess := NewSession(nil)
	if err := p.Expr.Init(sess, tenv); err != nil {
		return nil, nil, nil, err
	}
	v, err := p.Expr.eval(sess, venv, "")
	return v, p.Expr.Type, sess, err
}

func TestEvalSimple(t *testing.T) {
	for _, c := range []struct {
		e string
		t *types.T
		v values.T
	}{
		{`1`, types.Int, values.NewInt(1)},
		{`-1`, types.Int, values.NewInt(-1)},
		{`"hello, world"`, types.String, "hello, world"},
		{
			`{a: 123, b: ([1,2], "ok")}`,
			types.Struct(
				&types.Field{Name: "a", T: types.Int},
				&types.Field{Name: "b", T: types.Tuple(
					&types.Field{T: types.List(types.Int)},
					&types.Field{T: types.String})}),
			values.Struct{
				"a": values.NewInt(123),
				"b": values.Tuple{values.List{values.NewInt(1), values.NewInt(2)}, "ok"},
			},
		},
		{`["foo": 123, "bar": 999]`, types.Map(types.String, types.Int),
			values.MakeMap(types.String, "foo", values.NewInt(123), "bar", values.NewInt(999))},
		{`if "foo" == "bar" { "no" } else { "yes" }`, types.String, "yes"},
		{`{x := {a: "blah", b:321};  x.a }`, types.String, "blah"},
		{`(func(x, y string) => x+y)("hello", "world")`, types.String, "helloworld"},
		{`{m := ["foo": 123, "bar": 333]; m["foo"]}`, types.Int, values.NewInt(123)},
		{
			`{val [a, b, ...[c, ...de]] = ["a", "b", "c", "d", "e"]; (a, b, c, de)}`,
			types.Tuple(
				&types.Field{T: types.String},
				&types.Field{T: types.String},
				&types.Field{T: types.String},
				&types.Field{T: types.List(types.String)}),
			values.Tuple{"a", "b", "c", values.List{"d", "e"}},
		},
		{
			`{val (x, y, [_, b], _) = (1, "ok", [true, false], "blah"); (x, y, b)}`,
			types.Tuple(
				&types.Field{T: types.Int},
				&types.Field{T: types.String},
				&types.Field{T: types.Bool}),
			values.Tuple{values.NewInt(1), "ok", false},
		},
		{
			`#Foo(3)`,
			types.Sum(&types.Variant{Tag: "Foo", Elem: types.Int}),
			&values.Variant{Tag: "Foo", Elem: big.NewInt(3)},
		},
		{`switch 123 { case i: i + 333 }`, types.Int, values.NewInt(456)},
	} {
		v, typ, _, err := eval(c.e)
		if err != nil {
			t.Errorf("eval %q: %v", c.e, err)
			continue
		}
		if got, want := typ, c.t; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
			continue
		}
		if !values.Equal(v, c.v) {
			t.Errorf("got %v, want %v", values.Sprint(v, typ), values.Sprint(c.v, c.t))
		}
	}
}

func TestPat(t *testing.T) {
	_, _, _, err := eval(`{val [x, y] = [1]; 123}`)
	if got, want := err, errors.New("<input>:1:7: cannot match list pattern of size 2 with a list of size 1"); got.Error() != want.Error() {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestFileCompare(t *testing.T) {
	v, typ, _, err := eval(`
		file("s3://a") == file("s3://b")
`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := typ, types.Bool; !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	f := v.(*flow.Flow)
	if got, want := f.Op, flow.K; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := len(f.Deps), 2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := f.Deps[0].Op, flow.Coerce; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := len(f.Deps[0].Deps), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := f.Deps[0].Deps[0].Op, flow.Intern; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := f.Deps[0].Deps[0].URL.String(), "s3://a"; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := f.Deps[1].Op, flow.Coerce; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := len(f.Deps[1].Deps), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := f.Deps[1].Deps[0].Op, flow.Intern; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := f.Deps[1].Deps[0].URL.String(), "s3://b"; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestExec(t *testing.T) {
	v, typ, sess, err := eval(`
		exec(image := "ubuntu", mem := 32*GiB, cpu := 32) (out file) {"
			cat {{123}} {{file("s3://blah")}} > {{out}}
		"}
	`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := typ, types.File; !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	f := v.(*flow.Flow)

	// We get a K here due to the delay. But it has zero deps
	// so we can satisfy it.
	if got, want := f.Op, flow.K; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := len(f.Deps), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// We get a K here due to the file intern. We satisfy that here.
	fd := reflow.File{ID: reflow.Digester.FromString("test")}
	f = f.K([]values.T{fd})
	if got, want := f.Op, flow.Coerce; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := len(f.Deps), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	f = f.Deps[0]
	if got, want := f.Op, flow.Exec; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := len(f.Deps), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := f.Image, "ubuntu"; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := f.Resources, (reflow.Resources{"cpu": 32, "disk": 0, "mem": 32 << 30}); !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := f.Cmd, "\n\t\t\tcat 123 %s > %s\n\t\t"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := f.Argmap, []flow.ExecArg{{Index: 0}, {Out: true, Index: 0}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := f.OutputIsDir, []bool{false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	f = f.Deps[0]
	if got, want := f.Op, flow.Val; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	fs := reflow.Fileset{Map: map[string]reflow.File{".": fd}}
	if got, want := f.Value.(reflow.Fileset), fs; !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := sess.Images(), []string{"ubuntu"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// We have to test this manually because the eval tests aren't run with
// an executor.
//
// TODO(marius): fix this
func TestExecDelay(t *testing.T) {
	v, typ, _, err := eval(`
		exec(image := "ubuntu", mem := 32*GiB, cpu := 32) (out file) {"
			echo {{delay(123)}}
		"}
	`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := typ, types.File; !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	f := v.(*flow.Flow)
	// We get a K here due to the delay. But it has zero deps
	// so we can satisfy it.
	if got, want := f.Op, flow.K; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := len(f.Deps), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// We ignore the force machinery, and just supply the value directly.
	f = f.K([]values.T{values.NewInt(123)})
	if got, want := f.Op, flow.Coerce; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := len(f.Deps), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	f = f.Deps[0]
	if got, want := f.Op, flow.Exec; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	// Now make sure that the template expanded the lazily computed
	// value correctly.
	if got, want := f.Cmd, "\n\t\t\techo 123\n\t\t"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// evalDecls parses, type checks, and then evaluates a set of declarations
// and returns the value, type associated with the identifier "test".
func evalDecls(e string) (values.T, *types.T, *Session, error) {
	p := Parser{Body: bytes.NewReader([]byte(e)), Mode: ParseDecls}
	if err := p.Parse(); err != nil {
		return nil, nil, nil, err
	}
	tenv, venv := Stdlib()
	sess := NewSession(nil)
	var typ *types.T
	for _, d := range p.Decls {
		if err := d.Init(sess, tenv); err != nil {
			return nil, nil, nil, err
		}
		switch d.Kind {
		case DeclAssign, DeclDeclare:
			if err := d.Pat.BindTypes(tenv, d.Type, types.Always); err != nil {
				return nil, nil, nil, err
			}
		}
	}
	for _, d := range p.Decls {
		switch d.Kind {
		case DeclAssign, DeclDeclare:
			env := types.NewEnv()
			if err := d.Pat.BindTypes(env, d.Type, types.Unexported); err != nil {
				return nil, nil, nil, err
			}
			for id, t := range env.Symbols() {
				if id == "test" {
					typ = t
					break
				}
			}
		}
	}
	for _, d := range p.Decls {
		v, err := d.Expr.eval(sess, venv, d.ID(""))
		if err != nil {
			return nil, nil, nil, err
		}
		venv = venv.Push()
		for _, m := range d.Pat.Matchers() {
			w, err := coerceMatch(v, d.Type, d.Pat.Position, m.Path())
			if err != nil {
				return nil, nil, nil, err
			}
			if m.Ident != "" {
				venv.Bind(m.Ident, w)
			}
		}
	}
	return venv.Value("test"), typ, sess, nil
}

func TestExecDifferentImages(t *testing.T) {
	v1, typ1, _, err := evalDecls(`
		s := delay("str")
		f := file("s3://tmp/foo")
		image := "ubuntu1"
		test := exec(image := image) (out file) {"
            echo {{s}}
			cat {{f}} > {{out}}
		"}
	`)
	if err != nil {
		t.Fatal(err)
	}
	v2, _, _, err := evalDecls(`
		s := delay("str")
		f := file("s3://tmp/foo")
		image := "ubuntu2"
		test := exec(image := image) (out file) {"
            echo {{s}}
			cat {{f}} > {{out}}
		"}
	`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := typ1, types.File; !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	f1 := v1.(*flow.Flow)
	if got, want := f1.Op, flow.K; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	f2 := v2.(*flow.Flow)
	if got, want := f2.Op, flow.K; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if f1.Digest() == f2.Digest() {
		t.Fatalf("digests of different exec flows are not different: %v vs %v", f1.Digest(), f2.Digest())
	}
}

func TestExecImmediateNonFileDirDeps(t *testing.T) {
	v1, typ1, _, err := evalDecls(`
		s := "str"
		f := file("s3://tmp/foo")
		test := exec(image := "ubuntu", mem := 32*GiB, cpu := 32) (out file) {"
            echo {{s}}
			cat {{f}} > {{out}}
		"}
	`)
	if err != nil {
		t.Fatal(err)
	}
	v2, _, _, err := evalDecls(`
		s := "str"
		f := file("s3://tmp/bar")
		test := exec(image := "ubuntu", mem := 32*GiB, cpu := 32) (out file) {"
            echo {{s}}
			cat {{f}} > {{out}}
		"}
	`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := typ1, types.File; !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	f1 := v1.(*flow.Flow)
	if got, want := f1.Op, flow.K; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	f2 := v2.(*flow.Flow)
	if got, want := f2.Op, flow.K; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if f1.Digest() == f2.Digest() {
		t.Fatalf("digests of execs with different deps are not different: %v vs %v", f1.Digest(), f2.Digest())
	}
}

func TestExecDelayedNonFileDirDeps(t *testing.T) {
	v1, typ1, _, err := evalDecls(`
		s := delay("str")
		f := file("s3://tmp/foo")
		test := exec(image := "ubuntu", mem := 32*GiB, cpu := 32) (out file) {"
            echo {{s}}
			cat {{f}} > {{out}}
		"}
	`)
	if err != nil {
		t.Fatal(err)
	}
	v2, _, _, err := evalDecls(`
		s := delay("str")
		f := file("s3://tmp/bar")
		test := exec(image := "ubuntu", mem := 32*GiB, cpu := 32) (out file) {"
            echo {{s}}
			cat {{f}} > {{out}}
		"}
	`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := typ1, types.File; !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	f1 := v1.(*flow.Flow)
	if got, want := f1.Op, flow.K; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	f2 := v2.(*flow.Flow)
	if got, want := f2.Op, flow.K; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if f1.Digest() == f2.Digest() {
		t.Fatalf("digests of execs with different deps are not different: %v vs %v", f1.Digest(), f2.Digest())
	}
}

func TestExecDelayedNonFileDirDepsNoFileDirDeps(t *testing.T) {
	v1, typ1, _, err := evalDecls(`
		f := [file("s3://tmp/foo")]
		test := exec(image := "ubuntu", mem := 32*GiB, cpu := 32) (out file) {"
			cat {{f}} > {{out}}
		"}
	`)
	if err != nil {
		t.Fatal(err)
	}
	v2, _, _, err := evalDecls(`
		f := [file("s3://tmp/bar")]
		test := exec(image := "ubuntu", mem := 32*GiB, cpu := 32) (out file) {"
			cat {{f}} > {{out}}
		"}
	`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := typ1, types.File; !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	f1 := v1.(*flow.Flow)
	if got, want := f1.Op, flow.K; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	f2 := v2.(*flow.Flow)
	if got, want := f2.Op, flow.K; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	t.Logf("a: %v, b: %v", f1.Digest(), f2.Digest())
	if f1.Digest() == f2.Digest() {
		t.Fatalf("digests of execs with different deps are not different: %v vs %v", f1.Digest(), f2.Digest())
	}
}

func TestEvalErr(t *testing.T) {
	sess := NewSession(nil)
	for _, c := range []struct {
		file string
		err  string
	}{
		{"testdata/err1.rf", "testdata/err1.rf:2:7: cannot match list pattern of size 3 with a list of size 2"},
		{"testdata/err2.rf", "testdata/err2.rf:3:14: panic: panic!"},
		{"testdata/err3.rf", "testdata/err3.rf:2:7: cannot match list pattern of size 1 with a list of size 2"},
		{"testdata/err4.rf", "testdata/err4.rf:2:16: cannot reduce empty list"},
		{"testdata/err5.rf", "testdata/err5.rf:8:29 err5.Test: precondition was not met: no execs to repeat"},
		{"testdata/err6.rf", "testdata/err6.rf:8:6 failed assertion err6.TestAllFail[1]"},
		{"testdata/err7.rf", "testdata/lib.rf:8:6 failed assertion lib.AssertErr7[1]"},
		{"testdata/err8.rf", "testdata/err8.rf:8:6 failed assertion err8.TestAllFail[a, c]"},
	} {
		m, err := sess.Open(c.file)
		if err != nil {
			t.Errorf("%s: %v", c.file, err)
			continue
		}
		_, err = m.Make(sess, sess.Values)
		if err == nil {
			t.Errorf("%s: expected error", c.file)
			continue
		}
		if got, want := err.Error(), c.err; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestTypeErr(t *testing.T) {
	sess := NewSession(nil)
	for _, c := range []struct {
		file   string
		errpat string
	}{
		{"testdata/typerr1.rf", `testdata/typerr1.rf:2:16: expected tuple of size 3, got 2 \(\(int, int, int\)\)$`},
		{"testdata/typerr2.rf", `testdata/typerr2.rf:5:3: expected list or map, got \{a, b, c int\}$`},
		{"testdata/typerr3.rf", `testdata/typerr3.rf:4:13: cannot use type file as type string in argument to F \(type func\(x, y, z string\) string\)$`},
		{"testdata/typerr4.rf", `typerr4.rf:5:16: failed to open module ./typerr4mod.rf: .*typerr4mod.rf:1:10: identifier "x" not defined$`},
		{"testdata/typerr5.rf", `typerr5.rf:1:16: failed to open module ./typerr5.reflow: param "invalid-parameter-name" is not a valid Reflow identifier`},
		{"testdata/typerr6.rf", `typerr6.rf:2:15: parameter cpu is not immediate`},
		{"testdata/typerr7.rf", `typerr7.rf:3:16: exec parameter image is not immediate`},
		{"testdata/typerr8.rf", `testdata/typerr8.rf:1:18: pattern \(a, b\) is incompatible with type string`},
		{"testdata/typerr9.rf", "testdata/typerr9.rf:1:18: case patterns are not exhaustive"},
		{"testdata/typerr10a.rf", `testdata/typerr10a.rf:2:16: reduce expects first argument of type func\({a, b int}, {a, b int}\) {a, b int}, got func\(i, j {a int}\) {c int}`},
		{"testdata/typerr10b.rf", `testdata/typerr10b.rf:2:16: reduce expects first argument of type func\({a int}, {a int}\) {a int}, got func\(i {a int}, j {c int}\) {a int}`},
		{"testdata/typerr10c.rf", `testdata/typerr10c.rf:2:16: reduce expects first argument of type func\({a int}, {a int}\) {a int}, got func\(i {c int}, j {a int}\) {a int}`},
		{"testdata/typerr11.rf", `testdata/typerr11.rf:2:16: reduce expects a function as its first argument, got int`},
		{"testdata/typerr12.rf", `testdata/typerr12.rf:2:16: reduce expects first argument of type func\({a, b int}, {a, b int}\) {a, b int}, got func\(i {c int}\) {c int}`},
		{"testdata/typerr13.rf", `testdata/typerr13.rf:2:16: reduce expects a list as its second argument, got {a int}`},
		{"testdata/typerr14a.rf", `testdata/typerr14a.rf:2:14: fold expects first argument of type func\({a int}, {a, b int}\) {a int}, got func\(i, j {a int}\) {c int}`},
		{"testdata/typerr14b.rf", `testdata/typerr14b.rf:2:14: fold expects first argument of type func\({a int}, {a, b int}\) {a int}, got func\(i {a int}, j {c int}\) {a int}`},
		{"testdata/typerr14c.rf", `testdata/typerr14c.rf:2:14: fold expects first argument of type func\({a int}, {a, b int}\) {a int}, got func\(i {c int}, j {a int}\) {a int}`},
		{"testdata/typerr15.rf", `testdata/typerr15.rf:2:14: fold expects a function with two arguments as its first argument, got int`},
		{"testdata/typerr16.rf", `testdata/typerr16.rf:2:14: fold expects a function with two arguments as its first argument, got func\(i {c int}\) {c int}`},
		{"testdata/typerr17.rf", `testdata/typerr17.rf:2:14: fold expects a list as its second argument, got {a int}`},
		{"testdata/typerr18.rf", `testdata/typerr18.rf:2:14: fold expects first argument of type func\({a int}, {a int}\) {a int}, got func\(i, j {a, b int}\) {a, b int}`},
		{"testdata/typerr19.rf", `testdata/typerr19.rf:2:7: nondeterministic must be a bool`},
	} {
		_, terr := sess.Open(c.file)
		if terr == nil {
			t.Errorf("%s: expected error", c.file)
			continue
		}
		ok, err := regexp.MatchString(c.errpat, terr.Error())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("error %s did not match %s", terr, c.errpat)
		}
	}
}
