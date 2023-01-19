// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

func init() {
	yyErrorVerbose = true
	//	yyDebug = 1
}

func TestParseTypeOk(t *testing.T) {
	for _, c := range []struct {
		s string
		t *types.T
	}{
		{"int", types.Int},
		{"float", types.Float},
		{"string", types.String},
		{"bool", types.Bool},
		{"file", types.File},
		{"dir", types.Dir},
		{"[string]", types.List(types.String)},
		{"[string:dir]", types.Map(types.String, types.Dir)},
		{"(int, dir)", types.Tuple(&types.Field{Name: "", T: types.Int}, &types.Field{Name: "", T: types.Dir})},
		{"func(int, dir) dir", types.Func(types.Dir, &types.Field{Name: "", T: types.Int}, &types.Field{Name: "", T: types.Dir})},
		{
			"#One | #Two | #Three | #String(string)",
			types.Sum(
				&types.Variant{Tag: "One", Elem: nil},
				&types.Variant{Tag: "Two"},
				&types.Variant{Tag: "Three"},
				&types.Variant{Tag: "String", Elem: types.String},
			),
		},
		{
			"#Int(int) | #String(string) | #IntOrString(#Int(int) | #String(string))",
			types.Sum(
				&types.Variant{Tag: "Int", Elem: types.Int},
				&types.Variant{Tag: "String", Elem: types.String},
				&types.Variant{Tag: "IntOrString", Elem: types.Sum(
					&types.Variant{Tag: "Int", Elem: types.Int},
					&types.Variant{Tag: "String", Elem: types.String},
				)},
			),
		},
		{
			"{r1 file, r2 file, stats dir}",
			types.Struct(
				&types.Field{Name: "r1", T: types.File},
				&types.Field{Name: "r2", T: types.File},
				&types.Field{Name: "stats", T: types.Dir},
			),
		},
		{
			"{r1, r2 file, x string}",
			types.Struct(
				&types.Field{Name: "r1", T: types.File},
				&types.Field{Name: "r2", T: types.File},
				&types.Field{Name: "x", T: types.String},
			),
		},
		{
			"{x t1, y, z t3.x}",
			types.Struct(
				&types.Field{Name: "x", T: types.Ref("t1")},
				&types.Field{Name: "y", T: types.Ref("t3", "x")},
				&types.Field{Name: "z", T: types.Ref("t3", "x")},
			),
		},
	} {
		p := Parser{Mode: ParseType, Body: bytes.NewReader([]byte(c.s))}
		if err := p.Parse(); err != nil {
			t.Errorf("parse error on %q: %v", c.s, err)
			continue
		}
		if got, want := p.Type, c.t; !got.StructurallyEqual(want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := p.Type.String(), c.t.String(); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestParseTypeErr(t *testing.T) {
	for _, c := range []string{
		"[:int]", "{x, y, z}",
	} {
		p := Parser{Mode: ParseType, Body: bytes.NewReader([]byte(c))}
		err := p.Parse()
		if err == nil {
			t.Error("expected error")
		}
	}
}

func TestParseTypeRoundtrip(t *testing.T) {
	for _, c := range []string{
		"(int, string)",
		"(x int, y string)",
		"(x, y, z string, blah int)",
		"func(int, string) {x, y, z string}",
		"func(x, y, z file) dir",
		"func(x, y) dir",
		"{a, b (int, {a string, b bool}), c int}",
	} {
		p := Parser{Mode: ParseType, Body: bytes.NewReader([]byte(c))}
		if err := p.Parse(); err != nil {
			t.Errorf("parsing %s: %v", c, err)
			continue
		}
		if got, want := p.Type.String(), c; got != want {
			t.Errorf("got %q, want %q", got, want)
		}

	}
}

func TestParseDecls(t *testing.T) {
	p := Parser{Mode: ParseDecls, Body: bytes.NewReader([]byte(`
	val foo int = 111
	val Bar = 123
	val (a, b, _) = "ok"`))}
	if err := p.Parse(); err != nil {
		t.Fatal(err)
	}
	if got, want := len(p.Decls), 3; got != want {
		t.Fatalf("got %d, want %v", got, want)
	}
	for i, expect := range []*Decl{
		{Kind: DeclAssign, Pat: &Pat{Kind: PatIdent, Ident: "foo"}, Expr: &Expr{
			Kind: ExprAscribe,
			Type: types.Int,
			Left: &Expr{
				Kind: ExprLit,
				Type: types.Int,
				Val:  values.NewInt(111),
			},
		}},
		{Kind: DeclAssign, Pat: &Pat{Kind: PatIdent, Ident: "Bar"}, Expr: &Expr{
			Kind: ExprLit,
			Type: types.Int,
			Val:  values.NewInt(123),
		}},
		{
			Kind: DeclAssign,
			Pat: &Pat{Kind: PatTuple, List: []*Pat{
				{Kind: PatIdent, Ident: "a"},
				{Kind: PatIdent, Ident: "b"},
				{Kind: PatIgnore},
			}},
			Expr: &Expr{
				Kind: ExprLit,
				Type: types.String,
				Val:  "ok",
			},
		},
	} {
		if got, want := p.Decls[i], expect; !got.Equal(want) {
			t.Errorf("declaration %d: got %v, want %v", i, got, want)
		}
	}
}

func TestParseModule(t *testing.T) {
	p := Parser{Mode: ParseModule, Body: bytes.NewReader([]byte(`
		param (
			x, y, z int
			foo = "ok"
			bar (int, string) = (1, "ok")
		)

		val Xyz = {
			a: "x",
			b: 123,
			c: (1,2,3,4),
		}

		val Blah = {x, y, z, Xyz}

		val fun = func(x, y, z int) => 123
		func fun(x, y, z int, zz string) = {
			zzz := zz+"ok"
			fun(x, y, z)+zzz
		}

		val tuple = (1, "ok", ("nested", fun))

		val s = {r1: "ok", r2: "blah"}

		val test = if s.r1 == "ok" { "yep" } else { "nope" }

		val compr = [{x, y, z} | (x, y, z) <- l]
	`))}

	if err := p.Parse(); err != nil {
		t.Error(err)
	}
	expect := `module(keyspace(<nil>), params(<int>declare(x, int), <int>declare(y, int), <int>declare(z, int), assign(foo, <string>const("ok")), assign(bar, <(int, string)>ascribe(tuple(<int>const(1), <string>const("ok"))))), decls(assign(Xyz, struct(a:<string>const("x"), b:<int>const(123), c:tuple(<int>const(1), <int>const(2), <int>const(3), <int>const(4)))), assign(Blah, struct(x:ident("x"), y:ident("y"), z:ident("z"), Xyz:ident("Xyz"))), assign(fun, func((x, y, z int) => <int>const(123))), assign(fun, func((x, y, z int, zz string) => block(assign(zzz, binop(ident("zz"), "+", <string>const("ok"))) in binop(apply(ident("fun")(ident("x"), ident("y"), ident("z"))), "+", ident("zzz"))))), assign(tuple, tuple(<int>const(1), <string>const("ok"), tuple(<string>const("nested"), ident("fun")))), assign(s, struct(r1:<string>const("ok"), r2:<string>const("blah"))), assign(test, cond(binop(deref(ident("s"), r1), "==", <string>const("ok")), block( in <string>const("yep")), block( in block( in <string>const("nope"))))), assign(compr, compr(struct(x:ident("x"), y:ident("y"), z:ident("z")), enum(ident("l"), (x, y, z))))))`
	if got, want := p.Module.String(), expect; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestParseModuleListCompr(t *testing.T) {
	p := Parser{Mode: ParseModule, Body: bytes.NewReader([]byte(`
        param names [string]
		val compr = [n | n <- names]
	`))}

	if err := p.Parse(); err != nil {
		t.Error(err)
	}
	expect := `module(keyspace(<nil>), params(<[string]>declare(names, [string])), decls(assign(compr, compr(ident("n"), enum(ident("names"), n)))))`
	if got, want := p.Module.String(), expect; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestParseModuleDirCompr(t *testing.T) {
	p := Parser{Mode: ParseModule, Body: bytes.NewReader([]byte(`
        param items dir
		val compr = [n | n <- items]
	`))}

	if err := p.Parse(); err != nil {
		t.Error(err)
	}
	expect := `module(keyspace(<nil>), params(<dir>declare(items, dir)), decls(assign(compr, compr(ident("n"), enum(ident("items"), n)))))`
	if got, want := p.Module.String(), expect; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestParseDeclComments(t *testing.T) {
	p := Parser{Mode: ParseDecls, Body: bytes.NewReader([]byte(`
		// A non-doc
		// comment.

		// Foo computes 123
		// Line two
		val Foo = 123

		// A comment before an annotation.
		@requires(cpu := 1)
		val Bar = "ok"

		// A non-doc comment.

		val Baz = [0]
		`))}
	if err := p.Parse(); err != nil {
		t.Fatal(err)
	}
	if got, want := len(p.Decls), 3; got != want {
		t.Fatalf("got %d, want %v", got, want)
	}
	if got, want := p.Decls[0].Comment, "Foo computes 123\nLine two\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := p.Decls[1].Comment, "A comment before an annotation.\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := p.Decls[2].Comment, ""; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestParseParamComments(t *testing.T) {
	p := Parser{Mode: ParseModule, Body: bytes.NewReader([]byte(`
		// a is an int.
		param a int

		// b and c are ints.
		param b, c int

		// d and e are ints too.
		param /* override param commentary */ d, e int
	`))}
	if err := p.Parse(); err != nil {
		t.Fatal(err)
	}
	params := p.Module.ParamDecls
	if got, want := len(params), 5; got != want {
		t.Fatalf("got %d, want %v", got, want)
	}
	if got, want := params[0].Comment, "a is an int.\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := params[1].Comment, "b and c are ints.\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := params[2].Comment, "b and c are ints.\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// Let commentary on identifier override commentary on overall param
	// definition.
	if got, want := params[3].Comment, "override param commentary"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := params[4].Comment, "d and e are ints too.\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func template(s string) (*Template, error) {
	var b bytes.Buffer
	b.WriteString(`exec(image) (out file) {"`)
	b.WriteString(s)
	b.WriteString(`"}`)
	p := Parser{Mode: ParseExpr, Body: &b}
	if err := p.Parse(); err != nil {
		return nil, err
	}
	e := p.Expr
	if got, want := e.Kind, ExprExec; got != want {
		return nil, fmt.Errorf("got %v, want %v", got, want)
	}
	return e.Template, nil
}

func TestParseTemplate(t *testing.T) {
	for i, c := range []struct {
		temp  string
		frags []string
	}{
		{`echo {{out}} {{blah}}`, []string{"echo ", " ", ""}},
		{` `, []string{" "}},
		{`{{x}}`, []string{"", ""}},
		{`x{{x}}y`, []string{"x", "y"}},
		{`{{x}}{{y}}{{z}}`, []string{"", "", "", ""}},
	} {
		temp, err := template(c.temp)
		if err != nil {
			t.Errorf("error parsing template %d: %v", i, err)
			continue
		}
		if !reflect.DeepEqual(c.frags, temp.Frags) {
			t.Errorf("got %v, want %v", temp.Frags, c.frags)
			continue
		}
		if got, want := len(temp.Args), len(temp.Frags)-1; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func stringLit(s string) string {
	return `"` + s + `"`
}

func rawStringLit(s string) string {
	return "`" + s + "`"
}

func TestParseString(t *testing.T) {
	for i, c := range []struct {
		input string
		want  string
	}{
		{stringLit(``), ""},
		{stringLit(`hello`), "hello"},
		{stringLit(`hello world`), "hello world"},
		{stringLit(`hello\tworld`), "hello\tworld"},
		// Note literal tab in next line.
		{stringLit(`hello	world`), "hello\tworld"},
		{rawStringLit(``), ""},
		{rawStringLit(`hello`), "hello"},
		{rawStringLit(`hello world`), "hello world"},
		{rawStringLit(`hello\tworld`), "hello\\tworld"},
		// Note literal tab in next line.
		{rawStringLit(`hello	world`), "hello\tworld"},
	} {
		p := Parser{Mode: ParseExpr, Body: bytes.NewReader([]byte(c.input))}
		if err := p.Parse(); err != nil {
			t.Errorf("error parsing string %d: %v", i, c.input)
		}
		e := p.Expr
		if got, want := e.Kind, ExprLit; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := e.Type, types.String; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := e.Val, c.want; got != want {
			t.Errorf("got \"%v\", want \"%s\"", got, want)
		}
	}
}

func TestParsePat(t *testing.T) {
	for _, c := range []struct {
		input string
		pat   *Pat // nil means that we expect a parse failure.
	}{
		{"", nil},
		{"_", &Pat{Kind: PatIgnore}},
		{"[]", nil},
		{"[...]", nil},
		{"[..., a]", nil},
		{"[a, ..., b]", nil},
		{
			"[a]",
			&Pat{
				Kind: PatList,
				List: []*Pat{{Kind: PatIdent, Ident: "a"}},
			},
		},
		{
			"[a, b]",
			&Pat{
				Kind: PatList,
				List: []*Pat{
					{Kind: PatIdent, Ident: "a"},
					{Kind: PatIdent, Ident: "b"},
				},
			},
		},
		{
			"[a, b, ...]",
			&Pat{
				Kind: PatList,
				List: []*Pat{
					{Kind: PatIdent, Ident: "a"},
					{Kind: PatIdent, Ident: "b"},
				},
				Tail: &Pat{Kind: PatIgnore},
			},
		},
		{
			"[a, b, ...rest]",
			&Pat{
				Kind: PatList,
				List: []*Pat{
					{Kind: PatIdent, Ident: "a"},
					{Kind: PatIdent, Ident: "b"},
				},
				Tail: &Pat{Kind: PatIdent, Ident: "rest"},
			},
		},
		{
			"[a, b, ...[c]]",
			&Pat{
				Kind: PatList,
				List: []*Pat{
					{Kind: PatIdent, Ident: "a"},
					{Kind: PatIdent, Ident: "b"},
				},
				Tail: &Pat{
					Kind: PatList,
					List: []*Pat{{Kind: PatIdent, Ident: "c"}},
				},
			},
		},
		{
			"(a, b)",
			&Pat{
				Kind: PatTuple,
				List: []*Pat{
					{Kind: PatIdent, Ident: "a"},
					{Kind: PatIdent, Ident: "b"},
				},
			},
		},
	} {
		p := Parser{Mode: ParsePat, Body: bytes.NewReader([]byte(c.input))}
		err := p.Parse()
		switch {
		case err == nil && c.pat == nil:
			t.Errorf("input %s: got %v, want failure to parse", c.input, p.Pat)
		case err == nil && c.pat != nil:
			if got, want := p.Pat, c.pat; !p.Pat.Equal(c.pat) {
				t.Errorf("input %s: got %v, want %v", c.input, got, want)
			}
		case err != nil && c.pat == nil:
			// Expected an error and got an error, so we're good.
		case err != nil && c.pat != nil:
			t.Errorf("input %s: got error %v, want %v", c.input, err, c.pat)
		}
	}
}

func TestParseSwitch(t *testing.T) {
	p := Parser{Mode: ParseExpr, Body: bytes.NewReader([]byte(`
		switch ["a", "b"] {
			case [a, b]:
				val aa = a;
				val bb = b;
				(aa, bb)
			case [a]:
				val aa = a;
				(aa, aa)
		}
	`))}

	if err := p.Parse(); err != nil {
		t.Error(err)
	}
	expect := `switch(list(<string>const("a"), <string>const("b")), cases(case([a, b], block(assign(aa, ident("a")), assign(bb, ident("b")) in tuple(ident("aa"), ident("bb")))), case([a], block(assign(aa, ident("a")) in tuple(ident("aa"), ident("aa"))))))`
	if got, want := p.Expr.String(), expect; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
