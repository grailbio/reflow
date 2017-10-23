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
		{"string", types.String},
		{"bool", types.Bool},
		{"file", types.File},
		{"dir", types.Dir},
		{"[string]", types.List(types.String)},
		{"[string:dir]", types.Map(types.String, types.Dir)},
		{"(int, dir)", types.Tuple(&types.Field{"", types.Int}, &types.Field{"", types.Dir})},
		{"func(int, dir) dir", types.Func(types.Dir, &types.Field{"", types.Int}, &types.Field{"", types.Dir})},
		{
			"{r1 file, r2 file, stats dir}",
			types.Struct(
				&types.Field{"r1", types.File},
				&types.Field{"r2", types.File},
				&types.Field{"stats", types.Dir},
			),
		},
		{
			"{r1, r2 file, x string}",
			types.Struct(
				&types.Field{"r1", types.File},
				&types.Field{"r2", types.File},
				&types.Field{"x", types.String},
			),
		},
		{
			"{x t1, y, z t3.x}",
			types.Struct(
				&types.Field{"x", types.Ref("t1")},
				&types.Field{"y", types.Ref("t3", "x")},
				&types.Field{"z", types.Ref("t3", "x")},
			),
		},
	} {
		p := Parser{Mode: ParseType, Body: bytes.NewReader([]byte(c.s))}
		if err := p.Parse(); err != nil {
			t.Errorf("parse error: %v", err)
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
				Kind: ExprConst,
				Type: types.Int,
				Val:  values.NewInt(111),
			},
		}},
		{Kind: DeclAssign, Pat: &Pat{Kind: PatIdent, Ident: "Bar"}, Expr: &Expr{
			Kind: ExprConst,
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
				Kind: ExprConst,
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
	expect := `module(keyspace(<nil>), params(<int>declare(x, int), <int>declare(y, int), <int>declare(z, int), assign(foo, <string>const("ok")), assign(bar, <(int, string)>ascribe(tuple(<int>const(1), <string>const("ok"))))), decls(assign(Xyz, struct(a:<string>const("x"), b:<int>const(123), c:tuple(<int>const(1), <int>const(2), <int>const(3), <int>const(4)))), assign(Blah, struct(x:ident("x"), y:ident("y"), z:ident("z"), Xyz:ident("Xyz"))), assign(fun, func((x, y, z int) => <int>const(123))), assign(fun, func((x, y, z int, zz string) => block(assign(zzz, binop(ident("zz"), "+", <string>const("ok"))) in binop(apply(ident("fun")(ident("x"), ident("y"), ident("z"))), "+", ident("zzz"))))), assign(tuple, tuple(<int>const(1), <string>const("ok"), tuple(<string>const("nested"), ident("fun")))), assign(s, struct(r1:<string>const("ok"), r2:<string>const("blah"))), assign(test, cond(binop(deref(ident("s"), r1), "==", <string>const("ok")), block( in <string>const("yep")), block( in <string>const("nope")))), assign(compr, compr(struct(x:ident("x"), y:ident("y"), z:ident("z")), ident("l"), (x, y, z)))))`
	if got, want := p.Module.String(), expect; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestParseComments(t *testing.T) {
	p := Parser{Mode: ParseDecls, Body: bytes.NewReader([]byte(`
		// Foo computes 123
		// Line two
		val Foo = 123
		
		// A comment before an annotation.
		@requires(cpu := 1)
		val Bar = "ok"`))}
	if err := p.Parse(); err != nil {
		t.Fatal(err)
	}
	if got, want := len(p.Decls), 2; got != want {
		t.Fatalf("got %d, want %v", got, want)
	}
	if got, want := p.Decls[0].Comment, "Foo computes 123\nLine two\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := p.Decls[1].Comment, "A comment before an annotation.\n"; got != want {
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
