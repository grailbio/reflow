package syntax

import (
	"bytes"
	"testing"

	"github.com/grailbio/reflow/internal/scanner"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

func parsePat(t *testing.T, s string) *Pat {
	p := Parser{Body: bytes.NewReader([]byte(s)), Mode: ParsePat}
	if err := p.Parse(); err != nil {
		t.Fatalf("could not parse pattern %v", s)
	}
	return p.Pat
}

func TestComplement(t *testing.T) {
	for _, c := range []struct {
		t   *types.T
		s   string
		in  []values.T
		out []values.T
	}{
		{
			types.String,
			"_",
			[]values.T{
				"a",
			},
			[]values.T{},
		},
		{
			types.Tuple(
				&types.Field{T: types.String},
				&types.Field{T: types.String},
			),
			"(_, _)",
			[]values.T{
				values.Tuple{"a", "b"},
			},
			[]values.T{},
		},
		{
			types.Tuple(
				&types.Field{T: types.List(types.String)},
				&types.Field{T: types.List(types.String)},
			),
			"([_], [_, _])",
			[]values.T{
				values.Tuple{values.List{"a"}, values.List{"b", "c"}},
			},
			[]values.T{
				values.Tuple{values.List{}, values.List{"b"}},
				values.Tuple{values.List{}, values.List{"b", "c"}},
				values.Tuple{values.List{"a"}, values.List{"b"}},
				values.Tuple{values.List{"a", "b"}, values.List{"c", "d", "e"}},
			},
		},
		{
			types.List(types.String),
			"[_]",
			[]values.T{
				values.List{"a"},
			},
			[]values.T{
				values.List{},
				values.List{"a", "b"},
			},
		},
		{
			types.List(types.String),
			"[_, ...]",
			[]values.T{
				values.List{"a"},
				values.List{"a", "b"},
				values.List{"a", "b", "c"},
			},
			[]values.T{
				values.List{},
			},
		},
		{
			types.List(types.String),
			"[_, ...[_, ...]]",
			[]values.T{
				values.List{"a", "b"},
				values.List{"a", "b", "c"},
			},
			[]values.T{
				values.List{},
				values.List{"a"},
			},
		},
		{
			types.List(types.String),
			"[_, _]",
			[]values.T{
				values.List{"a", "b"},
			},
			[]values.T{
				values.List{},
				values.List{"a"},
				values.List{"a", "b", "c"},
			},
		},
		{
			types.List(types.List(types.String)),
			"[[_], [_, _]]",
			[]values.T{
				values.List{
					values.List{"a"},
					values.List{"b", "c"},
				},
			},
			[]values.T{
				values.List{},
				values.List{values.List{}},
				values.List{values.List{}, values.List{}},
				values.List{values.List{}, values.List{}, values.List{}},
				values.List{values.List{}, values.List{"b"}},
				values.List{values.List{"a"}, values.List{"b"}},
				values.List{values.List{"a"}, values.List{"b", "c", "d"}},
			},
		},
		{
			types.Struct(
				&types.Field{
					Name: "foo",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "bar",
					T:    types.List(types.String),
				},
			),
			"{foo: [_, _], bar: [_]}",
			[]values.T{
				values.Struct{
					"foo": values.List{"vfoo0", "vfoo1"},
					"bar": values.List{"vbar0"},
				},
			},
			[]values.T{
				values.Struct{
					"foo": values.List{"vfoo0"},
					"bar": values.List{"vbar0"},
				},
				values.Struct{
					"foo": values.List{"vfoo0", "vfoo1"},
					"bar": values.List{},
				},
				values.Struct{
					"foo": values.List{},
					"bar": values.List{},
				},
			},
		},
		{
			types.Struct(
				&types.Field{
					Name: "foo",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "bar",
					T: types.Struct(
						&types.Field{
							Name: "baz",
							T:    types.List(types.String),
						},
					),
				},
			),
			"{foo: [_, _], bar: {baz: [_]}}",
			[]values.T{
				values.Struct{
					"foo": values.List{"vfoo0", "vfoo1"},
					"bar": values.Struct{
						"baz": values.List{"vbar0"},
					},
				},
			},
			[]values.T{
				values.Struct{
					"foo": values.List{"vfoo0", "vfoo1"},
					"bar": values.Struct{
						"baz": values.List{},
					},
				},
				values.Struct{
					"foo": values.List{"vfoo0"},
					"bar": values.Struct{
						"baz": values.List{"vbar0"},
					},
				},
				values.Struct{
					"foo": values.List{"vfoo0"},
					"bar": values.Struct{
						"baz": values.List{},
					},
				},
				values.Struct{
					"foo": values.List{},
					"bar": values.Struct{
						"baz": values.List{},
					},
				},
			},
		},
		{
			types.Sum(
				&types.Variant{Tag: "A"},
				&types.Variant{Tag: "B", Elem: types.String},
				&types.Variant{Tag: "C", Elem: types.Struct(
					&types.Field{
						Name: "foo",
						T:    types.List(types.Int),
					},
				)},
			),
			"#B(s)",
			[]values.T{
				&values.Variant{Tag: "B", Elem: "hello"},
			},
			[]values.T{
				&values.Variant{Tag: "A"},
				&values.Variant{Tag: "C", Elem: values.Struct{"foo": values.List{}}},
			},
		},
		{
			types.Sum(
				&types.Variant{Tag: "A"},
				&types.Variant{Tag: "B", Elem: types.String},
				&types.Variant{Tag: "C", Elem: types.Struct(
					&types.Field{
						Name: "foo",
						T:    types.List(types.Int),
					},
				)},
			),
			"#C({foo: [_]})",
			[]values.T{
				&values.Variant{
					Tag:  "C",
					Elem: values.Struct{"foo": values.List{0}},
				},
			},
			[]values.T{
				&values.Variant{Tag: "A"},
				&values.Variant{Tag: "B", Elem: "hello"},
				&values.Variant{Tag: "C", Elem: values.Struct{"foo": values.List{}}},
				&values.Variant{
					Tag:  "C",
					Elem: values.Struct{"foo": values.List{0, 1, 2}},
				},
			},
		},
	} {
		u := caseUniv{c.t}
		p := parsePat(t, c.s)
		comp := u.Complement(p)
		for _, v := range c.in {
			// Elements of the `in` set should all match the given pattern.
			ok := p.checkMatch(v)
			if !ok {
				t.Errorf("value %v should match pattern %v", v, p)
			}
			// ...but not match anything in the complement.
			for _, compPat := range comp {
				ok := compPat.checkMatch(v)
				if ok {
					t.Errorf("value %v should not match complement pattern %v",
						v, compPat)
				}
			}
		}
		for _, v := range c.out {
			// Elements of the `out` set should not match the given pattern.
			ok := p.checkMatch(v)
			if ok {
				t.Errorf("value %v should not match pattern %v", v, p)
			}
			// ...but match something in the complement.
			matched := false
			for _, compPat := range comp {
				ok := compPat.checkMatch(v)
				if ok {
					matched = true
				}
			}
			if !matched {
				t.Errorf("value %v should match a complement of %v: %v", v,
					p, comp)
			}
		}
	}
}

func testOneIntersect(
	t *testing.T, u caseUniv, lhs, rhs *Pat, in, out []values.T) {

	intersection := u.IntersectOne(lhs, rhs)
	for _, v := range in {
		if intersection == nil {
			t.Errorf("value %v should match %v ∩ %v", v, lhs, rhs)
			continue
		}
		ok := intersection.checkMatch(v)
		if !ok {
			t.Errorf("value %v should match %v ∩ %v = %v", v, lhs, rhs, intersection)
		}
	}
	for _, v := range out {
		if intersection == nil {
			continue
		}
		ok := intersection.checkMatch(v)
		if ok {
			t.Errorf("value %v should not match %v ∩ %v = %v", v, lhs, rhs, intersection)
		}
	}
}

func TestIntersect(t *testing.T) {
	for _, c := range []struct {
		t   *types.T
		lhs string
		rhs string
		in  []values.T
		out []values.T
	}{
		{
			types.String,
			"_",
			"_",
			[]values.T{
				"a",
			},
			[]values.T{},
		},
		{
			types.Tuple(
				&types.Field{T: types.String},
				&types.Field{T: types.String},
			),
			"(_, _)",
			"_",
			[]values.T{
				values.Tuple{"a", "b"},
			},
			[]values.T{},
		},
		{
			types.List(types.String),
			"[_, _]",
			"_",
			[]values.T{
				values.List{"a", "b"},
			},
			[]values.T{
				values.List{},
				values.List{"a"},
				values.List{"a", "b", "c"},
				values.List{"a", "b", "c", "d"},
			},
		},
		{
			types.List(types.String),
			"[_, _]",
			"[_, _, _]",
			[]values.T{},
			[]values.T{
				values.List{},
				values.List{"a"},
				values.List{"a", "b"},
				values.List{"a", "b", "c"},
				values.List{"a", "b", "c", "d"},
			},
		},
		{
			types.List(types.List(types.String)),
			"[[_], [_, _], [_, _, _]]",
			"[[_], [_, _], [_]]",
			[]values.T{},
			[]values.T{
				values.List{},
				values.List{
					values.List{"a"},
					values.List{"a", "b"},
					values.List{"a", "b", "c"},
				},
				values.List{
					values.List{"a"},
					values.List{"a", "b"},
					values.List{"a"},
				},
			},
		},
		{
			types.List(types.List(types.String)),
			"[[_], [_, _], [_, _, _]]",
			"[[_], [_, _], ...[_, ...]]",
			[]values.T{
				values.List{
					values.List{"a"},
					values.List{"a", "b"},
					values.List{"a", "b", "c"},
				},
			},
			[]values.T{
				values.List{},
				values.List{
					values.List{"a"},
					values.List{"a", "b"},
					values.List{"a"},
				},
				values.List{
					values.List{"a"},
					values.List{"a", "b"},
					values.List{"a", "b"},
				},
				values.List{
					values.List{"a"},
					values.List{"a", "b"},
					values.List{"a", "b", "c"},
					values.List{"a", "b", "c"},
				},
				values.List{
					values.List{"a"},
					values.List{"a", "b"},
					values.List{"a", "b", "c", "d"},
				},
			},
		},
		{
			types.List(types.List(types.String)),
			"[[_], [_, _], ...[_, ...]]",
			"[[_], [_, _], ...[_, _, ...]]",
			[]values.T{
				values.List{
					values.List{"a"},
					values.List{"a", "b"},
					values.List{"a"},
					values.List{"a"},
					values.List{"a"},
				},
				values.List{
					values.List{"a"},
					values.List{"a", "b"},
					values.List{"a", "b", "c"},
					values.List{"a", "b", "c"},
				},
			},
			[]values.T{
				values.List{},
				values.List{
					values.List{"a"},
				},
				values.List{
					values.List{"a"},
					values.List{"a", "b"},
					values.List{"a"},
				},
				values.List{
					values.List{"a"},
					values.List{"a", "b"},
					values.List{"a", "b", "c"},
				},
				values.List{
					values.List{"a"},
					values.List{"a"},
					values.List{"a"},
					values.List{"a"},
					values.List{"a"},
				},
			},
		},
		{
			types.Struct(
				&types.Field{
					Name: "foo",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "bar",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "baz",
					T:    types.List(types.String),
				},
			),
			"{foo: [_, _], bar: [_], baz: [_, ...]}",
			"{foo: [_, ...], bar: _}",
			[]values.T{
				values.Struct{
					"foo": values.List{"a", "b"},
					"bar": values.List{"a"},
					"baz": values.List{"a"},
				},
			},
			[]values.T{
				values.Struct{
					"foo": values.List{"a"},
					"bar": values.List{"a"},
					"baz": values.List{},
				},
				values.Struct{
					"foo": values.List{"a", "b", "c"},
					"bar": values.List{"a"},
					"baz": values.List{"a"},
				},
			},
		},
		{
			types.Sum(
				&types.Variant{Tag: "A"},
				&types.Variant{Tag: "B", Elem: types.String},
				&types.Variant{Tag: "C", Elem: types.Struct(
					&types.Field{
						Name: "foo",
						T:    types.List(types.Int),
					},
				)},
			),
			"#A",
			"#B(_)",
			[]values.T{},
			[]values.T{
				&values.Variant{Tag: "A"},
				&values.Variant{Tag: "B", Elem: ""},
				&values.Variant{Tag: "B", Elem: "hello"},
			},
		},
		{
			types.Sum(
				&types.Variant{Tag: "A"},
				&types.Variant{Tag: "B", Elem: types.String},
				&types.Variant{Tag: "C", Elem: types.Struct(
					&types.Field{
						Name: "foo",
						T:    types.List(types.Int),
					},
				)},
			),
			"#C({foo: [_, ...]})",
			"#C({foo: [_, _, ...]})",
			[]values.T{
				&values.Variant{
					Tag: "C",
					Elem: values.Struct{
						"foo": values.List{1, 2},
					},
				},
				&values.Variant{
					Tag: "C",
					Elem: values.Struct{
						"foo": values.List{1, 2, 3},
					},
				},
			},
			[]values.T{
				&values.Variant{Tag: "A"},
				&values.Variant{Tag: "B", Elem: ""},
				&values.Variant{
					Tag: "C",
					Elem: values.Struct{
						"foo": values.List{1},
					},
				},
			},
		},
	} {
		u := caseUniv{c.t}
		lhsPat := parsePat(t, c.lhs)
		rhsPat := parsePat(t, c.rhs)
		testOneIntersect(t, u, lhsPat, rhsPat, c.in, c.out)
		// Intersection should commute, so we swap lhs and rhs and re-run the
		// same tests.
		testOneIntersect(t, u, rhsPat, lhsPat, c.in, c.out)
	}
}

func TestCheckPatExhaustiveness(t *testing.T) {
	for _, c := range []struct {
		t            *types.T
		patStrings   []string
		isExhaustive bool
	}{
		{
			types.String,
			[]string{
				"_",
			},
			true,
		},
		{
			types.List(types.String),
			[]string{
				"[a]",
			},
			false,
		},
		{
			types.List(types.String),
			[]string{
				"[a, b]",
				"[a]",
			},
			false,
		},
		{
			types.List(types.String),
			[]string{
				"[a, b]",
				"[a]",
				"a",
			},
			true,
		},
		{
			types.Tuple(
				&types.Field{T: types.String},
				&types.Field{T: types.String},
			),
			[]string{
				"(a, b)",
			},
			true,
		},
		{
			types.Struct(
				&types.Field{
					Name: "foo",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "bar",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "baz",
					T:    types.List(types.String),
				},
			),
			[]string{
				"{foo: [_], bar: [_]}",
				"{baz: [_]}",
			},
			false,
		},
		{
			types.Struct(
				&types.Field{
					Name: "foo",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "bar",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "baz",
					T:    types.List(types.String),
				},
			),
			[]string{
				"{foo: [_], bar: [_]}",
				"{baz: [_]}",
				"{foo: x, baz: _}",
			},
			true,
		},
	} {
		pats := make([]*Pat, len(c.patStrings))
		for i, s := range c.patStrings {
			p := Parser{Body: bytes.NewReader([]byte(s)), Mode: ParsePat}
			if err := p.Parse(); err != nil {
				t.Fatalf("could not parse pattern %v", s)
			}
			pats[i] = p.Pat
		}
		clauses := make([]*CaseClause, len(pats))
		for i, p := range pats {
			clauses[i] = &CaseClause{
				Pat:  p,
				Expr: &Expr{Kind: ExprLit},
			}
		}
		pos := scanner.Position{}
		err := checkCases(c.t, pos, clauses)
		if got, want := err == nil, c.isExhaustive; got != want {
			t.Errorf("got %v, want %v for %v", got, want, pats)
		}
	}
}

func TestCheckPatReachability(t *testing.T) {
	for _, c := range []struct {
		t              *types.T
		patStrings     []string
		hasUnreachable bool
	}{
		{
			types.String,
			[]string{
				"_",
			},
			false,
		},
		{
			types.List(types.String),
			[]string{
				"[a]",
				"_",
			},
			false,
		},
		{
			types.List(types.String),
			[]string{
				"[a, b]",
				"[a]",
				"_",
			},
			false,
		},
		{
			types.List(types.String),
			[]string{
				"[_, ...]",
				"[_]",
				"[_, _]",
				"_",
			},
			true,
		},
		{
			types.Tuple(
				&types.Field{T: types.String},
				&types.Field{T: types.String},
			),
			[]string{
				"(a, b)",
				"_",
			},
			true,
		},
		{
			types.Struct(
				&types.Field{
					Name: "foo",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "bar",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "baz",
					T:    types.List(types.String),
				},
			),
			[]string{
				"{foo: [_], bar: [_]}",
				"{baz: [_]}",
				"_",
			},
			false,
		},
		{
			types.Struct(
				&types.Field{
					Name: "foo",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "bar",
					T:    types.List(types.String),
				},
				&types.Field{
					Name: "baz",
					T:    types.List(types.String),
				},
			),
			[]string{
				"{foo: [_], bar: [_]}",
				"{baz: [_]}",
				"{foo: x, baz: _}",
			},
			false,
		},
	} {
		pats := make([]*Pat, len(c.patStrings))
		for i, s := range c.patStrings {
			p := Parser{Body: bytes.NewReader([]byte(s)), Mode: ParsePat}
			if err := p.Parse(); err != nil {
				t.Fatalf("could not parse pattern %v", s)
			}
			pats[i] = p.Pat
		}
		clauses := make([]*CaseClause, len(pats))
		for i, p := range pats {
			clauses[i] = &CaseClause{
				Pat:  p,
				Expr: &Expr{Kind: ExprLit},
			}
		}
		pos := scanner.Position{}
		err := checkCases(c.t, pos, clauses)
		if got, want := err != nil, c.hasUnreachable; got != want {
			t.Errorf("got %v, want %v for %v", got, want, pats)
		}
	}
}
