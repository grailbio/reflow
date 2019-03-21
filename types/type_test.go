// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package types

import (
	"sort"
	"testing"

	"github.com/grailbio/reflow/internal/scanner"
)

var (
	p0 = FreshPredicate()
	p1 = FreshPredicate()

	ty1 = Struct(
		&Field{Name: "a", T: Int},
		&Field{Name: "b", T: String},
		&Field{Name: "c", T: Tuple(&Field{T: Int}, &Field{T: String})})
	ty2 = Struct(
		&Field{Name: "a", T: Int},
		&Field{Name: "d", T: String},
		&Field{Name: "c", T: Tuple(&Field{T: Int}, &Field{T: String})})
	ty12 = Struct(
		&Field{Name: "a", T: Int},
		&Field{Name: "b", T: String},
		&Field{Name: "c", T: Tuple(&Field{T: Int}, &Field{T: String})},
		&Field{Name: "d", T: String})

	mty1 = Module(
		[]*Field{
			&Field{Name: "a", T: Int},
			&Field{Name: "b", T: String},
			&Field{Name: "c", T: Tuple(&Field{T: Int.Const(p0)}, &Field{T: String.Const(p1)})},
		},
		nil,
	)
	mty2 = Module(
		[]*Field{
			&Field{Name: "a", T: Int},
			&Field{Name: "d", T: String},
			&Field{Name: "c", T: Tuple(&Field{T: Int}, &Field{T: String})},
		},
		nil,
	)
	mty12 = Module(
		[]*Field{
			&Field{Name: "a", T: Int},
			&Field{Name: "b", T: String},
			&Field{Name: "c", T: Tuple(&Field{T: Int}, &Field{T: String})},
			&Field{Name: "d", T: String},
		},
		nil,
	)
	mty3 = Module(
		[]*Field{
			&Field{Name: "a", T: Int},
			&Field{Name: "b", T: String},
			&Field{Name: "c", T: Tuple(&Field{T: Int.Const(p0)}, &Field{T: String.Const(p1)}).Const()},
		},
		nil,
	)
	sty1 = Sum(
		&Variant{Tag: "A", Elem: ty1},
		&Variant{Tag: "B", Elem: Int},
		&Variant{Tag: "C", Elem: String},
	)
	sty2 = Sum(
		&Variant{Tag: "A", Elem: ty2},
		&Variant{Tag: "B", Elem: Int},
		&Variant{Tag: "C", Elem: String},
		&Variant{Tag: "D", Elem: Float},
	)
	sty3 = Sum(
		&Variant{Tag: "A", Elem: Int},
		&Variant{Tag: "B"},
	)
	sty4 = Sum(
		&Variant{Tag: "A", Elem: Int},
		&Variant{Tag: "B", Elem: Int},
	)
	sty5 = Sum(
		&Variant{Tag: "A", Elem: Int},
		&Variant{Tag: "B", Elem: String},
	)
	sty6 = Sum(
		&Variant{Tag: "A", Elem: ty12},
		&Variant{Tag: "B", Elem: Int},
	)
	sty7 = Sum(
		&Variant{Tag: "B"},
	)

	mapTy  = Map(Int, String)
	listTy = List(String)

	types = []*T{ty1, ty2, ty12, mty1, mty2, mty12}
)

func TestMakeSum(t *testing.T) {
	zeroVariants := Sum()
	if got, want := zeroVariants.Kind, ErrorKind; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	duplicateTags := Sum(&Variant{Tag: "One"}, &Variant{Tag: "One", Elem: String})
	if got, want := duplicateTags.Kind, ErrorKind; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	emptyTags := Sum(&Variant{Tag: "One"}, &Variant{Tag: ""})
	if got, want := emptyTags.Kind, ErrorKind; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	lowercaseTags := Sum(&Variant{Tag: "One"}, &Variant{Tag: "two"})
	if got, want := lowercaseTags.Kind, ErrorKind; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestEquality(t *testing.T) {
	for _, c := range []struct {
		lhs  *T
		rhs  *T
		want bool
	}{
		{
			Sum(
				&Variant{Tag: "A", Elem: Int},
				&Variant{Tag: "B", Elem: Int},
			),
			Sum(
				&Variant{Tag: "A", Elem: Int},
				&Variant{Tag: "B", Elem: Int},
			),
			true,
		},
		{
			Sum(
				&Variant{Tag: "A", Elem: Int},
				&Variant{Tag: "B"},
			),
			Sum(
				&Variant{Tag: "A", Elem: Int},
				&Variant{Tag: "B"},
			),
			true,
		},
		{
			Sum(
				&Variant{Tag: "A", Elem: Int},
				&Variant{Tag: "B"},
			),
			Sum(
				&Variant{Tag: "A", Elem: Int},
			),
			false,
		},
		{
			Sum(
				&Variant{Tag: "A", Elem: Int},
				&Variant{Tag: "B"},
			),
			Sum(
				&Variant{Tag: "A", Elem: Int},
				&Variant{Tag: "C"},
			),
			false,
		},
		{
			Sum(
				&Variant{Tag: "A", Elem: Int},
				&Variant{Tag: "B", Elem: Int},
			),
			Sum(
				&Variant{Tag: "A", Elem: Int},
				&Variant{Tag: "B", Elem: String},
			),
			false,
		},
	} {
		if got, want := c.lhs.Equal(c.rhs), c.want; got != want {
			t.Errorf("got %v, want %v: %v == %v", got, want, c.lhs, c.rhs)
		}
		// Check symmetry.
		if got, want := c.rhs.Equal(c.lhs), c.want; got != want {
			t.Errorf("got %v, want %v: %v == %v", got, want, c.rhs, c.lhs)
		}
		// Check reflexivity.
		if got, want := c.lhs.Equal(c.lhs), true; got != want {
			t.Errorf("got %v, want %v: %v == %v", got, want, c.lhs, c.lhs)
		}
		if got, want := c.rhs.Equal(c.rhs), true; got != want {
			t.Errorf("got %v, want %v: %v == %v", got, want, c.rhs, c.rhs)
		}
	}
}

func TestUnify(t *testing.T) {
	u := Unify(Const, ty1, ty2)
	if got, want := u.String(), "{a int, c (int, string)}"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	u = Unify(Const, mty1, mty2)
	if got, want := u.String(), "module{a int, c (int, string)}"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := Unify(Const, listTy, List(Bottom)), listTy; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := Unify(Const, mapTy, Map(Top, Bottom)), mapTy; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := Unify(Const, Map(Int, ty1), Map(Int, ty2)).String(), `[int:{a int, c (int, string)}]`; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := Unify(Const, Map(Int, Int), Map(String, Int)).Kind, ErrorKind; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := Unify(Const, sty1, sty2).String(), `#A({a int, c (int, string)}) | #B(int) | #C(string) | #D(float)`; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := Unify(Const, sty3, sty4).Kind, ErrorKind; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := Unify(Const, sty4, sty5).Kind, ErrorKind; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSub(t *testing.T) {
	for _, c := range []struct {
		lhs   *T
		rhs   *T
		isSub bool
	}{
		{ty1, ty2, false},
		{ty12, ty1, true},
		{ty12, ty2, true},
		{mty1, mty2, false},
		{mty12, mty1, true},
		{mty12, mty2, true},
		{sty1, sty2, false},
		{sty2, sty3, false},
		{sty3, sty4, false},
		{sty4, sty3, false},
		{sty4, sty5, false},
		{sty6, sty1, true},
		{sty6, sty1, true},
		{sty7, sty3, true},
	} {
		if c.isSub {
			if !c.lhs.Sub(c.rhs) {
				t.Errorf("%s is a subtype of %s", c.lhs, c.rhs)
			}
		} else {
			if c.lhs.Sub(c.rhs) {
				t.Errorf("%s is not a subtype of %s", c.lhs, c.rhs)
			}
		}
	}
	for _, typ := range types {
		if !Bottom.Sub(typ) {
			t.Errorf("bottom is a subtype of %v", typ)
		}
	}
	for _, typ := range types {
		if !typ.Sub(Top) {
			t.Errorf("%s is a subtype of top", typ)
		}
	}
}

func TestEnv(t *testing.T) {
	e := NewEnv()
	e.Bind("a", Int, scanner.Position{}, Never)
	e.Bind("b", String, scanner.Position{}, Never)
	e, save := e.Push(), e
	e.Bind("a", String, scanner.Position{}, Never)
	if got, want := e.Type("a"), String; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := e.Type("b"), String; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	e = save
	if got, want := e.Type("a"), Int; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := e.Type("b"), String; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestEnvUse(t *testing.T) {
	e := NewEnv()
	e.Bind("a", Int, scanner.Position{}, Never)
	e.Bind("b", String, scanner.Position{}, Unexported)
	e.Bind("C", String, scanner.Position{}, Unexported)

	requireContains(t, e, "b")
	e.Use("b")
	requireContains(t, e)
}

func TestConstUnify(t *testing.T) {
	cty := mty1.Field("c")
	if got, want := cty.Level, CanConst; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for _, f := range cty.Fields {
		if got, want := f.T.Level, Const; got != want {
			t.Errorf("field %v: got %v, want %v", f, got, want)
		}
	}

	ty := Unify(Const, mty1, mty1)
	if got, want := ty.Level, CanConst; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	cty = ty.Field("c")
	if got, want := cty.Level, CanConst; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(cty.Fields), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for i, f := range cty.Fields {
		if got, want := f.T.Level, Const; got != want {
			t.Errorf("field %v: got %v, want %v", f, got, want)
		}
		var ps Predicates
		switch i {
		case 0:
			ps.Add(p0)
		case 1:
			ps.Add(p1)
		}
		if !ps.Satisfies(f.T.Predicates) {
			t.Errorf("predicates %v not satisfied by %v", f.T.Predicates, ps)
		}
	}
}

func TestConstPromoteUnify(t *testing.T) {
	cty := mty3.Field("c")
	if got, want := cty.Level, Const; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	ty := Unify(Const, mty3, mty3)
	if got, want := ty.Level, CanConst; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	cty = ty.Field("c")
	if got, want := cty.Level, Const; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(cty.Fields), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for i, f := range cty.Fields {
		if got, want := f.T.Level, Const; got != want {
			t.Errorf("field %v: got %v, want %v", f, got, want)
		}
		var ps Predicates
		switch i {
		case 0:
			ps.Add(p0)
		case 1:
			ps.Add(p1)
		}
		if !ps.Satisfies(f.T.Predicates) {
			t.Errorf("predicates %v not satisfied by %v", f.T.Predicates, ps)
		}
	}
	ps := Predicates{p0: true, p1: true}
	if !ps.Satisfies(cty.Predicates) {
		t.Errorf("predicates %v not satisfied by %v", cty.Predicates, ps)
	}
}

func TestSatisfy(t *testing.T) {
	sty1 := mty1.Satisfied(Predicates{p0: true, p1: true})
	cty := sty1.Field("c")
	if got, want := len(cty.Fields), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for i := range cty.Fields {
		if !cty.Fields[i].T.IsConst(nil) {
			t.Errorf("%v field %d: expected const", cty, i)
		}
	}
}

func requireContains(t *testing.T, env *Env, want ...string) {
	t.Helper()
	unused := env.Unused()
	got := make([]string, len(unused))
	for i := range got {
		got[i] = unused[i].Name
	}
	if len(got) != len(want) {
		t.Errorf("got %v, want %v", got, want)
		return
	}
	sort.Strings(got)
	sort.Strings(want)
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("entry %d does not match, got %s, want %s", i, got[i], want[i])
		}
	}
}
