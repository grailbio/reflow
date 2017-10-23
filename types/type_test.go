package types

import (
	"testing"
)

var (
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
)

func TestUnify(t *testing.T) {
	u := ty1.Unify(ty2)
	if got, want := u.String(), "{a int, c (int, string)}"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSub(t *testing.T) {
	if ty1.Sub(ty2) {
		t.Errorf("%s is not a subtype of %s", ty1, ty2)
	}
	if !ty12.Sub(ty1) {
		t.Errorf("%s is a subtype of %s", ty12, ty1)
	}
	if !ty12.Sub(ty2) {
		t.Errorf("%s is a subtype of %s", ty12, ty2)
	}
}

func TestEnv(t *testing.T) {
	e := NewEnv()
	e.Bind("a", Int)
	e.Bind("b", String)
	e.Push()
	e.Bind("a", String)
	if got, want := e.Type("a"), String; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := e.Type("b"), String; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	e.Pop()
	if got, want := e.Type("a"), Int; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := e.Type("b"), String; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
