package lang

import "testing"

func TestFuncType(t *testing.T) {
	for arity := 0; arity < 10; arity++ {
		for rt := typeVoid; rt <= typeImage; rt++ {
			t1 := typeFunc(arity, rt)
			n, t2 := funcType(t1)
			if got, want := n, arity; got != want {
				t.Fatalf("got %v, want %v", got, want)
			}
			if got, want := t2, rt; got != want {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	}
}
