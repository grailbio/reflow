// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

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
