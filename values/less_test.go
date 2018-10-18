// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package values

import (
	"math/rand"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/types"
)

func TestLess(t *testing.T) {
	var (
		f0     = reflow.File{Source: "a"}
		f1     = reflow.File{Source: "b"}
		f2, f3 reflow.File
		r      = rand.New(rand.NewSource(0))
	)
	for !f2.ID.Less(f3.ID) {
		f2.ID = reflow.Digester.Rand(r)
		f3.ID = reflow.Digester.Rand(r)
	}
	less := []struct{ left, right T }{
		{NewInt(0), NewInt(1)},
		{"", "abc"},
		{NewFloat(0.1), NewFloat(100.3)},
		{f0, f1},
		{f1, f2},
		{f2, f3},
		{Dir{"x": f0}, Dir{"x": f1}},
		{Dir{"x": f0, "y": f2}, Dir{"x": f1, "y": f3}},
		{Dir{"x": f3}, Dir{"x": f3, "y": f0}},
		{List{}, List{f0, f1}},
		{List{f0, f1, f3}, List{f0, f2, f3}},
		{MakeMap(types.String, "1", f0), MakeMap(types.String, "1", f1)},
		{MakeMap(types.String, "1", f0), MakeMap(types.String, "1", f0, "2", f0)},
		{Tuple{"a", NewInt(1), "c"}, Tuple{"a", NewInt(2), "c"}},
		{Struct{"x": NewInt(3), "a": "b"}, Struct{"x": NewInt(3), "a": "c"}},
		{Module{"X": NewInt(3), "A": "b"}, Module{"X": NewInt(3), "A": "c"}},
	}
	for _, l := range less {
		if !Less(l.left, l.right) {
			t.Errorf("expected %v < %v", l.left, l.right)
		} else if Less(l.right, l.left) {
			t.Errorf("assymetric less! %v < %v", l.right, l.left)
		}
		if Less(l.left, l.left) {
			t.Errorf("value %v is equal, but reported as less", l.left)
		}
		if Less(l.right, l.right) {
			t.Errorf("value %v is equal, but reported as less", l.right)
		}
	}
}
