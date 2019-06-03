// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package values

import (
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/types"
)

func TestEqual(t *testing.T) {
	for _, c := range []struct {
		typ *types.T
		// val1, val2 are  either a T or a func() T to accommodate values that cannot be
		// constructed in a single expression.
		val1, val2 interface{}
		want       bool
	}{
		{
			types.File,
			reflow.File{
				ID:   reflow.Digester.FromString("contents 1"),
				Size: 54321,
			},
			reflow.File{
				ID:   reflow.Digester.FromString("contents 2"),
				Size: 54321,
			},
			false,
		},
		{
			types.File,
			reflow.File{
				ID:   reflow.Digester.FromString("same contents"),
				Size: 54321,
				Assertions: reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
					reflow.AssertionKey{"namespace", "subject", "object"}: "value",
				}),
			},
			reflow.File{
				ID:   reflow.Digester.FromString("same contents"),
				Size: 54321,
				Assertions: reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
					reflow.AssertionKey{"namespace2", "subject2", "object2"}: "different value",
				}),
			},
			true,
		},
	} {
		var v, w T
		if f, ok := c.val1.(func() T); ok {
			v = f()
		} else {
			v = c.val1
		}
		if f, ok := c.val2.(func() T); ok {
			w = f()
		} else {
			w = c.val2
		}
		if got, want := Equal(v, w), c.want; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
