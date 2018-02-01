// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package values

import (
	"testing"

	"github.com/grailbio/reflow/types"
)

func makeMap(entries map[string]string) Map {
	m := make(Map)
	for k, v := range entries {
		m.Insert(Digest(k, types.String), k, v)
	}
	return m
}

func TestPretty(t *testing.T) {
	for _, c := range []struct {
		v T
		t *types.T
		p string
	}{
		{List{"hello", "world"}, types.List(types.String), `["hello", "world"]`},
		{
			Struct{"a": NewInt(123), "b": Tuple{"ok", NewInt(321)}},
			types.Struct(
				&types.Field{"a", types.Int},
				&types.Field{"b", types.Tuple(&types.Field{T: types.String}, &types.Field{T: types.Int})}),
			`{a: 123, b: ("ok", 321)}`,
		},
		{
			makeMap(map[string]string{"a": "b"}),
			types.Map(types.String, types.String),
			`["a": "b"]`,
		},
	} {
		if got, want := Sprint(c.v, c.t), c.p; got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	}
}
