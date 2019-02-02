// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"testing"

	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

func TestForceStruct(t *testing.T) {
	typ := types.Struct(&types.Field{Name: "foo", T: types.String})
	v := values.Struct{
		"foo": "vfoo",
		"bar": "vbar",
	}
	vf := Force(v, typ)
	s, ok := vf.(values.Struct)
	if got, want := ok, true; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := s["foo"], "vfoo"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestForceModule(t *testing.T) {
	typ := types.Module(
		[]*types.Field{{Name: "foo", T: types.String}},
		[]*types.Field{},
	)
	v := values.Module{
		"foo": "vfoo",
		"bar": "vbar",
	}
	vf := Force(v, typ)
	m, ok := vf.(values.Module)
	if got, want := ok, true; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := m["foo"], "vfoo"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
