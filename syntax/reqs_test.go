// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"math/big"
	"strings"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

var requirementsType = types.Struct(
	&types.Field{"mem", types.Int},
	&types.Field{"cpu", types.Int},
	&types.Field{"disk", types.Int},
	&types.Field{"wide", types.Bool})

func TestRequirements(t *testing.T) {
	sess := NewSession()
	m, err := sess.Open("testdata/req.rf")
	if err != nil {
		t.Fatal(err)
	}
	var tests []string
	fm := m.Type.FieldMap()
	for _, f := range m.Type.Fields {
		if !strings.HasPrefix(f.Name, "Test") {
			continue
		}
		tests = append(tests, f.Name)
		expect := "Expect" + strings.TrimPrefix(f.Name, "Test")
		et := fm[expect]
		if et == nil {
			t.Fatalf("no symbol %s for test %s", expect, f.Name)
		}
		if !et.Sub(requirementsType) {
			t.Fatalf("bad type for %s; got %s, want %s", expect, et, requirementsType)
		}
	}
	if len(tests) == 0 {
		t.Fatal("no tests")
	}

	v, err := m.Make(sess, sess.Values)
	if err != nil {
		t.Fatalf("make: %s", err)
	}
	mod := v.(values.Module)
	for _, test := range tests {
		expect := "Expect" + strings.TrimPrefix(test, "Test")
		var min, max reflow.Resources
		if f, ok := Force(mod[test], fm[test]).(*reflow.Flow); ok {
			min, max = f.Requirements()
		}
		req := mod[expect].(values.Struct)
		emin := reflow.Resources{
			Memory: req["mem"].(*big.Int).Uint64(),
			CPU:    uint16(req["cpu"].(*big.Int).Uint64()),
			Disk:   req["disk"].(*big.Int).Uint64(),
		}
		emax := emin
		if req["wide"].(bool) {
			emax = reflow.MaxResources
		}
		if got, want := min, emin; got != want {
			t.Errorf("%s: got %v, want %v", test, got, want)
		}
		if got, want := max, emax; got != want {
			t.Errorf("%s: got %s, want %s", test, got, want)
		}
	}
}
