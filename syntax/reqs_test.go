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
	&types.Field{"cpu", types.Float},
	&types.Field{"disk", types.Int},
	&types.Field{"wide", types.Bool})

func TestRequirements(t *testing.T) {
	sess := NewSession(nil)
	m, err := sess.Open("testdata/req.rf")
	if err != nil {
		t.Fatal(err)
	}
	var tests []string
	fm := m.Type().FieldMap()
	for _, f := range m.Type().Fields {
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
		name := "Expect" + strings.TrimPrefix(test, "Test")
		var req reflow.Requirements
		if f, ok := Force(mod[test], fm[test]).(*reflow.Flow); ok {
			req = f.Requirements()
		}
		val := mod[name].(values.Struct)
		var expect reflow.Requirements
		expect.Min = reflow.Resources{
			"mem":  float64(val["mem"].(*big.Int).Uint64()),
			"disk": float64(val["disk"].(*big.Int).Uint64()),
		}
		expect.Min["cpu"], _ = val["cpu"].(*big.Float).Float64()
		for _, feature := range val["cpufeatures"].(values.List) {
			expect.Min[feature.(string)] = expect.Min["cpu"]
		}
		if val["wide"].(bool) {
			expect.Width = 1
		}
		if !req.Equal(expect) {
			t.Errorf("%s: got %s, want %s", test, req, expect)
		}
	}
}
