// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package values

import (
	"testing"

	"github.com/grailbio/reflow/types"
)

func TestDigest(t *testing.T) {
	typ := types.Map(
		types.String,
		types.Struct(
			&types.Field{"field1", types.Int},
			&types.Field{"field2", types.String}))
	m := new(Map)
	m.Insert(Digest("hello", types.String), "hello", Struct{
		"field1": NewInt(123),
		"field2": T("hello world"),
	})
	m.Insert(Digest("world", types.String), "world", Struct{
		"field1": NewInt(321),
		"field2": T("foo bar"),
	})
	d, _ := Digester.Parse("sha256:c1c3e68de6ccf619538b5810a4feaeac5049505b7719ad67321f62d0c63f52a9")
	if got, want := Digest(m, typ), d; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestNumericDigest(t *testing.T) {
	zeroDigest, _ := Digester.Parse("sha256:dbc1b4c900ffe48d575b5da5c638040125f65db0fe3e24494b76ea986457d986")
	if got, want := Digest(NewInt(0), types.Int), zeroDigest; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if pos, neg := Digest(NewInt(1), types.Int), Digest(NewInt(-1), types.Int); pos == neg {
		t.Error("hashes do not account for sign")
	}
	if pos, neg := Digest(NewFloat(1), types.Float), Digest(NewFloat(-1), types.Float); pos == neg {
		t.Error("hashes do not account for sign")
	}
}
