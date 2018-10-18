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
