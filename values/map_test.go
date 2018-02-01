// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package values

import (
	"testing"

	"github.com/grailbio/reflow/types"
)

func TestMap(t *testing.T) {
	m := MakeMap(types.String, "1", NewInt(1), "2", NewInt(2), "3", NewInt(3))
	if got, want := m.Lookup(Digest("1", types.String), "1"), NewInt(1); !Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := m.Len(), 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	m.Insert(Digest("1", types.String), "1", NewInt(123))
	if got, want := m.Lookup(Digest("1", types.String), "1"), NewInt(123); !Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := m.Len(), 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
