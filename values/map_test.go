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

func TestMapDigestCollision(t *testing.T) {
	const N = 100

	r := rand.New(rand.NewSource(0))
	d := reflow.Digester.Rand(r)

	m := new(Map)
	expect := make(map[int64]int64)
	for i := 0; i < N; i++ {
		k, v := r.Int63(), r.Int63()
		expect[k] = v
		m.Insert(d, NewInt(k), NewInt(v))
	}
	for k, v := range expect {
		if got, want := m.Lookup(d, NewInt(k)), NewInt(v); !Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	// Pick some keys to override and test these too.
	var i int
	for k := range expect {
		if i > 100 {
			break
		}
		i++
		v := r.Int63()
		expect[k] = v
		m.Insert(d, NewInt(k), NewInt(v))
	}

	for k, v := range expect {
		if got, want := m.Lookup(d, NewInt(k)), NewInt(v); !Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	if got, want := m.Len(), N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
