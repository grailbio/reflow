// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package values

import (
	"testing"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/types"
)

func TestEnv(t *testing.T) {
	var env *Env
	env = env.Push()
	env.Bind("hello", "world")
	env2 := env
	env = env.Push()
	env.Bind("hello", "ok")
	if got, want := env.Value("hello"), "ok"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := env.Digest("hello", types.String), Digest("ok", types.String); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := env2.Value("hello"), "world"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := env2.Digest("hello", types.String), Digest("world", types.String); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	env.Bind("hello", "onemore")
	if got, want := env.Value("hello"), "onemore"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := env.Digest("hello", types.String), Digest("onemore", types.String); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// Test for missing identifier
	var missing T
	if got, want := env.Value("missing"), missing; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestConcat(t *testing.T) {
	var e1, e2 *Env
	e1 = e1.Push()
	e2 = e2.Push()
	e1.Bind("x", "e1x")
	e2.Bind("x", "e2x")
	e1 = e1.Push()
	e1.Bind("y", "e1y")
	e2.Bind("z", "e2z")
	env := e1.Concat(e2)
	cases := map[string]struct {
		v interface{}
		d digest.Digest
	}{
		"x": {"e1x", Digest("e1x", types.String)},
		"y": {"e1y", Digest("e1y", types.String)},
		"z": {"e2z", Digest("e2z", types.String)},
	}
	for k, v := range cases {
		if got, want := env.Value(k), v.v; got != want {
			t.Errorf("got %v, want %v for key %v", got, want, k)
		}
		if got, want := env.Digest(k, types.String), v.d; got != want {
			t.Errorf("got %v, want %v for key %v", got, want, k)
		}
	}
	if got, want := e2.Digest("x", types.String), Digest("e2x", types.String); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
