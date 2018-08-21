// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package values

import (
	"testing"
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
	if got, want := env2.Value("hello"), "world"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	env.Bind("hello", "onemore")
	if got, want := env.Value("hello"), "onemore"; got != want {
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
	cases := map[string]interface{}{
		"x": "e1x",
		"y": "e1y",
		"z": "e2z",
	}
	for k, v := range cases {
		if got, want := env.Value(k), v; got != want {
			t.Errorf("got %v, want %v for key %v", got, want, k)
		}
	}
}
