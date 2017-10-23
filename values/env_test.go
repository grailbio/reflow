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
