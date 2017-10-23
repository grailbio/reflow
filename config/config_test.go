package config

import (
	"errors"
	"reflect"
	"testing"

	"github.com/grailbio/reflow"
)

type testCache struct {
	Config
	arg string
}

func (c *testCache) Cache() (reflow.Cache, error) {
	return nil, errors.New(c.arg)
}

func TestConfig(t *testing.T) {
	Register(Cache, "test", "test", "", func(cfg Config, arg string) (Config, error) {
		return &testCache{cfg, arg}, nil
	})

	cfg, err := Parse([]byte(`
cache: test,arg1
`))
	if err != nil {
		t.Fatal(err)
	}
	cache, err := cfg.Cache()
	if cache != nil {
		t.Errorf("expected nil cache, got %v", cache)
	}
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	if got, want := err.Error(), "arg1"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	b, err := Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg1, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg, cfg1) {
		t.Error("cfg, cfg1 not equal after marshal roundtrip")
	}
}
