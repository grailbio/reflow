// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"errors"
	"reflect"
	"testing"

	"github.com/grailbio/reflow/assoc"
)

type testAssoc struct {
	Config
	arg string
}

func (a *testAssoc) Assoc() (assoc.Assoc, error) {
	return nil, errors.New(a.arg)
}

func TestConfig(t *testing.T) {
	Register(Assoc, "test", "test", "", func(cfg Config, arg string) (Config, error) {
		return &testAssoc{cfg, arg}, nil
	})

	cfg, err := Parse([]byte(`
assoc: test,arg1
`))
	if err != nil {
		t.Fatal(err)
	}
	assoc, err := cfg.Assoc()
	if assoc != nil {
		t.Errorf("expected nil assoc, got %v", assoc)
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
