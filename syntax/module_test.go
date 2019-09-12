// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package syntax

import (
	"testing"

	"github.com/grailbio/reflow/flow"
)

func TestModuleFlag(t *testing.T) {
	sess := NewSession(nil)
	m, err := sess.Open("testdata/flag.rf")
	if err != nil {
		t.Fatal(err)
	}
	tenv, venv := Stdlib()
	fs, err := m.Flags(sess, venv.Push())
	if err != nil {
		t.Fatal(err)
	}
	if fs.Lookup("notFlag") != nil {
		t.Error("unexpected notFlag")
	}

	// y is a file and z is a dir, so their default values are of type *flow.Flow. All
	// params with default values of type *flow.Flow are evaluated in Make instead of Flags, so their
	// flag values are empty.
	for _, test := range []struct{ F, V string }{{"y", ""}, {"z", ""}} {
		f := fs.Lookup(test.F)
		if got, want := f.Value.String(), test.V; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	if got, want := m.FlagEnv(fs, venv, tenv).Error(), "missing mandatory flag -x"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	fs.Set("x", "blah")
	fs.Set("y", "localfile://notexist")
	if err := m.FlagEnv(fs, venv, tenv); err != nil {
		t.Fatal(err)
	}
	// Verify by examining the produced flow graph that the flag
	// was evaluated correctly.
	intern := venv.Value("y").(*flow.Flow).Visitor()
	for intern.Walk() && intern.Op != flow.Intern {
		intern.Visit()
	}
	if intern.Op != flow.Intern {
		t.Fatal("no intern node produced")
	}
	if got, want := intern.URL.String(), "localfile://notexist"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestModuleDefaultFlag(t *testing.T) {
	sess := NewSession(nil)
	m, err := sess.Open("testdata/flag1.rf")
	if err != nil {
		t.Fatal(err)
	}
	_, venv := Stdlib()
	_, err = m.Flags(sess, venv.Push())
	if err != nil {
		t.Fatal(err)
	}
	err = m.InjectArgs(sess, []string{"-a=newa", "-b=newb"})
	if err != nil {
		t.Fatal(err)
	}
	fs, err := m.Flags(sess, venv.Push())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fs.Lookup("a").Value.String(), "newa"; got != want {
		t.Errorf("expected 'newa', got %v", got)
	}
	if got, want := fs.Lookup("b").Value.String(), "newb"; got != want {
		t.Errorf("expected newb, got %v", got)
	}

	err = m.InjectArgs(sess, []string{"-a=newa2"})
	if err != nil {
		t.Fatal(err)
	}
	fs, err = m.Flags(sess, venv.Push())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fs.Lookup("a").Value.String(), "newa2"; got != want {
		t.Errorf("expected 'newa2', got %v", got)
	}
	if got, want := fs.Lookup("b").Value.String(), "newb"; got != want {
		t.Errorf("expected newb, got %v", got)
	}
}

func TestFlagDependence(t *testing.T) {
	files := []string{
		"testdata/flag_dependence1.rf",
		"testdata/flag_dependence2.rf",
		"testdata/flag_dependence3.rf",
	}
	for _, file := range files {
		sess := NewSession(nil)
		m, err := sess.Open(file)

		if err != nil {
			t.Fatal(err)
		}
		_, venv := Stdlib()
		if _, err = m.Flags(sess, venv.Push()); err != nil {
			t.Errorf("flag parameters may depend on other flag parameters")
		}
	}
}

func TestFlowFlag(t *testing.T) {
	file := "testdata/flow.rf"
	sess := NewSession(nil)
	m, err := sess.Open(file)

	if err != nil {
		t.Fatal(err)
	}
	tenv, venv := Stdlib()
	fs, err := m.Flags(sess, venv.Push())
	if err != nil {
		t.Fatal(err)
	}
	if fs.Lookup("a").Value.(*flagVal).set {
		t.Errorf("a param whose default value is of type *flow.Flow cannot be set without a value being parsed from the command line.")
	}
	if err := m.FlagEnv(fs, venv, tenv); err != nil {
		t.Fatal(err)
	}
}
