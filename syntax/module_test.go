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

	for _, test := range []struct{ F, V string }{{"y", "/dev/null"}, {"z", "."}} {
		f := fs.Lookup(test.F)
		if got, want := f.Value.String(), test.V; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	if got, want := m.FlagEnv(fs, venv, tenv).Error(), "missing mandatory flag -x"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	fs.Lookup("x").Value.Set("blah")
	fs.Lookup("y").Value.Set("localfile://notexist")
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
