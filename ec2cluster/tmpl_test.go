// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import "testing"

func TestTmpl(t *testing.T) {
	got := tmpl(`
		x, y, {{.z}}
			blah
		bloop
	`, args{"z": 1})
	want := `x, y, 1
	blah
bloop`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBadTmpl(t *testing.T) {
	v := recoverPanic(func() {
		tmpl(`
				x, y
			blah`, nil)
	})
	if v == nil {
		t.Fatal("expected panic")
	}
	if got, want := v.(string), `nonspace prefix in "\t\t\tblah"`; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func recoverPanic(f func()) (v interface{}) {
	defer func() {
		v = recover()
	}()
	f()
	return
}
