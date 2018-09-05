// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

import "testing"

func TestAbbrev(t *testing.T) {
	if got, want := abbrev("dd if=/dev/zero of=$tmp/foobar count=1024.sleep 200&.sleep 200&.sleep 200", 10), "dd i.. 200"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestLeftabbrev(t *testing.T) {
	for _, gw := range []struct{ s, want string }{
		{"helloworld", "..rld"},
		{"hello", "hello"},
		{"ok", "ok"},
	} {
		if got, want := leftabbrev(gw.s, 5), gw.want; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}

}

func TestTrimspace(t *testing.T) {
	if got, want := trimspace("\t\t\nspace bound \n\t  string\nok\n\n"), "space bound string.ok"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestTrimpath(t *testing.T) {
	for _, gw := range []struct{ s, want string }{
		{"/usr/bin/bwa -foo bar", "../bwa -foo bar"},
		{"/usr/bin/echo", "../echo"},
		{"/some/path/name/", "../name/"},
	} {
		if got, want := trimpath(gw.s), gw.want; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
