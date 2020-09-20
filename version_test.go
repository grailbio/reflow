// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow_test

import (
	"testing"

	"github.com/grailbio/reflow"
)

func TestIsReflowSemVer(t *testing.T) {
	for _, tc := range []struct {
		v    string
		want bool
	}{
		{"hello", false},
		{"reflow1.1.1safk", false},
		{"1.1", true},
		{"reflow1.1.1", true},
		{"reflow1.1.1-beta", true},
	} {
		if got, want := reflow.IsReflowSemVer(tc.v), tc.want; got != want {
			t.Errorf("IsReflowSemVer(%s): got %v, want %v", tc.v, got, want)
		}
	}
}

func TestVersion(t *testing.T) {
	for _, tc := range []struct {
		v1, v2 string
		want   bool
	}{
		{"reflow1.1.1", "reflow1.2.1", true},
		{"reflow1.124.1", "1.2.1", false},
		{"1.1.1", "reflow1.1.1", false},
	} {
		if got, want := reflow.IsOlderVersion(tc.v1, tc.v2), tc.want; got != want {
			t.Errorf("IsOlderVersion(%s, %s): got %v, want %v", tc.v1, tc.v2, got, want)
		}
	}
}
