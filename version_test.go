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
		{"reflowv1.1.1", true},
		{"v1.1.1", true},
	} {
		if got, want := reflow.IsReflowSemVer(tc.v), tc.want; got != want {
			t.Errorf("IsReflowSemVer(%s): got %v, want %v", tc.v, got, want)
		}
	}
}

func TestIsOlderVersion(t *testing.T) {
	for _, tc := range []struct {
		v1, v2 string
		want   reflow.CompareResult
	}{
		{"reflow1.1.1", "reflow1.2.1", reflow.Lesser},
		{"reflow1.124.1", "1.2.1", reflow.Greater},
		{"1.1.1", "reflow1.1.1", reflow.Equal},
		{"1.1.1", "1", reflow.Greater},
	} {
		if got, want := reflow.CompareVersions(tc.v1, tc.v2), tc.want; got != want {
			t.Errorf("IsOlderVersion(%s, %s): got %v, want %v", tc.v1, tc.v2, got, want)
		}
	}
}

func TestIsAnySameOrNewer(t *testing.T) {
	for _, tc := range []struct {
		v    string
		vs   []string
		want bool
	}{
		{"reflow1.1.1", nil, false},
		{"reflow1.1.1", []string{}, false},
		{"reflow1.1.1", []string{"hello"}, false},
		{"reflow1.1.1", []string{"reflow1.2.1"}, true},
		{"reflow1.2.0", []string{"reflow1.1.1", "reflow1.1.3", "reflow1.2.1"}, true},
		{"reflow1.1.1", []string{"reflow1.1.1", "reflow"}, true},
		{"reflow1.1.1", []string{"1.2.1"}, true},
		{"reflow1.2.0", []string{"1.1.1", "v1.1.3", "1.2.1"}, true},
		{"reflow1.1.1", []string{"1.1.1", "reflow"}, true},
		{"1.1.1", nil, false},
		{"1.1.1", []string{}, false},
		{"1.1.1", []string{"hello"}, false},
		{"1.1.1", []string{"reflow1.2.1"}, true},
		{"1.2.0", []string{"reflow1.1.1", "reflow1.1.3", "reflow1.2.1"}, true},
		{"1.1.1", []string{"reflow1.1.1", "reflow"}, true},
	} {
		if got, want := reflow.IsAnySameOrNewerVersion(tc.v, tc.vs), tc.want; got != want {
			t.Errorf("IsAnyOlderVersion(%s, %s): got %v, want %v", tc.v, tc.vs, got, want)
		}
	}
}
