// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"fmt"
	"strings"

	"golang.org/x/mod/semver"
)

// IsReflowSemVer returns whether the given string matches Reflow's semantic versioning format.
// Reflow use's the semantic versioning format described in https://semver.org/ with a slight variation
// wherein we may include a prefix of `reflow` and we may omit the typical prefix of `v`.
func IsReflowSemVer(v string) bool {
	return semver.IsValid(toSemver(v))
}

// IsOlderVersion returns whether the first of the given two Reflow version strings is strictly older.
// Panics if the either version is not valid (so user should call `IsReflowSemVer` first)
func IsOlderVersion(rv1, rv2 string) bool {
	if !IsReflowSemVer(rv1) {
		panic(fmt.Errorf("not a valid reflow version: %s", rv1))
	}
	if !IsReflowSemVer(rv2) {
		panic(fmt.Errorf("not a valid reflow version: %s", rv2))
	}
	return semver.Compare(toSemver(rv1), toSemver(rv2)) < 0
}

// toSemver converts reflow versioning string to match semver format
func toSemver(r string) string {
	if strings.HasPrefix(r, "reflow") {
		r = strings.TrimPrefix(r, "reflow")
	}
	if !strings.HasPrefix(r, "v") {
		r = "v" + r
	}
	return r
}
