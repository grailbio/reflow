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

// CompareResult defines how two versions compare.  See `CompareVersions`.
type CompareResult int

const (
	Equal CompareResult = iota
	Greater
	Lesser
)

// IsOlderVersion returns whether the first of the given two Reflow version strings is strictly older.
// Panics if the either version is not valid version (so user should call `IsReflowSemVer` first)
func CompareVersions(v, w string) CompareResult {
	must(v)
	must(w)
	switch semver.Compare(toSemver(v), toSemver(w)) {
	case -1:
		return Lesser
	case 1:
		return Greater
	default:
		return Equal
	}
}

// IsAnySameOrNewerVersion returns whether any of the given versions in `vs` is same or newer than `v`.
// Panics if `v` is not a valid (so user should call `IsReflowSemVer` first)
// Any invalid version in `vs` is ignored.
func IsAnySameOrNewerVersion(v string, vs []string) bool {
	must(v)
	for _, vi := range vs {
		if !IsReflowSemVer(vi) {
			continue
		}
		switch CompareVersions(vi, v) {
		case Lesser:
			break
		default:
			return true
		}
	}
	return false
}

func must(v string) {
	if !IsReflowSemVer(v) {
		panic(fmt.Errorf("not a valid reflow version: %s", v))
	}
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
