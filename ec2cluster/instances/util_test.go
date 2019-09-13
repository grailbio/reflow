// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package instances

import "testing"

func TestVerifiedSrc(t *testing.T) {
	const want = `// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.

package instances

// VerifiedStatus captures the verification status for each Instance type.
type VerifiedStatus struct {
	// Attempted denotes whether a verification attempt has been made.
	Attempted bool
	// Verified denotes whether the instance type is verified to work for Reflow.
	Verified bool
	// ApproxETASeconds is the approximate ETA (in seconds) for Reflow to become available on this instance type.
	ApproxETASeconds int64
}

// VerifiedByRegion stores mapping of instance types to VerifiedStatus by AWS Region.
var VerifiedByRegion = make(map[string]map[string]VerifiedStatus)

func init() {
	VerifiedByRegion["r1"] = map[string]VerifiedStatus{
		"a": {true, true, 10},
		"b": {true, false, -1},
		"c": {false, false, -1},
	}
	VerifiedByRegion["r2"] = map[string]VerifiedStatus{
		"a": {true, true, 20},
		"b": {true, true, 10},
		"c": {false, false, -1},
	}
	VerifiedByRegion["r3"] = map[string]VerifiedStatus{
		"c": {true, true, 10},
	}

}
`
	vm := make(map[string]map[string]VerifiedStatus)
	vm["r1"] = make(map[string]VerifiedStatus)
	vm["r1"]["a"] = VerifiedStatus{true, true, 10}
	vm["r1"]["b"] = VerifiedStatus{true, false, -1}
	vm["r2"] = make(map[string]VerifiedStatus)
	vm["r2"]["a"] = VerifiedStatus{true, false, -1}
	vm["r2"]["b"] = VerifiedStatus{true, true, 10}
	vm["r3"] = make(map[string]VerifiedStatus)
	vm["r3"]["c"] = VerifiedStatus{true, true, 10}
	g := VerifiedSrcGenerator{"instances", vm}

	// Modify some status
	vm["r2"]["a"] = VerifiedStatus{true, true, 20}
	// Add some new types
	g.AddTypes([]string{"c"})
	src, err := g.AddTypes([]string{"c"}).Source()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(src), want; got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}
