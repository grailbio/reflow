// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package instances

import "testing"

func TestVerifiedSrc(t *testing.T) {
	const want = `// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT DIRECTLY UNLESS
// when making changes to the format of the file, in which case, one would
// need to edit it first, and then update the template accordingly.

package instances

// VerifiedStatus captures the verification status for each Instance type.
type VerifiedStatus struct {
	// Attempted denotes whether a verification attempt has been made.
	Attempted bool
	// Verified denotes whether the instance type is verified to work for Reflow.
	Verified bool
	// ApproxETASeconds is the approximate ETA (in seconds) for Reflow to become available on this instance type.
	ApproxETASeconds int64
	// MemoryBytes is memory bytes reported as available on this Instance type.
	MemoryBytes int64
}

// ExpectedMemoryBytes is the amount of memory we can expect to be available based on verification.
func (v VerifiedStatus) ExpectedMemoryBytes() int64 {
	// samplingErrorDiscount is used to discount the amount of memory to account for sampling variation.
	// Since we are modeling the expected available memory on an instance type based on
	// one sample (collected during verification), this provides a buffer.
	const samplingErrorDiscount = 0.02 // 2 percent

	return int64(float64(v.MemoryBytes) * (1 - samplingErrorDiscount))
}

// VerifiedByRegion stores mapping of instance types to VerifiedStatus by AWS Region.
var VerifiedByRegion = make(map[string]map[string]VerifiedStatus)

func init() {
	VerifiedByRegion["r1"] = map[string]VerifiedStatus{
		"a": {true, true, 10, 1073741824},
		"b": {true, false, -1, 0},
		"c": {false, false, -1, 0},
	}
	VerifiedByRegion["r2"] = map[string]VerifiedStatus{
		"a": {true, true, 20, 3221225472},
		"b": {true, true, 10, 2147483648},
		"c": {false, false, -1, 0},
	}
	VerifiedByRegion["r3"] = map[string]VerifiedStatus{
		"c": {true, true, 10, 1073741824},
	}

}
`
	vm := make(map[string]map[string]VerifiedStatus)
	vm["r1"] = make(map[string]VerifiedStatus)
	vm["r1"]["a"] = VerifiedStatus{true, true, 10, 1 << 30}
	vm["r1"]["b"] = VerifiedStatus{true, false, -1, 0}
	vm["r2"] = make(map[string]VerifiedStatus)
	vm["r2"]["a"] = VerifiedStatus{true, false, -1, 0}
	vm["r2"]["b"] = VerifiedStatus{true, true, 10, 2 << 30}
	vm["r3"] = make(map[string]VerifiedStatus)
	vm["r3"]["c"] = VerifiedStatus{true, true, 10, 1 << 30}
	g := VerifiedSrcGenerator{"instances", vm}

	// Modify some status
	vm["r2"]["a"] = VerifiedStatus{true, true, 20, 3 << 30}
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
