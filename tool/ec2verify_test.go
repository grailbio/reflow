// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"reflect"
	"testing"

	"github.com/grailbio/reflow/ec2cluster/instances"
)

func TestFilterInstanceTypes(t *testing.T) {
	instanceTypes := []instances.Type{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
		{Name: "d"},
	}
	existing := map[string]instances.VerifiedStatus{
		"a": {Attempted: true, Verified: true, ApproxETASeconds: 10, MemoryBytes: 0},
		"b": {Attempted: true, Verified: false, ApproxETASeconds: 70, MemoryBytes: 0},
		"c": {Attempted: false, Verified: false, ApproxETASeconds: -1, MemoryBytes: 0},
	}
	for _, tt := range []struct {
		instanceTypes []instances.Type
		existing      map[string]instances.VerifiedStatus
		retry         bool
		toverify      []string
	}{
		{instanceTypes, map[string]instances.VerifiedStatus{}, false, []string{"a", "b", "c", "d"}},
		{instanceTypes, existing, false, []string{"c", "d"}},
		{instanceTypes, existing, true, []string{"b", "c", "d"}},
	} {
		toverify := instancesToVerify(tt.instanceTypes, tt.existing, tt.retry)
		if got, want := toverify, tt.toverify; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v want %v", got, want)
		}
	}
}

func TestExpectedMemoryBytes(t *testing.T) {
	for _, tt := range []struct {
		instanceType string
		expectedMem  int64
	}{
		{"c3.large", 3654045523},
		{"m5.large", 7563137260},
		{"c5.4xlarge", 30567888547},
		{"m5dn.xlarge", 15341268848},
	} {
		if got, want := instances.VerifiedByRegion["us-west-2"][tt.instanceType].ExpectedMemoryBytes(), tt.expectedMem; got != want {
			t.Errorf("%s: got %d, want %d", tt.instanceType, got, want)
		}
	}
}
