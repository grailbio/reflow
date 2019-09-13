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
	instanceTypes := []string{"a", "b", "c", "d"}
	existing := make(map[string]instances.VerifiedStatus)
	existing["a"] = instances.VerifiedStatus{true, true, 10}
	existing["b"] = instances.VerifiedStatus{true, false, 70}
	existing["c"] = instances.VerifiedStatus{false, false, -1}

	verified, toverify := filterInstanceTypes(instanceTypes, existing, false)
	if got, want := verified, []string{"a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
	if got, want := toverify, []string{"c", "d"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}

	verified, toverify = filterInstanceTypes(instanceTypes, existing, true)
	if got, want := verified, []string{"a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
	if got, want := toverify, []string{"b", "c", "d"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}
