// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow_test

import (
	"reflect"
	"testing"

	"github.com/grailbio/reflow"
)

func TestResources(t *testing.T) {
	r1 := reflow.Resources{"mem": 10, "cpu": 5, "disk": 1}
	r2 := reflow.Resources{"mem": 5, "cpu": 2, "disk": 3}
	var got, want reflow.Resources
	got.Sub(r1, r2)
	if want := (reflow.Resources{"mem": 5, "cpu": 3, "disk": -2}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	got.Add(r1, r2)
	if want := (reflow.Resources{"mem": 15, "cpu": 7, "disk": 4}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	got.Add(r1, r2)
	want.Add(r2, r1)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if r1.Available(r2) {
		t.Errorf("expected %v to be unavailable in %v", r2, r1)
	}
	r3 := reflow.Resources{"mem": 3, "cpu": 1, "disk": 1}
	if !r1.Available(r3) {
		t.Errorf("expected %v to be available in %v", r3, r1)
	}
	got.Min(r1, r2)
	if want := (reflow.Resources{"mem": 5, "cpu": 2, "disk": 1}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	got.Max(r1, r2)
	if want := (reflow.Resources{"mem": 10, "cpu": 5, "disk": 3}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := r1.Div(r2), map[string]float64{"mem": 2, "cpu": 2.5, "disk": 1 / 3}; reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := r2.Div(r1), map[string]float64{"mem": 0.5, "cpu": 2 / 5, "disk": 3}; reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func assertRequirements(t *testing.T, req reflow.Requirements, min, max reflow.Resources) {
	t.Helper()
	if got, want := req.Min, min; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := req.Max(), max; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRequirements(t *testing.T) {
	var req reflow.Requirements
	req.AddSerial(reflow.Resources{"mem": 10, "cpu": 5, "disk": 5})
	req.AddSerial(reflow.Resources{"mem": 20, "cpu": 3, "disk": 1})
	assertRequirements(t, req,
		reflow.Resources{"mem": 20, "cpu": 5, "disk": 5},
		reflow.Resources{"mem": 20, "cpu": 5, "disk": 5})
	req.AddParallel(reflow.Resources{"mem": 10, "cpu": 5, "disk": 1})
	assertRequirements(t, req,
		reflow.Resources{"mem": 20, "cpu": 5, "disk": 5},
		reflow.Resources{"mem": 40, "cpu": 10, "disk": 10})

	req = reflow.Requirements{Min: reflow.Resources{"mem": 2, "cpu": 1}, Width: 10}
	assertRequirements(t, req, reflow.Resources{"mem": 2, "cpu": 1}, reflow.Resources{"mem": 22, "cpu": 11})

	req = reflow.Requirements{}
	req.AddSerial(reflow.Resources{"mem": 3, "cpu": 1})
	assertRequirements(t, req, reflow.Resources{"mem": 3, "cpu": 1}, reflow.Resources{"mem": 3, "cpu": 1})

	req.AddParallel(reflow.Resources{"mem": 5, "cpu": 2})
	assertRequirements(t, req, reflow.Resources{"mem": 5, "cpu": 2}, reflow.Resources{"mem": 10, "cpu": 4})

	req.AddParallel(reflow.Resources{"mem": 10, "cpu": 4})
	assertRequirements(t, req, reflow.Resources{"mem": 10, "cpu": 4}, reflow.Resources{"mem": 30, "cpu": 12})

	req = reflow.Requirements{}
	req.AddParallel(reflow.Resources{"mem": 5, "cpu": 2})
	assertRequirements(t, req, reflow.Resources{"mem": 5, "cpu": 2}, reflow.Resources{"mem": 5, "cpu": 2})

	req.AddParallel(reflow.Resources{"mem": 5, "cpu": 2})
	assertRequirements(t, req, reflow.Resources{"mem": 5, "cpu": 2}, reflow.Resources{"mem": 10, "cpu": 4})
}
