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

func TestResourcesEqual(t *testing.T) {
	for _, tt := range []struct{
		a, b reflow.Resources
		want bool
	} {
		{reflow.Resources{"mem": 10, "cpu": 5, "disk": 1}, reflow.Resources{"mem": 5, "cpu": 2, "disk": 3}, false},
		{reflow.Resources{"mem": 0, "cpu": 0}, reflow.Resources{}, true},
		{reflow.Resources{"mem": 0, "cpu": 0}, nil, true},
		{reflow.Resources{}, nil, true},
	} {
		if got, want := tt.a.Equal(tt.b), tt.want; got != want {
			t.Errorf("%s.Equal(%s): got %v, want %v", tt.a, tt.b, got, want)
		}
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

func TestDivAndMaxRatio(t *testing.T) {
	for _, tt := range []struct {
		r, s         reflow.Resources
		rDivS, sDivR map[string]float64
		rToS, sToR   float64
	}{
		{
			reflow.Resources{}, reflow.Resources{"mem": 5, "cpu": 10},
			map[string]float64{}, map[string]float64{},
			0.0, 0.0,
		},
		{
			reflow.Resources{"mem": 1}, reflow.Resources{"mem": 5, "cpu": 10},
			map[string]float64{"mem": 0.2}, map[string]float64{"mem": 5},
			0.2, 5,
		},
		{
			reflow.Resources{"cpu": 2}, reflow.Resources{"mem": 10, "cpu": 8},
			map[string]float64{"cpu": 0.25}, map[string]float64{"cpu": 4},
			0.25, 4,
		},
		{
			reflow.Resources{"mem": 5, "cpu": 2}, reflow.Resources{"mem": 10, "cpu": 10},
			map[string]float64{"mem": 0.5, "cpu": 0.2}, map[string]float64{"mem": 2, "cpu": 5},
			0.5, 5,
		},
		{
			reflow.Resources{"mem": 10, "cpu": 10}, reflow.Resources{"mem": 100, "cpu": 10},
			map[string]float64{"mem": 0.1, "cpu": 1.0}, map[string]float64{"mem": 10, "cpu": 1.0},
			1.0, 10,
		},
		{
			reflow.Resources{"mem": 32, "cpu": 8}, reflow.Resources{"mem": 128, "cpu": 16},
			map[string]float64{"mem": 0.25, "cpu": 0.5}, map[string]float64{"mem": 4, "cpu": 2},
			0.5, 4,
		},
	} {
		if got, want := tt.r.Div(tt.s), tt.rDivS; !mapEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := tt.s.Div(tt.r), tt.sDivR; !mapEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := tt.r.MaxRatio(tt.s), tt.rToS; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := tt.s.MaxRatio(tt.r), tt.sToR; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func mapEqual(r, s map[string]float64) bool {
	for key, val := range s {
		if r[key] != val {
			return false
		}
	}
	for key, val := range r {
		if s[key] != val {
			return false
		}
	}
	return true
}
