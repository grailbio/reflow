// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

import "testing"

func TestStats(t *testing.T) {
	var s stats
	if got, want := s.N(), 0; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}

	s.Add(10)
	s.Add(20)
	if got, want := s.N(), 2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := s.Mean(), float64(15); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	expect := []struct {
		pct int
		v   float64
	}{
		{1, 10},
		{20, 10},
		{49, 10},
		{51, 20},
		{99, 20},
	}
	for _, e := range expect {
		if got, want := s.Percentile(e.pct), e.v; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if got, want := s.Summary("%.1f"), "10.0/15.0/20.0"; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := s.SummaryScaled("%.1f", 0.1), "1.0/1.5/2.0"; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := s.SummaryScaled("%.2f", 0.033), "0.33/0.49/0.66"; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
