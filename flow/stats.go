// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

import (
	"fmt"
	"sort"
)

// stats is a simple data structure to keep track of summary
// statistics and histograms. stats stores all samples,
// so it should be used only for small datasets.
type stats struct {
	samples []float64
}

func (s stats) N() int {
	return len(s.samples)
}

func (s *stats) Add(v float64) {
	s.samples = append(s.samples, v)
	sort.Sort(s)
}

func (s stats) Mean() float64 {
	var total float64
	if len(s.samples) == 0 {
		return total
	}
	for _, d := range s.samples {
		total += d
	}
	return total / float64(len(s.samples))
}

func (s stats) Percentile(pct int) float64 {
	n := len(s.samples)
	if n == 0 {
		return 0
	}
	if pct == 100 {
		return s.samples[len(s.samples)-1]
	}
	idx := n * pct / 100
	return s.samples[idx]
}

func (s stats) Summary(format string) string {
	return fmt.Sprintf(format+"/"+format+"/"+format,
		s.Percentile(0), s.Mean(), s.Percentile(100))
}

func (s stats) Len() int           { return len(s.samples) }
func (s stats) Less(i, j int) bool { return s.samples[i] < s.samples[j] }
func (s stats) Swap(i, j int)      { s.samples[i], s.samples[j] = s.samples[j], s.samples[i] }
