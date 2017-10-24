// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"math"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/internal/walker"
)

// stats stores runtime statistics for a container invocation.
type stats map[string]struct {
	N            int64
	Sum          float64
	SumOfSquares float64
	Max          float64
}

func (s stats) Max(stat string) float64 {
	return s[stat].Max
}

func (s stats) Mean(stat string) float64 {
	if s[stat].N == 0 {
		return math.NaN()
	}
	return s[stat].Sum / float64(s[stat].N)
}

func (s stats) Observe(stat string, v float64) {
	e := s[stat]
	e.N++
	if v > e.Max {
		e.Max = v
	}
	e.Sum += v
	e.SumOfSquares += v * v
	s[stat] = e
}

func (s stats) Profile() reflow.Profile {
	prof := make(reflow.Profile)
	for name := range s {
		// TODO(marius): return intermediate stats as well.
		p := prof[name]
		p.Max = s.Max(name)
		p.Mean = s.Mean(name)
		// Because JSON is terrible.
		if math.IsNaN(p.Mean) {
			p.Mean = -1
		}
		prof[name] = p
	}
	return prof
}

func du(path string) (uint64, error) {
	var (
		w walker.Walker
		n uint64
	)
	w.Init(path)
	for w.Scan() {
		if w.Info().IsDir() {
			continue
		}
		n += uint64(w.Info().Size())
	}
	return n, w.Err()
}
