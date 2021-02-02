// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/grailbio/base/data"
)

// Resources describes a set of labeled resources. Each resource is
// described by a string label and assigned a value. The zero value
// of Resources represents the resources with zeros for all labels.
type Resources map[string]float64

// String renders a Resources. All nonzero-valued labels are included;
// mem, cpu, and disk are always included regardless of their value.
func (r Resources) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	r.writeResources(&b)
	b.WriteString("}")
	return b.String()
}

func (r Resources) writeResources(b *bytes.Buffer) {
	if r["mem"] != 0 || r["cpu"] != 0 || r["disk"] != 0 {
		fmt.Fprintf(b, "mem:%s cpu:%g disk:%s", data.Size(r["mem"]), r["cpu"], data.Size(r["disk"]))
	}
	var keys []string
	for key := range r {
		switch key {
		case "mem", "cpu", "disk":
		default:
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if r[key] == 0 {
			continue
		}
		fmt.Fprintf(b, " %s:%g", key, r[key])
	}
}

// Available tells if s resources are available from r.
func (r Resources) Available(s Resources) bool {
	for key := range s {
		if r[key] < s[key] {
			return false
		}
	}
	return true
}

// Sub sets r to the difference x[key]-y[key] for all keys and returns r.
func (r *Resources) Sub(x, y Resources) *Resources {
	r.Set(x)
	for key := range y {
		(*r)[key] = x[key] - y[key]
	}
	return r
}

// Add sets r to the sum x[key]+y[key] for all keys and returns r.
func (r *Resources) Add(x, y Resources) *Resources {
	r.Set(x)
	for key := range y {
		(*r)[key] += y[key]
	}
	return r
}

// Set sets r[key]=s[key] for all keys and returns r.
func (r *Resources) Set(s Resources) *Resources {
	*r = make(Resources)
	for key, val := range s {
		(*r)[key] = val
	}
	return r
}

// Min sets r to the minimum min(x[key], y[key]) for all keys
// and returns r.
func (r *Resources) Min(x, y Resources) *Resources {
	r.Set(x)
	for key, val := range y {
		if val < (*r)[key] {
			(*r)[key] = val
		}
	}
	return r
}

// Max sets r to the maximum max(x[key], y[key]) for all keys
// and returns r.
func (r *Resources) Max(x, y Resources) *Resources {
	r.Set(x)
	for key, val := range y {
		if val > (*r)[key] {
			(*r)[key] = val
		}
	}
	return r
}

// Scale sets r to the scaled resources s[key]*factor for all keys
// and returns r.
func (r *Resources) Scale(s Resources, factor float64) *Resources {
	if *r == nil {
		*r = make(Resources)
	}
	for key, val := range s {
		(*r)[key] = val * factor
	}
	return r
}

// ScaledDistance returns the distance between two resources computed as a sum
// of the differences in memory, cpu and disk with some predefined scaling.
func (r Resources) ScaledDistance(u Resources) float64 {
	// Consider 6G Memory and 1 CPU are somewhat the same cost
	// when we compute "distance" between the resources.
	// % reflow ec2instances | awk '{s += $2/$3; n++} END{print s/n}'
	// 5.98788
	const (
		G             = 1 << 30
		memoryScaling = 1.0 / (6 * G)
		cpuScaling    = 1
	)
	return math.Abs(float64(r["mem"])-float64(u["mem"]))*memoryScaling +
		math.Abs(float64(r["cpu"])-float64(u["cpu"]))*cpuScaling
}

// Equal tells whether the resources r and s are equal in all dimensions
// of both r and s.
func (r Resources) Equal(s Resources) bool {
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

// Div returns a mapping of each key in s to the fraction r[key]/s[key].
// Since the returned value cannot be treated as Resources, Div simply returns a map.
func (r Resources) Div(s Resources) map[string]float64 {
	f := make(map[string]float64)
	for key := range s {
		f[key] = r[key] / s[key]
	}
	return f
}

// Requirements stores resource requirements, comprising the minimum
// amount of acceptable resources and a width.
type Requirements struct {
	// Min is the smallest amount of resources that must be allocated
	// to satisfy the requirements.
	Min Resources
	// Width is the width of the requirements. A width of zero indicates
	// a "narrow" job: minimum describes the exact resources needed.
	// Widths greater than zero require a multiple (ie, 1 + Width) of the minimum requirement.
	Width int
}

// AddParallel adds the provided resources s to the requirements,
// and also increases the requirement's width by one.
func (r *Requirements) AddParallel(s Resources) {
	empty := r == nil || r.Min.Equal(nil)
	r.Min.Max(r.Min, s)
	if !empty {
		r.Width++
	}
}

// AddSerial adds the provided resources s to the requirements.
func (r *Requirements) AddSerial(s Resources) {
	r.Min.Max(r.Min, s)
}

// Max is the maximum amount of resources represented by this
// resource request.
func (r *Requirements) Max() Resources {
	var max Resources
	max.Scale(r.Min, float64(1+r.Width))
	return max
}

// Add adds the provided requirements s to the requirements r.
// R's minimum requirements are set to the larger of the two;
// the two widths are added.
func (r *Requirements) Add(s Requirements) {
	r.Min.Max(r.Min, s.Min)
	r.Width += s.Width
}

// Equal reports whether r and s represent the same requirements.
func (r Requirements) Equal(s Requirements) bool {
	return r.Min.Equal(s.Min) && r.Width == s.Width
}

// String renders a human-readable representation of r.
func (r Requirements) String() string {
	s := r.Min.String()
	if r.Width > 1 {
		return s + fmt.Sprintf("#%d", r.Width)
	}
	return s
}
