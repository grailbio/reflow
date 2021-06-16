// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import "fmt"

// Cost tracks the cost of something along with whether its an exact cost.
type Cost struct {
	value float64
	// exact determines whether the value is exact or if it is an upper bound.
	exact bool
}

// NewCostUB creates a cost which is an Upper bound cost.
func NewCostUB(value float64) Cost {
	return Cost{value: value}
}

// NewCostExact creates an exact cost.
func NewCostExact(value float64) Cost {
	return Cost{value, true}
}

func (c *Cost) Add(d Cost) {
	c.value += d.value
	c.exact = c.exact && d.exact
}

func (c *Cost) Mul(v float64) {
	c.value *= v
}

func (c Cost) String() string {
	if c.value == 0.0 {
		return "<unknown>"
	}
	prefix := ""
	if !c.exact {
		prefix = "<"
	}
	return fmt.Sprintf("%s%5.4f", prefix, c.value)
}
