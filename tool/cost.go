// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import "fmt"

type costType int

const (
	costTypeUnknown costType = iota
	costTypeExact
	costTypeUpperBound
)

// Cost tracks the cost of something along with whether its an exact cost.
type Cost struct {
	value float64
	// type determines the cost's type (ie, exact or upper bound)
	typ costType
}

// NewCostUB creates a cost which is an Upper bound cost.
func NewCostUB(value float64) Cost {
	return Cost{value, costTypeUpperBound}
}

// NewCostExact creates an exact cost.
func NewCostExact(value float64) Cost {
	return Cost{value, costTypeExact}
}

func (c *Cost) Add(d Cost) {
	c.value += d.value
	switch {
	case d.typ == costTypeUnknown:
		// do nothing
	case c.typ == costTypeUnknown:
		c.typ = d.typ
	case c.typ == costTypeUpperBound || d.typ == costTypeUpperBound:
		c.typ = costTypeUpperBound
	default:
		c.typ = costTypeExact
	}
}

func (c *Cost) Mul(v float64) {
	if c.typ == costTypeUnknown {
		panic("Cost.Mul on uninitialized cost")
	}
	c.value *= v
}

func (c Cost) String() string {
	if c.typ == costTypeUnknown {
		return "<unknown>"
	}
	prefix := ""
	if c.typ == costTypeUpperBound {
		prefix = "<"
	}
	return fmt.Sprintf("%s%5.4f", prefix, c.value)
}
